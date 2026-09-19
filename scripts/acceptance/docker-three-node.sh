#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
repo_root=$(CDPATH= cd "$script_dir/../.." && pwd)

for program in cargo docker jq ssh-keygen; do
    if ! command -v "$program" >/dev/null 2>&1; then
        printf 'docker-three-node: required command not found: %s\n' "$program" >&2
        exit 2
    fi
done

docker_bin=$(command -v docker)
if "$docker_bin" info >/dev/null 2>&1; then
    docker_cmd() { "$docker_bin" "$@"; }
elif command -v sudo >/dev/null 2>&1 \
    && sudo -n "$docker_bin" info >/dev/null 2>&1; then
    docker_cmd() { sudo -n "$docker_bin" "$@"; }
else
    printf '%s\n' 'docker-three-node: Docker daemon is unavailable to this user (or via sudo -n)' >&2
    exit 2
fi

docker_user=${MACHINE_FABRIC_DOCKER_USER:-fabric}
case "$docker_user" in
    fabric|root) ;;
    *) printf '%s\n' 'docker-three-node: MACHINE_FABRIC_DOCKER_USER must be fabric or root' >&2; exit 2 ;;
esac
docker_init=${MACHINE_FABRIC_DOCKER_INIT:-yes}
case "$docker_init" in
    yes) docker_init_option=--init ;;
    no) docker_init_option= ;;
    *) printf '%s\n' 'docker-three-node: MACHINE_FABRIC_DOCKER_INIT must be yes or no' >&2; exit 2 ;;
esac
provided_image=${MACHINE_FABRIC_DOCKER_NODE_IMAGE:-}

cargo build --release --manifest-path "$repo_root/Cargo.toml" --bin machine-fabric

temporary=$(mktemp -d "${TMPDIR:-/tmp}/machine-fabric-acceptance.XXXXXX")
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
network=machine-fabric-accept-$run_id
image=${provided_image:-machine-fabric-acceptance:$run_id}
image_owned=no
container_mac=machine-fabric-accept-$run_id-mac
container_linux=machine-fabric-accept-$run_id-linux
container_windows=machine-fabric-accept-$run_id-windows

cleanup() {
    cleanup_status=$?
    trap - 0 HUP INT TERM
    set +e
    cleanup_containers=$(docker_cmd container ls --all --quiet \
        --filter "label=machine-fabric.acceptance=$run_id" 2>/dev/null)
    for cleanup_id in $cleanup_containers; do
        cleanup_label=$(docker_cmd container inspect \
            --format '{{ index .Config.Labels "machine-fabric.acceptance" }}' \
            "$cleanup_id" 2>/dev/null)
        if [ "$cleanup_label" = "$run_id" ]; then
            docker_cmd container rm --force "$cleanup_id" >/dev/null 2>&1 \
                || printf 'docker-three-node: could not remove container %s\n' "$cleanup_id" >&2
        fi
    done
    cleanup_networks=$(docker_cmd network ls --quiet \
        --filter "label=machine-fabric.acceptance=$run_id" 2>/dev/null)
    for cleanup_id in $cleanup_networks; do
        cleanup_label=$(docker_cmd network inspect \
            --format '{{ index .Labels "machine-fabric.acceptance" }}' \
            "$cleanup_id" 2>/dev/null)
        if [ "$cleanup_label" = "$run_id" ]; then
            docker_cmd network rm "$cleanup_id" >/dev/null 2>&1 \
                || printf 'docker-three-node: could not remove network %s\n' "$cleanup_id" >&2
        fi
    done
    if [ "$image_owned" = yes ]; then
        docker_cmd image rm "$image" >/dev/null 2>&1 || true
    fi
    case "$temporary" in
        "${TMPDIR:-/tmp}"/machine-fabric-acceptance.*) rm -rf "$temporary" ;;
        *) printf 'docker-three-node: refusing to remove unexpected temp path: %s\n' "$temporary" >&2 ;;
    esac
    exit "$cleanup_status"
}

trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

for existing_name in "$container_mac" "$container_linux" "$container_windows"; do
    if docker_cmd container inspect "$existing_name" >/dev/null 2>&1; then
        printf 'docker-three-node: refusing to reuse existing container: %s\n' "$existing_name" >&2
        exit 2
    fi
done
if docker_cmd network inspect "$network" >/dev/null 2>&1; then
    printf 'docker-three-node: refusing to reuse existing network: %s\n' "$network" >&2
    exit 2
fi

context=$temporary/context
mkdir "$context"
ssh-keygen -q -t ed25519 -N '' -C machine-fabric-local-acceptance \
    -f "$temporary/id_ed25519"

if [ -z "$provided_image" ]; then
    cp "$repo_root/target/release/machine-fabric" "$context/machine-fabric"
    cp "$repo_root/tests/docker/Dockerfile" "$context/Dockerfile"
    cp "$repo_root/tests/docker/node-entrypoint.sh" "$context/node-entrypoint.sh"
    cp "$repo_root/tests/docker/sshd_config" "$context/sshd_config"
    cp "$repo_root/tests/docker/ssh_config" "$context/ssh_config"
    docker_cmd build --tag "$image" "$context"
    image_owned=yes
elif ! docker_cmd image inspect "$image" >/dev/null 2>&1; then
    printf 'docker-three-node: requested node image does not exist locally: %s\n' \
        "$image" >&2
    exit 2
fi
docker_cmd network create --driver bridge --internal \
    --label "machine-fabric.acceptance=$run_id" "$network" >/dev/null

start_node() {
    start_id=$1
    start_name=$2
    docker_cmd run --detach $docker_init_option \
        --name "$start_name" \
        --label "machine-fabric.acceptance=$run_id" \
        --network "$network" --network-alias "$start_id" \
        --env "MACHINE_FABRIC_NODE_ID=$start_id" \
        --env "MACHINE_FABRIC_DOCKER_USER=$docker_user" \
        --volume "$temporary/id_ed25519:/run/acceptance/id_ed25519:ro" \
        --volume "$temporary/id_ed25519.pub:/run/acceptance/id_ed25519.pub:ro" \
        "$image" >/dev/null
}

start_node node-mac "$container_mac"
start_node node-linux "$container_linux"
start_node node-windows "$container_windows"

container_for() {
    case "$1" in
        node-mac) printf '%s\n' "$container_mac" ;;
        node-linux) printf '%s\n' "$container_linux" ;;
        node-windows) printf '%s\n' "$container_windows" ;;
        *) printf 'unknown simulated node: %s\n' "$1" >&2; return 2 ;;
    esac
}

call_node() {
    call_container=$(container_for "$1")
    shift
    docker_cmd exec --user "$docker_user" --env XDG_STATE_HOME=/var/lib \
        "$call_container" /usr/local/bin/machine-fabric \
        --socket /var/lib/machine-fabric/controller.sock call "$@"
}

wait_node_ready() {
    ready_name=$1
    ready_attempt=0
    while [ "$ready_attempt" -lt 60 ]; do
        if docker_cmd exec "$ready_name" /usr/local/bin/machine-fabric \
            --socket /var/lib/machine-fabric/controller.sock status >/dev/null 2>&1 \
            && docker_cmd exec "$ready_name" /usr/local/bin/machine-fabric \
                --socket /var/lib/machine-fabric/executor.sock status >/dev/null 2>&1; then
            return 0
        fi
        ready_attempt=$((ready_attempt + 1))
        sleep 1
    done
    docker_cmd logs "$ready_name" >&2 || true
    printf 'docker-three-node: node did not become ready: %s\n' "$ready_name" >&2
    return 1
}

for ready_name in "$container_mac" "$container_linux" "$container_windows"; do
    wait_node_ready "$ready_name"
done

start_peer() {
    peer_source=$1
    peer_target=$2
    peer_container=$(container_for "$peer_source")
    peer_state="/var/lib/machine-fabric/peers/$peer_target/status.json"
    docker_cmd exec --detach --user "$docker_user" --env XDG_STATE_HOME=/var/lib \
        "$peer_container" /usr/local/bin/machine-fabric peer connect \
        --id "$peer_target" --local-id "$peer_source" --host "$peer_target" \
        --local-controller-socket /var/lib/machine-fabric/controller.sock \
        --local-executor-socket /var/lib/machine-fabric/executor.sock \
        --expose-controller-socket "/var/lib/machine-fabric/fabric/$peer_target-controller.sock" \
        --expose-executor-socket "/var/lib/machine-fabric/fabric/$peer_target-executor.sock" \
        --remote-executable /usr/local/bin/machine-fabric \
        --remote-state-root /var/lib/machine-fabric \
        --state "$peer_state" >/dev/null
}

wait_peer_ready() {
    peer_source=$1
    peer_target=$2
    peer_container=$(container_for "$peer_source")
    peer_state="/var/lib/machine-fabric/peers/$peer_target/status.json"
    peer_attempt=0
    while [ "$peer_attempt" -lt 60 ]; do
        peer_status=$(docker_cmd exec --user "$docker_user" --env XDG_STATE_HOME=/var/lib \
            "$peer_container" /usr/local/bin/machine-fabric peer status \
            --state "$peer_state" 2>/dev/null || true)
        if printf '%s' "$peer_status" | jq -e '.state == "ready"' >/dev/null 2>&1; then
            return 0
        fi
        peer_attempt=$((peer_attempt + 1))
        sleep 1
    done
    docker_cmd logs "$peer_container" >&2 || true
    printf 'docker-three-node: peer did not become ready: %s -> %s\n' \
        "$peer_source" "$peer_target" >&2
    return 1
}

start_peer node-mac node-linux
start_peer node-mac node-windows
start_peer node-linux node-windows
wait_peer_ready node-mac node-linux
wait_peer_ready node-mac node-windows
wait_peer_ready node-linux node-windows

register_executor() {
    register_node=$1
    register_id=$2
    register_socket=$3
    register_params=$(jq -cn --arg id "$register_id" --arg socket "$register_socket" \
        '{executorId:$id,endpoint:{transport:"local",socket:$socket}}')
    register_result=$(call_node "$register_node" executor.register "$register_params")
    if ! printf '%s' "$register_result" | jq -e '.ok == true' >/dev/null; then
        printf 'docker-three-node: executor registration failed on %s: %s\n' \
            "$register_node" "$register_result" >&2
        return 1
    fi
}

for node_id in node-mac node-linux node-windows; do
    register_executor "$node_id" "$node_id-executor" /var/lib/machine-fabric/executor.sock
done

register_link() {
    link_source=$1
    link_target=$2
    register_executor "$link_source" "$link_target-executor" \
        "/var/lib/machine-fabric/fabric/$link_target-executor.sock"
    register_executor "$link_target" "$link_source-executor" \
        "/var/lib/machine-fabric/fabric/$link_source-executor.sock"
}

register_link node-mac node-linux
register_link node-mac node-windows
register_link node-linux node-windows

context_params=$(jq -cn \
    --arg executor node-windows-executor \
    --arg resource command:/workspace \
    '{executorId:$executor,resources:[$resource]}')
context_result=$(call_node node-mac fabric.context "$context_params")
if ! printf '%s' "$context_result" | jq -e \
    '.ok == true and .result.executors[0].executorId == "node-windows-executor"' >/dev/null; then
    printf 'docker-three-node: Agent context could not see the remote Executor: %s\n' \
        "$context_result" >&2
    exit 1
fi

create_session() {
    session_node=$1
    session_id=$2
    session_payload=$(jq -cn --arg id "$session_id" \
        '{apiVersion:"machine-fabric.dev/v1",kind:"WorkspaceSession",metadata:{id:$id,labels:{},createdAt:1,updatedAt:1},objective:"docker acceptance",state:"active"}')
    session_result=$(call_node "$session_node" session.put "$session_payload")
    if ! printf '%s' "$session_result" | jq -e '.ok == true' >/dev/null; then
        printf 'docker-three-node: session creation failed on %s: %s\n' \
            "$session_node" "$session_result" >&2
        return 1
    fi
}

driver_token() {
    token_node=$1
    token_session=$2
    token_owner=$3
    token_params=$(jq -cn \
        --arg resource "workspace:$token_session" \
        --arg owner "$token_owner" \
        '{resource:$resource,owner:$owner,ttlMs:600000}')
    token_result=$(call_node "$token_node" driver.acquire "$token_params")
    printf '%s' "$token_result" | jq -er '.result.token'
}

session_mac=accept-mac-$run_id
session_linux=accept-linux-$run_id
create_session node-mac "$session_mac"
create_session node-linux "$session_linux"
token_mac=$(driver_token node-mac "$session_mac" agent-mac)
token_linux=$(driver_token node-linux "$session_linux" agent-linux)

invoke_payload() {
    invoke_session=$1
    invoke_owner=$2
    invoke_token=$3
    invoke_marker=$4
    invoke_idempotency=$5
    jq -cn \
        --arg executor node-windows-executor \
        --arg session "$invoke_session" \
        --arg owner "$invoke_owner" \
        --arg token "$invoke_token" \
        --arg marker "$invoke_marker" \
        --arg key "$invoke_idempotency" \
        --arg command 'printf "%s start\n" "$MF_MARKER" >> order.log; sleep 2; printf "%s end\n" "$MF_MARKER" >> order.log' \
        '{executorId:$executor,capability:"command.run",workspaceSessionId:$session,owner:$owner,idempotencyKey:$key,driverToken:$token,input:{cwd:"/workspace",argv:["/bin/sh","-c",$command],env:{MF_MARKER:$marker}}}'
}

payload_mac=$(invoke_payload "$session_mac" agent-mac "$token_mac" mac-agent "mac-$run_id")
payload_linux=$(invoke_payload "$session_linux" agent-linux "$token_linux" linux-agent "linux-$run_id")

container_mac=$(container_for node-mac)
container_linux=$(container_for node-linux)
docker_cmd exec --user "$docker_user" --env XDG_STATE_HOME=/var/lib "$container_mac" \
    /usr/local/bin/machine-fabric --socket /var/lib/machine-fabric/controller.sock \
    call capability.invoke "$payload_mac" >"$temporary/mac-result.json" &
pid_mac=$!
docker_cmd exec --user "$docker_user" --env XDG_STATE_HOME=/var/lib "$container_linux" \
    /usr/local/bin/machine-fabric --socket /var/lib/machine-fabric/controller.sock \
    call capability.invoke "$payload_linux" >"$temporary/linux-result.json" &
pid_linux=$!

queued=0
queue_attempt=0
while [ "$queue_attempt" -lt 30 ]; do
    context_result=$(call_node node-mac fabric.context "$context_params" 2>/dev/null || true)
    queued=$(printf '%s' "$context_result" | jq -r \
        '.result.executors[0].availability.queued // 0' 2>/dev/null || printf '0')
    case "$queued" in ''|*[!0-9]*) queued=0 ;; esac
    if [ "$queued" -ge 1 ]; then
        break
    fi
    queue_attempt=$((queue_attempt + 1))
    sleep 0.1
done
if [ "$queued" -lt 1 ]; then
    printf '%s\n' 'docker-three-node: simultaneous Controllers did not observe remote Executor queueing' >&2
    docker_cmd logs "$container_windows" >&2 || true
    exit 1
fi

if ! wait "$pid_mac"; then
    printf '%s\n' 'docker-three-node: Mac Controller invocation failed' >&2
    cat "$temporary/mac-result.json" >&2 || true
    exit 1
fi
if ! wait "$pid_linux"; then
    printf '%s\n' 'docker-three-node: Linux Controller invocation failed' >&2
    cat "$temporary/linux-result.json" >&2 || true
    exit 1
fi
for result_path in "$temporary/mac-result.json" "$temporary/linux-result.json"; do
    if ! jq -e '.ok == true and .result.task.state == "succeeded"' \
        "$result_path" >/dev/null; then
        printf 'docker-three-node: task did not succeed: %s\n' "$result_path" >&2
        cat "$result_path" >&2
        exit 1
    fi
done

assert_task_home() {
    owner_node=$1
    result_path=$2
    task_id=$(jq -er '.result.task.id' "$result_path")
    task_params=$(jq -cn --arg id "$task_id" '{taskId:$id}')
    owner_result=$(call_node "$owner_node" task.get "$task_params")
    if ! printf '%s' "$owner_result" | jq -e \
        '.ok == true and .result.state == "succeeded"' >/dev/null; then
        printf 'docker-three-node: originating Controller lost task %s: %s\n' \
            "$task_id" "$owner_result" >&2
        return 1
    fi
    remote_result=$(call_node node-windows task.get "$task_params" 2>/dev/null || true)
    if ! printf '%s' "$remote_result" | jq -e \
        '.ok == false and .error.code == "TASK_NOT_FOUND"' >/dev/null; then
        printf 'docker-three-node: remote Executor node unexpectedly owns task %s: %s\n' \
            "$task_id" "$remote_result" >&2
        return 1
    fi
}

assert_task_home node-mac "$temporary/mac-result.json"
assert_task_home node-linux "$temporary/linux-result.json"

order=$(docker_cmd exec "$container_windows" cat /workspace/order.log)
line_one=$(printf '%s\n' "$order" | sed -n '1p')
line_two=$(printf '%s\n' "$order" | sed -n '2p')
line_three=$(printf '%s\n' "$order" | sed -n '3p')
line_four=$(printf '%s\n' "$order" | sed -n '4p')
if ! {
    [ "$line_one" = "mac-agent start" ] \
        && [ "$line_two" = "mac-agent end" ] \
        && [ "$line_three" = "linux-agent start" ] \
        && [ "$line_four" = "linux-agent end" ]
} && ! {
    [ "$line_one" = "linux-agent start" ] \
        && [ "$line_two" = "linux-agent end" ] \
        && [ "$line_three" = "mac-agent start" ] \
        && [ "$line_four" = "mac-agent end" ]
}; then
    printf 'docker-three-node: shared resource writes overlapped or reordered:\n%s\n' "$order" >&2
    exit 1
fi

printf '%s\n' 'docker-three-node: PASS'
printf '%s\n' '  - three isolated Linux containers connected through local SSH peers'
printf '%s\n' '  - Mac-role and Linux-role Controllers invoked the Windows-role Executor'
printf '%s\n' '  - the remote Executor queued conflicting work and serialized the shared resource'
printf '%s\n' '  - task records remained on the originating Controllers, not the Executor node'
