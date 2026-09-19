#!/bin/sh
set -eu

: "${MACHINE_FABRIC_NODE_ID:?MACHINE_FABRIC_NODE_ID is required}"

state_root=/var/lib/machine-fabric
mkdir -p /run/sshd "$state_root/fabric" "$state_root/peers" /workspace
mkdir -p /var/empty
node_user=${MACHINE_FABRIC_DOCKER_USER:-fabric}
if [ "$node_user" = root ]; then
    ssh_home=/root
    mkdir -p "$ssh_home/.ssh"
    chmod 0700 "$ssh_home" "$ssh_home/.ssh"
    cp /run/acceptance/id_ed25519 "$ssh_home/.ssh/id_ed25519"
    cp /run/acceptance/id_ed25519.pub "$ssh_home/.ssh/authorized_keys"
    sed \
        -e 's/User fabric/User root/' \
        -e 's#/home/fabric/.ssh/id_ed25519#/root/.ssh/id_ed25519#' \
        /etc/machine-fabric/ssh_config >"$ssh_home/.ssh/config"
    sed \
        -e 's/^PermitRootLogin no$/PermitRootLogin prohibit-password/' \
        -e 's/^AllowUsers fabric$/AllowUsers root/' \
        /etc/ssh/sshd_config.d/90-machine-fabric.conf >/tmp/sshd_config
    sshd_config=/tmp/sshd_config
else
    ssh_home=/home/fabric
    install -d -m 0700 -o fabric -g fabric "$ssh_home/.ssh"
    cp /run/acceptance/id_ed25519 "$ssh_home/.ssh/id_ed25519"
    cp /run/acceptance/id_ed25519.pub "$ssh_home/.ssh/authorized_keys"
    cp /etc/machine-fabric/ssh_config "$ssh_home/.ssh/config"
    chown -R fabric:fabric "$ssh_home/.ssh"
    chown -R fabric:fabric "$state_root" /workspace
    sshd_config=/etc/ssh/sshd_config.d/90-machine-fabric.conf
fi
chmod 0600 "$ssh_home/.ssh/id_ed25519" "$ssh_home/.ssh/authorized_keys"
chmod 0600 "$ssh_home/.ssh/config"
ssh-keygen -A

/usr/sbin/sshd -f "$sshd_config" -D -e &
sshd_pid=$!
if [ "$node_user" = root ]; then
    env HOME=/root XDG_STATE_HOME=/var/lib \
        /usr/local/bin/machine-fabric \
        --socket "$state_root/controller.sock" \
        controller serve --id "$MACHINE_FABRIC_NODE_ID" \
        --state "$state_root/controller.json" &
else
    runuser -u fabric -- env \
        HOME=/home/fabric \
        XDG_STATE_HOME=/var/lib \
        /usr/local/bin/machine-fabric \
        --socket "$state_root/controller.sock" \
        controller serve --id "$MACHINE_FABRIC_NODE_ID" \
        --state "$state_root/controller.json" &
fi
controller_pid=$!
if [ "$node_user" = root ]; then
    env HOME=/root XDG_STATE_HOME=/var/lib \
        MACHINE_FABRIC_EXECUTOR_MAX_CONCURRENCY=8 \
        MACHINE_FABRIC_EXECUTOR_MAX_QUEUE=32 \
        MACHINE_FABRIC_EXECUTOR_QUEUE_WAIT_MS=15000 \
        /usr/local/bin/machine-fabric \
        --socket "$state_root/executor.sock" \
        executor serve --id "$MACHINE_FABRIC_NODE_ID-executor" \
        --allow-root /workspace --state "$state_root/executor-fences.json" &
else
    runuser -u fabric -- env \
        HOME=/home/fabric \
        XDG_STATE_HOME=/var/lib \
        MACHINE_FABRIC_EXECUTOR_MAX_CONCURRENCY=8 \
        MACHINE_FABRIC_EXECUTOR_MAX_QUEUE=32 \
        MACHINE_FABRIC_EXECUTOR_QUEUE_WAIT_MS=15000 \
        /usr/local/bin/machine-fabric \
        --socket "$state_root/executor.sock" \
        executor serve --id "$MACHINE_FABRIC_NODE_ID-executor" \
        --allow-root /workspace --state "$state_root/executor-fences.json" &
fi
executor_pid=$!

cleanup() {
    result=$?
    trap - 0 HUP INT TERM
    kill "$controller_pid" "$executor_pid" "$sshd_pid" 2>/dev/null || true
    wait "$controller_pid" 2>/dev/null || true
    wait "$executor_pid" 2>/dev/null || true
    wait "$sshd_pid" 2>/dev/null || true
    exit "$result"
}

trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

while kill -0 "$controller_pid" 2>/dev/null \
    && kill -0 "$executor_pid" 2>/dev/null \
    && kill -0 "$sshd_pid" 2>/dev/null; do
    sleep 1
done
exit 1
