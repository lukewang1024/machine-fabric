#!/bin/sh
set -eu

usage() {
  echo 'usage: connect-termux-peer.sh SSH_HOST [LOCAL_NODE_ID]' >&2
}

host=${1:-}
local_id=${2:-$(hostname -s)-termux}
test -n "$host" || { usage; exit 2; }
case $host:$local_id in *[!0-9A-Za-z._:-]*|'':*) echo 'connect-termux-peer: invalid host or node id' >&2; exit 2;; esac
test -n "${PREFIX:-}" || { echo 'connect-termux-peer: run this inside Termux' >&2; exit 2; }
command -v sv >/dev/null 2>&1 || { echo 'connect-termux-peer: termux-services is required' >&2; exit 2; }
if [ -f "$PREFIX/etc/profile.d/start-services.sh" ]; then
  . "$PREFIX/etc/profile.d/start-services.sh"
fi

fabric=$HOME/.local/bin/machine-fabric
test -x "$fabric" || { echo "connect-termux-peer: missing $fabric" >&2; exit 2; }
state_home=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_root=$state_home/machine-fabric
controller_socket=$state_root/controller.sock
executor_socket=$state_root/executor.sock
peer_root=$state_root/peers/$host
service_name=machine-fabric-peer-$host
service_dir=$PREFIX/var/service/$service_name
remote_home=$(ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" 'printf %s "$HOME"')
remote_state_root=$remote_home/.local/state/machine-fabric
remote_binary=.local/bin/machine-fabric

mkdir -p "$peer_root" "$service_dir/log" "$state_root/log/$service_name"
run_tmp=$service_dir/run.$$.tmp
{
  printf '%s\n' '#!/bin/sh' 'exec 2>&1'
  printf "exec '%s' peer connect --id '%s' --local-id '%s' --host '%s' --expose-controller-socket '%s' --expose-executor-socket '%s' --remote-executable '%s' --remote-state-root '%s' --remote-platform posix --state '%s' --local-controller-socket '%s' --local-executor-socket '%s'\n" \
    "$fabric" "$host" "$local_id" "$host" "$peer_root/controller.sock" "$peer_root/executor.sock" \
    "$remote_binary" "$remote_state_root" "$peer_root/status.json" "$controller_socket" "$executor_socket"
} >"$run_tmp"
chmod 755 "$run_tmp"
mv "$run_tmp" "$service_dir/run"
{
  printf '%s\n' '#!/bin/sh'
  printf "exec svlogd -tt '%s'\n" "$state_root/log/$service_name"
} >"$service_dir/log/run"
chmod 755 "$service_dir/log/run"
sv up "$service_name" >/dev/null
sv restart "$service_name" >/dev/null

attempt=0
while [ "$attempt" -lt 150 ]; do
  if "$fabric" peer status --state "$peer_root/status.json" 2>/dev/null | grep '"state": "ready"' >/dev/null; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.2
done
test "$attempt" -lt 150 || { echo "connect-termux-peer: peer to $host did not become ready" >&2; exit 1; }

remote_executor_id=$(ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
  '"$HOME/.local/bin/machine-fabric" --socket "${XDG_STATE_HOME:-$HOME/.local/state}/machine-fabric/executor.sock" status' |
  sed -n 's/.*"executorId":[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
local_executor_id=$("$fabric" --socket "$executor_socket" status |
  sed -n 's/.*"executorId":[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
test -n "$remote_executor_id" && test -n "$local_executor_id" || {
  echo 'connect-termux-peer: could not resolve executor identities' >&2
  exit 1
}

json_register() {
  kind=$1
  id=$2
  socket=$3
  printf '{"%sId":"%s","endpoint":{"transport":"local","socket":"%s"}}' "$kind" "$id" "$socket"
}
"$fabric" --socket "$controller_socket" call executor.register \
  "$(json_register executor "$remote_executor_id" "$peer_root/executor.sock")" >/dev/null
"$fabric" --socket "$controller_socket" call controller.register \
  "$(json_register controller "$host" "$peer_root/controller.sock")" >/dev/null

reverse_executor=$remote_state_root/fabric/$local_id-executor.sock
reverse_controller=$remote_state_root/fabric/$local_id-controller.sock
remote_executor_params=$(json_register executor "$local_executor_id" "$reverse_executor")
remote_controller_params=$(json_register controller "$local_id" "$reverse_controller")
ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
  "\"\$HOME/.local/bin/machine-fabric\" --socket '$remote_state_root/controller.sock' call executor.register '$remote_executor_params' >/dev/null && \"\$HOME/.local/bin/machine-fabric\" --socket '$remote_state_root/controller.sock' call controller.register '$remote_controller_params' >/dev/null"
printf 'connect-termux-peer: %s is connected to %s\n' "$local_id" "$host"
