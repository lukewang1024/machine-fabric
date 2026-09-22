#!/bin/sh
set -eu

binary=${1:-target/release/machine-fabric}
executor_id=${2:-$(hostname -s)}
controller_id=${MACHINE_FABRIC_CONTROLLER_ID:-$(hostname -s)}
shift_count=0
if [ "$#" -ge 1 ]; then shift_count=1; fi
if [ "$#" -ge 2 ]; then shift_count=2; fi
if [ "$shift_count" -eq 1 ]; then shift; fi
if [ "$shift_count" -eq 2 ]; then shift 2; fi

if [ ! -x "$binary" ]; then
  echo "install-linux-user: executable not found: $binary" >&2
  exit 2
fi
if ! command -v systemctl >/dev/null 2>&1; then
  echo "install-linux-user: systemctl is required" >&2
  exit 2
fi

state_home=${XDG_STATE_HOME:-"$HOME/.local/state"}
config_home=${XDG_CONFIG_HOME:-"$HOME/.config"}
installed_binary=$HOME/.local/bin/machine-fabric
unit_root=$config_home/systemd/user
state_root=$state_home/machine-fabric
controller_socket=$state_root/controller.sock
executor_socket=$state_root/executor.sock
controller_state=$state_root/controller.json
backup_root=$state_root/backups/$(date -u +%Y%m%dT%H%M%SZ)

clipboard_config=$config_home/machine-fabric
clipboard_display=${MACHINE_FABRIC_CLIPBOARD_DISPLAY:-}
if [ -z "$clipboard_display" ] && [ -f "$clipboard_config/clipboard-env" ]; then
  clipboard_display=$(sed -n 's/^DISPLAY=//p' "$clipboard_config/clipboard-env")
fi
clipboard_after=
clipboard_env=
clipboard_service=
if [ -n "$clipboard_display" ]; then
  case $clipboard_display in :*) ;; *) echo "invalid clipboard display" >&2; exit 2;; esac
  case ${clipboard_display#:} in ''|*[!0-9]*) echo "invalid clipboard display" >&2; exit 2;; esac
  xvfb=$(command -v Xvfb 2>/dev/null || true)
  [ -n "$xvfb" ] || { echo "install-linux-user: Xvfb is required for clipboard support" >&2; exit 2; }
  command -v xauth >/dev/null 2>&1 || { echo "install-linux-user: xauth is required for clipboard support" >&2; exit 2; }
fi

mkdir -p "$(dirname "$installed_binary")" "$unit_root" "$state_root"
if [ -f "$controller_state" ] || [ -f "$state_root/executor-fences.json" ] || [ -f "$installed_binary" ]; then
  mkdir -p "$backup_root"
  for state_file in "$controller_state" "$state_root/executor-fences.json"; do
    if [ -f "$state_file" ]; then cp -p "$state_file" "$backup_root/"; fi
  done
  if [ -f "$installed_binary" ]; then cp -p "$installed_binary" "$backup_root/machine-fabric"; fi
fi
temporary=$installed_binary.$$.tmp
cp "$binary" "$temporary"
chmod 755 "$temporary"
mv "$temporary" "$installed_binary"

allow_args=
if [ "$#" -eq 0 ]; then
  set -- "$HOME/Code" "$HOME/Workspace" "$state_home"
fi
for root in "$@"; do
  case $root in
    /*) ;;
    *) echo "install-linux-user: allow-root must be absolute: $root" >&2; exit 2 ;;
  esac
  allow_args="$allow_args --allow-root $root"
done

controller_unit=$unit_root/machine-fabric-controller.service
executor_unit=$unit_root/machine-fabric-executor.service

if [ -n "$clipboard_display" ]; then
  clipboard_auth=$state_root/clipboard-Xauthority
  if [ ! -s "$clipboard_auth" ] || [ -z "$(xauth -f "$clipboard_auth" list "$clipboard_display")" ]; then
    (umask 077; touch "$clipboard_auth")
    clipboard_cookie=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
    xauth -f "$clipboard_auth" add "$clipboard_display" MIT-MAGIC-COOKIE-1 "$clipboard_cookie"
  fi
  chmod 600 "$clipboard_auth"
  mkdir -p "$clipboard_config"
  (umask 077; printf 'DISPLAY=%s\nXAUTHORITY=%s\n' "$clipboard_display" "$clipboard_auth" > "$clipboard_config/clipboard-env")
  clipboard_service=machine-fabric-clipboard-x11.service
  clipboard_after="Requires=$clipboard_service
After=$clipboard_service"
  clipboard_env="Environment=DISPLAY=$clipboard_display
Environment=XAUTHORITY=$clipboard_auth"
  sed -e "s|@XVFB@|$xvfb|g" -e "s|@DISPLAY@|$clipboard_display|g" \
    -e "s|@XAUTHORITY@|$clipboard_auth|g" -e "s|@EXECUTOR_SERVICE@|machine-fabric-executor.service|g" \
    packaging/machine-fabric-clipboard-x11.service.in >"$unit_root/$clipboard_service"
fi

controller_tmp=$controller_unit.$$.tmp
sed \
  -e "s|@BINARY@|$installed_binary|g" \
  -e "s|@SOCKET@|$controller_socket|g" \
  -e "s|@STATE@|$controller_state|g" \
  -e "s|@CONTROLLER_ID@|$controller_id|g" \
  packaging/machine-fabric-controller.service.in >"$controller_tmp"
mv "$controller_tmp" "$controller_unit"

executor_tmp=$executor_unit.$$.tmp
sed \
  -e "s|@BINARY@|$installed_binary|g" \
  -e "s|@SOCKET@|$executor_socket|g" \
  -e "s|@EXECUTOR_ID@|$executor_id|g" \
  -e "s|@ALLOW_ROOTS@|$allow_args|g" \
  packaging/machine-fabric-executor.service.in |
  MF_CLIPBOARD_AFTER="$clipboard_after" MF_CLIPBOARD_ENV="$clipboard_env" awk '
    $0 == "@CLIPBOARD_AFTER@" { print ENVIRON["MF_CLIPBOARD_AFTER"]; next }
    $0 == "@CLIPBOARD_ENV@" { print ENVIRON["MF_CLIPBOARD_ENV"]; next }
    { print }
  ' >"$executor_tmp"
mv "$executor_tmp" "$executor_unit"

systemctl --user daemon-reload
[ -z "$clipboard_service" ] || systemctl --user enable --now "$clipboard_service"
systemctl --user enable --now machine-fabric-controller.service
systemctl --user enable --now machine-fabric-executor.service
systemctl --user restart machine-fabric-controller.service
systemctl --user restart machine-fabric-executor.service
systemctl --user list-unit-files 'machine-fabric-peer-*.service' --no-legend 2>/dev/null |
  while IFS=' ' read -r peer_unit _; do
    case $peer_unit in
      machine-fabric-peer-*.service) systemctl --user restart "$peer_unit" ;;
    esac
  done

attempt=0
while [ "$attempt" -lt 100 ]; do
  if "$installed_binary" --socket "$controller_socket" status >/dev/null 2>&1 \
    && "$installed_binary" --socket "$executor_socket" status >/dev/null 2>&1; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ "$attempt" -ge 100 ]; then
  echo "install-linux-user: services did not become ready" >&2
  exit 1
fi

params=$(printf '{"executorId":"%s","endpoint":{"transport":"local","socket":"%s"}}' "$executor_id" "$executor_socket")
"$installed_binary" --socket "$controller_socket" call executor.register "$params" >/dev/null
installed_version=$($installed_binary --version | awk 'NR == 1 {print $2}')
"$(dirname "$0")/prune-state.sh" --state-root "$state_root" --installed-version "$installed_version" --apply
printf '%s\n' "$installed_binary"
