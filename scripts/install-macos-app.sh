#!/bin/sh
set -eu

state_home=${XDG_STATE_HOME:-"$HOME/.local/state"}
state_root=$state_home/machine-fabric
data_home=${XDG_DATA_HOME:-"$HOME/.local/share"}
bin_home=${XDG_BIN_HOME:-"$HOME/.local/bin"}
source_binary=${1:-target/release/machine-fabric-macos-agent}
app_root=$data_home/machine-fabric/Machine\ Fabric.app
contents=$app_root/Contents
executable=$contents/MacOS/machine-fabric-macos-agent
controller_executable=$data_home/machine-fabric/libexec/fabric-controller
plist=$contents/Info.plist
launch_agents=$HOME/Library/LaunchAgents
launch_plist=$launch_agents/dev.machine-fabric.macos-agent.plist
template=packaging/dev.machine-fabric.macos-agent.plist.in
controller_launch_plist=$launch_agents/dev.machine-fabric.controller.plist
controller_template=packaging/dev.machine-fabric.controller.plist.in
socket=$state_home/machine-fabric/executor.sock
controller_socket=$state_home/machine-fabric/controller.sock
controller_state=$state_home/machine-fabric/controller.json
log_path=$state_home/machine-fabric/macos-agent.log
controller_log=$state_home/machine-fabric/controller.log
node_id=${MACHINE_FABRIC_NODE_ID:-$(hostname -s)}
backup_root=$state_root/backups/$(date -u +%Y%m%dT%H%M%SZ)
allow_roots=${MACHINE_FABRIC_LOCAL_ALLOW_ROOTS:-"$HOME/Code
$HOME/Workspace
$state_home"}

if [ ! -x "$source_binary" ]; then
  echo "install-macos-app: executable not found: $source_binary" >&2
  exit 2
fi
if [ ! -f "$template" ] || [ ! -f "$controller_template" ]; then
  echo "install-macos-app: launch agent template is missing" >&2
  exit 2
fi
app_version=$($source_binary --version | awk 'NR == 1 { print $2 }')

mkdir -p "$contents/MacOS" "$(dirname "$controller_executable")" "$bin_home" "$launch_agents" "$state_home/machine-fabric"
allow_roots_file=$state_root/.allow-roots.$$.xml
trap 'rm -f "$allow_roots_file" "$launch_plist.$$.tmp" "$launch_plist.$$.tmp.2"' EXIT HUP INT TERM
printf '%s\n' "$allow_roots" | sh scripts/render-allow-roots.sh >"$allow_roots_file"
if [ -f "$controller_state" ] || [ -f "$state_home/machine-fabric/executor-fences.json" ]; then
  mkdir -p "$backup_root"
  for state_file in "$controller_state" "$state_home/machine-fabric/executor-fences.json"; do
    if [ -f "$state_file" ]; then cp -p "$state_file" "$backup_root/"; fi
  done
fi
domain=gui/$(id -u)
# Stop the existing launchd job and any legacy LaunchServices child before
# replacing a signed bundle. Older releases used `open -W`, which could leave
# the app process reparented to PID 1 after the launcher exited.
/bin/launchctl bootout "$domain/dev.machine-fabric.macos-agent" 2>/dev/null || true
if [ -x "$executable" ]; then
  old_pids=$(/bin/ps -axo pid=,command= | awk -v prefix="$executable " 'index($0, prefix) { print $1 }')
  for old_pid in $old_pids; do
    kill -TERM "$old_pid" 2>/dev/null || true
  done
  attempt=0
  while [ "$attempt" -lt 50 ]; do
    alive=false
    for old_pid in $old_pids; do
      if kill -0 "$old_pid" 2>/dev/null; then alive=true; fi
    done
    if [ "$alive" = false ]; then break; fi
    attempt=$((attempt + 1))
    sleep 0.1
  done
fi
app_changed=false
source_uuid=$(/usr/bin/dwarfdump --uuid "$source_binary" 2>/dev/null | awk 'NR == 1 { print $2 }')
installed_uuid=
if [ -f "$executable" ]; then
  installed_uuid=$(/usr/bin/dwarfdump --uuid "$executable" 2>/dev/null | awk 'NR == 1 { print $2 }')
fi
if [ ! -f "$executable" ] || [ -z "$source_uuid" ] || [ "$source_uuid" != "$installed_uuid" ]; then
  temporary=$executable.$$.tmp
  cp "$source_binary" "$temporary"
  chmod 755 "$temporary"
  mv "$temporary" "$executable"
  app_changed=true
fi
if [ ! -f "$controller_executable" ] || ! cmp -s "$source_binary" "$controller_executable"; then
  controller_temporary=$controller_executable.$$.tmp
  cp "$source_binary" "$controller_temporary"
  chmod 755 "$controller_temporary"
  mv "$controller_temporary" "$controller_executable"
fi

plist_temporary=$plist.$$.tmp
plutil -create xml1 "$plist_temporary"
plutil -insert CFBundleIdentifier -string dev.machine-fabric.macos-agent "$plist_temporary"
plutil -insert CFBundleName -string 'Machine Fabric' "$plist_temporary"
plutil -insert CFBundleDisplayName -string 'Machine Fabric' "$plist_temporary"
plutil -insert CFBundleExecutable -string machine-fabric-macos-agent "$plist_temporary"
plutil -insert CFBundlePackageType -string APPL "$plist_temporary"
plutil -insert CFBundleShortVersionString -string "$app_version" "$plist_temporary"
plutil -insert LSUIElement -bool true "$plist_temporary"
if [ ! -f "$plist" ] || ! cmp -s "$plist_temporary" "$plist"; then
  mv "$plist_temporary" "$plist"
  app_changed=true
else
  rm -f "$plist_temporary"
fi

# An ad-hoc signature's identity changes when the executable changes, which
# invalidates macOS TCC grants.  Do not rewrite or re-sign an identical app:
# repeat installs must preserve the exact identity the user already approved.
if [ "$app_changed" = true ] || ! /usr/bin/codesign --verify --deep --strict "$app_root" >/dev/null 2>&1; then
  /usr/bin/codesign --force --sign - \
    --identifier dev.machine-fabric.macos-agent \
    --requirements '=designated => identifier "dev.machine-fabric.macos-agent"' \
    "$app_root"
fi
/usr/bin/codesign --verify --deep --strict "$app_root"
ln -sfn "$executable" "$bin_home/machine-fabric"
ln -sfn "$executable" "$bin_home/machine-fabric-macos-agent"

escape_sed() {
  printf '%s' "$1" | sed 's/[\\&|]/\\&/g'
}

sed \
  -e "s|@APP_ROOT@|$(escape_sed "$app_root")|g" \
  -e "s|@APP_EXECUTABLE@|$(escape_sed "$executable")|g" \
  -e "s|@SOCKET@|$(escape_sed "$socket")|g" \
  -e "s|@EXECUTOR_ID@|$(escape_sed "$node_id-rust")|g" \
  -e "s|@CODE_ROOT@|$(escape_sed "$HOME/Code")|g" \
  -e "s|@WORKSPACE_ROOT@|$(escape_sed "$HOME/Workspace")|g" \
  -e "s|@STATE_ROOT@|$(escape_sed "$state_home")|g" \
  -e "s|@LOG_PATH@|$(escape_sed "$log_path")|g" \
  "$template" >"$launch_plist.$$.tmp"
awk -v roots_file="$allow_roots_file" '
  $0 == "    @ALLOW_ROOTS@" {
    while ((getline line < roots_file) > 0) print line
    close(roots_file)
    next
  }
  { print }
' "$launch_plist.$$.tmp" >"$launch_plist.$$.tmp.2"
mv "$launch_plist.$$.tmp.2" "$launch_plist.$$.tmp"
plutil -lint "$launch_plist.$$.tmp" >/dev/null
mv "$launch_plist.$$.tmp" "$launch_plist"

sed \
  -e "s|@EXECUTABLE@|$(escape_sed "$controller_executable")|g" \
  -e "s|@CONTROLLER_SOCKET@|$(escape_sed "$controller_socket")|g" \
  -e "s|@CONTROLLER_STATE@|$(escape_sed "$controller_state")|g" \
  -e "s|@CONTROLLER_ID@|$(escape_sed "$node_id")|g" \
  -e "s|@CONTROLLER_LOG@|$(escape_sed "$controller_log")|g" \
  "$controller_template" >"$controller_launch_plist.$$.tmp"
plutil -lint "$controller_launch_plist.$$.tmp" >/dev/null
mv "$controller_launch_plist.$$.tmp" "$controller_launch_plist"

bootstrap_job() {
  job_plist=$1
  attempt=0
  while ! /bin/launchctl bootstrap "$domain" "$job_plist" 2>/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
      echo "install-macos-app: cannot bootstrap $job_plist" >&2
      return 1
    fi
    sleep 0.1
  done
}

/bin/launchctl bootout "$domain/dev.machine-fabric.controller" 2>/dev/null || true
bootstrap_job "$launch_plist"
bootstrap_job "$controller_launch_plist"
/bin/launchctl kickstart -k "$domain/dev.machine-fabric.macos-agent"
/bin/launchctl kickstart -k "$domain/dev.machine-fabric.controller"

attempt=0
while [ "$attempt" -lt 100 ]; do
  if "$executable" --socket "$controller_socket" status >/dev/null 2>&1 \
    && "$executable" --socket "$socket" status >/dev/null 2>&1; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 0.1
done
if [ "$attempt" -ge 100 ]; then
  echo "install-macos-app: controller or executor did not become ready" >&2
  exit 1
fi
executor_params=$(printf '{"executorId":"%s-rust","endpoint":{"transport":"local","socket":"%s"}}' "$node_id" "$socket")
"$executable" --socket "$controller_socket" call executor.register "$executor_params" >/dev/null
"$(dirname "$0")/prune-state.sh" --state-root "$state_root" --installed-version "$app_version" --apply

printf '%s\n' "$app_root"
