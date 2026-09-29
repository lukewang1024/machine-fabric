#!/bin/sh
set -eu

# Only a registered peer Controller proves that an Executor belongs to Fabric.
# A domain command provider may also end in -rust or -native.
selected=" $* "
root=${XDG_STATE_HOME:-$HOME/.local/state}/machine-fabric
fabric=$HOME/.local/bin/machine-fabric
controllers=$("$fabric" --socket "$root/controller.sock" call controller.list)
for peer in $(printf '%s\n' "$controllers" | sed -n 's/.*"id":[[:space:]]*"\([^"]*\)".*/\1/p'); do
  case $peer in
    *[!0-9A-Za-z._-]*|'')
      printf 'invalid registered peer name: %s\n' "$peer" >&2
      exit 1
      ;;
  esac
  case $selected in *" $peer "*) continue ;; esac
  "$fabric" --socket "$root/controller.sock" call controller.unregister \
    "{\"controllerId\":\"$peer\"}" >/dev/null
  for suffix in rust native; do
    "$fabric" --socket "$root/controller.sock" call executor.unregister \
      "{\"executorId\":\"$peer-$suffix\"}" >/dev/null
  done
  printf 'bootstrap-fabric: removed unselected peer registration: %s\n' "$peer"
done
