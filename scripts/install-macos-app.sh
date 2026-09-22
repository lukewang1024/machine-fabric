#!/bin/sh
set -eu
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
command -v python3 >/dev/null 2>&1 || {
  printf '%s\n' 'install-macos-app: Python 3 is required before staging or stopping services' >&2
  exit 2
}
exec python3 "$script_dir/install-macos-transaction.py" "${1:-target/release/machine-fabric-macos-agent}"
