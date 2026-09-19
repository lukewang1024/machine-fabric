#!/bin/sh
set -eu

state_home=${XDG_STATE_HOME:-"$HOME/.local/state"}
data_home=${XDG_DATA_HOME:-"$HOME/.local/share"}
bin_home=${XDG_BIN_HOME:-"$HOME/.local/bin"}
source_binary=${1:-target/release/machine-fabric-macos-agent}
install_root=$data_home/machine-fabric/libexec
target_binary=$install_root/machine-fabric-macos-agent

if [ ! -x "$source_binary" ]; then
  echo "install-macos-agent: executable not found: $source_binary" >&2
  exit 2
fi

mkdir -p "$install_root" "$bin_home" "$state_home/machine-fabric"
temporary=$target_binary.$$.tmp
cp "$source_binary" "$temporary"
chmod 755 "$temporary"
mv "$temporary" "$target_binary"
ln -sfn "$target_binary" "$bin_home/machine-fabric-macos-agent"
printf '%s\n' "$target_binary"

