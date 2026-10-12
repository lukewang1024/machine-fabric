#!/bin/sh
set -eu

version=${1:?exact version required}
target=${2:?Windows target required}
cache=${3:?owned cache directory required}
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || exit 2
case $target in x86_64-pc-windows-msvc|aarch64-pc-windows-msvc) ;; *) exit 2 ;; esac
base_url=${MACHINE_FABRIC_RELEASE_BASE_URL:-}
case $base_url in https://*) ;; *) exit 2 ;; esac
test -d "$cache" && test ! -L "$cache" || exit 2
archive=machine-fabric-$version-$target.zip
base=${base_url%/}/releases/v$version
for name in SHA256SUMS "$archive"; do
  test ! -L "$cache/$name" || exit 1
  if [ ! -f "$cache/$name" ]; then
    curl -fsSL --retry 2 -H 'X-Tos-Access: internal' "$base/$name" -o "$cache/$name"
  fi
done
expected=$(awk -v name="$archive" '{sub(/^\*/, "", $2)} $2 == name {value=$1; count++} END {if(count != 1) exit 1; print value}' "$cache/SHA256SUMS")
printf '%s\n' "$expected" | grep -Eq '^[a-fA-F0-9]{64}$' || exit 1
expected=$(printf '%s' "$expected" | tr 'A-F' 'a-f')
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$cache/$archive" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$cache/$archive" | awk '{print $1}')
fi
test "$actual" = "$expected" || { printf '%s\n' 'bootstrap-fabric: Windows release checksum mismatch' >&2; exit 1; }
printf '%s\n' "$expected"
