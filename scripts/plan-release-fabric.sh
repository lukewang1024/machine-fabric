#!/bin/sh
set -eu

version=${1:?version is required}
manifest=${2:?Fabric manifest is required}
termux_marker=${TERMUX_VERSION:-}
if [ "${PREFIX:-}" = /data/data/com.termux/files/usr ]; then
  termux_marker=termux
fi
case "$termux_marker:$(uname -s):$(uname -m)" in
  ?*:Linux:aarch64|?*:Linux:arm64) target=aarch64-linux-android ;;
  :Darwin:arm64) target=aarch64-apple-darwin ;;
  :Linux:x86_64) target=x86_64-unknown-linux-musl ;;
  *) printf 'plan-release-fabric: unsupported initiator: %s %s\n' "$(uname -s)" "$(uname -m)" >&2; exit 2 ;;
esac
case $version in v*) version=${version#v} ;; esac
case $version in *[!0-9A-Za-z._-]*|'') printf '%s\n' 'plan-release-fabric: invalid version' >&2; exit 2 ;; esac
test -f "$manifest" || { printf 'plan-release-fabric: missing manifest: %s\n' "$manifest" >&2; exit 2; }
base_url=${MACHINE_FABRIC_RELEASE_BASE_URL:-}
case $base_url in https://*) ;; *) printf '%s\n' 'plan-release-fabric: set MACHINE_FABRIC_RELEASE_BASE_URL to the internal release CDN root' >&2; exit 2 ;; esac

archive=machine-fabric-$version-$target.tar.gz
base=${base_url%/}/releases/v$version
temporary=$(mktemp -d "${TMPDIR:-/tmp}/machine-fabric-plan.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
curl -fsSL -H 'X-Tos-Access: internal' "$base/$archive" -o "$temporary/$archive"
curl -fsSL -H 'X-Tos-Access: internal' "$base/SHA256SUMS" -o "$temporary/SHA256SUMS"
expected=$(awk -v name="$archive" '$2 == name {print $1}' "$temporary/SHA256SUMS")
test -n "$expected" || { printf '%s\n' 'plan-release-fabric: checksum missing' >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$temporary/$archive" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$temporary/$archive" | awk '{print $1}')
fi
test "$actual" = "$expected" || { printf '%s\n' 'plan-release-fabric: checksum mismatch' >&2; exit 1; }
tar -C "$temporary" -xzf "$temporary/$archive"
"$temporary/machine-fabric-$version-$target/bin/machine-fabric" manifest plan --file "$manifest"
