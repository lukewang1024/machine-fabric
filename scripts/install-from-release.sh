#!/bin/sh
set -eu

version=${1:?exact version is required; private releases do not use a mutable latest channel}
shift
case $(uname -s):$(uname -m) in
  Linux:x86_64) target=x86_64-unknown-linux-musl ;;
  Darwin:arm64) target=aarch64-apple-darwin ;;
  *) echo "install-from-release: unsupported platform: $(uname -s) $(uname -m)" >&2; exit 2 ;;
esac

case $version in v*) version=${version#v};; esac
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || {
  echo "install-from-release: version must be exact semver" >&2
  exit 2
}
base_url=${MACHINE_FABRIC_RELEASE_BASE_URL:-}
case $base_url in
  https://*) ;;
  *) echo "install-from-release: set MACHINE_FABRIC_RELEASE_BASE_URL to the internal release CDN root" >&2; exit 2 ;;
esac

archive=machine-fabric-$version-$target.tar.gz
base=${base_url%/}/releases/v$version
temporary=$(mktemp -d "${TMPDIR:-/tmp}/machine-fabric.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
curl -fsSL --retry 2 -H 'X-Tos-Access: internal' "$base/$archive" -o "$temporary/$archive"
curl -fsSL --retry 2 -H 'X-Tos-Access: internal' "$base/SHA256SUMS" -o "$temporary/SHA256SUMS"
expected=$(awk -v name="$archive" '$2 == name {print $1}' "$temporary/SHA256SUMS")
test -n "$expected" || { echo "install-from-release: checksum missing" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$temporary/$archive" | awk '{print $1}')
else
  actual=$(shasum -a 256 "$temporary/$archive" | awk '{print $1}')
fi
test "$actual" = "$expected" || { echo "install-from-release: checksum mismatch" >&2; exit 1; }
tar -C "$temporary" -xzf "$temporary/$archive"
root=$temporary/machine-fabric-$version-$target
case $target in
  *-linux-musl)
    executor_id=${MACHINE_FABRIC_EXECUTOR_ID:-$(hostname -s)}
    if [ "$#" -gt 0 ]; then
      (cd "$root" && scripts/install-linux-user.sh "$root/bin/machine-fabric" "$executor_id" "$@")
    else
      (cd "$root" && scripts/install-linux-user.sh "$root/bin/machine-fabric" "$executor_id")
    fi
    ;;
  *-apple-darwin)
    (cd "$root" && scripts/install-macos-app.sh bin/machine-fabric-macos-agent)
    ;;
esac
