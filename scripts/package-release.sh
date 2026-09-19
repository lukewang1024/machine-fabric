#!/bin/sh
set -eu

version=${1:?version is required}
target=${2:?target is required}
build_dir=${3:?build directory is required}
output_dir=${4:-dist}
package=machine-fabric-$version-$target
staging=$output_dir/$package

case $version in *[!0-9A-Za-z._-]*|'') echo "invalid version: $version" >&2; exit 2;; esac
case $target in *[!0-9A-Za-z._-]*|'') echo "invalid target: $target" >&2; exit 2;; esac

mkdir -p "$staging/bin" "$staging/scripts" "$staging/packaging" "$staging/skills"
case $target in
  *-windows-msvc)
    cp "$build_dir/machine-fabric.exe" "$staging/bin/machine-fabric.exe"
    ;;
  *-apple-darwin)
    cp "$build_dir/machine-fabric" "$staging/bin/machine-fabric"
    chmod 755 "$staging/bin/machine-fabric"
    cp "$build_dir/machine-fabric-macos-agent" "$staging/bin/machine-fabric-macos-agent"
    chmod 755 "$staging/bin/machine-fabric-macos-agent"
    ;;
  *)
    cp "$build_dir/machine-fabric" "$staging/bin/machine-fabric"
    chmod 755 "$staging/bin/machine-fabric"
    ;;
esac
cp README.md LICENSE "$staging/"
cp -R skills/machine-fabric "$staging/skills/machine-fabric"
cp scripts/install-from-release.sh scripts/install-from-release.ps1 scripts/install-linux-user.sh scripts/install-macos-agent.sh scripts/install-macos-app.sh scripts/install-windows.ps1 scripts/install-windows-peer.ps1 scripts/bootstrap-fabric.sh scripts/preflight-fabric.sh scripts/plan-release-fabric.sh scripts/install-agent-skills.sh scripts/prune-state.sh "$staging/scripts/"
cp packaging/* "$staging/packaging/"
case $target in
  *-windows-msvc)
    if command -v zip >/dev/null 2>&1; then
      (cd "$output_dir" && zip -qr "$package.zip" "$package")
    elif command -v 7z >/dev/null 2>&1; then
      (cd "$output_dir" && 7z a -bd -tzip "$package.zip" "$package" >/dev/null)
    else
      echo "package-release: zip or 7z is required for Windows archives" >&2
      exit 2
    fi
    ;;
  *) tar -C "$output_dir" -czf "$output_dir/$package.tar.gz" "$package" ;;
esac
rm -rf "$staging"
