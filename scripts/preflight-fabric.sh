#!/bin/sh
set -eu

usage() {
  printf '%s\n' 'usage: scripts/preflight-fabric.sh --version VERSION HOST|windows:HOST ...'
}

test "${1:-}" = --version && test "$#" -ge 3 || { usage >&2; exit 2; }
version=$2
shift 2
case $version in v*) version=${version#v};; esac
printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || {
  printf '%s\n' 'preflight-fabric: version must be exact semver' >&2
  exit 2
}
base_url=${MACHINE_FABRIC_RELEASE_BASE_URL:-}
case $base_url in https://*) ;; *)
  printf '%s\n' 'preflight-fabric: set MACHINE_FABRIC_RELEASE_BASE_URL to the internal release CDN root' >&2
  exit 2
esac
case $base_url in *[!A-Za-z0-9:/._~-]*) printf '%s\n' 'preflight-fabric: invalid release CDN URL' >&2; exit 2;; esac
base=${base_url%/}/releases/v$version
nodes=
windows_nodes=
for spec in "$@"; do
  case $spec in
    windows:*) host=${spec#windows:}; windows_nodes="$windows_nodes $host" ;;
    *) host=$spec ;;
  esac
  case $host in
    *[!0-9A-Za-z._-]*|'') printf 'preflight-fabric: invalid SSH alias: %s\n' "$host" >&2; exit 2 ;;
  esac
  nodes="$nodes $host"
done
# Aliases are restricted to whitespace-free characters above.
# shellcheck disable=SC2086
set -- $nodes

platform_of() {
  candidate=$1
  for windows_node in $windows_nodes; do
    if [ "$candidate" = "$windows_node" ]; then printf '%s\n' windows; return; fi
  done
  printf '%s\n' posix
}

for host in "$@"; do
  printf 'preflight-fabric: %s: checking platform and release prerequisites\n' "$host"
  if [ "$(platform_of "$host")" = windows ]; then
    ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
      "powershell.exe -NoProfile -NonInteractive -Command \"if (\$env:PROCESSOR_ARCHITECTURE -ne 'AMD64') { throw 'x86_64 Windows is required' }; Get-Command Invoke-WebRequest,Expand-Archive,ssh.exe | Out-Null; \$h=@{'X-Tos-Access'='internal'}; Invoke-WebRequest -Method Head -Headers \$h -UseBasicParsing '$base/machine-fabric-$version-x86_64-pc-windows-msvc.zip' | Out-Null; Invoke-WebRequest -Method Head -Headers \$h -UseBasicParsing '$base/SHA256SUMS' | Out-Null; Write-Output ready\"" \
      >/dev/null
  else
    ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
      "test \"\$(uname -s)\" = Linux; test \"\$(uname -m)\" = x86_64; command -v tar >/dev/null; command -v systemctl >/dev/null; systemctl --user show-environment >/dev/null; command -v ssh >/dev/null; curl -fsSI -H 'X-Tos-Access: internal' '$base/machine-fabric-$version-x86_64-unknown-linux-musl.tar.gz' >/dev/null; curl -fsSI -H 'X-Tos-Access: internal' '$base/SHA256SUMS' >/dev/null; printf ready" \
      >/dev/null
  fi
done

for node_a in "$@"; do
  for node_b in "$@"; do
    test "$node_a" != "$node_b" || continue
    platform_a=$(platform_of "$node_a")
    platform_b=$(platform_of "$node_b")
    if [ "$platform_a" != "$platform_b" ]; then
      if [ "$platform_a" = posix ]; then first=$node_a; else first=$node_b; fi
    else
      first=$(printf '%s\n%s\n' "$node_a" "$node_b" | LC_ALL=C sort | head -1)
    fi
    test "$node_a" = "$first" || continue
    printf 'preflight-fabric: %s -> %s: checking peer SSH\n' "$node_a" "$node_b"
    if [ "$platform_a" = windows ]; then
      ssh -o BatchMode=yes -o ClearAllForwardings=yes "$node_a" \
        "powershell.exe -NoProfile -NonInteractive -Command \"& ssh.exe -o BatchMode=yes -o ClearAllForwardings=yes '$node_b' 'powershell.exe -NoProfile -NonInteractive -Command \\\"Write-Output ready\\\"'\"" \
        >/dev/null
    else
      ssh -o BatchMode=yes -o ClearAllForwardings=yes "$node_a" \
        "ssh -o BatchMode=yes -o ClearAllForwardings=yes '$node_b' 'printf ready'" \
        >/dev/null
    fi
  done
done

printf '%s\n' 'preflight-fabric: selected nodes satisfy release and SSH prerequisites'
