#!/bin/sh
set -eu

state_root=${XDG_STATE_HOME:-"$HOME/.local/state"}/machine-fabric
apply=0
keep_backups=${MACHINE_FABRIC_KEEP_BACKUPS:-5}
keep_deployments=${MACHINE_FABRIC_KEEP_DEPLOYMENTS:-1}
installed_version=

usage() {
  printf '%s\n' 'Usage: prune-state.sh [--state-root PATH] [--installed-version VERSION] [--apply]'
  printf '%s\n' 'Default: dry-run. Keeps 5 backups, the installed deployment, and 1 newest deployment.'
}

while [ "$#" -gt 0 ]; do
  case $1 in
    --state-root)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      state_root=$2
      shift 2
      ;;
    --installed-version)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      installed_version=$2
      shift 2
      ;;
    --apply) apply=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

case $state_root in
  /*/machine-fabric) ;;
  *) printf 'prune-state: refusing unsafe state root: %s\n' "$state_root" >&2; exit 1 ;;
esac
case $keep_backups:$keep_deployments in
  *[!0-9:]*|:*|*:) printf '%s\n' 'prune-state: retention counts must be non-negative integers' >&2; exit 2 ;;
esac
case $installed_version in
  ''|*[!0-9A-Za-z._-]*)
    [ -z "$installed_version" ] || { printf 'prune-state: invalid installed version: %s\n' "$installed_version" >&2; exit 2; }
    ;;
esac

[ -d "$state_root" ] || { printf 'prune-state: nothing to prune: %s\n' "$state_root"; exit 0; }

temporary=$(mktemp -d "${TMPDIR:-/tmp}/machine-fabric-prune.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
selected=$temporary/selected
protected=$temporary/protected
: > "$selected"
: > "$protected"

retain_newest() {
  parent=$1
  pattern=$2
  count=$3
  [ -d "$parent" ] || return 0
  if [ "$count" -gt 0 ]; then
    find "$parent" -mindepth 1 -maxdepth 1 -type d -name "$pattern" -print |
      sort -r | sed -n "1,${count}p" >> "$protected"
  fi
  find "$parent" -mindepth 1 -maxdepth 1 -type d -name "$pattern" -print >> "$selected"
}

retain_newest "$state_root/backups" '*' "$keep_backups"
retain_newest "$state_root/agent-skill-backups" '*' "$keep_backups"
deploy_ranked=$temporary/deploy-ranked
: > "$deploy_ranked"
find "$state_root" -mindepth 1 -maxdepth 1 -type d -name 'deploy-*' -print |
while IFS= read -r deploy_dir; do
  deploy_mtime=$(stat -f '%m' "$deploy_dir" 2>/dev/null || stat -c '%Y' "$deploy_dir" 2>/dev/null || printf '0')
  printf '%s\t%s\n' "$deploy_mtime" "$deploy_dir" >> "$deploy_ranked"
  printf '%s\n' "$deploy_dir" >> "$selected"
done
if [ "$keep_deployments" -gt 0 ]; then
  sort -rn "$deploy_ranked" | sed -n "1,${keep_deployments}p" | cut -f 2- >> "$protected"
fi
if [ -n "$installed_version" ] && [ -d "$state_root/deploy-$installed_version" ]; then
  printf '%s\n' "$state_root/deploy-$installed_version" >> "$protected"
fi

sort -u "$protected" -o "$protected"
sort -u "$selected" -o "$selected"

count=0
total_kb=0
printf '%s\n' 'Protected state: '
sed 's/^/  KEEP    /' "$protected"
printf '%s\n' 'Selected stale state: '
while IFS= read -r item; do
  [ -d "$item" ] || continue
  grep -F -x "$item" "$protected" >/dev/null 2>&1 && continue
  case $item in "$state_root"/*) ;; *) continue ;; esac
  size_kb=$(du -sk "$item" 2>/dev/null | awk '{print $1}')
  size_kb=${size_kb:-0}
  count=$((count + 1))
  total_kb=$((total_kb + size_kb))
  awk -v kb="$size_kb" -v path="$item" 'BEGIN {printf "  DELETE  %8.2f GiB  %s\n", kb / 1048576, path}'
  if [ "$apply" -eq 1 ]; then rm -rf -- "$item"; fi
done < "$selected"

awk -v count="$count" -v kb="$total_kb" 'BEGIN {printf "Summary: %d paths, approximately %.2f GiB.\n", count, kb / 1048576}'
if [ "$apply" -eq 1 ]; then
  printf '%s\n' 'Selected state was permanently removed.'
else
  printf '%s\n' 'Dry-run only. Re-run with --apply to permanently delete selected state.'
fi
