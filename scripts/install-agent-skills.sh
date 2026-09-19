#!/bin/sh
set -eu

usage() {
  printf '%s\n' 'usage: scripts/install-agent-skills.sh [--verify-only] HOST SKILL_DIR ...'
}

verify_only=false
if test "${1:-}" = --verify-only; then
  verify_only=true
  shift
fi
test "$#" -ge 2 || { usage >&2; exit 2; }
host=$1
shift
case $host in *[!0-9A-Za-z._-]*|'') printf 'install-agent-skills: invalid SSH alias: %s\n' "$host" >&2; exit 2;; esac

temporary=$(mktemp -d "${TMPDIR:-/tmp}/machine-fabric-skills.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
staging=$temporary/skills
mkdir -p "$staging"
names=
for source in "$@"; do
  test -f "$source/SKILL.md" || { printf 'install-agent-skills: missing SKILL.md: %s\n' "$source" >&2; exit 2; }
  name=$(basename "$source")
  case $name in *[!0-9a-z-]*|'') printf 'install-agent-skills: invalid skill name: %s\n' "$name" >&2; exit 2;; esac
  test ! -e "$staging/$name" || { printf 'install-agent-skills: duplicate skill: %s\n' "$name" >&2; exit 2; }
  cp -R -L "$source" "$staging/$name"
  names="$names $name"
done
if test "$verify_only" = true; then
  ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
    "set -eu; destination=\${AGENTS_SKILLS_HOME:-\$HOME/.agents/skills}; for name in $names; do test -f \"\$destination/\$name/SKILL.md\"; done"
  printf 'install-agent-skills: verified on %s:%s\n' "$host" "$names"
  exit 0
fi
archive=$temporary/agent-skills.tar.gz
COPYFILE_DISABLE=1 tar -C "$staging" -czf "$archive" .
remote_archive=/tmp/machine-fabric-agent-skills.$$.tar.gz
scp -q "$archive" "$host:$remote_archive"
ssh -o BatchMode=yes -o ClearAllForwardings=yes "$host" \
  "set -eu; archive='$remote_archive'; temporary=\$(mktemp -d /tmp/machine-fabric-skills.XXXXXX); trap 'rm -rf \"\$temporary\" \"\$archive\"' EXIT HUP INT TERM; tar -C \"\$temporary\" -xzf \"\$archive\"; destination=\${AGENTS_SKILLS_HOME:-\$HOME/.agents/skills}; backup=\${XDG_STATE_HOME:-\$HOME/.local/state}/machine-fabric/agent-skill-backups/\$(date -u +%Y%m%dT%H%M%SZ); mkdir -p \"\$destination\"; for name in $names; do test -f \"\$temporary/\$name/SKILL.md\"; if [ -e \"\$destination/\$name\" ] || [ -L \"\$destination/\$name\" ]; then mkdir -p \"\$backup\"; mv \"\$destination/\$name\" \"\$backup/\$name\"; fi; mv \"\$temporary/\$name\" \"\$destination/\$name\"; done; for name in $names; do test -f \"\$destination/\$name/SKILL.md\"; done"
printf 'install-agent-skills: installed on %s:%s\n' "$host" "$names"
