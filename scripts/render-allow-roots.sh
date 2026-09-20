#!/bin/sh
# Render one absolute path per line as launchd argv, preserving spaces and XML.
set -eu
count=0
while IFS= read -r root || [ -n "$root" ]; do
  case $root in
    /*) ;;
    *) echo 'allow-root must be a nonempty absolute path' >&2; exit 2 ;;
  esac
  escaped=$(printf '%s' "$root" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g')
  printf '    <string>--allow-root</string>\n    <string>%s</string>\n' "$escaped"
  count=$((count + 1))
done
[ "$count" -gt 0 ] || { echo 'at least one allow-root is required' >&2; exit 2; }
