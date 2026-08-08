#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

sources=(
  "README.md"
  "docs/embedding.md"
  "docs/observation.md"
  "docs/retry-and-ordering.md"
  "docs/shutdown-and-resources.md"
)

case "${1:-}" in
  --check | --write)
    mode="$1"
    ;;
  *)
    echo "usage: $0 --check|--write" >&2
    exit 2
    ;;
esac

status=0
for source in "${sources[@]}"; do
  packaged="crates/shelterwood/doctests/$source"
  if [[ "$mode" == "--write" ]]; then
    mkdir -p "$repo_root/$(dirname "$packaged")"
    cp "$repo_root/$source" "$repo_root/$packaged"
  elif ! cmp -s "$repo_root/$source" "$repo_root/$packaged"; then
    echo "$packaged is not synchronized with $source" >&2
    status=1
  fi
done

if [[ "$status" -ne 0 ]]; then
  echo "run 'just packaged-docs-sync' and commit the synchronized copies" >&2
fi

exit "$status"
