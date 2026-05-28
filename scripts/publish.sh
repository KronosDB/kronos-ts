#!/usr/bin/env bash
# Publish all public @kronos-ts/* packages with `bun publish`.
#
# Why this exists: `changeset publish` shells out to `npm publish`, which does
# not understand Bun's workspace protocol. Published manifests end up with
# literal `"workspace:*"` strings and consumers can't install. `bun publish`
# resolves `workspace:*` to the concrete version before uploading.
#
# Versioning/changelogs are still handled by `changeset version` (run
# separately via `bun run version-packages`). This script only handles the
# publish step.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PACKAGES=(
  packages/common
  packages/messaging
  packages/modelling
  packages/eventsourcing
  packages/app
  packages/test
  packages/extensions/axon-server
  packages/extensions/drizzle
  packages/extensions/knex
  packages/extensions/kronosdb
  packages/extensions/kysely
  packages/extensions/opentelemetry
  packages/extensions/postgres
  packages/extensions/prisma
  packages/extensions/rabbitmq
  packages/extensions/typeorm
)

published=0
skipped=0
failed=0

for dir in "${PACKAGES[@]}"; do
  pkg_json="$ROOT/$dir/package.json"
  if [[ ! -f "$pkg_json" ]]; then
    echo "skip: $dir (no package.json)"
    continue
  fi

  name=$(node -p "require('$pkg_json').name")
  version=$(node -p "require('$pkg_json').version")
  private=$(node -p "require('$pkg_json').private === true")

  if [[ "$private" == "true" ]]; then
    echo "skip: $name (private)"
    skipped=$((skipped + 1))
    continue
  fi

  # Skip if this exact version is already on npm.
  if npm view "$name@$version" version >/dev/null 2>&1; then
    echo "skip: $name@$version (already published)"
    skipped=$((skipped + 1))
    continue
  fi

  echo "publish: $name@$version"
  (cd "$dir" && bun publish --access public) || { failed=$((failed + 1)); continue; }
  published=$((published + 1))
done

echo ""
echo "summary: published=$published skipped=$skipped failed=$failed"
[[ $failed -eq 0 ]]
