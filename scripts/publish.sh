#!/usr/bin/env bash
# Publish all public @kronos-ts/* packages with `npm publish`.
#
# Why npm (not bun): publishing authenticates via npm Trusted Publishing (OIDC)
# in CI, which `bun publish` does not implement (it errors with "missing
# authentication"). npm >= 11.5.1 performs the OIDC token exchange automatically.
#
# Why the rewrite step: `npm publish` does not understand Bun's workspace
# protocol, so it would upload literal `"workspace:*"` specifiers and consumers
# couldn't install. `scripts/resolve-workspace.mjs` rewrites those to concrete
# versions before each publish, and the manifest is restored (git checkout)
# afterwards so the working tree is left untouched.
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
  # Resolve workspace:* -> concrete versions, publish via npm (OIDC), then
  # restore the manifest regardless of outcome.
  node "$ROOT/scripts/resolve-workspace.mjs" "$pkg_json"
  if (cd "$dir" && npm publish --access public); then
    published=$((published + 1))
  else
    failed=$((failed + 1))
  fi
  git checkout -- "$pkg_json"
done

echo ""
echo "summary: published=$published skipped=$skipped failed=$failed"
[[ $failed -eq 0 ]]
