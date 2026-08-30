#!/usr/bin/env bash
set -euo pipefail

cargo_bin="${LENSO_CARGO_BIN:-cargo}"
package_flags=(--locked)
if [[ "${LENSO_PACKAGE_ALLOW_DIRTY:-0}" == "1" ]]; then
  package_flags+=(--allow-dirty)
fi

for capability in lenso-capability-billing-meter-sink lenso-capability-usage-billing; do
  "$cargo_bin" package "${package_flags[@]}" -p "$capability"
done

plugin_sources="$("$cargo_bin" package "${package_flags[@]}" --list -p lenso-usage-billing-postgres-plugin)"
for required in Cargo.toml configuration.schema.json migrations/001_create_usage_billing.sql src/lib.rs src/storage.rs; do
  if ! grep -Fxq "$required" <<<"$plugin_sources"; then
    echo "Plugin package source set is missing $required" >&2
    exit 1
  fi
done
