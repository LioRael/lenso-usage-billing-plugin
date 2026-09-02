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
"$cargo_bin" package "${package_flags[@]}" -p lenso-usage-billing-postgres-plugin
