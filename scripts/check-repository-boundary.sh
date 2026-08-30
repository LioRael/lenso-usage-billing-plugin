#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-billing-meter-sink\nlenso-capability-usage-billing\nlenso-usage-billing-postgres-plugin'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"
if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi

if rg -n 'path\s*=\s*"(\.\./\.\./|/)' --glob 'Cargo.toml' .; then
  echo "cross-repository or absolute path dependencies are not allowed" >&2
  exit 1
fi
if rg -n 'HashMap|Mutex<.*Vec|in.memory|memory fallback' crates --glob '*.rs'; then
  echo "ambient in-memory durable state is not allowed" >&2
  exit 1
fi
if rg -n 'lenso-platform-|lenso-module-|HostBuilder|HostLinkedModule|ModuleManifest' Cargo.toml crates README.md docs --glob '!**/generated.rs'; then
  echo "legacy Lenso framework dependency or API found" >&2
  exit 1
fi
if rg -n 'CREATE TABLE (usage_entries|usage_aggregates|stripe_|organizations|memberships|roles|permissions|invoices|payments)' crates/lenso-usage-billing-postgres-plugin/migrations; then
  echo "Usage Billing crossed a Usage Meter, provider, Organization, RBAC, invoice, or payment owner boundary" >&2
  exit 1
fi
for capability in 'lenso.usage-billing@1' 'lenso.billing-meter-sink@1'; do
  if ! rg -q "$capability" README.md docs crates; then
    echo "documented Capability is missing: $capability" >&2
    exit 1
  fi
done
printf 'repository boundary is usage-billing-only and provider-neutral\n'
