# Lenso Usage Billing Plugin

A removable PostgreSQL backend that turns exact Usage Meter windows into
immutable billing-period snapshots and delivers their quantities through a
replaceable billing provider.

## Capabilities

The Plugin provides `lenso.usage-billing@1` for:

- full-replacement account and meter configuration with CAS revisions;
- get/list account inspection;
- closing a bounded period from exact `lenso.usage-meter@1` windows;
- get/list immutable period snapshots;
- bounded delivery reconciliation and explicit delivery inspection/resolution.

This repository also publishes `lenso.billing-meter-sink@1`. A provider such as
the Stripe Subscription Plugin implements that role; the Usage Billing Plugin
consumes it without importing Stripe-specific ids or APIs.

The Plugin additionally requires Secrets, Organization Membership, and Access
Control. Administrative requests require an exact configured caller, an Auth
ActorAssertion bound to the exact operation, active membership, and a target-owned
permission. Reconciliation uses a separate exact worker allowlist.

## Period and delivery guarantees

- PostgreSQL is the only runtime state; no aggregate or retry state is in memory.
- Each account update and period close is caller/actor/operation idempotent.
- Account configuration uses decimal CAS revisions and replaces all meter mappings
  atomically, so a period records one coherent account revision.
- Closing a period snapshots each quantity and Usage Meter aggregate revision.
  Arbitrary-precision integers compute informational minor-unit line totals without
  overflowing `i64`.
- Overlapping periods for one account are rejected while holding the account row.
- Every period line has a deterministic delivery id, lease generation, bounded
  attempts, provider reference, and explicit pending/delivered/failed/unknown state.
- A worker completion must match the exact worker and attempt generation, preventing
  a stale lease holder from overwriting a newer attempt.
- Any provider Runtime failure becomes `unknown` before it is propagated. An
  administrator must explicitly retry, confirm delivery, or confirm failure.

The internal amount snapshot is evidence and reporting data. A provider's own Price
configuration remains authoritative for the actual invoice; the sink receives the
whole-number quantity, not a locally fabricated charge.

## Permissions

Access Control uses `{ kind: "organization", id: organization_id }`:

- `billing.accounts.read`
- `billing.accounts.write`
- `billing.periods.close`
- `billing.deliveries.resolve`

## Schema lifecycle

`UsageBillingOperator::setup` creates the owned schema and migration ledger.
`UsageBillingOperator::upgrade` applies pending migrations. Activation resolves
the database URL with Secrets and only verifies/opens the authored schema.

## Verification

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
./scripts/check-public-packages.sh
./scripts/check-repository-boundary.sh
```

Set `LENSO_USAGE_BILLING_TEST_DATABASE_URL` to run the restart, snapshot, lease,
and delivery acceptance slice against PostgreSQL.

## Honest limits

v1 supports whole-number usage and explicit UTC period windows. It does not own
tax, discounts, proration, credit notes, invoice finalization, payment collection,
or a scheduler. A Host worker must invoke `reconcile_next`; cross-provider network
delivery is not claimed to be exactly-once.
