# Usage Billing v1 Plugin card

## Owner and deletion boundary

The PostgreSQL Plugin owns billing account configuration, meter mappings, period
snapshots, command receipts, leases, attempts, and delivery resolution. Removing
it leaves raw Usage Meter events and the selected billing provider untouched.

## Composition

`lenso.usage-billing@1` reads exact windows from `lenso.usage-meter@1` and sends
period-line quantities to one `lenso.billing-meter-sink@1`. The provider-neutral
sink is published here; Stripe or another payment Plugin implements it.

## Authorization boundary

Management paths require exact callers, exact-operation Auth, active Organization
membership, and Access Control. Worker authority is a distinct exact allowlist and
cannot mutate account configuration. Dependency Runtime failures never become
success decisions.

## Durability and concurrency

Account updates use revision CAS. Period closure locks the account before overlap
validation and records the exact account and usage revisions. Delivery claims use
`SKIP LOCKED`, bounded leases and attempts; completion matches worker plus attempt.
Every uncertain provider result becomes durable `unknown` state requiring an
explicit operator resolution.

## Honest limits

The Plugin snapshots evidence and orchestrates meter events. It is not an invoice,
tax, payment, dunning, scheduler, or financial-ledger system, and it does not claim
cross-system exactly-once delivery.
