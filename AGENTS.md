# Agent instructions

This repository owns billable account configuration, immutable period snapshots,
delivery outbox state, and provider-neutral meter delivery orchestration. It does
not own raw usage events, Organizations, identities, RBAC, payment collection,
subscriptions, or provider customer records.

Capability source is `capability.json` plus package-local JSON Schemas. Generated
Rust is locked output and must not be hand-edited. Use the workspace Cargo wrapper
when available and read `docs/release-process.md` before release work.
