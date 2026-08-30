# Release process

Publish in this order:

1. `lenso-capability-billing-meter-sink`
2. `lenso-capability-usage-billing`
3. `lenso-usage-billing-postgres-plugin`

Publication is manual-only from reviewed `main` through
`.github/workflows/release-plz.yml`.

Configure a crates.io Trusted Publisher separately for every crate:

- owner: `LioRael`
- repository: `lenso-usage-billing-plugin`
- workflow: `release-plz.yml`
- environment: unset

The live workflow has no long-lived Cargo token fallback. It requests a short-lived
credential through GitHub OIDC and requires `main`, `live=true`, and literal
confirmation `publish`.

crates.io cannot configure a Trusted Publisher for a name that has never been
allocated. For the first release only, allocate each name in dependency order with
a temporary new-package token and revoke it immediately. Never store that token in
this repository, GitHub Secrets, shell history, or logs.

Before publishing, run all README gates and the real PostgreSQL acceptance test.
Until both Capability names exist in the registry, the package gate validates their
archives and the Plugin's exact source set; the release dry-run becomes a full
normalized consumer check after publication.
