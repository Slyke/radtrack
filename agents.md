# Radiacode Agent Guide

Before changing code, read these files in order:

1. [`./README.md`](./README.md)
2. [`./docs/README.md`](./docs/README.md)
3. [`./docs/architecture.md`](./docs/architecture.md)
4. [`./docs/configuration.md`](./docs/configuration.md)
5. [`./docs/development.md`](./docs/development.md)
6. [`../styleguide/agents.md`](../styleguide/agents.md)
7. [`../styleguide/style.md`](../styleguide/style.md)
8. [`../styleguide/CODING_STYLE.md`](../styleguide/CODING_STYLE.md)
9. [`../styleguide/logging/logging.md`](../styleguide/logging/logging.md)
10. [`../mqttctl/AGENTS.md`](../mqttctl/AGENTS.md)
11. [`../mqttctl/mqttctl/AGENTS.md`](../mqttctl/mqttctl/AGENTS.md)
12. [`../mqttctl/mqttctl/mqttctl-api/AGENTS.md`](../mqttctl/mqttctl/mqttctl-api/AGENTS.md)
13. [`../mqttctl/mqttctl/mqttctl-fe/AGENTS.md`](../mqttctl/mqttctl/mqttctl-fe/AGENTS.md)
14. [`../wui-button/AGENTS.md`](../wui-button/AGENTS.md)

## Repository Shape

- `api/` owns backend runtime behavior, auth, imports, DB, Redis caching, audit logging, and monolith serving
- `wui/` owns Svelte routes, browser state, map UX, and API consumption
- `docs/` owns durable operator and contributor documentation
- `config/` holds sample bootstrap config and secrets files

## Durable Rules

- Keep the UI token-driven, monospace, and rooted in `html[data-theme]` and `html[data-font]`.
- Keep backend auth, permission checks, CSRF, import validation, and export authorization server-owned.
- Keep JSON5 config and secrets file locations env-driven through `RADIACODE_CONFIG_PATH` and `RADIACODE_SECRETS_PATH`.
- Keep DB-backed settings as the runtime source for mutable non-secret behavior.
- Keep combined datasets virtual in `v0.0.1`.
- Keep PostGIS optional, not required.
- Keep raw uploaded artifacts in Postgres blobs, not long-term disk storage.
- Keep logging structured and correlation-aware.

## Documentation Maintenance

- Update `docs/architecture.md` when service boundaries or persistence behavior change.
- Update `docs/configuration.md` when config, secrets, env vars, or admin-manageable settings change.
- Update `docs/development.md` when build, run, or verification workflows change.
- Update this file when the reading order or repo boundaries change.
