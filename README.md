# radiacode

Radiacode track import and mapping app built as two deployables:

- `api/`: Node.js control plane for auth, imports, Postgres persistence, Redis-backed aggregate caching, exports, moderation, and optional monolith frontend serving
- `wui/`: Svelte 5 web UI for login, imports, datasets, map exploration, exports, and administration

## Documentation Map

- [`agents.md`](./agents.md): repo-specific guidance for future agents and contributors
- [`docs/README.md`](./docs/README.md): docs index
- [`docs/architecture.md`](./docs/architecture.md): runtime model, storage model, and major service boundaries
- [`docs/configuration.md`](./docs/configuration.md): config/secrets bootstrap, env vars, and runtime settings model
- [`docs/development.md`](./docs/development.md): local development workflow

## Reference Inputs

Implementation follows these local references in priority order:

1. `../styleguide`
2. `../mqttctl`
3. `../wui-button`

## Runtime Model

- Bootstrap config path env var: `RADIACODE_CONFIG_PATH`
- Bootstrap secrets path env var: `RADIACODE_SECRETS_PATH`
- Postgres is required for primary data storage
- Redis is used for aggregate cache entries
- UI and API run separately by default
- API can serve the built UI in monolith mode

## Workspace Commands

```bash
npm install
npm run dev:api
npm run dev:wui
npm run build
```

## Versioning

Current app version: `v0.0.1`

At runtime the API resolves build labels as `v0.0.1-<commit>` and falls back to `v0.0.1-unknown` when the current git short hash is unavailable.
