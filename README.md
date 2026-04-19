# RadTrack

RadTrack track import and mapping app built as two deployables:

- `api/`: Node.js control plane for auth, imports, Postgres persistence, Redis-backed aggregate caching, exports, moderation, and optional monolith frontend serving
- `wui/`: Svelte 5 web UI for login, imports, live track ingest keys, datasets, map exploration, exports, audit history, and administration

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

- Bootstrap config path env var: `RADTRACK_CONFIG_PATH`
- Bootstrap secrets path env var: `RADTRACK_SECRETS_PATH`
- Postgres is required for primary data storage
- Redis is used for aggregate cache entries
- UI and API run separately by default
- API can serve the built UI in monolith mode
- Tracks can be created from imported files or as live ingest targets with per-track API keys

## Workspace Commands

```bash
npm install
npm run dev
npm run dev:logs
npm run dev:down
npm run dev:local:api
npm run dev:local:wui
npm run build
```

`npm run dev` now starts the detached development compose stack from the repository root.
Both `npm run dev` and `npm run prod:up` inject the current git short hash into the containers so health responses and startup logs report the same build label format as the UI.

## Compose Files

- `docker-compose.dev.yml`: detached development stack for API, WUI, Postgres, and Redis
- `docker-compose.yml`: production-like local stack for API, WUI, Postgres, and Redis

The compose-mounted bootstrap config files live under `config/compose/gui-api/`, following the same pattern used in `../mqttctl`.

## Health Endpoints

- API:
  - `GET /health`
  - `GET /healthz`
- WUI:
  - `GET /health`
  - `GET /healthz`
  - `GET /wui-health` as a compatibility alias

All return unauthenticated JSON in this shape:

```json
{
  "ok": true,
  "message": "ok",
  "service": "radtrack-api",
  "version": "0.0.1",
  "build": "v0.0.1-<commit>"
}
```

## Versioning

Current app version: `v0.0.1`

At runtime the API resolves build labels as `v0.0.1-<commit>` and falls back to `v0.0.1-unknown` when the current git short hash is unavailable.
