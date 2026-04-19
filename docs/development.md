# Development

## Local Setup

1. Run `npm install`.
2. Start the detached dev stack from the repo root:
   - `npm run dev`
3. Tail logs when needed:
   - `npm run dev:logs`
4. Stop the stack:
   - `npm run dev:down`

For direct host-side development without compose:

1. Set:
   - `RADTRACK_CONFIG_PATH`
   - `RADTRACK_SECRETS_PATH`
2. Ensure Postgres and Redis are reachable.
3. Run `npm run dev:local:api` and `npm run dev:local:wui`.

## Scripts

- `npm run dev`: build and start the detached development compose stack
- `npm run dev:logs`: tail detached dev-stack logs
- `npm run dev:down`: stop and remove the detached dev stack
- `npm run dev:local:api`: start the Node API on the host
- `npm run dev:local:wui`: start the Svelte dev server on the host
- `npm run prod:up`: start the production-like local compose stack
- `npm run prod:down`: stop the production-like local compose stack
- `npm run build`: build the WUI for monolith or production serving
- `npm run check`: run SvelteKit type and consistency checks

The compose `up` scripts inject the current git short hash into the API and WUI as `RADTRACK_BUILD_COMMIT`, so container startup logs, health endpoints, and the WUI footer use the same build label format without needing `.git` inside the container.

The development compose stack bind-mounts source from the host into the containers. The API mounts `api/` directly and runs a legacy-watch `nodemon` process for bind-mount-safe restarts. The WUI mounts the repo root so `.svelte-kit` generated files can resolve the workspace-root `node_modules`, clears stale generated caches on startup, and enables polling in Vite so host edits trigger reloads reliably under Docker.

## Verification Targets

- API startup logs build label and enabled auth modes
- `GET /health` and `GET /healthz` on the API are unauthenticated and return `ok`, `message`, `version`, and `build`
- `GET /health` and `GET /healthz` on the WUI are unauthenticated and return the same `ok`, `message`, `version`, and `build` shape
- `GET /wui-health` remains available as a compatibility alias for the WUI
- `GET /api/build` returns version, commit hash, and label
- WUI shows the build label in the shell footer
- The default WUI host port is `4096`
- single `.rctrk`, bulk `.rctrk`, `.zip`, and `.zrctrk` imports complete
- aggregate queries warm and reuse Redis cache entries
- export applies exclusion areas when requested

## Current Notes

- The original `backend/` and `frontend/` directories are legacy prototype code and are no longer the intended runtime path.
- Compose-mounted dev and prod bootstrap files live under `config/compose/gui-api/`.
