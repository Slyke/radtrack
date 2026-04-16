# Development

## Local Setup

1. Copy the sample bootstrap files from `config/`.
2. Set:
   - `RADIACODE_CONFIG_PATH`
   - `RADIACODE_SECRETS_PATH`
3. Ensure Postgres and Redis are reachable.
4. Run `npm install`.

## Scripts

- `npm run dev:api`: start the Node API
- `npm run dev:wui`: start the Svelte dev server
- `npm run build`: build the WUI for monolith or production serving
- `npm run check`: run SvelteKit type and consistency checks

## Verification Targets

- API startup logs build label and enabled auth modes
- `GET /health` responds with build label and dependency reachability
- `GET /api/build` returns version, commit hash, and label
- WUI shows the build label in the shell footer
- single `.rctrk`, bulk `.rctrk`, `.zip`, and `.zrctrk` imports complete
- aggregate queries warm and reuse Redis cache entries
- export applies exclusion areas when requested

## Current Notes

- The original `backend/` and `frontend/` directories are legacy prototype code and are no longer the intended runtime path.
- The current workspace is not itself a git repository, so local build labels fall back to `unknown` until the app runs inside a git checkout.
