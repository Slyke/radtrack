# Configuration

## Bootstrap Files

Runtime bootstrap is intentionally minimal and env-driven:

- `RADIACODE_CONFIG_PATH`
- `RADIACODE_SECRETS_PATH`

Both point to JSON5 files.

## Bootstrap-Only Behavior

These values are read at process startup and require restart to change:

- public base URL
- Postgres connection host, port, database, and username
- Redis connection URL
- enabled auth modes
- OIDC endpoints and trusted-header settings
- bootstrap admin credentials
- `resetAdminPassword`
- monolith serving behavior

## Runtime Settings

Mutable non-secret settings are seeded from bootstrap files into Postgres and then read from Postgres at runtime. Initial seeded settings include:

- default theme
- default font
- default map metric
- default aggregation shape
- aggregate mode bucket precision
- raw point cap
- Redis cache TTL seconds
- map tile URL template and attribution

## Additional Env Vars

- `PORT`: API listen port, default `8192`
- `TRUST_PROXY`: set to `1` behind a reverse proxy
- `MONOLITH`: set to `true` to enable API-hosted WUI serving
- `MONOLITH_MODE`: `auto`, `serve`, or `proxy`
- `MONOLITH_DEV_PROXY`: allow dev-proxy fallback when the built WUI is unavailable
- `PUBLIC_API_URL`: WUI browser-side API base for split deployments
- `LOG_*` and `K8S_*`: structured logging overrides following `../styleguide`

## Sample Files

- [`../config/radiacode.config.example.json5`](../config/radiacode.config.example.json5)
- [`../config/radiacode.secrets.example.json5`](../config/radiacode.secrets.example.json5)
