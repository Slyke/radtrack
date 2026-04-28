# Configuration

## Bootstrap Files

Runtime bootstrap is intentionally minimal and env-driven:

- `RADTRACK_CONFIG_PATH`
- `RADTRACK_SECRETS_PATH`

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
- default language
- default map metric
- default aggregation shape
- aggregate mode bucket precision
- raw point cap
- live update polling interval seconds
- Redis cache TTL seconds
- map tile URL template and attribution

`aggregation.cellCacheRefreshTtlOnRead` is a bootstrap default for each user's Settings page. It defaults to `false`; when a user enables the setting, aggregate cell cache hits reset the configured `aggregation.cacheTtlSeconds` timer on read.

`ui.defaultLanguage` comes from the bootstrap config file and currently controls the WUI language with `en-US` fallback. There is no runtime language picker.

## Additional Env Vars

- `PORT`: API listen port, default `8192`
- `TRUST_PROXY`: set to `1` behind a reverse proxy
- `MONOLITH`: set to `true` to enable API-hosted WUI serving
- `MONOLITH_MODE`: `auto`, `serve`, or `proxy`
- `MONOLITH_DEV_PROXY`: allow dev-proxy fallback when the built WUI is unavailable
- `PUBLIC_API_URL`: WUI browser-side API base for split deployments
- `LOG_*` and `K8S_*`: structured logging overrides following `../styleguide`

## Logging

Bootstrap config can define logging sinks and feature logging:

```json5
logging: {
  sinks: {
    console: {
      enabled: true,
      levels: ["debug", "info", "warn", "error"],
      format: "text"
    },
    file: {
      enabled: false,
      levels: ["info", "warn", "error"],
      format: "json",
      path: "/var/log/radtrack/api.jsonl"
    }
  },
  features: {
    cache: { enabled: true, level: "debug" },
    query: { enabled: true, level: "debug" }
  }
}
```

`logging.features.cache` logs Redis reads, writes, TTL checks, key deletes, pattern deletes, and aggregate cache hit/write summaries.
`logging.features.query` logs raw-point and aggregate query preparation, including cache eligibility and the reason a request is not cacheable.

Environment variables still override sink settings:

- `LOG_CONSOLE_ENABLED`, `LOG_CONSOLE_LEVELS`, `LOG_CONSOLE_FORMAT`
- `LOG_FILE_ENABLED`, `LOG_FILE_LEVELS`, `LOG_FILE_FORMAT`, `LOG_FILE_PATH`

## Sample Files

- [`../config/radtrack.config.example.json5`](../config/radtrack.config.example.json5)
- [`../config/radtrack.secrets.example.json5`](../config/radtrack.secrets.example.json5)
- [`../config/compose/gui-api/radtrack.config.example-localauth.json5`](../config/compose/gui-api/radtrack.config.example-localauth.json5)
- [`../config/compose/gui-api/radtrack.secrets.example-localauth.json5`](../config/compose/gui-api/radtrack.secrets.example-localauth.json5)
- [`../config/compose/gui-api/radtrack.config.example-oidc-localauth.json5`](../config/compose/gui-api/radtrack.config.example-oidc-localauth.json5)
- [`../config/compose/gui-api/radtrack.secrets.example-oidc.json5`](../config/compose/gui-api/radtrack.secrets.example-oidc.json5)
- [`../config/compose/gui-api/radtrack.config.json5`](../config/compose/gui-api/radtrack.config.json5)
- [`../config/compose/gui-api/radtrack.secrets.json5`](../config/compose/gui-api/radtrack.secrets.json5)
