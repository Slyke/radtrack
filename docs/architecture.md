# Architecture

## Runtime Shape

The app is split into:

- `api/`: Node.js API and monolith host
- `wui/`: Svelte 5 web UI

Default deployment runs them separately. Monolith mode lets the API serve the built WUI, matching the `wui-button` deployment pattern.

## Storage Model

- Postgres stores users, auth identities, sessions, audit events, raw uploaded blobs, datasets, shares, datalogs, datalog ingest keys, readings, exclude areas, combined datasets, and runtime settings.
- Redis stores aggregate query result caches and per-aggregate-cell caches with normal TTL expiry.
- Temporary filesystem use is allowed only during request processing and is not relied on for durable storage.
- Datalog field definitions live on the datalog itself:
  - `number` and `time` fields are plottable and can also be shown in popups
  - `string` fields are popup-only and are deduped in aggregate popups
  - core reading props such as `occurredAt`, `latitude`, `longitude`, `altitudeMeters`, and `accuracy` are only exposed on the map when they are explicitly listed in the datalog field definitions
  - numeric field values are stored in `reading_numeric_values`
  - string field values are resolved from reading metadata columns, stored `components`, and `extra_json`
- Reserved synthetic popup-only fields can be added manually in the datalog field editor:
  - `radtrackDataCount`
  - `radtrackCacheKey`
  - `radtrackCacheSource`
  - `radtrackCacheTtlSeconds`
  - `radtrackDataCount` controls the aggregate popup badge and shows the number of data points in the clicked cell
  - on aggregate popups these refer to the specific rendered cell, not the whole viewport query

## Auth Model

- Local DB users are canonical.
- OIDC and trusted-header auth link to local users through `auth_identities`.
- No self-registration exists.
- First startup bootstraps an admin from secrets or logs a generated password once.
- Local-password users and bootstrap admins are forced to rotate passwords on first login.

## Import Pipeline

1. Intake:
   - accept one or more uploads
   - detect track vs archive payloads from content and extension
   - archive raw artifacts in Postgres
   - safely enumerate archive contents
2. Parse:
   - preserve raw header lines and unknown metadata
   - parse device identifiers when safe
   - tolerate malformed rows, duplicates, blanks, trailing tabs, and extra columns
3. Post-processing:
   - derive track date ranges
   - link or create per-user device records
   - invalidate related aggregate cache entries

## Live Ingest Model

- Datalogs may also be created as `live` datalogs inside existing datasets.
- Each live datalog gets a short per-owner ingest id used by `POST /api/ingest/datalogs/:ingestDatalogId/points`.
- One live datalog can have multiple generated API keys, but each key belongs to only one datalog.
- API keys are stored hashed, exposed in plaintext only once at creation or rotation time, and can be revoked or rotated independently.
- Ingested points store both the sender timestamp (`occurredAt`) and the server acceptance timestamp (`receivedAt`).
- Root-level numeric props are treated as measurements. Root-level non-numeric props are normalized into `extra_json` unless they map to known reading metadata like `deviceId`.
- Scoped audit events capture ingest key lifecycle actions and rejected ingest attempts.

## Query Model

- Raw-point and aggregate queries are server-side.
- Filters support dataset scope, combined dataset scope, viewport, date range, metric ranges, and exclusion application.
- Aggregate shapes support square, circle, and hex layouts.
- Aggregate mode computes min, max, mean, median, rounded-bucket mode, and count.
- Aggregate cells are cached independently from viewport query caches so the same cell can be reused across nearby viewport/hash changes.
- Future live-point invalidation can target aggregate cells directly through `queryService.invalidateAggregateCellsForPoint(...)`, while still clearing broader viewport caches for affected datasets.

## Permission Model

- `view_only`: read and export visible data
- `standard`: import, manage own datasets, share owned datasets, hide points on editable datasets, define exclude areas, and create combined datasets
- `moderator`: standard dataset powers plus user moderation and import inspection across the system
- `admin`: full system control including auth/bootstrap/runtime settings and all datasets
