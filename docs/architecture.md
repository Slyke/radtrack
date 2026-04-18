# Architecture

## Runtime Shape

The app is split into:

- `api/`: Node.js API and monolith host
- `wui/`: Svelte 5 web UI

Default deployment runs them separately. Monolith mode lets the API serve the built WUI, matching the `wui-button` deployment pattern.

## Storage Model

- Postgres stores users, auth identities, sessions, audit events, raw uploaded blobs, datasets, shares, tracks, track ingest keys, readings, exclude areas, combined datasets, and runtime settings.
- Redis stores aggregate query result caches with read-refresh TTL behavior.
- Temporary filesystem use is allowed only during request processing and is not relied on for durable storage.

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

- Tracks may also be created as `live` tracks inside existing datasets.
- Each live track gets a short per-owner ingest id used by `POST /api/ingest/tracks/:ingestTrackId/points`.
- One live track can have multiple generated API keys, but each key belongs to only one track.
- API keys are stored hashed, exposed in plaintext only once at creation or rotation time, and can be revoked or rotated independently.
- Ingested points store both the sender timestamp (`occurredAt`) and the server acceptance timestamp (`receivedAt`).
- Scoped audit events capture ingest key lifecycle actions and rejected ingest attempts.

## Query Model

- Raw-point and aggregate queries are server-side.
- Filters support dataset scope, combined dataset scope, viewport, date range, metric ranges, and exclusion application.
- Aggregate shapes support square, circle, and hex layouts.
- Aggregate mode computes min, max, mean, median, rounded-bucket mode, and count.

## Permission Model

- `view_only`: read and export visible data
- `standard`: import, manage own datasets, share owned datasets, hide points on editable datasets, define exclude areas, and create combined datasets
- `moderator`: standard dataset powers plus user moderation and import inspection across the system
- `admin`: full system control including auth/bootstrap/runtime settings and all datasets
