# Live Tracks API Advanced

This document covers the authenticated automation flow for:

1. logging in
2. creating a live track inside a dataset
3. creating an ingest key for that track
4. submitting points with the generated key

Most users should not need this flow. The normal path is to create the live track and ingest key in the UI, then use [`live-tracks-api.md`](./live-tracks-api.md) for payload submission examples.

The examples assume:

- the API is reachable at `http://127.0.0.1:8192`
- local auth is enabled
- `jq` is available for parsing JSON responses

## Authenticate And Capture CSRF

```bash
BASE_URL=http://127.0.0.1:8192
COOKIE_JAR=/tmp/radtrack-live-track.cookies.txt
USERNAME=admin
PASSWORD=admin123A

curl -sS -c "$COOKIE_JAR" \
  -H 'content-type: application/json' \
  --data "{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\"}" \
  "$BASE_URL/auth/local/login"

ME_JSON=$(curl -sS -b "$COOKIE_JAR" "$BASE_URL/api/me")
CSRF_HEADER=$(printf '%s' "$ME_JSON" | jq -r '.csrf.headerName')
CSRF_TOKEN=$(printf '%s' "$ME_JSON" | jq -r '.csrf.token')
```

## Create A Live Track

Set `DATASET_ID` to the dataset that should own the live track.

```bash
DATASET_ID=replace-with-dataset-id

LIVE_TRACK_JSON=$(curl -sS -b "$COOKIE_JAR" \
  -H "$CSRF_HEADER: $CSRF_TOKEN" \
  -H 'content-type: application/json' \
  --data '{"name":"Harbour walk live"}' \
  "$BASE_URL/api/datasets/$DATASET_ID/live-tracks")

TRACK_ID=$(printf '%s' "$LIVE_TRACK_JSON" | jq -r '.track.id')
INGEST_TRACK_ID=$(printf '%s' "$LIVE_TRACK_JSON" | jq -r '.track.ingestTrackId')
```

The create response includes both:

- `track.id`: used for management APIs like ingest key creation
- `track.ingestTrackId`: used by the public point ingest endpoint

## Create An Ingest Key

This returns the plaintext key once. Save it somewhere secure if you need to reuse it.

```bash
KEY_JSON=$(curl -sS -b "$COOKIE_JAR" \
  -H "$CSRF_HEADER: $CSRF_TOKEN" \
  -H 'content-type: application/json' \
  --data '{"label":"curl demo key","notes":"used by docs/live-tracks-api-advanced.md"}' \
  "$BASE_URL/api/tracks/$TRACK_ID/ingest-keys")

INGEST_KEY=$(printf '%s' "$KEY_JSON" | jq -r '.result.plaintextKey')
```

## Submit A Point

```bash
curl -sS -X POST \
  -H 'content-type: application/json' \
  -H "x-radtrack-api-key: $INGEST_KEY" \
  --data '{
    "occurredAt": "2026-04-17T19:15:00.000Z",
    "latitude": 49.2827,
    "longitude": -123.1207,
    "usv": 0.11
  }' \
  "$BASE_URL/api/ingest/tracks/$INGEST_TRACK_ID/points"
```
