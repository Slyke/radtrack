# Live Datalog API

These examples assume the live datalog already exists and the ingest key has already been created in the UI.

Use the UI for setup:

1. create the live datalog in the dataset page
2. open the live datalog details page
3. generate an ingest key and copy it when it is shown

Then use the known values with `curl`:

- `INGEST_DATALOG_ID`: the live datalog ingest id
- `INGEST_KEY`: the plaintext key created in the UI

The examples assume:

- the API is reachable at `http://127.0.0.1:8192`
- you already know the live datalog ingest id and API key
- you are only submitting points, not creating datalogs or keys automatically

```bash
BASE_URL=http://127.0.0.1:8192
INGEST_DATALOG_ID=replace-with-ingest-datalog-id
INGEST_KEY=replace-with-plaintext-ingest-key
```

## Submit A Minimal Point

The ingest endpoint does not use CSRF. It authenticates with the `x-radtrack-api-key` header.

Required fields:

- `occurredAt` or `timestamp`
- `latitude`
- `longitude`
- at least one numeric field inside `measurements` such as `doseRate` or `countRate`

```bash
curl -sS -X POST \
  -H 'content-type: application/json' \
  -H "x-radtrack-api-key: $INGEST_KEY" \
  --data '{
    "occurredAt": "2026-04-17T19:15:00.000Z",
    "latitude": 49.2827,
    "longitude": -123.1207,
    "measurements": {
      "doseRate": 0.11
    }
  }' \
  "$BASE_URL/api/ingest/datalogs/$INGEST_DATALOG_ID/points"
```

## Notes

- If you need to automate live track creation or ingest key generation through authenticated APIs, see [`live-tracks-api-advanced.md`](./live-tracks-api-advanced.md).
- The plaintext ingest key is only shown when the key is created or rotated.
- `measurements` is the canonical numeric field bag.
- `components` is the canonical popup-text field bag.
- `deviceId` stays at the top level for device identity and popup display.
- `extra` remains stored metadata and is not intended for autodetected popup fields.
- For backwards compatibility, root-level numeric props are still accepted as measurement fields and the older top-level metadata props still map into their matching `components` keys.
- Datalog field definitions support `valueType: "number"`, `valueType: "time"`, and `valueType: "string"`.
- Reserved synthetic popup-only field keys that may be added manually to a datalog are:
  - `radtrackCacheKey`
  - `radtrackCacheSource`
  - `radtrackCacheTtlSeconds`
  - for aggregate popups these expose the clicked cell's cache key/source/remaining TTL, not the full-screen aggregate query cache

## Submit A Full Payload

```bash
curl -sS -X POST \
  -H 'content-type: application/json' \
  -H "x-radtrack-api-key: $INGEST_KEY" \
  --data '{
    "occurredAt": "2026-04-17T19:15:00.000Z",
    "latitude": 49.2827,
    "longitude": -123.1207,
    "accuracy": 4.2,
    "altitudeMeters": 18.5,
    "measurements": {
      "usv": 0.11,
      "countRate": 84,
      "temperatureC": 21.3,
      "humidityPct": 42,
      "pressureHpa": 1014.6,
      "batteryPct": 88
    },
    "deviceId": "rc-001",
    "components": {
      "deviceName": "Pocket Gamma",
      "deviceType": "RadTrack 102",
      "deviceCalibration": "factory-2026-01",
      "firmwareVersion": "1.2.3",
      "sourceReadingId": "reading-000123",
      "comment": "Delayed upload after reconnect",
      "custom": "{\"trip\":\"harbour\"}"
    },
    "extra": {
      "gpsFix": "3d",
      "satelliteCount": 14
    }
  }' \
  "$BASE_URL/api/ingest/datalogs/$INGEST_DATALOG_ID/points"
```

## Submit With `timestamp`

`timestamp` is accepted as an alias for `occurredAt`.

```bash
curl -sS -X POST \
  -H 'content-type: application/json' \
  -H "x-radtrack-api-key: $INGEST_KEY" \
  --data '{
    "timestamp": "2026-04-17T19:20:00.000Z",
    "latitude": 49.2831,
    "longitude": -123.1214,
    "measurements": {
      "countRate": 91
    }
  }' \
  "$BASE_URL/api/ingest/datalogs/$INGEST_DATALOG_ID/points"
```

## Duplicate Protection With `sourceReadingId`

If the sender retries the same reading with the same `sourceReadingId`, the API returns HTTP `200` with `"duplicate": true` instead of creating another row.

```bash
curl -i -sS -X POST \
  -H 'content-type: application/json' \
  -H "x-radtrack-api-key: $INGEST_KEY" \
  --data '{
    "occurredAt": "2026-04-17T19:25:00.000Z",
    "latitude": 49.2835,
    "longitude": -123.1222,
    "measurements": {
      "doseRate": 0.13
    },
    "components": {
      "sourceReadingId": "reading-duplicate-demo"
    }
  }' \
  "$BASE_URL/api/ingest/datalogs/$INGEST_DATALOG_ID/points"
```
