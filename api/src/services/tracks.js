import { randomBytes } from 'node:crypto';
import { createAppError } from '../lib/errors.js';
import { createOpaqueId, sha256Hex } from '../utils/ids.js';
import { parseTrackBuffer } from '../utils/track.js';
import { canShare } from './permissions.js';

const ingestKeyHeaderName = 'x-radtrack-api-key';

const createIngestTrackId = () => randomBytes(8).toString('hex');

const createPlainIngestKey = () => `rtk-${randomBytes(10).toString('hex')}`;

const normalizeIngestKey = ({ value }) => String(value ?? '').trim().toLowerCase();

const asTrimmedString = ({ value, field, required = false, maxLength = 255, correlationId = null }) => {
  if (value === undefined || value === null || value === '') {
    if (!required) {
      return null;
    }

    throw createAppError({
      caller: 'tracks::asTrimmedString',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  const stringValue = String(value).trim();
  if (!stringValue) {
    if (!required) {
      return null;
    }

    throw createAppError({
      caller: 'tracks::asTrimmedString',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  if (stringValue.length > maxLength) {
    throw createAppError({
      caller: 'tracks::asTrimmedString',
      reason: `${field} is too long.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return stringValue;
};

const asOptionalNumber = ({ value, field, correlationId = null }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw createAppError({
      caller: 'tracks::asOptionalNumber',
      reason: `${field} must be a finite number.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return parsed;
};

const asRequiredNumber = ({ value, field, correlationId = null }) => {
  const parsed = asOptionalNumber({ value, field, correlationId });
  if (parsed === null) {
    throw createAppError({
      caller: 'tracks::asRequiredNumber',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return parsed;
};

const asIsoTimestamp = ({ value, field, correlationId = null }) => {
  if (value === undefined || value === null || value === '') {
    throw createAppError({
      caller: 'tracks::asIsoTimestamp',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    throw createAppError({
      caller: 'tracks::asIsoTimestamp',
      reason: `${field} must be a valid timestamp.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return {
    raw: String(value),
    iso: timestamp.toISOString()
  };
};

const asOptionalIsoTimestamp = ({ value, field, correlationId = null }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  return asIsoTimestamp({ value, field, correlationId }).iso;
};

const ensureLatLon = ({ latitude, longitude, correlationId = null }) => {
  if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
    throw createAppError({
      caller: 'tracks::ensureLatLon',
      reason: 'Latitude or longitude is out of range.',
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }
};

const mapTrackRow = ({ row }) => ({
  id: row.id,
  datasetId: row.dataset_id,
  ownerUserId: row.owner_user_id,
  sourceType: row.source_type,
  ingestTrackId: row.ingest_track_id,
  trackName: row.track_name,
  deviceIdentifierRaw: row.device_identifier_raw,
  deviceModel: row.device_model,
  deviceSerial: row.device_serial,
  rowCount: row.row_count,
  validRowCount: row.valid_row_count,
  warningCount: row.warning_count,
  errorCount: row.error_count,
  skippedRowCount: row.skipped_row_count,
  startedAt: row.started_at,
  endedAt: row.ended_at,
  createdAt: row.created_at
});

const mapKeyRow = ({ row }) => ({
  id: row.id,
  keyPrefix: row.key_prefix,
  label: row.label,
  notes: row.notes,
  createdByUserId: row.created_by_user_id,
  revokedByUserId: row.revoked_by_user_id,
  createdAt: row.created_at,
  lastUsedAt: row.last_used_at,
  revokedAt: row.revoked_at,
  active: !row.revoked_at
});

const mapReadingRow = ({ row }) => ({
  id: row.id,
  rowNumber: row.row_number,
  rawTimestamp: row.raw_timestamp,
  parsedTimeText: row.parsed_time_text,
  occurredAt: row.occurred_at,
  receivedAt: row.received_at,
  latitude: row.latitude,
  longitude: row.longitude,
  accuracy: row.accuracy,
  altitudeMeters: row.altitude_meters,
  usv: row.dose_rate,
  countRate: row.count_rate,
  temperatureC: row.temperature_c,
  humidityPct: row.humidity_pct,
  pressureHpa: row.pressure_hpa,
  batteryPct: row.battery_pct,
  firmwareVersion: row.firmware_version,
  deviceId: row.device_id,
  deviceName: row.device_name,
  deviceType: row.device_type,
  deviceCalibration: row.device_calibration,
  sourceReadingId: row.source_reading_id,
  comment: row.comment,
  custom: row.custom_text,
  isModified: row.is_modified,
  modifiedAt: row.modified_at,
  modifiedByUserId: row.modified_by_user_id,
  extra: row.extra_json
});

const batchInsertReadings = async ({ client, trackId, rows, actorCreatedAt }) => {
  const chunkSize = 250;

  for (let startIndex = 0; startIndex < rows.length; startIndex += chunkSize) {
    const slice = rows.slice(startIndex, startIndex + chunkSize);
    const values = [];
    const placeholders = [];

    slice.forEach((row, index) => {
      const base = index * 17;
      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}::jsonb, $${base + 15}::jsonb, $${base + 16}, $${base + 17}, FALSE, NULL, NULL)`
      );
      values.push(
        createOpaqueId(),
        trackId,
        row.rawTimestamp,
        row.parsedTimeText,
        row.occurredAt,
        row.latitude,
        row.longitude,
        row.accuracy,
        row.altitudeMeters ?? null,
        row.doseRate,
        row.countRate,
        row.comment,
        row.rowNumber,
        JSON.stringify(row.warningFlags ?? []),
        JSON.stringify(row.extraJson ?? {}),
        actorCreatedAt,
        actorCreatedAt
      );
    });

    await client.query(
      `INSERT INTO readings (
        id,
        track_id,
        raw_timestamp,
        parsed_time_text,
        occurred_at,
        latitude,
        longitude,
        accuracy,
        altitude_meters,
        dose_rate,
        count_rate,
        comment,
        row_number,
        warning_flags_json,
        extra_json,
        received_at,
        created_at,
        is_modified,
        modified_by_user_id,
        modified_at
      ) VALUES ${placeholders.join(', ')}`,
      values
    );
  }
};

export const createTrackService = ({ db, audit, datasetService }) => {
  const allocateIngestTrackId = async ({ client, ownerUserId, correlationId = null }) => {
    for (let attempt = 0; attempt < 12; attempt += 1) {
      const candidate = createIngestTrackId();
      const existing = await client.query(
        `SELECT 1
         FROM tracks
         WHERE owner_user_id = $1 AND ingest_track_id = $2`,
        [ownerUserId, candidate]
      );
      if (!existing.rows.length) {
        return candidate;
      }
    }

    throw createAppError({
      caller: 'tracks::allocateIngestTrackId',
      reason: 'Failed allocating a unique ingest track id.',
      errorKey: 'ERR_UNKNOWN',
      correlationId,
      status: 500
    });
  };

  const loadTrackAccess = async ({ trackId, user, correlationId = null }) => {
    const trackResult = await db.query(
      `SELECT
         t.*,
         d.name AS dataset_name,
         d.owner_user_id AS dataset_owner_user_id
       FROM tracks t
       JOIN datasets d ON d.id = t.dataset_id
       WHERE t.id = $1`,
      [trackId]
    );
    const track = trackResult.rows[0] ?? null;
    if (!track) {
      throw createAppError({
        caller: 'tracks::loadTrackAccess',
        reason: 'Track was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    let datasetAccess = null;
    try {
      datasetAccess = await datasetService.getDatasetAccess({
        datasetId: track.dataset_id,
        user,
        correlationId
      });
    } catch (error) {
      if (error?.status !== 403) {
        throw error;
      }
    }

    const trackShareResult = await db.query(
      `SELECT access_level
       FROM track_shares
       WHERE track_id = $1
         AND target_user_id = $2
       LIMIT 1`,
      [trackId, user.id]
    );
    const trackShareAccessLevel = trackShareResult.rows[0]?.access_level ?? null;
    const trackAccessLevel = (
      datasetAccess?.access_level === 'edit' || trackShareAccessLevel === 'edit'
    )
      ? 'edit'
      : (
        datasetAccess?.access_level === 'view' || trackShareAccessLevel === 'view'
          ? 'view'
          : null
      );

    if (!trackAccessLevel) {
      throw createAppError({
        caller: 'tracks::loadTrackAccess',
        reason: 'Track is not visible to the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return {
      track,
      dataset: {
        id: track.dataset_id,
        name: datasetAccess?.name ?? track.dataset_name,
        accessLevel: datasetAccess?.access_level ?? null
      },
      trackAccessLevel
    };
  };

  const requireEditableTrackAccess = async ({ trackId, user, correlationId = null }) => {
    const access = await loadTrackAccess({ trackId, user, correlationId });
    if (access.trackAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'tracks::requireEditableTrackAccess',
        reason: 'Track is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return access;
  };

  const listTrackKeys = async ({ trackId }) => {
    const result = await db.query(
      `SELECT *
       FROM track_ingest_keys
       WHERE track_id = $1
       ORDER BY revoked_at NULLS FIRST, created_at DESC`,
      [trackId]
    );
    return result.rows.map((row) => mapKeyRow({ row }));
  };

  const refreshTrackBounds = async ({ client, trackId }) => {
    await client.query(
      `UPDATE tracks
       SET started_at = (
             SELECT MIN(occurred_at)
             FROM readings
             WHERE track_id = $1
           ),
           ended_at = (
             SELECT MAX(occurred_at)
             FROM readings
             WHERE track_id = $1
           )
       WHERE id = $1`,
      [trackId]
    );
  };

  const loadTrackReadingRow = async ({ trackId, readingId, client = db, correlationId = null }) => {
    const result = await client.query(
      `SELECT
         id,
         track_id,
         raw_timestamp,
         parsed_time_text,
         occurred_at,
         received_at,
         latitude,
         longitude,
         accuracy,
         altitude_meters,
         dose_rate,
         count_rate,
         temperature_c,
         humidity_pct,
         pressure_hpa,
         battery_pct,
         firmware_version,
         device_id,
         device_name,
         device_type,
         device_calibration,
         source_reading_id,
         comment,
         custom_text,
         row_number,
         is_modified,
         modified_at,
         modified_by_user_id,
         extra_json
       FROM readings
       WHERE id = $1
         AND track_id = $2`,
      [readingId, trackId]
    );
    const row = result.rows[0] ?? null;

    if (!row) {
      throw createAppError({
        caller: 'tracks::loadTrackReadingRow',
        reason: 'Track reading was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    return row;
  };

  const listTrackReadings = async ({ trackId, limit = 100, offset = 0 }) => {
    const normalizedLimit = Math.max(1, Math.min(250, Number(limit) || 100));
    const normalizedOffset = Math.max(0, Number(offset) || 0);
    const [result, totalCountResult] = await Promise.all([
      db.query(
        `SELECT
           id,
           raw_timestamp,
           parsed_time_text,
           occurred_at,
           received_at,
           latitude,
           longitude,
           accuracy,
           altitude_meters,
           dose_rate,
           count_rate,
           temperature_c,
           humidity_pct,
           pressure_hpa,
           battery_pct,
           firmware_version,
           device_id,
           device_name,
           device_type,
           device_calibration,
           source_reading_id,
           comment,
           custom_text,
           row_number,
           is_modified,
           modified_at,
           modified_by_user_id,
           extra_json
         FROM readings
         WHERE track_id = $1
         ORDER BY row_number ASC, created_at ASC
         LIMIT $2
         OFFSET $3`,
        [trackId, normalizedLimit, normalizedOffset]
      ),
      db.query(
        `SELECT COUNT(*)::integer AS total_count
         FROM readings
         WHERE track_id = $1`,
        [trackId]
      )
    ]);

    return {
      offset: normalizedOffset,
      limit: normalizedLimit,
      totalCount: Number(totalCountResult.rows[0]?.total_count ?? 0),
      readings: result.rows.map((row) => mapReadingRow({ row }))
    };
  };

  const getTrackDetail = async ({ trackId, user, correlationId = null, limit = 100, offset = 0 }) => {
    const { track, dataset, trackAccessLevel } = await loadTrackAccess({ trackId, user, correlationId });
    const canManageIngest = trackAccessLevel === 'edit' && track.source_type === 'live';
    const [keys, readingsPage, shares] = await Promise.all([
      canManageIngest ? listTrackKeys({ trackId }) : Promise.resolve([]),
      listTrackReadings({ trackId, limit, offset }),
      trackAccessLevel === 'edit'
        ? db.query(
            `SELECT ts.*, u.username
             FROM track_shares ts
             JOIN users u ON u.id = ts.target_user_id
             WHERE ts.track_id = $1
             ORDER BY u.username`,
            [trackId]
          ).then((result) => result.rows.map((row) => ({
            id: row.id,
            targetUserId: row.target_user_id,
            username: row.username,
            accessLevel: row.access_level,
            createdBy: row.created_by,
            createdAt: row.created_at
          })))
        : Promise.resolve([])
    ]);

    return {
      ...mapTrackRow({ row: track }),
      accessLevel: trackAccessLevel,
      dataset: {
        id: dataset.id,
        name: dataset.name,
        accessLevel: dataset.accessLevel
      },
      canManageIngest,
      canRestoreOriginal: trackAccessLevel === 'edit' && track.source_type === 'import' && Boolean(track.raw_file_id),
      ingest: track.source_type === 'live'
        ? {
            headerName: ingestKeyHeaderName,
            endpointPath: `/api/ingest/tracks/${track.ingest_track_id}/points`
          }
        : null,
      keys,
      shares,
      readingsPage
    };
  };

  const createLiveTrack = async ({ datasetId, user, name, correlationId = null }) => {
    const dataset = await datasetService.getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'tracks::createLiveTrack',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const trimmedName = asTrimmedString({
      value: name,
      field: 'name',
      required: true,
      maxLength: 255,
      correlationId
    });

    const trackId = createOpaqueId();
    const createdAt = new Date().toISOString();

    await db.withTransaction(async (client) => {
      const ingestTrackId = await allocateIngestTrackId({
        client,
        ownerUserId: user.id,
        correlationId
      });

      await client.query(
        `INSERT INTO tracks (
          id,
          dataset_id,
          raw_file_id,
          owner_user_id,
          device_identifier_raw,
          device_model,
          device_serial,
          track_name,
          raw_header_line,
          raw_columns_json,
          header_metadata_json,
          row_count,
          valid_row_count,
          warning_count,
          error_count,
          skipped_row_count,
          started_at,
          ended_at,
          created_at,
          source_type,
          ingest_track_id
        ) VALUES (
          $1, $2, NULL, $3, NULL, NULL, NULL, $4, '', '[]'::jsonb, '{}'::jsonb,
          0, 0, 0, 0, 0, NULL, NULL, $5, 'live', $6
        )`,
        [trackId, datasetId, user.id, trimmedName, createdAt, ingestTrackId]
      );
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'track.created',
      entityType: 'track',
      entityId: trackId,
      payload: {
        datasetId,
        sourceType: 'live',
        trackName: trimmedName
      }
    });

    return getTrackDetail({ trackId, user, correlationId });
  };

  const updateTrack = async ({ trackId, user, name, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });

    const trimmedName = asTrimmedString({
      value: name,
      field: 'name',
      required: true,
      maxLength: 255,
      correlationId
    });

    if (trimmedName === track.track_name) {
      return {
        datasetId: track.dataset_id,
        updated: false
      };
    }

    await db.query(
      `UPDATE tracks
       SET track_name = $2
       WHERE id = $1`,
      [trackId, trimmedName]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track.updated',
      entityType: 'track',
      entityId: trackId,
      payload: {
        datasetId: track.dataset_id,
        oldTrackName: track.track_name,
        newTrackName: trimmedName
      }
    });

    return {
      datasetId: track.dataset_id,
      updated: true
    };
  };

  const deleteTrack = async ({ trackId, user, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });

    await db.query('DELETE FROM tracks WHERE id = $1', [trackId]);

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track.deleted',
      entityType: 'track',
      entityId: trackId,
      payload: {
        datasetId: track.dataset_id,
        sourceType: track.source_type,
        trackName: track.track_name
      }
    });

    return {
      datasetId: track.dataset_id
    };
  };

  const updateTrackReading = async ({
    trackId,
    readingId,
    user,
    occurredAt,
    latitude,
    longitude,
    accuracy,
    altitudeMeters,
    usv,
    countRate,
    comment,
    correlationId = null
  }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });
    const normalizedOccurredAt = asOptionalIsoTimestamp({
      value: occurredAt,
      field: 'occurredAt',
      correlationId
    });
    const normalizedLatitude = asRequiredNumber({
      value: latitude,
      field: 'latitude',
      correlationId
    });
    const normalizedLongitude = asRequiredNumber({
      value: longitude,
      field: 'longitude',
      correlationId
    });
    const normalizedAccuracy = asOptionalNumber({
      value: accuracy,
      field: 'accuracy',
      correlationId
    });
    const normalizedAltitudeMeters = asOptionalNumber({
      value: altitudeMeters,
      field: 'altitudeMeters',
      correlationId
    });
    const normalizedUsv = asOptionalNumber({
      value: usv,
      field: 'usv',
      correlationId
    });
    const normalizedCountRate = asOptionalNumber({
      value: countRate,
      field: 'countRate',
      correlationId
    });
    const normalizedComment = asTrimmedString({
      value: comment,
      field: 'comment',
      required: false,
      maxLength: 4000,
      correlationId
    });

    ensureLatLon({
      latitude: normalizedLatitude,
      longitude: normalizedLongitude,
      correlationId
    });

    await loadTrackReadingRow({
      trackId,
      readingId,
      correlationId
    });

    await db.withTransaction(async (client) => {
      await client.query(
        `UPDATE readings
         SET occurred_at = $3,
             latitude = $4,
             longitude = $5,
             accuracy = $6,
             altitude_meters = $7,
             dose_rate = $8,
             count_rate = $9,
             comment = $10,
             is_modified = TRUE,
             modified_by_user_id = $11,
             modified_at = $12
         WHERE id = $1
           AND track_id = $2`,
        [
          readingId,
          trackId,
          normalizedOccurredAt,
          normalizedLatitude,
          normalizedLongitude,
          normalizedAccuracy,
          normalizedAltitudeMeters,
          normalizedUsv,
          normalizedCountRate,
          normalizedComment,
          user.id,
          new Date().toISOString()
        ]
      );

      await refreshTrackBounds({
        client,
        trackId
      });
    });

    const reading = mapReadingRow({
      row: await loadTrackReadingRow({
        trackId,
        readingId,
        correlationId
      })
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'reading.updated',
      entityType: 'reading',
      entityId: readingId,
      payload: {
        trackId,
        datasetId: track.dataset_id
      }
    });

    return {
      datasetId: track.dataset_id,
      reading
    };
  };

  const restoreTrackOriginal = async ({ trackId, user, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });

    if (track.source_type !== 'import' || !track.raw_file_id) {
      throw createAppError({
        caller: 'tracks::restoreTrackOriginal',
        reason: 'Only imported tracks with archived source files can be restored.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const rawFileResult = await db.query(
      `SELECT original_filename, blob_data
       FROM raw_files
       WHERE id = $1`,
      [track.raw_file_id]
    );
    const rawFile = rawFileResult.rows[0] ?? null;

    if (!rawFile) {
      throw createAppError({
        caller: 'tracks::restoreTrackOriginal',
        reason: 'The archived source file for this track could not be found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    const parsed = parseTrackBuffer({
      buffer: rawFile.blob_data,
      fileName: rawFile.original_filename,
      correlationId
    });

    await db.withTransaction(async (client) => {
      await client.query(
        `UPDATE tracks
         SET device_identifier_raw = $2,
             device_model = $3,
             device_serial = $4,
             track_name = $5,
             raw_header_line = $6,
             raw_columns_json = $7::jsonb,
             header_metadata_json = $8::jsonb,
             row_count = $9,
             valid_row_count = $10,
             warning_count = $11,
             error_count = $12,
             skipped_row_count = $13,
             started_at = $14,
             ended_at = $15
         WHERE id = $1`,
        [
          trackId,
          parsed.device.deviceIdentifierRaw,
          parsed.device.deviceModel,
          parsed.device.deviceSerial,
          parsed.trackName,
          parsed.rawHeaderLine,
          JSON.stringify(parsed.rawColumns),
          JSON.stringify(parsed.headerMetadata),
          parsed.rowCount,
          parsed.validRowCount,
          parsed.warningCount,
          parsed.errorCount,
          parsed.skippedRowCount,
          parsed.startedAt,
          parsed.endedAt
        ]
      );

      await client.query(
        `DELETE FROM readings
         WHERE track_id = $1`,
        [trackId]
      );

      await batchInsertReadings({
        client,
        trackId,
        rows: parsed.rows,
        actorCreatedAt: new Date().toISOString()
      });

      await refreshTrackBounds({
        client,
        trackId
      });
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track.restored_original',
      entityType: 'track',
      entityId: trackId,
      payload: {
        datasetId: track.dataset_id,
        rawFileId: track.raw_file_id
      }
    });

    return {
      datasetId: track.dataset_id
    };
  };

  const upsertTrackShare = async ({ trackId, user, targetUserId, accessLevel, correlationId = null }) => {
    if (!canShare({ user })) {
      throw createAppError({
        caller: 'tracks::upsertTrackShare',
        reason: 'This user is not allowed to share tracks.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const { trackAccessLevel } = await loadTrackAccess({ trackId, user, correlationId });
    if (trackAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'tracks::upsertTrackShare',
        reason: 'Track is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    if (!['view', 'edit'].includes(accessLevel)) {
      throw createAppError({
        caller: 'tracks::upsertTrackShare',
        reason: 'Share access level must be view or edit.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const shareId = createOpaqueId();
    await db.query(
      `INSERT INTO track_shares (id, track_id, target_user_id, access_level, created_by, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (track_id, target_user_id) DO UPDATE
       SET access_level = EXCLUDED.access_level,
           created_by = EXCLUDED.created_by,
           created_at = EXCLUDED.created_at`,
      [shareId, trackId, targetUserId, accessLevel, user.id, new Date().toISOString()]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'track.share_upserted',
      entityType: 'track',
      entityId: trackId,
      payload: { targetUserId, accessLevel }
    });
  };

  const removeTrackShare = async ({ trackId, shareId, user, correlationId = null }) => {
    const { trackAccessLevel } = await loadTrackAccess({ trackId, user, correlationId });
    if (trackAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'tracks::removeTrackShare',
        reason: 'Track is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query(
      `DELETE FROM track_shares
       WHERE id = $1
         AND track_id = $2`,
      [shareId, trackId]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'track.share_removed',
      entityType: 'track',
      entityId: trackId,
      payload: { shareId }
    });
  };

  const createTrackIngestKeyRecord = async ({
    client,
    trackId,
    user,
    label,
    notes,
    correlationId = null
  }) => {
    const trimmedLabel = asTrimmedString({
      value: label,
      field: 'label',
      required: true,
      maxLength: 120,
      correlationId
    });
    const trimmedNotes = asTrimmedString({
      value: notes,
      field: 'notes',
      required: false,
      maxLength: 500,
      correlationId
    });

    for (let attempt = 0; attempt < 12; attempt += 1) {
      const plaintextKey = createPlainIngestKey();
      const keyHash = sha256Hex({ value: plaintextKey });
      const keyPrefix = plaintextKey.slice(0, 12);
      const keyId = createOpaqueId();
      const createdAt = new Date().toISOString();

      try {
        await client.query(
          `INSERT INTO track_ingest_keys (
            id,
            track_id,
            key_hash,
            key_prefix,
            label,
            notes,
            created_by_user_id,
            revoked_by_user_id,
            created_at,
            last_used_at,
            revoked_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, NULL, $8, NULL, NULL
          )`,
          [keyId, trackId, keyHash, keyPrefix, trimmedLabel, trimmedNotes, user.id, createdAt]
        );

        return {
          key: {
            id: keyId,
            keyPrefix,
            label: trimmedLabel,
            notes: trimmedNotes,
            createdByUserId: user.id,
            revokedByUserId: null,
            createdAt,
            lastUsedAt: null,
            revokedAt: null,
            active: true
          },
          plaintextKey
        };
      } catch (error) {
        if (error?.code === '23505') {
          continue;
        }
        throw error;
      }
    }

    throw createAppError({
      caller: 'tracks::createTrackIngestKeyRecord',
      reason: 'Failed generating a unique ingest key.',
      errorKey: 'ERR_UNKNOWN',
      correlationId,
      status: 500
    });
  };

  const createTrackIngestKey = async ({ trackId, user, label, notes, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });
    if (track.source_type !== 'live') {
      throw createAppError({
        caller: 'tracks::createTrackIngestKey',
        reason: 'Only live tracks can accept ingest keys.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const result = await db.withTransaction(async (client) => createTrackIngestKeyRecord({
      client,
      trackId,
      user,
      label,
      notes,
      correlationId
    }));

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track_ingest_key.created',
      entityType: 'track_ingest_key',
      entityId: result.key.id,
      payload: {
        trackId,
        keyPrefix: result.key.keyPrefix,
        label: result.key.label
      }
    });

    return result;
  };

  const revokeTrackIngestKey = async ({ trackId, keyId, user, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });
    const result = await db.query(
      `UPDATE track_ingest_keys
       SET revoked_at = COALESCE(revoked_at, $3),
           revoked_by_user_id = COALESCE(revoked_by_user_id, $4)
       WHERE id = $1 AND track_id = $2
       RETURNING *`,
      [keyId, trackId, new Date().toISOString(), user.id]
    );
    const row = result.rows[0] ?? null;
    if (!row) {
      throw createAppError({
        caller: 'tracks::revokeTrackIngestKey',
        reason: 'Track ingest key was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track_ingest_key.revoked',
      entityType: 'track_ingest_key',
      entityId: row.id,
      payload: {
        trackId,
        keyPrefix: row.key_prefix,
        label: row.label
      }
    });

    return mapKeyRow({ row });
  };

  const rotateTrackIngestKey = async ({ trackId, keyId, user, correlationId = null }) => {
    const { track } = await requireEditableTrackAccess({ trackId, user, correlationId });
    const result = await db.withTransaction(async (client) => {
      const existingResult = await client.query(
        `SELECT *
         FROM track_ingest_keys
         WHERE id = $1 AND track_id = $2
         FOR UPDATE`,
        [keyId, trackId]
      );
      const existing = existingResult.rows[0] ?? null;
      if (!existing) {
        throw createAppError({
          caller: 'tracks::rotateTrackIngestKey',
          reason: 'Track ingest key was not found.',
          errorKey: 'DATASET_NOT_FOUND',
          correlationId,
          status: 404
        });
      }

      await client.query(
        `UPDATE track_ingest_keys
         SET revoked_at = COALESCE(revoked_at, $3),
             revoked_by_user_id = COALESCE(revoked_by_user_id, $4)
         WHERE id = $1 AND track_id = $2`,
        [keyId, trackId, new Date().toISOString(), user.id]
      );

      const replacement = await createTrackIngestKeyRecord({
        client,
        trackId,
        user,
        label: existing.label,
        notes: existing.notes,
        correlationId
      });

      return {
        replaced: mapKeyRow({
          row: {
            ...existing,
            revoked_at: existing.revoked_at ?? new Date().toISOString(),
            revoked_by_user_id: existing.revoked_by_user_id ?? user.id
          }
        }),
        replacement
      };
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: track.owner_user_id,
      eventType: 'track_ingest_key.rotated',
      entityType: 'track_ingest_key',
      entityId: result.replacement.key.id,
      payload: {
        trackId,
        replacedKeyId: keyId,
        replacedKeyPrefix: result.replaced.keyPrefix,
        newKeyPrefix: result.replacement.key.keyPrefix,
        label: result.replacement.key.label
      }
    });

    return result;
  };

  const ingestPoint = async ({ ingestTrackId, apiKey, input, correlationId = null }) => {
    const normalizedTrackId = asTrimmedString({
      value: String(ingestTrackId ?? '').toLowerCase(),
      field: 'ingestTrackId',
      required: true,
      maxLength: 64,
      correlationId
    });
    const plaintextKey = asTrimmedString({
      value: normalizeIngestKey({ value: apiKey }),
      field: ingestKeyHeaderName,
      required: true,
      maxLength: 128,
      correlationId
    });
    const apiKeyHash = sha256Hex({ value: plaintextKey });

    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      throw createAppError({
        caller: 'tracks::ingestPoint',
        reason: 'Ingest payload must be a JSON object.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const timestamp = asIsoTimestamp({
      value: input.occurredAt ?? input.timestamp,
      field: 'occurredAt',
      correlationId
    });
    const latitude = asRequiredNumber({ value: input.latitude, field: 'latitude', correlationId });
    const longitude = asRequiredNumber({ value: input.longitude, field: 'longitude', correlationId });
    ensureLatLon({ latitude, longitude, correlationId });

    const accuracy = asOptionalNumber({ value: input.accuracy, field: 'accuracy', correlationId });
    const altitudeMeters = asOptionalNumber({ value: input.altitudeMeters, field: 'altitudeMeters', correlationId });
    const usv = asOptionalNumber({ value: input.usv, field: 'usv', correlationId });
    const countRate = asOptionalNumber({ value: input.countRate, field: 'countRate', correlationId });
    if (usv === null && countRate === null) {
      throw createAppError({
        caller: 'tracks::ingestPoint',
        reason: 'Ingest payload must include usv, countRate, or both.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const payload = {
      accuracy,
      altitudeMeters,
      usv,
      countRate,
      comment: asTrimmedString({ value: input.comment, field: 'comment', required: false, maxLength: 1000, correlationId }),
      deviceId: asTrimmedString({ value: input.deviceId, field: 'deviceId', required: false, maxLength: 255, correlationId }),
      deviceName: asTrimmedString({ value: input.deviceName, field: 'deviceName', required: false, maxLength: 255, correlationId }),
      deviceType: asTrimmedString({ value: input.deviceType, field: 'deviceType', required: false, maxLength: 255, correlationId }),
      deviceCalibration: asTrimmedString({ value: input.deviceCalibration, field: 'deviceCalibration', required: false, maxLength: 1000, correlationId }),
      temperatureC: asOptionalNumber({ value: input.temperatureC, field: 'temperatureC', correlationId }),
      humidityPct: asOptionalNumber({ value: input.humidityPct, field: 'humidityPct', correlationId }),
      pressureHpa: asOptionalNumber({ value: input.pressureHpa, field: 'pressureHpa', correlationId }),
      batteryPct: asOptionalNumber({ value: input.batteryPct, field: 'batteryPct', correlationId }),
      firmwareVersion: asTrimmedString({ value: input.firmwareVersion, field: 'firmwareVersion', required: false, maxLength: 255, correlationId }),
      sourceReadingId: asTrimmedString({ value: input.sourceReadingId, field: 'sourceReadingId', required: false, maxLength: 255, correlationId }),
      custom: asTrimmedString({ value: input.custom, field: 'custom', required: false, maxLength: 4000, correlationId }),
      extra: input.extra && typeof input.extra === 'object' && !Array.isArray(input.extra)
        ? input.extra
        : {}
    };

    const receivedAt = new Date().toISOString();

    try {
      return await db.withTransaction(async (client) => {
        const trackResult = await client.query(
          `SELECT
             track.id,
             track.dataset_id,
             track.owner_user_id,
             track.track_name,
             track.row_count,
             track.valid_row_count,
             track.started_at,
             track.ended_at,
             ingest_key.id AS ingest_key_id,
             ingest_key.key_prefix
           FROM track_ingest_keys ingest_key
           JOIN tracks track ON track.id = ingest_key.track_id
          WHERE ingest_key.key_hash = $1
             AND ingest_key.revoked_at IS NULL
             AND track.ingest_track_id = $2
             AND track.source_type = 'live'
           FOR UPDATE OF track, ingest_key`,
          [apiKeyHash, normalizedTrackId]
        );
        const track = trackResult.rows[0] ?? null;
        if (!track) {
          throw createAppError({
            caller: 'tracks::ingestPoint',
            reason: 'Track ingest authentication failed.',
            errorKey: 'AUTH_FORBIDDEN',
            correlationId,
            status: 401
          });
        }

        const rowNumber = Number(track.row_count ?? 0) + 1;
        const readingId = createOpaqueId();

        await client.query(
          `INSERT INTO readings (
            id,
            track_id,
            raw_timestamp,
            parsed_time_text,
            occurred_at,
            latitude,
            longitude,
            accuracy,
            dose_rate,
            count_rate,
            comment,
            row_number,
            warning_flags_json,
            extra_json,
            created_at,
            received_at,
            altitude_meters,
            temperature_c,
            humidity_pct,
            pressure_hpa,
            battery_pct,
            device_id,
            device_name,
            device_type,
            device_calibration,
            firmware_version,
            source_reading_id,
            custom_text,
            ingest_key_id
          ) VALUES (
            $1, $2, $3, $3, $4, $5, $6, $7, $8, $9, $10, $11, '[]'::jsonb, $12::jsonb, $13, $13,
            $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
          )`,
          [
            readingId,
            track.id,
            timestamp.raw,
            timestamp.iso,
            latitude,
            longitude,
            payload.accuracy,
            payload.usv,
            payload.countRate,
            payload.comment,
            rowNumber,
            JSON.stringify(payload.extra),
            receivedAt,
            payload.altitudeMeters,
            payload.temperatureC,
            payload.humidityPct,
            payload.pressureHpa,
            payload.batteryPct,
            payload.deviceId,
            payload.deviceName,
            payload.deviceType,
            payload.deviceCalibration,
            payload.firmwareVersion,
            payload.sourceReadingId,
            payload.custom,
            track.ingest_key_id
          ]
        );

        await client.query(
          `UPDATE tracks
           SET row_count = row_count + 1,
               valid_row_count = valid_row_count + 1,
               started_at = CASE
                 WHEN $2::timestamptz IS NULL THEN started_at
                 WHEN started_at IS NULL OR $2::timestamptz < started_at THEN $2::timestamptz
                 ELSE started_at
               END,
               ended_at = CASE
                 WHEN $2::timestamptz IS NULL THEN ended_at
                 WHEN ended_at IS NULL OR $2::timestamptz > ended_at THEN $2::timestamptz
                 ELSE ended_at
               END
           WHERE id = $1`,
          [track.id, timestamp.iso]
        );

        await client.query(
          `UPDATE track_ingest_keys
           SET last_used_at = $2
           WHERE id = $1`,
          [track.ingest_key_id, receivedAt]
        );

        return {
          duplicate: false,
          trackId: track.id,
          datasetId: track.dataset_id,
          reading: {
            id: readingId,
            occurredAt: timestamp.iso,
            receivedAt,
            latitude,
            longitude,
            accuracy: payload.accuracy,
            altitudeMeters: payload.altitudeMeters,
            usv: payload.usv,
            countRate: payload.countRate,
            temperatureC: payload.temperatureC,
            humidityPct: payload.humidityPct,
            pressureHpa: payload.pressureHpa,
            batteryPct: payload.batteryPct,
            firmwareVersion: payload.firmwareVersion,
            deviceId: payload.deviceId,
            deviceName: payload.deviceName,
            deviceType: payload.deviceType,
            deviceCalibration: payload.deviceCalibration,
            sourceReadingId: payload.sourceReadingId,
            comment: payload.comment,
            custom: payload.custom,
            extra: payload.extra
          }
        };
      });
    } catch (error) {
      if (error?.code === '23505' && error?.constraint === 'idx_readings_track_source_reading_id') {
        const duplicateResult = await db.query(
          `SELECT
             readings.id,
             tracks.id AS track_id,
             tracks.dataset_id,
             readings.occurred_at,
             readings.received_at,
             readings.latitude,
             readings.longitude,
             readings.accuracy,
             readings.altitude_meters,
             readings.dose_rate,
             readings.count_rate,
             readings.temperature_c,
             readings.humidity_pct,
             readings.pressure_hpa,
             readings.battery_pct,
             readings.firmware_version,
             readings.device_id,
             readings.device_name,
             readings.device_type,
             readings.device_calibration,
             readings.source_reading_id,
             readings.comment,
             readings.custom_text,
             readings.extra_json
           FROM readings
           JOIN tracks ON tracks.id = readings.track_id
           JOIN track_ingest_keys auth_key ON auth_key.track_id = tracks.id
           WHERE auth_key.key_hash = $1
             AND tracks.ingest_track_id = $2
             AND readings.source_reading_id = $3
           LIMIT 1`,
          [apiKeyHash, normalizedTrackId, payload.sourceReadingId]
        );
        const row = duplicateResult.rows[0] ?? null;
        if (row) {
          await db.query(
            `UPDATE track_ingest_keys
             SET last_used_at = $2
             WHERE key_hash = $1`,
            [apiKeyHash, receivedAt]
          );
          return {
            duplicate: true,
            trackId: row.track_id,
            datasetId: row.dataset_id,
            reading: mapReadingRow({ row })
          };
        }
      }

      if (error?.status === 401) {
        const trackResult = await db.query(
          `SELECT track.id, track.owner_user_id
           FROM track_ingest_keys ingest_key
           JOIN tracks track ON track.id = ingest_key.track_id
           WHERE ingest_key.key_hash = $1
           LIMIT 1`,
          [apiKeyHash]
        );
        const scopedTrack = trackResult.rows[0] ?? null;
        await audit.record({
          actorUserId: null,
          scopeUserId: scopedTrack?.owner_user_id ?? null,
          eventType: 'track_ingest.rejected',
          entityType: 'track',
          entityId: scopedTrack?.id ?? null,
          payload: {
            reason: 'authentication_failed',
            ingestTrackId: normalizedTrackId
          }
        });
      }

      throw error;
    }
  };

  return {
    ingestKeyHeaderName,
    getTrackDetail,
    createLiveTrack,
    updateTrack,
    deleteTrack,
    updateTrackReading,
    restoreTrackOriginal,
    upsertTrackShare,
    removeTrackShare,
    createTrackIngestKey,
    revokeTrackIngestKey,
    rotateTrackIngestKey,
    ingestPoint
  };
};
