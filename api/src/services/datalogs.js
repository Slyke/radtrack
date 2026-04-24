import { randomBytes } from 'node:crypto';
import { createAppError } from '../lib/errors.js';
import {
  coerceMeasurementNumericValue,
  coerceMeasurementValueMap,
  getDefaultSupportedFields,
  getAggregateTimePropKey,
  getCoreMetricColumn,
  inferSupportedFieldsFromMeasurementSets,
  mergeMetricFields,
  normalizePropKey,
  normalizeSupportedFields
} from '../utils/datalog-fields.js';
import {
  buildStoredComponents,
  componentsExtraJsonKey,
  getKnownComponentField,
  knownComponentFields,
  stripStoredComponentsFromExtra
} from '../utils/datalog-components.js';
import { createOpaqueId, sha256Hex } from '../utils/ids.js';
import { parseTrackBuffer } from '../utils/track.js';
import { canShare } from './permissions.js';

const ingestKeyHeaderName = 'x-radtrack-api-key';

const createIngestDatalogId = () => randomBytes(8).toString('hex');

const createPlainIngestKey = () => `rtk-${randomBytes(10).toString('hex')}`;

const normalizeIngestKey = ({ value }) => String(value ?? '').trim().toLowerCase();

const implicitMeasurementReservedKeys = new Set([
  'occurredAt',
  'timestamp',
  'rawTimestamp',
  'parsedTimeText',
  'receivedAt',
  'latitude',
  'longitude',
  'altitudeMeters',
  'accuracy',
  'comment',
  'custom',
  'extra',
  'measurements',
  'measurementValues',
  'components',
  'deviceId',
  'deviceName',
  'deviceType',
  'deviceCalibration',
  'firmwareVersion',
  'sourceReadingId'
]);

const knownComponentFieldMap = new Map(knownComponentFields.map((field) => [field.propKey, field]));

const asPlainObject = (value) => (
  value && typeof value === 'object' && !Array.isArray(value)
    ? value
    : null
);

const asTrimmedString = ({ value, field, required = false, maxLength = 255, correlationId = null }) => {
  if (value === undefined || value === null || value === '') {
    if (!required) {
      return null;
    }

    throw createAppError({
      caller: 'datalogs::asTrimmedString',
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
      caller: 'datalogs::asTrimmedString',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  if (stringValue.length > maxLength) {
    throw createAppError({
      caller: 'datalogs::asTrimmedString',
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
      caller: 'datalogs::asOptionalNumber',
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
      caller: 'datalogs::asRequiredNumber',
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
      caller: 'datalogs::asIsoTimestamp',
      reason: `${field} is required.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    throw createAppError({
      caller: 'datalogs::asIsoTimestamp',
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
      caller: 'datalogs::ensureLatLon',
      reason: 'Latitude or longitude is out of range.',
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }
};

const resolveStoredSupportedFields = ({ row, measurementSets = [] }) => {
  const normalized = normalizeSupportedFields({ value: row?.supported_fields_json });
  if (normalized.length) {
    return normalized;
  }

  return inferSupportedFieldsFromMeasurementSets({ measurementSets });
};

const normalizeMeasurementPatch = ({ value, correlationId = null }) => {
  const objectValue = asPlainObject(value);
  if (!objectValue) {
    return null;
  }

  const patch = {};
  for (const [rawKey, rawValue] of Object.entries(objectValue)) {
    const propKey = normalizePropKey({ value: rawKey });
    if (!propKey) {
      continue;
    }

    if (rawValue === '' || rawValue === null || rawValue === undefined) {
      patch[propKey] = null;
      continue;
    }

    const parsed = coerceMeasurementNumericValue({ value: rawValue });
    if (parsed === null) {
      throw createAppError({
        caller: 'datalogs::normalizeMeasurementPatch',
        reason: `${propKey} must be a finite number.`,
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    patch[propKey] = parsed;
  }

  return patch;
};

const collectImplicitMeasurementValues = ({ input }) => {
  const values = {};

  for (const [rawKey, rawValue] of Object.entries(input ?? {})) {
    if (implicitMeasurementReservedKeys.has(rawKey)) {
      continue;
    }

    const propKey = normalizePropKey({ value: rawKey });
    if (!propKey) {
      continue;
    }

    const parsed = coerceMeasurementNumericValue({ value: rawValue });
    if (parsed === null) {
      continue;
    }

    values[propKey] = parsed;
  }

  return values;
};

const buildIngestComponents = ({ input, correlationId = null }) => {
  const components = {};

  for (const field of knownComponentFields) {
    const value = asTrimmedString({
      value: input?.[field.propKey],
      field: field.propKey,
      required: false,
      maxLength: field.maxLength,
      correlationId
    });
    if (value !== null) {
      components[field.propKey] = value;
    }
  }

  const explicitComponents = asPlainObject(input?.components) ?? {};
  for (const [rawKey, rawValue] of Object.entries(explicitComponents)) {
    const propKey = normalizePropKey({ value: rawKey });
    if (!propKey || propKey === 'deviceId' || (rawValue && typeof rawValue === 'object')) {
      continue;
    }

    const knownField = getKnownComponentField({ propKey });
    const value = asTrimmedString({
      value: rawValue,
      field: `components.${propKey}`,
      required: false,
      maxLength: knownField?.maxLength ?? 4000,
      correlationId
    });
    if (value === null) {
      continue;
    }

    components[propKey] = value;
  }

  return components;
};

const splitStoredComponents = ({ components }) => {
  const storedColumns = {};
  const nestedComponents = {};

  for (const [propKey, value] of Object.entries(components ?? {})) {
    if (knownComponentFieldMap.has(propKey)) {
      storedColumns[propKey] = value;
      continue;
    }

    nestedComponents[propKey] = value;
  }

  return {
    storedColumns,
    nestedComponents
  };
};

const buildStoredExtraJson = ({ extra, components }) => {
  const normalizedExtra = asPlainObject(extra) ?? {};
  const { nestedComponents } = splitStoredComponents({ components });
  if (!Object.keys(nestedComponents).length) {
    return normalizedExtra;
  }

  return {
    ...normalizedExtra,
    [componentsExtraJsonKey]: nestedComponents
  };
};

const collectImplicitExtraValues = ({ input }) => {
  const values = {};

  for (const [rawKey, rawValue] of Object.entries(input ?? {})) {
    if (implicitMeasurementReservedKeys.has(rawKey)) {
      continue;
    }

    const propKey = normalizePropKey({ value: rawKey });
    if (!propKey) {
      continue;
    }

    if (rawValue === undefined || rawValue === null || rawValue === '') {
      continue;
    }

    if (typeof rawValue === 'object') {
      continue;
    }

    const numericValue = Number(rawValue);
    if (Number.isFinite(numericValue)) {
      continue;
    }

    values[propKey] = String(rawValue);
  }

  return values;
};

const mapDatalogRow = ({ row, supportedFields }) => ({
  id: row.id,
  datasetId: row.dataset_id,
  ownerUserId: row.owner_user_id,
  sourceType: row.source_type,
  ingestDatalogId: row.ingest_datalog_id,
  datalogName: row.datalog_name,
  deviceIdentifierRaw: row.device_identifier_raw,
  deviceModel: row.device_model,
  deviceSerial: row.device_serial,
  supportedFields,
  metricFields: mergeMetricFields({ supportedFields }),
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

const mapReadingRow = ({ row, measurements }) => ({
  id: row.id,
  rowNumber: row.row_number,
  rawTimestamp: row.raw_timestamp,
  parsedTimeText: row.parsed_time_text,
  occurredAt: row.occurred_at,
  receivedAt: row.received_at,
  latitude: row.latitude,
  longitude: row.longitude,
  altitudeMeters: row.altitude_meters,
  accuracy: row.accuracy,
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
  measurements,
  components: buildStoredComponents({
    source: row,
    extraJson: row.extra_json
  }),
  extra: stripStoredComponentsFromExtra({ extraJson: row.extra_json })
});

const batchInsertReadings = async ({ client, datalogId, rows, actorCreatedAt }) => {
  const chunkSize = 250;

  for (let startIndex = 0; startIndex < rows.length; startIndex += chunkSize) {
    const slice = rows.slice(startIndex, startIndex + chunkSize);
    const readingValues = [];
    const readingPlaceholders = [];
    const numericValues = [];
    const numericPlaceholders = [];

    slice.forEach((row) => {
      const readingId = createOpaqueId();
      const base = readingValues.length;
      readingPlaceholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}, $${base + 14}, $${base + 15}, $${base + 16}, $${base + 17}, $${base + 18}, $${base + 19}, $${base + 20}::jsonb, $${base + 21}::jsonb, $${base + 22})`
      );
      readingValues.push(
        readingId,
        datalogId,
        row.rawTimestamp,
        row.parsedTimeText,
        row.occurredAt,
        row.receivedAt ?? actorCreatedAt,
        row.latitude,
        row.longitude,
        row.altitudeMeters ?? null,
        row.accuracy ?? null,
        row.deviceId ?? null,
        row.deviceName ?? null,
        row.deviceType ?? null,
        row.deviceCalibration ?? null,
        row.firmwareVersion ?? null,
        row.sourceReadingId ?? null,
        row.comment ?? null,
        row.custom ?? row.customText ?? null,
        row.rowNumber,
        JSON.stringify(row.warningFlags ?? []),
        JSON.stringify(row.extraJson ?? row.extra ?? {}),
        actorCreatedAt
      );

      for (const [propKey, numericValue] of Object.entries(row.measurements ?? {})) {
        const normalizedNumericValue = Number(numericValue);
        if (!Number.isFinite(normalizedNumericValue)) {
          continue;
        }

        const valueBase = numericValues.length;
        numericPlaceholders.push(
          `($${valueBase + 1}, $${valueBase + 2}, $${valueBase + 3}, $${valueBase + 4}, $${valueBase + 5}, $${valueBase + 6})`
        );
        numericValues.push(
          createOpaqueId(),
          readingId,
          datalogId,
          propKey,
          normalizedNumericValue,
          actorCreatedAt
        );
      }
    });

    await client.query(
      `INSERT INTO readings (
        id,
        datalog_id,
        raw_timestamp,
        parsed_time_text,
        occurred_at,
        received_at,
        latitude,
        longitude,
        altitude_meters,
        accuracy,
        device_id,
        device_name,
        device_type,
        device_calibration,
        firmware_version,
        source_reading_id,
        comment,
        custom_text,
        row_number,
        warning_flags_json,
        extra_json,
        created_at
      ) VALUES ${readingPlaceholders.join(', ')}`,
      readingValues
    );

    if (numericValues.length) {
      await client.query(
        `INSERT INTO reading_numeric_values (
          id,
          reading_id,
          datalog_id,
          prop_key,
          numeric_value,
          created_at
        ) VALUES ${numericPlaceholders.join(', ')}`,
        numericValues
      );
    }
  }
};

export const createDatalogService = ({ db, audit, datasetService }) => {
  const allocateIngestDatalogId = async ({ client, ownerUserId, correlationId = null }) => {
    for (let attempt = 0; attempt < 12; attempt += 1) {
      const candidate = createIngestDatalogId();
      const existing = await client.query(
        `SELECT 1
         FROM datalogs
         WHERE owner_user_id = $1 AND ingest_datalog_id = $2`,
        [ownerUserId, candidate]
      );
      if (!existing.rows.length) {
        return candidate;
      }
    }

    throw createAppError({
      caller: 'datalogs::allocateIngestDatalogId',
      reason: 'Failed allocating a unique ingest datalog id.',
      errorKey: 'ERR_UNKNOWN',
      correlationId,
      status: 500
    });
  };

  const loadMeasurementMaps = async ({ readingIds, client = db }) => {
    if (!readingIds.length) {
      return new Map();
    }

    const result = await client.query(
      `SELECT reading_id, prop_key, numeric_value
       FROM reading_numeric_values
       WHERE reading_id = ANY($1::text[])
       ORDER BY reading_id, prop_key`,
      [readingIds]
    );

    const maps = new Map(readingIds.map((readingId) => [readingId, {}]));
    for (const row of result.rows) {
      const measurements = maps.get(row.reading_id) ?? {};
      measurements[row.prop_key] = row.numeric_value;
      maps.set(row.reading_id, measurements);
    }

    return maps;
  };

  const replaceReadingMeasurements = async ({ client, readingId, datalogId, measurements, createdAt }) => {
    await client.query(
      `DELETE FROM reading_numeric_values
       WHERE reading_id = $1`,
      [readingId]
    );

    const entries = Object.entries(measurements);
    if (!entries.length) {
      return;
    }

    const values = [];
    const placeholders = [];
    entries.forEach(([propKey, numericValue], index) => {
      const base = index * 6;
      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`
      );
      values.push(
        createOpaqueId(),
        readingId,
        datalogId,
        propKey,
        numericValue,
        createdAt
      );
    });

    await client.query(
      `INSERT INTO reading_numeric_values (
        id,
        reading_id,
        datalog_id,
        prop_key,
        numeric_value,
        created_at
      ) VALUES ${placeholders.join(', ')}`,
      values
    );
  };

  const loadDatalogAccess = async ({ datalogId, user, correlationId = null }) => {
    const datalogResult = await db.query(
      `SELECT
         dlog.*,
         d.name AS dataset_name
       FROM datalogs dlog
       JOIN datasets d ON d.id = dlog.dataset_id
       WHERE dlog.id = $1`,
      [datalogId]
    );
    const datalog = datalogResult.rows[0] ?? null;
    if (!datalog) {
      throw createAppError({
        caller: 'datalogs::loadDatalogAccess',
        reason: 'Datalog was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    let datasetAccess = null;
    try {
      datasetAccess = await datasetService.getDatasetAccess({
        datasetId: datalog.dataset_id,
        user,
        correlationId
      });
    } catch (error) {
      if (error?.status !== 403) {
        throw error;
      }
    }

    const shareResult = await db.query(
      `SELECT access_level
       FROM datalog_shares
       WHERE datalog_id = $1
         AND target_user_id = $2
       LIMIT 1`,
      [datalogId, user.id]
    );
    const shareAccessLevel = shareResult.rows[0]?.access_level ?? null;
    const datalogAccessLevel = (
      datasetAccess?.access_level === 'edit' || shareAccessLevel === 'edit'
    )
      ? 'edit'
      : (
        datasetAccess?.access_level === 'view' || shareAccessLevel === 'view'
          ? 'view'
          : null
      );

    if (!datalogAccessLevel) {
      throw createAppError({
        caller: 'datalogs::loadDatalogAccess',
        reason: 'Datalog is not visible to the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return {
      datalog,
      dataset: {
        id: datalog.dataset_id,
        name: datasetAccess?.name ?? datalog.dataset_name,
        accessLevel: datasetAccess?.access_level ?? null
      },
      datalogAccessLevel
    };
  };

  const requireEditableDatalogAccess = async ({ datalogId, user, correlationId = null }) => {
    const access = await loadDatalogAccess({ datalogId, user, correlationId });
    if (access.datalogAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'datalogs::requireEditableDatalogAccess',
        reason: 'Datalog is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return access;
  };

  const loadArchivedOriginalRawFile = async ({
    datalog,
    correlationId = null,
    caller = 'datalogs::loadArchivedOriginalRawFile'
  }) => {
    if (datalog.source_type !== 'import' || !datalog.raw_file_id) {
      throw createAppError({
        caller,
        reason: 'Only imported datalogs with archived source files are supported.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const rawFileResult = await db.query(
      `SELECT original_filename, blob_data
       FROM raw_files
       WHERE id = $1`,
      [datalog.raw_file_id]
    );
    const rawFile = rawFileResult.rows[0] ?? null;
    if (!rawFile) {
      throw createAppError({
        caller,
        reason: 'The archived source file for this datalog could not be found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    return {
      fileName: rawFile.original_filename,
      buffer: rawFile.blob_data
    };
  };

  const listDatalogKeys = async ({ datalogId }) => {
    const result = await db.query(
      `SELECT *
       FROM datalog_ingest_keys
       WHERE datalog_id = $1
       ORDER BY revoked_at NULLS FIRST, created_at DESC`,
      [datalogId]
    );
    return result.rows.map((row) => mapKeyRow({ row }));
  };

  const refreshDatalogBounds = async ({ client, datalogId }) => {
    await client.query(
      `UPDATE datalogs
       SET started_at = (
             SELECT MIN(occurred_at)
             FROM readings
             WHERE datalog_id = $1
           ),
           ended_at = (
             SELECT MAX(occurred_at)
             FROM readings
             WHERE datalog_id = $1
           )
       WHERE id = $1`,
      [datalogId]
    );
  };

  const loadReadingRow = async ({ datalogId, readingId, client = db, correlationId = null }) => {
    const result = await client.query(
      `SELECT
         id,
         datalog_id,
         raw_timestamp,
         parsed_time_text,
         occurred_at,
         received_at,
         latitude,
         longitude,
         altitude_meters,
         accuracy,
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
         AND datalog_id = $2`,
      [readingId, datalogId]
    );
    const row = result.rows[0] ?? null;

    if (!row) {
      throw createAppError({
        caller: 'datalogs::loadReadingRow',
        reason: 'Datalog reading was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    return row;
  };

  const listDatalogReadings = async ({ datalogId, limit = 100, offset = 0 }) => {
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
           altitude_meters,
           accuracy,
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
         WHERE datalog_id = $1
         ORDER BY row_number ASC, created_at ASC
         LIMIT $2
         OFFSET $3`,
        [datalogId, normalizedLimit, normalizedOffset]
      ),
      db.query(
        `SELECT COUNT(*)::integer AS total_count
         FROM readings
         WHERE datalog_id = $1`,
        [datalogId]
      )
    ]);
    const measurementMaps = await loadMeasurementMaps({
      readingIds: result.rows.map((row) => row.id)
    });

    return {
      offset: normalizedOffset,
      limit: normalizedLimit,
      totalCount: Number(totalCountResult.rows[0]?.total_count ?? 0),
      readings: result.rows.map((row) => mapReadingRow({
        row,
        measurements: measurementMaps.get(row.id) ?? {}
      }))
    };
  };

  const listSupportedFieldWarnings = async ({ datalogId, supportedFields }) => {
    const numericFields = supportedFields.filter((field) => (
      field.valueType !== 'string'
      && field.source !== 'synthetic'
    ));
    if (!numericFields.length) {
      return [];
    }

    const measurementFields = numericFields.filter((field) => field.source !== 'core');
    const coreFields = numericFields.filter((field) => field.source === 'core');
    const counts = new Map();

    if (measurementFields.length) {
      const result = await db.query(
        `SELECT prop_key, COUNT(*)::integer AS value_count
         FROM reading_numeric_values
         WHERE datalog_id = $1
           AND prop_key = ANY($2::text[])
         GROUP BY prop_key`,
        [datalogId, measurementFields.map((field) => field.propKey)]
      );
      for (const row of result.rows) {
        counts.set(row.prop_key, Number(row.value_count ?? 0));
      }
    }

    if (coreFields.length) {
      const coreCountResult = await db.query(
        `SELECT
           COUNT(*) FILTER (WHERE COALESCE(occurred_at, received_at) IS NOT NULL)::integer AS occurred_at_count,
           COUNT(*) FILTER (WHERE latitude IS NOT NULL)::integer AS latitude_count,
           COUNT(*) FILTER (WHERE longitude IS NOT NULL)::integer AS longitude_count,
           COUNT(*) FILTER (WHERE altitude_meters IS NOT NULL)::integer AS altitude_meters_count,
           COUNT(*) FILTER (WHERE accuracy IS NOT NULL)::integer AS accuracy_count
         FROM readings
         WHERE datalog_id = $1`,
        [datalogId]
      );
      const coreCountsRow = coreCountResult.rows[0] ?? {};

      for (const field of coreFields) {
        const coreColumn = getCoreMetricColumn({ propKey: field.propKey });
        const countKey = field.propKey === 'occurredAt'
          ? 'occurred_at_count'
          : `${coreColumn}_count`;
        counts.set(field.propKey, Number(coreCountsRow[countKey] ?? 0));
      }
    }

    return numericFields.flatMap((field) => (
      (counts.get(field.propKey) ?? 0) > 0
        ? []
        : [{
            propKey: field.propKey,
            displayName: field.displayName,
            reason: 'Supported field has no usable values in this datalog yet.'
        }]
    ));
  };

  const listAutodetectSupportedFields = async ({ datalogId, supportedFields }) => {
    const existingPropKeys = new Set(
      normalizeSupportedFields({ value: supportedFields }).map((field) => field.propKey)
    );
    const autodetectedFields = [];

    const appendField = (field) => {
      const normalizedField = normalizeSupportedFields({
        value: [{
          ...field,
          popupDefaultEnabled: false
        }]
      })[0];
      if (!normalizedField || existingPropKeys.has(normalizedField.propKey)) {
        return;
      }

      autodetectedFields.push({
        propKey: normalizedField.propKey,
        displayName: normalizedField.displayName,
        valueType: normalizedField.valueType,
        popupDefaultEnabled: false,
        metricListEnabled: normalizedField.metricListEnabled !== false
      });
      existingPropKeys.add(normalizedField.propKey);
    };

    const [coreCountsResult, measurementFieldResult, genericComponentResult] = await Promise.all([
      db.query(
        `SELECT
           COUNT(*) FILTER (WHERE COALESCE(occurred_at, received_at) IS NOT NULL)::integer AS occurred_at_count,
           COUNT(*) FILTER (WHERE latitude IS NOT NULL)::integer AS latitude_count,
           COUNT(*) FILTER (WHERE longitude IS NOT NULL)::integer AS longitude_count,
           COUNT(*) FILTER (WHERE altitude_meters IS NOT NULL)::integer AS altitude_meters_count,
           COUNT(*) FILTER (WHERE accuracy IS NOT NULL)::integer AS accuracy_count,
           COUNT(*) FILTER (WHERE device_id IS NOT NULL AND btrim(device_id) <> '')::integer AS device_id_count,
           COUNT(*) FILTER (WHERE device_name IS NOT NULL AND btrim(device_name) <> '')::integer AS device_name_count,
           COUNT(*) FILTER (WHERE device_type IS NOT NULL AND btrim(device_type) <> '')::integer AS device_type_count,
           COUNT(*) FILTER (WHERE device_calibration IS NOT NULL AND btrim(device_calibration) <> '')::integer AS device_calibration_count,
           COUNT(*) FILTER (WHERE firmware_version IS NOT NULL AND btrim(firmware_version) <> '')::integer AS firmware_version_count,
           COUNT(*) FILTER (WHERE source_reading_id IS NOT NULL AND btrim(source_reading_id) <> '')::integer AS source_reading_id_count,
           COUNT(*) FILTER (WHERE comment IS NOT NULL AND btrim(comment) <> '')::integer AS comment_count,
           COUNT(*) FILTER (WHERE custom_text IS NOT NULL AND btrim(custom_text) <> '')::integer AS custom_count
         FROM readings
         WHERE datalog_id = $1`,
        [datalogId]
      ),
      db.query(
        `SELECT prop_key
         FROM reading_numeric_values
         WHERE datalog_id = $1
         GROUP BY prop_key
         ORDER BY prop_key`,
        [datalogId]
      ),
      db.query(
        `SELECT component_key AS prop_key
         FROM readings
         CROSS JOIN LATERAL jsonb_each_text(COALESCE(extra_json -> $2::text, '{}'::jsonb)) AS component_map(component_key, component_value)
         WHERE datalog_id = $1
           AND btrim(component_value) <> ''
         GROUP BY component_key
         ORDER BY component_key`,
        [datalogId, componentsExtraJsonKey]
      )
    ]);

    const coreCounts = coreCountsResult.rows[0] ?? {};
    if (Number(coreCounts.occurred_at_count ?? 0) > 0) {
      appendField({ propKey: 'occurredAt', valueType: 'time', metricListEnabled: false });
      appendField({
        propKey: getAggregateTimePropKey({ value: 'occurredAt' }),
        valueType: 'time',
        metricListEnabled: false
      });
    }
    if (Number(coreCounts.latitude_count ?? 0) > 0) {
      appendField({ propKey: 'latitude', valueType: 'number', metricListEnabled: false });
    }
    if (Number(coreCounts.longitude_count ?? 0) > 0) {
      appendField({ propKey: 'longitude', valueType: 'number', metricListEnabled: false });
    }
    if (Number(coreCounts.altitude_meters_count ?? 0) > 0) {
      appendField({ propKey: 'altitudeMeters', valueType: 'number', metricListEnabled: false });
    }
    if (Number(coreCounts.accuracy_count ?? 0) > 0) {
      appendField({ propKey: 'accuracy', valueType: 'number', metricListEnabled: false });
    }
    if (Number(coreCounts.device_id_count ?? 0) > 0) {
      appendField({ propKey: 'deviceId', valueType: 'string' });
    }

    for (const field of knownComponentFields) {
      const countKey = `${field.snakeKey}_count`;
      if (Number(coreCounts[countKey] ?? 0) > 0) {
        appendField({ propKey: field.propKey, valueType: 'string' });
      }
    }

    for (const row of measurementFieldResult.rows) {
      appendField({
        propKey: row.prop_key,
        valueType: 'number',
        metricListEnabled: true
      });
    }

    for (const row of genericComponentResult.rows) {
      appendField({
        propKey: row.prop_key,
        valueType: 'string'
      });
    }

    return autodetectedFields;
  };

  const getDatalogDetail = async ({ datalogId, user, correlationId = null, limit = 100, offset = 0 }) => {
    const { datalog, dataset, datalogAccessLevel } = await loadDatalogAccess({ datalogId, user, correlationId });
    const canManageIngest = datalogAccessLevel === 'edit' && datalog.source_type === 'live';
    const supportedFields = resolveStoredSupportedFields({ row: datalog });
    const [keys, readingsPage, shares, supportedFieldWarnings, autodetectSupportedFields] = await Promise.all([
      canManageIngest ? listDatalogKeys({ datalogId }) : Promise.resolve([]),
      listDatalogReadings({ datalogId, limit, offset }),
      db.query(
        `SELECT ds.*, u.username
         FROM datalog_shares ds
         JOIN users u ON u.id = ds.target_user_id
         WHERE ds.datalog_id = $1
         ORDER BY u.username`,
        [datalogId]
      ).then((result) => result.rows.map((row) => ({
        id: row.id,
        targetUserId: row.target_user_id,
        username: row.username,
        accessLevel: row.access_level,
        createdBy: row.created_by,
        createdAt: row.created_at
      }))),
      listSupportedFieldWarnings({ datalogId, supportedFields }),
      datalogAccessLevel === 'edit'
        ? listAutodetectSupportedFields({ datalogId, supportedFields })
        : Promise.resolve([])
    ]);
    const hasArchivedOriginal = datalog.source_type === 'import' && Boolean(datalog.raw_file_id);

    return {
      ...mapDatalogRow({ row: datalog, supportedFields }),
      accessLevel: datalogAccessLevel,
      dataset: {
        id: dataset.id,
        name: dataset.name,
        accessLevel: dataset.accessLevel
      },
      canManageIngest,
      canRestoreOriginal: datalogAccessLevel === 'edit' && hasArchivedOriginal,
      originalFileDownloadPath: hasArchivedOriginal ? `/api/datalogs/${datalogId}/original-file` : null,
      ingest: datalog.source_type === 'live'
        ? {
            headerName: ingestKeyHeaderName,
            endpointPath: `/api/ingest/datalogs/${datalog.ingest_datalog_id}/points`
          }
        : null,
      keys,
      shares,
      supportedFieldWarnings,
      autodetectSupportedFields,
      readingsPage
    };
  };

  const createLiveDatalog = async ({ datasetId, user, name, supportedFields, correlationId = null }) => {
    const dataset = await datasetService.getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datalogs::createLiveDatalog',
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
    const normalizedSupportedFields = supportedFields === undefined
      ? getDefaultSupportedFields()
      : normalizeSupportedFields({ value: supportedFields });
    const datalogId = createOpaqueId();
    const createdAt = new Date().toISOString();

    await db.withTransaction(async (client) => {
      const ingestDatalogId = await allocateIngestDatalogId({
        client,
        ownerUserId: user.id,
        correlationId
      });

      await client.query(
        `INSERT INTO datalogs (
          id,
          dataset_id,
          raw_file_id,
          owner_user_id,
          device_identifier_raw,
          device_model,
          device_serial,
          datalog_name,
          raw_header_line,
          raw_columns_json,
          header_metadata_json,
          supported_fields_json,
          row_count,
          valid_row_count,
          warning_count,
          error_count,
          skipped_row_count,
          started_at,
          ended_at,
          created_at,
          source_type,
          ingest_datalog_id
        ) VALUES (
          $1, $2, NULL, $3, NULL, NULL, NULL, $4, '', '[]'::jsonb, '{}'::jsonb, $5::jsonb,
          0, 0, 0, 0, 0, NULL, NULL, $6, 'live', $7
        )`,
        [
          datalogId,
          datasetId,
          user.id,
          trimmedName,
          JSON.stringify(normalizedSupportedFields),
          createdAt,
          ingestDatalogId
        ]
      );
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'datalog.created',
      entityType: 'datalog',
      entityId: datalogId,
      payload: {
        datasetId,
        sourceType: 'live',
        datalogName: trimmedName,
        supportedFieldCount: normalizedSupportedFields.length
      }
    });

    return getDatalogDetail({ datalogId, user, correlationId });
  };

  const updateDatalog = async ({ datalogId, user, name, supportedFields, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
    const trimmedName = name === undefined
      ? null
      : asTrimmedString({
          value: name,
          field: 'name',
          required: true,
          maxLength: 255,
          correlationId
        });
    const normalizedSupportedFields = supportedFields === undefined
      ? null
      : normalizeSupportedFields({ value: supportedFields });

    if (trimmedName === null && normalizedSupportedFields === null) {
      return {
        datasetId: datalog.dataset_id,
        updated: false
      };
    }

    await db.query(
      `UPDATE datalogs
       SET datalog_name = COALESCE($2, datalog_name),
           supported_fields_json = COALESCE($3::jsonb, supported_fields_json)
       WHERE id = $1`,
      [
        datalogId,
        trimmedName,
        normalizedSupportedFields ? JSON.stringify(normalizedSupportedFields) : null
      ]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog.updated',
      entityType: 'datalog',
      entityId: datalogId,
      payload: {
        datasetId: datalog.dataset_id,
        oldDatalogName: datalog.datalog_name,
        newDatalogName: trimmedName ?? datalog.datalog_name,
        supportedFieldCount: normalizedSupportedFields?.length ?? null
      }
    });

    return {
      datasetId: datalog.dataset_id,
      updated: true
    };
  };

  const deleteDatalog = async ({ datalogId, user, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });

    await db.query('DELETE FROM datalogs WHERE id = $1', [datalogId]);

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog.deleted',
      entityType: 'datalog',
      entityId: datalogId,
      payload: {
        datasetId: datalog.dataset_id,
        sourceType: datalog.source_type,
        datalogName: datalog.datalog_name
      }
    });

    return {
      datasetId: datalog.dataset_id
    };
  };

  const updateDatalogReading = async ({
    datalogId,
    readingId,
    user,
    occurredAt,
    latitude,
    longitude,
    accuracy,
    altitudeMeters,
    comment,
    measurements,
    correlationId = null
  }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
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
    const normalizedComment = asTrimmedString({
      value: comment,
      field: 'comment',
      required: false,
      maxLength: 4000,
      correlationId
    });
    const measurementPatch = normalizeMeasurementPatch({
      value: measurements,
      correlationId
    }) ?? {};

    ensureLatLon({
      latitude: normalizedLatitude,
      longitude: normalizedLongitude,
      correlationId
    });

    await loadReadingRow({ datalogId, readingId, correlationId });

    await db.withTransaction(async (client) => {
      const existingMeasurements = (await loadMeasurementMaps({
        client,
        readingIds: [readingId]
      })).get(readingId) ?? {};
      const nextMeasurements = { ...existingMeasurements };

      for (const [propKey, numericValue] of Object.entries(measurementPatch)) {
        if (numericValue === null) {
          delete nextMeasurements[propKey];
          continue;
        }

        nextMeasurements[propKey] = numericValue;
      }

      const modifiedAt = new Date().toISOString();

      await client.query(
        `UPDATE readings
         SET occurred_at = $3,
             latitude = $4,
             longitude = $5,
             accuracy = $6,
             altitude_meters = $7,
             comment = $8,
             is_modified = TRUE,
             modified_by_user_id = $9,
             modified_at = $10
         WHERE id = $1
           AND datalog_id = $2`,
        [
          readingId,
          datalogId,
          normalizedOccurredAt,
          normalizedLatitude,
          normalizedLongitude,
          normalizedAccuracy,
          normalizedAltitudeMeters,
          normalizedComment,
          user.id,
          modifiedAt
        ]
      );

      await replaceReadingMeasurements({
        client,
        readingId,
        datalogId,
        measurements: nextMeasurements,
        createdAt: modifiedAt
      });

      await refreshDatalogBounds({ client, datalogId });
    });

    const readingRow = await loadReadingRow({ datalogId, readingId, correlationId });
    const measurementsMap = await loadMeasurementMaps({ readingIds: [readingId] });
    const reading = mapReadingRow({
      row: readingRow,
      measurements: measurementsMap.get(readingId) ?? {}
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'reading.updated',
      entityType: 'reading',
      entityId: readingId,
      payload: {
        datalogId,
        datasetId: datalog.dataset_id
      }
    });

    return {
      datasetId: datalog.dataset_id,
      reading
    };
  };

  const restoreDatalogOriginal = async ({ datalogId, user, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
    const rawFile = await loadArchivedOriginalRawFile({
      datalog,
      correlationId,
      caller: 'datalogs::restoreDatalogOriginal'
    });

    const parsed = parseTrackBuffer({
      buffer: rawFile.buffer,
      fileName: rawFile.fileName,
      correlationId
    });
    const supportedFields = resolveStoredSupportedFields({
      row: {
        supported_fields_json: parsed.supportedFields
      },
      measurementSets: parsed.rows.map((row) => row.measurements ?? {})
    });

    await db.withTransaction(async (client) => {
      await client.query(
        `UPDATE datalogs
         SET device_identifier_raw = $2,
             device_model = $3,
             device_serial = $4,
             datalog_name = $5,
             raw_header_line = $6,
             raw_columns_json = $7::jsonb,
             header_metadata_json = $8::jsonb,
             supported_fields_json = $9::jsonb,
             row_count = $10,
             valid_row_count = $11,
             warning_count = $12,
             error_count = $13,
             skipped_row_count = $14,
             started_at = $15,
             ended_at = $16
         WHERE id = $1`,
        [
          datalogId,
          parsed.device.deviceIdentifierRaw,
          parsed.device.deviceModel,
          parsed.device.deviceSerial,
          parsed.trackName,
          parsed.rawHeaderLine,
          JSON.stringify(parsed.rawColumns),
          JSON.stringify(parsed.headerMetadata),
          JSON.stringify(supportedFields),
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
         WHERE datalog_id = $1`,
        [datalogId]
      );

      await batchInsertReadings({
        client,
        datalogId,
        rows: parsed.rows,
        actorCreatedAt: new Date().toISOString()
      });

      await refreshDatalogBounds({ client, datalogId });
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog.restored_original',
      entityType: 'datalog',
      entityId: datalogId,
      payload: {
        datasetId: datalog.dataset_id,
        rawFileId: datalog.raw_file_id
      }
    });

    return {
      datasetId: datalog.dataset_id
    };
  };

  const getDatalogOriginalFile = async ({ datalogId, user, correlationId = null }) => {
    const { datalog } = await loadDatalogAccess({ datalogId, user, correlationId });
    return loadArchivedOriginalRawFile({
      datalog,
      correlationId,
      caller: 'datalogs::getDatalogOriginalFile'
    });
  };

  const upsertDatalogShare = async ({ datalogId, user, targetUserId, accessLevel, correlationId = null }) => {
    if (!canShare({ user })) {
      throw createAppError({
        caller: 'datalogs::upsertDatalogShare',
        reason: 'This user is not allowed to share datalogs.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const { datalogAccessLevel } = await loadDatalogAccess({ datalogId, user, correlationId });
    if (datalogAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'datalogs::upsertDatalogShare',
        reason: 'Datalog is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    if (!['view', 'edit'].includes(accessLevel)) {
      throw createAppError({
        caller: 'datalogs::upsertDatalogShare',
        reason: 'Share access level must be view or edit.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const shareId = createOpaqueId();
    await db.query(
      `INSERT INTO datalog_shares (id, datalog_id, target_user_id, access_level, created_by, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (datalog_id, target_user_id) DO UPDATE
       SET access_level = EXCLUDED.access_level,
           created_by = EXCLUDED.created_by,
           created_at = EXCLUDED.created_at`,
      [shareId, datalogId, targetUserId, accessLevel, user.id, new Date().toISOString()]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'datalog.share_upserted',
      entityType: 'datalog',
      entityId: datalogId,
      payload: { targetUserId, accessLevel }
    });
  };

  const removeDatalogShare = async ({ datalogId, shareId, user, correlationId = null }) => {
    const { datalogAccessLevel } = await loadDatalogAccess({ datalogId, user, correlationId });
    if (datalogAccessLevel !== 'edit') {
      throw createAppError({
        caller: 'datalogs::removeDatalogShare',
        reason: 'Datalog is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query(
      `DELETE FROM datalog_shares
       WHERE id = $1
         AND datalog_id = $2`,
      [shareId, datalogId]
    );

    await audit.record({
      actorUserId: user.id,
      scopeUserId: user.id,
      eventType: 'datalog.share_removed',
      entityType: 'datalog',
      entityId: datalogId,
      payload: { shareId }
    });
  };

  const createDatalogIngestKeyRecord = async ({
    client,
    datalogId,
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
          `INSERT INTO datalog_ingest_keys (
            id,
            datalog_id,
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
          [keyId, datalogId, keyHash, keyPrefix, trimmedLabel, trimmedNotes, user.id, createdAt]
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
      caller: 'datalogs::createDatalogIngestKeyRecord',
      reason: 'Failed generating a unique ingest key.',
      errorKey: 'ERR_UNKNOWN',
      correlationId,
      status: 500
    });
  };

  const createDatalogIngestKey = async ({ datalogId, user, label, notes, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
    if (datalog.source_type !== 'live') {
      throw createAppError({
        caller: 'datalogs::createDatalogIngestKey',
        reason: 'Only live datalogs can accept ingest keys.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const result = await db.withTransaction(async (client) => createDatalogIngestKeyRecord({
      client,
      datalogId,
      user,
      label,
      notes,
      correlationId
    }));

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog_ingest_key.created',
      entityType: 'datalog_ingest_key',
      entityId: result.key.id,
      payload: {
        datalogId,
        keyPrefix: result.key.keyPrefix,
        label: result.key.label
      }
    });

    return result;
  };

  const revokeDatalogIngestKey = async ({ datalogId, keyId, user, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
    const result = await db.query(
      `UPDATE datalog_ingest_keys
       SET revoked_at = COALESCE(revoked_at, $3),
           revoked_by_user_id = COALESCE(revoked_by_user_id, $4)
       WHERE id = $1 AND datalog_id = $2
       RETURNING *`,
      [keyId, datalogId, new Date().toISOString(), user.id]
    );
    const row = result.rows[0] ?? null;
    if (!row) {
      throw createAppError({
        caller: 'datalogs::revokeDatalogIngestKey',
        reason: 'Datalog ingest key was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog_ingest_key.revoked',
      entityType: 'datalog_ingest_key',
      entityId: row.id,
      payload: {
        datalogId,
        keyPrefix: row.key_prefix,
        label: row.label
      }
    });

    return mapKeyRow({ row });
  };

  const rotateDatalogIngestKey = async ({ datalogId, keyId, user, correlationId = null }) => {
    const { datalog } = await requireEditableDatalogAccess({ datalogId, user, correlationId });
    const result = await db.withTransaction(async (client) => {
      const existingResult = await client.query(
        `SELECT *
         FROM datalog_ingest_keys
         WHERE id = $1 AND datalog_id = $2
         FOR UPDATE`,
        [keyId, datalogId]
      );
      const existing = existingResult.rows[0] ?? null;
      if (!existing) {
        throw createAppError({
          caller: 'datalogs::rotateDatalogIngestKey',
          reason: 'Datalog ingest key was not found.',
          errorKey: 'DATASET_NOT_FOUND',
          correlationId,
          status: 404
        });
      }

      const revokedAt = new Date().toISOString();
      await client.query(
        `UPDATE datalog_ingest_keys
         SET revoked_at = COALESCE(revoked_at, $3),
             revoked_by_user_id = COALESCE(revoked_by_user_id, $4)
         WHERE id = $1 AND datalog_id = $2`,
        [keyId, datalogId, revokedAt, user.id]
      );

      const replacement = await createDatalogIngestKeyRecord({
        client,
        datalogId,
        user,
        label: existing.label,
        notes: existing.notes,
        correlationId
      });

      return {
        replaced: mapKeyRow({
          row: {
            ...existing,
            revoked_at: existing.revoked_at ?? revokedAt,
            revoked_by_user_id: existing.revoked_by_user_id ?? user.id
          }
        }),
        replacement
      };
    });

    await audit.record({
      actorUserId: user.id,
      scopeUserId: datalog.owner_user_id,
      eventType: 'datalog_ingest_key.rotated',
      entityType: 'datalog_ingest_key',
      entityId: result.replacement.key.id,
      payload: {
        datalogId,
        replacedKeyId: keyId,
        replacedKeyPrefix: result.replaced.keyPrefix,
        newKeyPrefix: result.replacement.key.keyPrefix,
        label: result.replacement.key.label
      }
    });

    return result;
  };

  const buildIngestMeasurementValues = ({ input }) => {
    const explicitMeasurements = coerceMeasurementValueMap({
      input: asPlainObject(input.measurements) ?? asPlainObject(input.measurementValues) ?? {}
    });

    return {
      ...collectImplicitMeasurementValues({ input }),
      ...explicitMeasurements
    };
  };

  const loadDuplicateIngestReading = async ({ datalogId, sourceReadingId, correlationId = null }) => {
    const result = await db.query(
      `SELECT
         id,
         datalog_id,
         raw_timestamp,
         parsed_time_text,
         occurred_at,
         received_at,
         latitude,
         longitude,
         altitude_meters,
         accuracy,
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
       WHERE datalog_id = $1
         AND source_reading_id = $2
       LIMIT 1`,
      [datalogId, sourceReadingId]
    );
    const row = result.rows[0] ?? null;
    if (!row) {
      throw createAppError({
        caller: 'datalogs::loadDuplicateIngestReading',
        reason: 'Duplicate reading could not be loaded.',
        errorKey: 'ERR_UNKNOWN',
        correlationId,
        status: 500
      });
    }

    const measurementMaps = await loadMeasurementMaps({ readingIds: [row.id] });
    return mapReadingRow({
      row,
      measurements: measurementMaps.get(row.id) ?? {}
    });
  };

  const ingestPoint = async ({ ingestDatalogId, apiKey, input, correlationId = null }) => {
    const normalizedDatalogId = asTrimmedString({
      value: String(ingestDatalogId ?? '').toLowerCase(),
      field: 'ingestDatalogId',
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
        caller: 'datalogs::ingestPoint',
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

    const payload = {
      accuracy: asOptionalNumber({ value: input.accuracy, field: 'accuracy', correlationId }),
      altitudeMeters: asOptionalNumber({ value: input.altitudeMeters, field: 'altitudeMeters', correlationId }),
      deviceId: asTrimmedString({
        value: input.deviceId ?? asPlainObject(input.components)?.deviceId,
        field: 'deviceId',
        required: false,
        maxLength: 255,
        correlationId
      }),
      components: buildIngestComponents({ input, correlationId }),
      extra: {
        ...collectImplicitExtraValues({ input }),
        ...(asPlainObject(input.extra) ?? {})
      },
      measurements: buildIngestMeasurementValues({ input })
    };
    const storedComponents = splitStoredComponents({ components: payload.components }).storedColumns;
    const storedExtraJson = buildStoredExtraJson({
      extra: payload.extra,
      components: payload.components
    });

    const inferredSupportedFields = inferSupportedFieldsFromMeasurementSets({
      measurementSets: [payload.measurements]
    });
    const receivedAt = new Date().toISOString();

    try {
      return await db.withTransaction(async (client) => {
        const datalogResult = await client.query(
          `SELECT
             dlog.id,
             dlog.dataset_id,
             dlog.owner_user_id,
             dlog.datalog_name,
             dlog.row_count,
             dlog.valid_row_count,
             dlog.started_at,
             dlog.ended_at,
             dlog.supported_fields_json,
             ingest_key.id AS ingest_key_id,
             ingest_key.key_prefix
           FROM datalog_ingest_keys ingest_key
           JOIN datalogs dlog ON dlog.id = ingest_key.datalog_id
          WHERE ingest_key.key_hash = $1
             AND ingest_key.revoked_at IS NULL
             AND dlog.ingest_datalog_id = $2
             AND dlog.source_type = 'live'
           FOR UPDATE OF dlog, ingest_key`,
          [apiKeyHash, normalizedDatalogId]
        );
        const datalog = datalogResult.rows[0] ?? null;
        if (!datalog) {
          throw createAppError({
            caller: 'datalogs::ingestPoint',
            reason: 'Datalog ingest authentication failed.',
            errorKey: 'AUTH_FORBIDDEN',
            correlationId,
            status: 401
          });
        }

        const rowNumber = Number(datalog.row_count ?? 0) + 1;
        const readingId = createOpaqueId();

        await client.query(
          `INSERT INTO readings (
            id,
            datalog_id,
            raw_timestamp,
            parsed_time_text,
            occurred_at,
            latitude,
            longitude,
            altitude_meters,
            accuracy,
            comment,
            row_number,
            warning_flags_json,
            extra_json,
            created_at,
            received_at,
            device_id,
            device_name,
            device_type,
            device_calibration,
            firmware_version,
            source_reading_id,
            custom_text,
            ingest_key_id
          ) VALUES (
            $1, $2, $3, $3, $4, $5, $6, $7, $8, $9, $10, '[]'::jsonb, $11::jsonb, $12, $12,
            $13, $14, $15, $16, $17, $18, $19, $20
          )`,
          [
            readingId,
            datalog.id,
            timestamp.raw,
            timestamp.iso,
            latitude,
            longitude,
            payload.altitudeMeters,
            payload.accuracy,
            storedComponents.comment ?? null,
            rowNumber,
            JSON.stringify(storedExtraJson),
            receivedAt,
            payload.deviceId,
            storedComponents.deviceName ?? null,
            storedComponents.deviceType ?? null,
            storedComponents.deviceCalibration ?? null,
            storedComponents.firmwareVersion ?? null,
            storedComponents.sourceReadingId ?? null,
            storedComponents.custom ?? null,
            datalog.ingest_key_id
          ]
        );

        await replaceReadingMeasurements({
          client,
          readingId,
          datalogId: datalog.id,
          measurements: payload.measurements,
          createdAt: receivedAt
        });

        await client.query(
          `UPDATE datalogs
           SET row_count = row_count + 1,
               valid_row_count = valid_row_count + 1,
               supported_fields_json = CASE
                 WHEN jsonb_array_length(supported_fields_json) = 0 AND $3::jsonb IS NOT NULL THEN $3::jsonb
                 ELSE supported_fields_json
               END,
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
          [
            datalog.id,
            timestamp.iso,
            inferredSupportedFields.length ? JSON.stringify(inferredSupportedFields) : null
          ]
        );

        await client.query(
          `UPDATE datalog_ingest_keys
           SET last_used_at = $2
           WHERE id = $1`,
          [datalog.ingest_key_id, receivedAt]
        );

        return {
          duplicate: false,
          datalogId: datalog.id,
          datasetId: datalog.dataset_id,
          reading: {
            id: readingId,
            occurredAt: timestamp.iso,
            receivedAt,
            latitude,
            longitude,
            altitudeMeters: payload.altitudeMeters,
            accuracy: payload.accuracy,
            deviceId: payload.deviceId,
            deviceName: storedComponents.deviceName ?? null,
            deviceType: storedComponents.deviceType ?? null,
            deviceCalibration: storedComponents.deviceCalibration ?? null,
            firmwareVersion: storedComponents.firmwareVersion ?? null,
            sourceReadingId: storedComponents.sourceReadingId ?? null,
            comment: storedComponents.comment ?? null,
            custom: storedComponents.custom ?? null,
            measurements: payload.measurements,
            components: payload.components,
            extra: payload.extra
          }
        };
      });
    } catch (error) {
      if (error?.code === '23505' && error?.constraint === 'idx_readings_datalog_source_reading_id') {
        const duplicateReading = await loadDuplicateIngestReading({
          datalogId: (await db.query(
            `SELECT id
             FROM datalogs
             WHERE ingest_datalog_id = $1`,
            [normalizedDatalogId]
          )).rows[0]?.id ?? '',
          sourceReadingId: asTrimmedString({
            value: storedComponents.sourceReadingId,
            field: 'sourceReadingId',
            required: true,
            maxLength: 255,
            correlationId
          }),
          correlationId
        });

        return {
          duplicate: true,
          reading: duplicateReading
        };
      }

      throw error;
    }
  };

  return {
    ingestKeyHeaderName,
    getDatalogDetail,
    createLiveDatalog,
    updateDatalog,
    deleteDatalog,
    updateDatalogReading,
    restoreDatalogOriginal,
    getDatalogOriginalFile,
    upsertDatalogShare,
    removeDatalogShare,
    createDatalogIngestKey,
    revokeDatalogIngestKey,
    rotateDatalogIngestKey,
    ingestPoint
  };
};
