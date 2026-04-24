import Busboy from 'busboy';
import { AppError, createAppError } from '../lib/errors.js';
import { logError } from '../lib/logger.js';
import {
  coerceMeasurementValueMap,
  inferSupportedFieldsFromMeasurementSets,
  normalizePropKey,
  normalizeSupportedFields,
  rowHasSupportedFieldValue
} from '../utils/datalog-fields.js';
import { componentsExtraJsonKey, knownComponentFields } from '../utils/datalog-components.js';
import { createOpaqueId, sha256Hex } from '../utils/ids.js';
import {
  computeParsedTrackSemanticChecksum,
  detectImportKind,
  extractSupportedArchiveEntries,
  parseExportEnvelopeBuffer,
  parseTrackBuffer
} from '../utils/track.js';
import { canImport } from './permissions.js';

const knownComponentPropKeySet = new Set(knownComponentFields.map((field) => field.propKey));

const batchInsertReadings = async ({ client, datalogId, rows, actorCreatedAt }) => {
  const chunkSize = 250;

  for (let startIndex = 0; startIndex < rows.length; startIndex += chunkSize) {
    const slice = rows.slice(startIndex, startIndex + chunkSize);
    const readingValues = [];
    const readingPlaceholders = [];
    const numericValuePlaceholders = [];
    const numericValues = [];

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
        row.accuracy,
        row.deviceId ?? null,
        row.deviceName ?? null,
        row.deviceType ?? null,
        row.deviceCalibration ?? null,
        row.firmwareVersion ?? null,
        row.sourceReadingId ?? null,
        row.comment,
        row.custom ?? row.customText ?? null,
        row.rowNumber,
        JSON.stringify(row.warningFlags ?? []),
        JSON.stringify(row.extraJson ?? row.extra ?? {}),
        actorCreatedAt
      );

      for (const [propKey, numericValue] of Object.entries(row.measurements ?? row.measurementValues ?? {})) {
        const normalizedNumericValue = Number(numericValue);
        if (!Number.isFinite(normalizedNumericValue)) {
          continue;
        }

        const numericBase = numericValues.length;
        numericValuePlaceholders.push(
          `($${numericBase + 1}, $${numericBase + 2}, $${numericBase + 3}, $${numericBase + 4}, $${numericBase + 5}, $${numericBase + 6})`
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
        ) VALUES ${numericValuePlaceholders.join(', ')}`,
        numericValues
      );
    }
  }
};

const parseMultipartUpload = ({ req, logger, correlationId = null }) => new Promise((resolve, reject) => {
  let busboy;
  const fileSizeLimitBytes = 256 * 1024 * 1024;

  try {
    busboy = Busboy({
      headers: req.headers,
      limits: {
        files: 50,
        fileSize: fileSizeLimitBytes
      }
    });
  } catch (cause) {
    reject(logError({
      logger,
      caller: 'imports::parseMultipartUpload',
      reason: 'Failed initializing multipart parser.',
      errorKey: 'IMPORT_REQUEST_MULTIPART_INIT_FAILED',
      correlationId,
      context: {
        contentType: req.headers['content-type'] ?? null
      },
      cause,
      status: 400
    }));
    return;
  }

  const files = [];
  const fields = {};

  busboy.on('file', (fieldName, file, info) => {
    const chunks = [];

    file.on('data', (chunk) => chunks.push(chunk));
    file.on('limit', () => reject(logError({
      logger,
      caller: 'imports::parseMultipartUpload',
      reason: 'Upload file exceeded the configured size limit.',
      errorKey: 'IMPORT_REQUEST_FILE_TOO_LARGE',
      correlationId,
      context: {
        fieldName,
        fileName: info.filename,
        sizeLimitBytes: fileSizeLimitBytes
      },
      status: 413
    })));
    file.on('end', () => {
      files.push({
        fieldName,
        fileName: info.filename,
        mimeType: info.mimeType,
        buffer: Buffer.concat(chunks)
      });
    });
  });

  busboy.on('field', (fieldName, value) => {
    fields[fieldName] = value;
  });
  busboy.on('error', (cause) => reject(logError({
    logger,
    caller: 'imports::parseMultipartUpload',
    reason: 'Multipart stream parsing failed.',
    errorKey: 'IMPORT_REQUEST_MULTIPART_STREAM_FAILED',
    correlationId,
    context: {
      contentType: req.headers['content-type'] ?? null
    },
    cause,
    status: 400
  })));
  busboy.on('finish', () => resolve({ files, fields }));
  req.pipe(busboy);
});

const defaultDatasetName = ({ files }) => {
  if (files.length === 1) {
    return files[0].fileName;
  }

  return `Import ${new Date().toISOString()}`;
};

const rawFileSummary = ({ rawFileId, originalFilename, checksum, sizeBytes, parseStatus, summary, duplicateOfRawFileId = null }) => ({
  rawFileId,
  originalFilename,
  checksum,
  sizeBytes,
  parseStatus,
  duplicateOfRawFileId,
  summary
});

const parseBooleanField = ({ value, defaultValue = false }) => {
  if (value === undefined || value === null || value === '') {
    return defaultValue;
  }

  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(normalized)) {
    return false;
  }

  return defaultValue;
};

const isSplitArchiveFileName = ({ fileName }) => {
  const normalized = String(fileName ?? '').trim().toLowerCase();
  return normalized.endsWith('.zrctrk') || normalized.endsWith('.zip');
};

const toDatasetNameFromFileName = ({ fileName }) => {
  const normalized = String(fileName ?? '').split(/[\\/]/).pop()?.trim() ?? '';
  if (!normalized) {
    return 'Imported Track';
  }

  return normalized.replace(/\.[^.]+$/, '') || normalized;
};

const asObject = (value) => (
  value && typeof value === 'object' && !Array.isArray(value)
    ? value
    : null
);

const asStringOrNull = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  return typeof value === 'string' ? value : String(value);
};

const deriveDeviceIdentity = ({ deviceIdentifierRaw, deviceModel, deviceSerial }) => {
  const raw = asStringOrNull(deviceIdentifierRaw);
  const model = asStringOrNull(deviceModel);
  const serial = asStringOrNull(deviceSerial);

  if (!raw || model || serial) {
    return {
      deviceIdentifierRaw: raw,
      deviceModel: model,
      deviceSerial: serial
    };
  }

  const match = raw.trim().match(/^(.*)-([^-]+)$/);
  return {
    deviceIdentifierRaw: raw,
    deviceModel: match ? match[1] : null,
    deviceSerial: match ? match[2] : null
  };
};

const asFiniteNumberOrNull = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const asIsoTimestampOrNull = (value) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
};

const normalizeBackupPoint = ({ point, index, correlationId = null }) => {
  const normalizedPoint = asObject(point);
  if (!normalizedPoint) {
    throw createAppError({
      caller: 'imports::normalizeBackupPoint',
      reason: 'Backup export contains an invalid raw point entry.',
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  const componentValues = {};
  const rawComponents = asObject(normalizedPoint.components) ?? {};
  for (const [rawKey, rawValue] of Object.entries(rawComponents)) {
    const propKey = normalizePropKey({ value: rawKey });
    const value = asStringOrNull(rawValue);
    if (!propKey || !value || propKey === 'deviceId') {
      continue;
    }

    componentValues[propKey] = value;
  }

  const genericComponents = Object.fromEntries(
    Object.entries(componentValues).filter(([propKey]) => !knownComponentPropKeySet.has(propKey))
  );
  const extraJson = {
    ...(asObject(normalizedPoint.extraJson) ?? asObject(normalizedPoint.extra) ?? {}),
    ...(Object.keys(genericComponents).length
      ? { [componentsExtraJsonKey]: genericComponents }
      : {})
  };

  return {
    rawTimestamp: asStringOrNull(normalizedPoint.rawTimestamp),
    parsedTimeText: asStringOrNull(normalizedPoint.parsedTimeText),
    occurredAt: asIsoTimestampOrNull(normalizedPoint.occurredAt),
    receivedAt: asIsoTimestampOrNull(normalizedPoint.receivedAt),
    latitude: asFiniteNumberOrNull(normalizedPoint.latitude),
    longitude: asFiniteNumberOrNull(normalizedPoint.longitude),
    accuracy: asFiniteNumberOrNull(normalizedPoint.accuracy),
    altitudeMeters: asFiniteNumberOrNull(normalizedPoint.altitudeMeters),
    measurements: coerceMeasurementValueMap({
      input: asObject(normalizedPoint.measurements)
        ?? asObject(normalizedPoint.measurementValues)
        ?? {}
    }),
    deviceId: asStringOrNull(normalizedPoint.deviceId),
    deviceName: componentValues.deviceName ?? asStringOrNull(normalizedPoint.deviceName),
    deviceType: componentValues.deviceType ?? asStringOrNull(normalizedPoint.deviceType),
    deviceCalibration: componentValues.deviceCalibration ?? asStringOrNull(normalizedPoint.deviceCalibration),
    firmwareVersion: componentValues.firmwareVersion ?? asStringOrNull(normalizedPoint.firmwareVersion),
    sourceReadingId: componentValues.sourceReadingId ?? asStringOrNull(normalizedPoint.sourceReadingId),
    comment: componentValues.comment ?? asStringOrNull(normalizedPoint.comment) ?? '',
    custom: componentValues.custom ?? asStringOrNull(normalizedPoint.custom),
    rowNumber: Number.isInteger(normalizedPoint.rowNumber) ? normalizedPoint.rowNumber : index + 1,
    warningFlags: Array.isArray(normalizedPoint.warningFlags) ? normalizedPoint.warningFlags.map(String) : [],
    extraJson
  };
};

const deriveBackupTimeRange = ({ rows }) => {
  const timestamps = rows
    .map((row) => row.occurredAt)
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right));

  return {
    startedAt: timestamps[0] ?? null,
    endedAt: timestamps.at(-1) ?? null
  };
};

const buildBackupParsedShape = ({ trackMeta, rows }) => ({
  device: deriveDeviceIdentity({
    deviceIdentifierRaw: trackMeta?.deviceIdentifierRaw,
    deviceModel: trackMeta?.deviceModel,
    deviceSerial: trackMeta?.deviceSerial
  }),
  supportedFields: normalizeSupportedFields({
    value: trackMeta?.supportedFields
  }),
  rows: rows.map((row) => ({
    occurredAt: row.occurredAt ?? null,
    rawTimestamp: row.rawTimestamp ?? null,
    parsedTimeText: row.parsedTimeText ?? null,
    latitude: row.latitude ?? null,
    longitude: row.longitude ?? null,
    altitudeMeters: row.altitudeMeters ?? null,
    accuracy: row.accuracy ?? null,
    deviceId: row.deviceId ?? null,
    components: Object.fromEntries(
      Object.entries({
        deviceName: row.deviceName ?? null,
        deviceType: row.deviceType ?? null,
        deviceCalibration: row.deviceCalibration ?? null,
        firmwareVersion: row.firmwareVersion ?? null,
        sourceReadingId: row.sourceReadingId ?? null,
        comment: row.comment ?? '',
        custom: row.custom ?? null
      }).filter(([, value]) => value !== null && value !== '')
    ),
    deviceName: row.deviceName ?? null,
    deviceType: row.deviceType ?? null,
    deviceCalibration: row.deviceCalibration ?? null,
    firmwareVersion: row.firmwareVersion ?? null,
    sourceReadingId: row.sourceReadingId ?? null,
    custom: row.custom ?? null,
    measurements: row.measurements ?? {},
    comment: row.comment ?? '',
    extraJson: row.extraJson ?? {}
  }))
});

const deriveBackupWarningBreakdown = ({ rows }) => {
  const counts = new Map();

  for (const row of rows) {
    for (const warningFlag of row.warningFlags ?? []) {
      counts.set(warningFlag, (counts.get(warningFlag) ?? 0) + 1);
    }
  }

  return Array.from(counts.entries()).map(([type, count]) => ({
    type,
    count,
    reason: type
  }));
};

const resolveSupportedFieldsForRows = ({ requestedSupportedFields, rows }) => {
  const normalizedRequested = normalizeSupportedFields({ value: requestedSupportedFields });
  const inferredSupportedFields = inferSupportedFieldsFromMeasurementSets({
    measurementSets: rows.map((row) => row.measurements ?? {})
  });
  const supportedFields = normalizedRequested.length
    ? normalizedRequested
    : inferredSupportedFields;
  const missingSupportedFields = supportedFields.filter((field) => (
    field.valueType !== 'string'
    && field.source !== 'synthetic'
    && !rows.some((row) => rowHasSupportedFieldValue({ field, row }))
  ));

  return {
    supportedFields,
    missingSupportedFields
  };
};

const toBackupChildName = ({ trackMeta, trackId, fileName }) => (
  asStringOrNull(trackMeta?.datalogName ?? trackMeta?.trackName)
  ?? asStringOrNull(trackId)
  ?? toDatasetNameFromFileName({ fileName })
);

const summarizeFileResults = ({ datasetId, datasetName, fileResults }) => {
  const flattenedChildren = fileResults.flatMap((fileResult) => fileResult.children ?? []);
  const fileLevelFailureCount = fileResults.filter((fileResult) => Boolean(fileResult.error)).length;

  return {
    datasetId,
    datasetName,
    fileCount: fileResults.length,
    parsedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'parsed').length,
    duplicateFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'duplicate_skipped').length,
    failedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'failed').length + fileLevelFailureCount,
    warningCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.warningCount ?? 0), 0),
    errorCount: flattenedChildren.reduce((total, entry) => (
      total + (entry.summary?.errorCount ?? (entry.parseStatus === 'failed' ? 1 : 0))
    ), fileLevelFailureCount),
    skippedRowCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.skippedRowCount ?? 0), 0),
    files: fileResults
  };
};

const resolveImportStatus = ({ summary }) => (
  summary.failedFileCount > 0
    ? (summary.parsedFileCount > 0 ? 'completed_with_errors' : 'failed')
    : (summary.warningCount > 0 || summary.duplicateFileCount > 0 ? 'completed_with_warnings' : 'completed')
);

const buildDatasetImportResult = ({ datasetId, datasetName, fileResults }) => {
  const summary = summarizeFileResults({
    datasetId,
    datasetName,
    fileResults
  });

  return {
    datasetId,
    datasetName,
    status: resolveImportStatus({ summary }),
    summary
  };
};

const buildUploadBatchSummary = ({
  batchFileResults,
  datasetResults,
  splitBulkArchivesIntoDatasets,
  advancedTrackDeduplication
}) => {
  const flattenedChildren = batchFileResults.flatMap((fileResult) => fileResult.children ?? []);
  const fileLevelFailureCount = batchFileResults.filter((fileResult) => Boolean(fileResult.error)).length;

  return {
    datasetId: datasetResults.length === 1 ? datasetResults[0].datasetId : null,
    datasetName: datasetResults.length === 1 ? datasetResults[0].datasetName : null,
    datasetIds: datasetResults.map((datasetResult) => datasetResult.datasetId),
    datasetCount: datasetResults.length,
    fileCount: batchFileResults.length,
    parsedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'parsed').length,
    duplicateFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'duplicate_skipped').length,
    failedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'failed').length + fileLevelFailureCount,
    warningCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.warningCount ?? 0), 0),
    errorCount: flattenedChildren.reduce((total, entry) => (
      total + (entry.summary?.errorCount ?? (entry.parseStatus === 'failed' ? 1 : 0))
    ), fileLevelFailureCount),
    skippedRowCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.skippedRowCount ?? 0), 0),
    splitBulkArchivesIntoDatasets,
    advancedTrackDeduplication,
    files: batchFileResults,
    datasets: datasetResults
  };
};

export const createImportService = ({ db, audit, logger }) => {
  const toLoggedAppError = ({
    caller,
    reason,
    errorKey,
    correlationId = null,
    context = null,
    cause = null,
    status = 500,
    preserveExisting = true
  }) => {
    if (cause instanceof AppError && preserveExisting) {
      return cause;
    }

    if (cause instanceof AppError) {
      const wrapped = new AppError({
        caller,
        reason,
        errorKey,
        correlationId,
        context,
        cause,
        status
      });

      logger.error({
        caller,
        message: reason,
        correlationId,
        errorKey: wrapped.errorKey,
        errorCode: wrapped.errorCode,
        context,
        rootCause: cause
      });

      return wrapped;
    }

    return logError({
      logger,
      caller,
      reason,
      errorKey,
      correlationId,
      context,
      cause,
      status
    });
  };

  const ensureUserDevice = async ({ client, userId, device, correlationId = null }) => {
    if (!device.deviceIdentifierRaw) {
      return;
    }

    await client.query(
      `INSERT INTO user_devices (
        id,
        user_id,
        device_identifier_raw,
        device_model,
        device_serial,
        nickname,
        preferred_display_unit,
        calibration_json,
        metadata_json,
        created_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, NULL, NULL, NULL, '{}'::jsonb, $6, $6)
      ON CONFLICT (user_id, device_identifier_raw) DO UPDATE
      SET device_model = EXCLUDED.device_model,
          device_serial = EXCLUDED.device_serial,
          updated_at = EXCLUDED.updated_at`,
      [
        createOpaqueId(),
        userId,
        device.deviceIdentifierRaw,
        device.deviceModel,
        device.deviceSerial,
        new Date().toISOString()
      ]
    );
  };

  const createDataset = async ({ client, ownerUserId, datasetName, description = null }) => {
    const datasetId = createOpaqueId();
    const now = new Date().toISOString();

    await client.query(
      `INSERT INTO datasets (id, owner_user_id, name, description, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $5)`,
      [datasetId, ownerUserId, datasetName, description, now]
    );

    return datasetId;
  };

  const findDuplicateRawFile = async ({ client, ownerUserId, checksum }) => {
    const result = await client.query(
      `SELECT raw_files.id
       FROM raw_files
       INNER JOIN upload_batches ON upload_batches.id = raw_files.upload_batch_id
       WHERE upload_batches.uploader_user_id = $1
         AND raw_files.checksum = $2
       ORDER BY raw_files.created_at ASC
       LIMIT 1`,
      [ownerUserId, checksum]
    );
    return result.rows[0]?.id ?? null;
  };

  const findDuplicateTrackBySemanticKey = async ({ client, ownerUserId, semanticDedupeKey }) => {
    const result = await client.query(
      `SELECT id, raw_file_id
       FROM datalogs
       WHERE owner_user_id = $1
         AND semantic_dedupe_key = $2
       ORDER BY created_at ASC
       LIMIT 1`,
      [ownerUserId, semanticDedupeKey]
    );
    return result.rows[0] ?? null;
  };

  const persistRawFile = async ({
    client,
    batchId,
    parentRawFileId = null,
    kind,
    originalFilename,
    checksum,
    sizeBytes,
    buffer,
    sourceType,
    provenance,
    parseStatus = 'pending',
    parseSummary = {}
  }) => {
    const rawFileId = createOpaqueId();
    await client.query(
      `INSERT INTO raw_files (
        id,
        upload_batch_id,
        parent_raw_file_id,
        kind,
        original_filename,
        checksum,
        size_bytes,
        blob_data,
        source_type,
        provenance_json,
        parse_status,
        parse_summary_json,
        created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12::jsonb, $13)`,
      [
        rawFileId,
        batchId,
        parentRawFileId,
        kind,
        originalFilename,
        checksum,
        sizeBytes,
        buffer,
        sourceType,
        JSON.stringify(provenance),
        parseStatus,
        JSON.stringify(parseSummary),
        new Date().toISOString()
      ]
    );
    return rawFileId;
  };

  const createTrackFromParsed = async ({
    client,
    datasetId,
    rawFileId,
    ownerUserId,
    parsed,
    semanticDedupeKey
  }) => {
    const trackId = createOpaqueId();
    const now = new Date().toISOString();
    const { supportedFields } = resolveSupportedFieldsForRows({
      requestedSupportedFields: parsed.supportedFields,
      rows: parsed.rows
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
        semantic_dedupe_key,
        row_count,
        valid_row_count,
        warning_count,
        error_count,
        skipped_row_count,
        started_at,
        ended_at,
        created_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb, $13,
        $14, $15, $16, $17, $18, $19, $20, $21
      )`,
      [
        trackId,
        datasetId,
        rawFileId,
        ownerUserId,
        parsed.device.deviceIdentifierRaw,
        parsed.device.deviceModel,
        parsed.device.deviceSerial,
        parsed.trackName,
        parsed.rawHeaderLine,
        JSON.stringify(parsed.rawColumns),
        JSON.stringify(parsed.headerMetadata),
        JSON.stringify(supportedFields),
        semanticDedupeKey,
        parsed.rowCount,
        parsed.validRowCount,
        parsed.warningCount,
        parsed.errorCount,
        parsed.skippedRowCount,
        parsed.startedAt,
        parsed.endedAt,
        now
      ]
    );

    await batchInsertReadings({
      client,
      datalogId: trackId,
      rows: parsed.rows,
      actorCreatedAt: now
    });

    return trackId;
  };

  const createTrackFromBackup = async ({
    client,
    datasetId,
    ownerUserId,
    trackMeta,
    rows,
    semanticDedupeKey
  }) => {
    const trackId = createOpaqueId();
    const now = new Date().toISOString();
    const derivedTimeRange = deriveBackupTimeRange({ rows });
    const { supportedFields, missingSupportedFields } = resolveSupportedFieldsForRows({
      requestedSupportedFields: trackMeta?.supportedFields,
      rows
    });
    const warningBreakdown = [
      ...deriveBackupWarningBreakdown({ rows }),
      ...missingSupportedFields.map((field) => ({
        type: 'supported_field_missing',
        count: 1,
        reason: `Supported field "${field.displayName}" never appeared with a usable value.`
      }))
    ];
    const derivedWarningCount = warningBreakdown.reduce((total, entry) => total + entry.count, 0);
    const device = deriveDeviceIdentity({
      deviceIdentifierRaw: trackMeta?.deviceIdentifierRaw,
      deviceModel: trackMeta?.deviceModel,
      deviceSerial: trackMeta?.deviceSerial
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
        semantic_dedupe_key,
        row_count,
        valid_row_count,
        warning_count,
        error_count,
        skipped_row_count,
        started_at,
        ended_at,
        created_at,
        source_type
      ) VALUES (
        $1, $2, NULL, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12,
        $13, $14, $15, $16, $17, $18, $19, $20, 'import'
      )`,
      [
        trackId,
        datasetId,
        ownerUserId,
        device.deviceIdentifierRaw,
        device.deviceModel,
        device.deviceSerial,
        asStringOrNull(trackMeta?.datalogName ?? trackMeta?.trackName) ?? 'Imported Backup Datalog',
        asStringOrNull(trackMeta?.rawHeaderLine) ?? '',
        JSON.stringify(Array.isArray(trackMeta?.rawColumns) ? trackMeta.rawColumns : []),
        JSON.stringify(asObject(trackMeta?.headerMetadata) ?? {}),
        JSON.stringify(supportedFields),
        semanticDedupeKey,
        Number.isInteger(trackMeta?.rowCount) ? trackMeta.rowCount : rows.length,
        Number.isInteger(trackMeta?.validRowCount) ? trackMeta.validRowCount : rows.length,
        Number.isInteger(trackMeta?.warningCount) ? trackMeta.warningCount : derivedWarningCount,
        Number.isInteger(trackMeta?.errorCount) ? trackMeta.errorCount : 0,
        Number.isInteger(trackMeta?.skippedRowCount) ? trackMeta.skippedRowCount : 0,
        asIsoTimestampOrNull(trackMeta?.startedAt) ?? derivedTimeRange.startedAt,
        asIsoTimestampOrNull(trackMeta?.endedAt) ?? derivedTimeRange.endedAt,
        asIsoTimestampOrNull(trackMeta?.createdAt) ?? now
      ]
    );

    await batchInsertReadings({
      client,
      datalogId: trackId,
      rows,
      actorCreatedAt: now
    });

    return {
      trackId,
      warningBreakdown,
      warningCount: Number.isInteger(trackMeta?.warningCount) ? trackMeta.warningCount : derivedWarningCount,
      rowCount: Number.isInteger(trackMeta?.rowCount) ? trackMeta.rowCount : rows.length,
      validRowCount: Number.isInteger(trackMeta?.validRowCount) ? trackMeta.validRowCount : rows.length,
      errorCount: Number.isInteger(trackMeta?.errorCount) ? trackMeta.errorCount : 0,
      skippedRowCount: Number.isInteger(trackMeta?.skippedRowCount) ? trackMeta.skippedRowCount : 0
    };
  };

  const processBackupTrack = async ({
    client,
    datasetId,
    ownerUserId,
    fileName,
    originalTrackId,
    trackMeta,
    points,
    advancedTrackDeduplication = true,
    correlationId = null
  }) => {
    const childPayload = JSON.stringify({
      originalTrackId: asStringOrNull(originalTrackId),
      trackMeta: asObject(trackMeta) ?? {},
      points
    });
    const checksum = sha256Hex({ value: childPayload });
    const sizeBytes = Buffer.byteLength(childPayload);
    const originalFilename = toBackupChildName({ trackMeta, trackId: originalTrackId, fileName });

    try {
      const rows = points
        .map((point, index) => normalizeBackupPoint({ point, index, correlationId }))
        .sort((left, right) => (
          (left.occurredAt ?? '9999-12-31T23:59:59.999Z').localeCompare(
            right.occurredAt ?? '9999-12-31T23:59:59.999Z'
          )
          || left.rowNumber - right.rowNumber
        ));
      const parsedShape = buildBackupParsedShape({ trackMeta, rows });
      const semanticDedupeKey = computeParsedTrackSemanticChecksum({ parsed: parsedShape });

      if (advancedTrackDeduplication) {
        const duplicateTrack = await findDuplicateTrackBySemanticKey({
          client,
          ownerUserId,
          semanticDedupeKey
        });

        if (duplicateTrack) {
          const duplicateSummary = {
            duplicateOfRawFileId: duplicateTrack.raw_file_id ?? null,
            duplicateOfDatalogId: duplicateTrack.id,
            dedupeMethod: 'semantic_track',
            skippedParsing: true,
            warningBreakdown: [{
              type: 'duplicate_track',
              count: 1,
              reason: 'Datalog matched an earlier imported datalog after normalizing device and readings.'
            }]
          };

          return rawFileSummary({
            rawFileId: null,
            originalFilename,
            checksum,
            sizeBytes,
            parseStatus: 'duplicate_skipped',
            duplicateOfRawFileId: duplicateTrack.raw_file_id ?? null,
            summary: duplicateSummary
          });
        }
      }

      await ensureUserDevice({
        client,
        userId: ownerUserId,
        device: parsedShape.device,
        correlationId
      });

      const createdTrack = await createTrackFromBackup({
        client,
        datasetId,
        ownerUserId,
        trackMeta,
        rows,
        semanticDedupeKey
      });

      return rawFileSummary({
        rawFileId: null,
        originalFilename,
        checksum,
        sizeBytes,
        parseStatus: 'parsed',
        summary: {
          datalogId: createdTrack.trackId,
          sourceDatalogId: asStringOrNull(originalTrackId),
          rowCount: createdTrack.rowCount,
          validRowCount: createdTrack.validRowCount,
          warningCount: createdTrack.warningCount,
          warningBreakdown: createdTrack.warningBreakdown,
          errorCount: createdTrack.errorCount,
          skippedRowCount: createdTrack.skippedRowCount
        }
      });
    } catch (cause) {
      const appError = toLoggedAppError({
        caller: 'imports::processBackupTrack',
        reason: 'Backup track processing failed.',
        errorKey: 'IMPORT_TRACK_PROCESS_FAILED',
        correlationId,
        context: {
          datasetId,
          ownerUserId,
          fileName,
          originalTrackId,
          pointCount: points.length
        },
        cause,
        preserveExisting: false
      });

      return rawFileSummary({
        rawFileId: null,
        originalFilename,
        checksum,
        sizeBytes,
        parseStatus: 'failed',
        summary: {
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        }
      });
    }
  };

  const processBackupExportAsDatasets = async ({
    client,
    batchId,
    ownerUserId,
    requestedDatasetName,
    description,
    file,
    advancedTrackDeduplication = true,
    correlationId = null
  }) => {
    const rootChecksum = sha256Hex({ value: file.buffer });
    const duplicateOfRawFileId = await findDuplicateRawFile({
      client,
      ownerUserId,
      checksum: rootChecksum
    });
    const duplicateSummary = duplicateOfRawFileId
      ? {
          duplicateOfRawFileId,
          dedupeMethod: 'raw_checksum',
          skippedParsing: true,
          warningBreakdown: [{
            type: 'duplicate_file',
            count: 1,
            reason: 'File matched an earlier raw artifact and was skipped.'
          }]
        }
      : {};
    const rootRawFileId = await persistRawFile({
      client,
      batchId,
      kind: 'backup_json',
      originalFilename: file.fileName,
      checksum: rootChecksum,
      sizeBytes: file.buffer.length,
      buffer: file.buffer,
      sourceType: 'backup_json',
      provenance: {
        uploadFieldName: file.fieldName,
        mimeType: file.mimeType
      },
      parseStatus: duplicateOfRawFileId ? 'duplicate_skipped' : 'pending',
      parseSummary: duplicateSummary
    });

    if (duplicateOfRawFileId) {
      const duplicateChildSummary = rawFileSummary({
        rawFileId: rootRawFileId,
        originalFilename: file.fileName,
        checksum: rootChecksum,
        sizeBytes: file.buffer.length,
        parseStatus: 'duplicate_skipped',
        duplicateOfRawFileId,
        summary: duplicateSummary
      });

      return {
        batchFileResult: {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind: 'backup_json',
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: [duplicateChildSummary],
          duplicateOfRawFileId
        },
        datasetResults: []
      };
    }

    try {
      const envelope = parseExportEnvelopeBuffer({
        buffer: file.buffer,
        fileName: file.fileName,
        correlationId
      });
      const rawPayload = asObject(envelope.raw);
      const rawPoints = Array.isArray(rawPayload?.points) ? rawPayload.points : null;

      if (!rawPayload || !rawPoints) {
        throw createAppError({
          caller: 'imports::processBackupExportAsDatasets',
          reason: 'Backup import requires raw points in the exported JSON.',
          errorKey: 'REQUEST_INVALID',
          correlationId,
          status: 400
        });
      }

      if (!rawPoints.length) {
        throw createAppError({
          caller: 'imports::processBackupExportAsDatasets',
          reason: 'Backup export did not contain any raw points to import.',
          errorKey: 'REQUEST_INVALID',
          correlationId,
          status: 400
        });
      }

      if (
        Boolean(rawPayload.capped)
        || (
          Number.isFinite(Number(rawPayload.totalCount))
          && Number(rawPayload.totalCount) > rawPoints.length
        )
      ) {
        throw createAppError({
          caller: 'imports::processBackupExportAsDatasets',
          reason: 'Backup export is incomplete because raw points were capped during export.',
          errorKey: 'REQUEST_INVALID',
          correlationId,
          status: 400
        });
      }

      const pointRecords = rawPoints.map((point, index) => {
        const record = asObject(point);
        if (!record) {
          throw createAppError({
            caller: 'imports::processBackupExportAsDatasets',
            reason: `Backup export point ${index + 1} is not a valid object.`,
            errorKey: 'REQUEST_INVALID',
            correlationId,
            status: 400
          });
        }

        return record;
      });
      const exportedDatasets = Array.isArray(envelope.datasets)
        ? envelope.datasets.map(asObject).filter(Boolean)
        : [];
      const exportedTracks = Array.isArray(envelope.datalogs)
        ? envelope.datalogs.map(asObject).filter(Boolean)
        : [];
      const datasetMetadataById = new Map(
        exportedDatasets
          .map((entry) => [asStringOrNull(entry.id), entry])
          .filter(([datasetId]) => datasetId)
      );
      const trackMetadataById = new Map(
        exportedTracks
          .map((entry) => [asStringOrNull(entry.id), entry])
          .filter(([trackId]) => trackId)
      );
      const datasetOrder = [];
      const seenDatasetIds = new Set();

      for (const entry of exportedDatasets) {
        const datasetId = asStringOrNull(entry.id);
        if (datasetId && !seenDatasetIds.has(datasetId)) {
          seenDatasetIds.add(datasetId);
          datasetOrder.push(datasetId);
        }
      }

      for (const point of pointRecords) {
        const datasetId = asStringOrNull(point.datasetId) ?? 'backup-default-dataset';
        if (!seenDatasetIds.has(datasetId)) {
          seenDatasetIds.add(datasetId);
          datasetOrder.push(datasetId);
        }
      }

      const childSummaries = [];
      const datasetResults = [];
      const fallbackBaseName = toDatasetNameFromFileName({ fileName: file.fileName });

      for (const [datasetIndex, sourceDatasetId] of datasetOrder.entries()) {
        const datasetPoints = pointRecords.filter((point) => (
          (asStringOrNull(point.datasetId) ?? 'backup-default-dataset') === sourceDatasetId
        ));
        if (!datasetPoints.length) {
          continue;
        }

        const datasetMeta = datasetMetadataById.get(sourceDatasetId) ?? null;
        const datasetName = (
          asStringOrNull(datasetMeta?.name)
          ?? (
            datasetOrder.length === 1
              ? requestedDatasetName
              : `${fallbackBaseName} ${datasetIndex + 1}`
          )
          ?? fallbackBaseName
        );
        const datasetId = await createDataset({
          client,
          ownerUserId,
          datasetName,
          description: asStringOrNull(datasetMeta?.description) ?? description
        });
        const trackOrder = [];
        const seenTrackIds = new Set();
        const trackPointsById = new Map();

        for (const entry of exportedTracks) {
          const trackId = asStringOrNull(entry.id);
          if (
            trackId
            && asStringOrNull(entry.datasetId) === sourceDatasetId
            && !seenTrackIds.has(trackId)
          ) {
            seenTrackIds.add(trackId);
            trackOrder.push(trackId);
          }
        }

        for (const [pointIndex, point] of datasetPoints.entries()) {
          const trackId = asStringOrNull(point.datalogId)
            ?? `${sourceDatasetId}-datalog-${pointIndex + 1}`;
          if (!seenTrackIds.has(trackId)) {
            seenTrackIds.add(trackId);
            trackOrder.push(trackId);
          }

          const existing = trackPointsById.get(trackId) ?? [];
          existing.push(point);
          trackPointsById.set(trackId, existing);
        }

        const datasetChildSummaries = [];
        for (const trackId of trackOrder) {
          const trackPoints = trackPointsById.get(trackId) ?? [];
          if (!trackPoints.length) {
            continue;
          }

          const fallbackTrackMeta = {
            id: trackId,
            datasetId: sourceDatasetId,
            trackName: asStringOrNull(trackPoints[0].datalogName)
          };
          const childSummary = await processBackupTrack({
            client,
            datasetId,
            ownerUserId,
            fileName: file.fileName,
            originalTrackId: trackId,
            trackMeta: trackMetadataById.get(trackId) ?? fallbackTrackMeta,
            points: trackPoints,
            advancedTrackDeduplication,
            correlationId
          });

          childSummaries.push(childSummary);
          datasetChildSummaries.push(childSummary);
        }

        if (datasetChildSummaries.some((entry) => entry.parseStatus === 'parsed')) {
          datasetResults.push(buildDatasetImportResult({
            datasetId,
            datasetName,
            fileResults: [{
              originalFilename: file.fileName,
              importKind: 'backup_json',
              checksum: rootChecksum,
              sizeBytes: file.buffer.length,
              children: datasetChildSummaries
            }]
          }));
        } else {
          await client.query(`DELETE FROM datasets WHERE id = $1`, [datasetId]);
        }
      }

      await client.query(
        `UPDATE raw_files
         SET parse_status = 'parsed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [
          rootRawFileId,
          JSON.stringify({
            formatVersion: Number.isInteger(envelope.formatVersion) ? envelope.formatVersion : null,
            exportTime: asIsoTimestampOrNull(envelope.exportTime),
            importedDatasetCount: datasetResults.length,
            importedDatalogCount: childSummaries.filter((entry) => entry.parseStatus === 'parsed').length,
            rawPointCount: pointRecords.length,
            children: childSummaries
          })
        ]
      );

      return {
        batchFileResult: {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind: 'backup_json',
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: childSummaries
        },
        datasetResults
      };
    } catch (cause) {
      const appError = toLoggedAppError({
        caller: 'imports::processBackupExportAsDatasets',
        reason: 'Backup JSON processing failed.',
        errorKey: 'IMPORT_TRACK_PROCESS_FAILED',
        correlationId,
        context: {
          batchId,
          ownerUserId,
          fileName: file.fileName,
          rootRawFileId,
          sizeBytes: file.buffer.length
        },
        cause,
        preserveExisting: false
      });

      await client.query(
        `UPDATE raw_files
         SET parse_status = 'failed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [rootRawFileId, JSON.stringify({
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        })]
      );

      return {
        batchFileResult: {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind: 'backup_json',
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: [],
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        },
        datasetResults: []
      };
    }
  };

  const processTrackPayload = async ({
    client,
    batchId,
    datasetId,
    ownerUserId,
    fileName,
    buffer,
    parentRawFileId = null,
    sourceType,
    provenance,
    advancedTrackDeduplication = true,
    correlationId = null
  }) => {
    const checksum = sha256Hex({ value: buffer });
    const duplicateOfRawFileId = await findDuplicateRawFile({
      client,
      ownerUserId,
      checksum
    });
    const duplicateSummary = duplicateOfRawFileId
      ? {
          duplicateOfRawFileId,
          dedupeMethod: 'raw_checksum',
          skippedParsing: true,
          warningBreakdown: [{
            type: 'duplicate_file',
            count: 1,
            reason: 'File matched an earlier raw artifact and was skipped.'
          }]
        }
      : {};

    const rawFileId = await persistRawFile({
      client,
      batchId,
      parentRawFileId,
      kind: 'datalog_file',
      originalFilename: fileName,
      checksum,
      sizeBytes: buffer.length,
      buffer,
      sourceType,
      provenance,
      parseStatus: duplicateOfRawFileId ? 'duplicate_skipped' : 'pending',
      parseSummary: duplicateSummary
    });

    if (duplicateOfRawFileId) {
      return rawFileSummary({
        rawFileId,
        originalFilename: fileName,
        checksum,
        sizeBytes: buffer.length,
        parseStatus: 'duplicate_skipped',
        duplicateOfRawFileId,
        summary: duplicateSummary
      });
    }

    try {
      const parsed = parseTrackBuffer({ buffer, fileName, correlationId });
      const semanticDedupeKey = computeParsedTrackSemanticChecksum({ parsed });

      if (advancedTrackDeduplication) {
        const duplicateTrack = await findDuplicateTrackBySemanticKey({
          client,
          ownerUserId,
          semanticDedupeKey
        });

        if (duplicateTrack) {
          const semanticDuplicateSummary = {
            duplicateOfRawFileId: duplicateTrack.raw_file_id ?? null,
            duplicateOfDatalogId: duplicateTrack.id,
            dedupeMethod: 'semantic_track',
            skippedParsing: true,
            warningBreakdown: [{
              type: 'duplicate_track',
              count: 1,
              reason: 'Datalog matched an earlier imported datalog after normalizing device and readings.'
            }]
          };

          await client.query(
            `UPDATE raw_files
             SET parse_status = 'duplicate_skipped',
                 parse_summary_json = $2::jsonb
             WHERE id = $1`,
            [rawFileId, JSON.stringify(semanticDuplicateSummary)]
          );

          return rawFileSummary({
            rawFileId,
            originalFilename: fileName,
            checksum,
            sizeBytes: buffer.length,
            parseStatus: 'duplicate_skipped',
            duplicateOfRawFileId: duplicateTrack.raw_file_id ?? null,
            summary: semanticDuplicateSummary
          });
        }
      }

      await ensureUserDevice({ client, userId: ownerUserId, device: parsed.device, correlationId });
      const trackId = await createTrackFromParsed({
        client,
        datasetId,
        rawFileId,
        ownerUserId,
        parsed,
        semanticDedupeKey
      });

      const summary = {
        datalogId: trackId,
        rowCount: parsed.rowCount,
        validRowCount: parsed.validRowCount,
        warningCount: parsed.warningCount,
        errorCount: parsed.errorCount,
        skippedRowCount: parsed.skippedRowCount,
        warningBreakdown: parsed.warningBreakdown,
        warnings: parsed.warnings
      };

      await client.query(
        `UPDATE raw_files
         SET parse_status = 'parsed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [rawFileId, JSON.stringify(summary)]
      );

      return rawFileSummary({
        rawFileId,
        originalFilename: fileName,
        checksum,
        sizeBytes: buffer.length,
        parseStatus: 'parsed',
        summary
      });
    } catch (cause) {
      const appError = toLoggedAppError({
        caller: 'imports::processTrackPayload',
        reason: 'Datalog payload processing failed.',
        errorKey: 'IMPORT_TRACK_PROCESS_FAILED',
        correlationId,
        context: {
          batchId,
          datasetId,
          ownerUserId,
          fileName,
          parentRawFileId,
          sourceType,
          sizeBytes: buffer.length
        },
        cause,
        preserveExisting: false
      });
      const summary = {
        error: appError.reason,
        errorKey: appError.errorKey,
        errorCode: appError.errorCode
      };
      await client.query(
        `UPDATE raw_files
         SET parse_status = 'failed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [rawFileId, JSON.stringify(summary)]
      );

      return rawFileSummary({
        rawFileId,
        originalFilename: fileName,
        checksum,
        sizeBytes: buffer.length,
        parseStatus: 'failed',
        summary
      });
    }
  };

  const processUploadedFile = async ({
    client,
    batchId,
    datasetId,
    ownerUserId,
    file,
    advancedTrackDeduplication = true,
    correlationId = null
  }) => {
    const importKind = detectImportKind({ fileName: file.fileName, buffer: file.buffer });

    logger.info({
      caller: 'imports::processUploadedFile',
      message: 'Processing uploaded file.',
      correlationId,
      context: {
        batchId,
        datasetId,
        ownerUserId,
        fileName: file.fileName,
        importKind,
        sizeBytes: file.buffer.length
      }
    });

    if (importKind === 'archive') {
      const rootChecksum = sha256Hex({ value: file.buffer });
      const rootRawFileId = await persistRawFile({
        client,
        batchId,
        kind: 'archive',
        originalFilename: file.fileName,
        checksum: rootChecksum,
        sizeBytes: file.buffer.length,
        buffer: file.buffer,
        sourceType: importKind,
        provenance: {
          uploadFieldName: file.fieldName,
          mimeType: file.mimeType
        }
      });
      try {
        const children = await extractSupportedArchiveEntries({ buffer: file.buffer, correlationId });
        const childSummaries = [];
        for (const child of children) {
          childSummaries.push(await processTrackPayload({
            client,
            batchId,
            datasetId,
            ownerUserId,
          fileName: child.fileName,
          buffer: child.buffer,
          parentRawFileId: rootRawFileId,
          sourceType: 'archive_child',
          provenance: {
            archiveName: file.fileName
          },
          advancedTrackDeduplication,
          correlationId
        }));
      }

        await client.query(
          `UPDATE raw_files
           SET parse_status = 'parsed',
               parse_summary_json = $2::jsonb
           WHERE id = $1`,
          [
            rootRawFileId,
            JSON.stringify({
              extractedChildCount: childSummaries.length,
              children: childSummaries
            })
          ]
        );

        return {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind,
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: childSummaries
        };
      } catch (cause) {
        const appError = toLoggedAppError({
          caller: 'imports::processUploadedFile',
          reason: 'Archive payload processing failed.',
          errorKey: 'IMPORT_ARCHIVE_PROCESS_FAILED',
          correlationId,
          context: {
            batchId,
            datasetId,
            ownerUserId,
            fileName: file.fileName,
            rootRawFileId,
            sizeBytes: file.buffer.length
          },
          cause,
          preserveExisting: false
        });
        await client.query(
          `UPDATE raw_files
           SET parse_status = 'failed',
               parse_summary_json = $2::jsonb
           WHERE id = $1`,
          [rootRawFileId, JSON.stringify({
            error: appError.reason,
            errorKey: appError.errorKey,
            errorCode: appError.errorCode
          })]
        );

        return {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind,
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: [],
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        };
      }
    }

    const summary = await processTrackPayload({
      client,
      batchId,
      datasetId,
      ownerUserId,
      fileName: file.fileName,
      buffer: file.buffer,
      parentRawFileId: null,
      sourceType: 'single_file',
      provenance: {
        uploadFieldName: file.fieldName,
        mimeType: file.mimeType
      },
      advancedTrackDeduplication,
      correlationId
    });

    return {
      rootRawFileId: summary.rawFileId,
      originalFilename: file.fileName,
      importKind,
      checksum: summary.checksum,
      sizeBytes: file.buffer.length,
      children: [summary]
    };
  };

  const processBulkArchiveAsDatasets = async ({
    client,
    batchId,
    ownerUserId,
    description,
    file,
    advancedTrackDeduplication = true,
    correlationId = null
  }) => {
    const rootChecksum = sha256Hex({ value: file.buffer });
    const rootRawFileId = await persistRawFile({
      client,
      batchId,
      kind: 'archive',
      originalFilename: file.fileName,
      checksum: rootChecksum,
      sizeBytes: file.buffer.length,
      buffer: file.buffer,
      sourceType: 'archive',
      provenance: {
        uploadFieldName: file.fieldName,
        mimeType: file.mimeType
      }
    });

    try {
      const children = await extractSupportedArchiveEntries({ buffer: file.buffer, correlationId });
      const childSummaries = [];
      const datasetResults = [];

      for (const child of children) {
        const datasetId = await createDataset({
          client,
          ownerUserId,
          datasetName: toDatasetNameFromFileName({ fileName: child.fileName }),
          description
        });
        const childSummary = await processTrackPayload({
          client,
          batchId,
          datasetId,
          ownerUserId,
          fileName: child.fileName,
          buffer: child.buffer,
          parentRawFileId: rootRawFileId,
          sourceType: 'archive_child',
          provenance: {
            archiveName: file.fileName
          },
          advancedTrackDeduplication,
          correlationId
        });

        childSummaries.push(childSummary);
        if (childSummary.parseStatus === 'parsed') {
          datasetResults.push(buildDatasetImportResult({
            datasetId,
            datasetName: toDatasetNameFromFileName({ fileName: child.fileName }),
            fileResults: [{
              originalFilename: child.fileName,
              sourceArchiveFilename: file.fileName,
              importKind: 'datalog_file',
              checksum: childSummary.checksum,
              sizeBytes: child.buffer.length,
              children: [childSummary]
            }]
          }));
        } else {
          await client.query(`DELETE FROM datasets WHERE id = $1`, [datasetId]);
        }
      }

      await client.query(
        `UPDATE raw_files
         SET parse_status = 'parsed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [
          rootRawFileId,
          JSON.stringify({
            extractedChildCount: childSummaries.length,
            children: childSummaries
          })
        ]
      );

      return {
        batchFileResult: {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind: 'archive',
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: childSummaries
        },
        datasetResults
      };
    } catch (cause) {
      const appError = toLoggedAppError({
        caller: 'imports::processBulkArchiveAsDatasets',
        reason: 'Archive payload processing failed.',
        errorKey: 'IMPORT_ARCHIVE_PROCESS_FAILED',
        correlationId,
        context: {
          batchId,
          ownerUserId,
          fileName: file.fileName,
          rootRawFileId,
          sizeBytes: file.buffer.length
        },
        cause,
        preserveExisting: false
      });
      await client.query(
        `UPDATE raw_files
         SET parse_status = 'failed',
             parse_summary_json = $2::jsonb
         WHERE id = $1`,
        [rootRawFileId, JSON.stringify({
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        })]
      );

      return {
        batchFileResult: {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind: 'archive',
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: [],
          error: appError.reason,
          errorKey: appError.errorKey,
          errorCode: appError.errorCode
        },
        datasetResults: []
      };
    }
  };

  const importRequest = async ({ req, user, correlationId = null }) => {
    if (!canImport({ user })) {
      throw createAppError({
        caller: 'imports::importRequest',
        reason: 'This user is not allowed to import datasets.',
        errorKey: 'IMPORT_REQUEST_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    logger.info({
      caller: 'imports::importRequest',
      message: 'Starting import request.',
      correlationId,
      context: {
        userId: user.id,
        username: user.username
      }
    });

    let parsedUpload;
    try {
      parsedUpload = await parseMultipartUpload({ req, logger, correlationId });
    } catch (cause) {
      throw toLoggedAppError({
        caller: 'imports::importRequest',
        reason: 'Failed parsing multipart upload request.',
        errorKey: 'IMPORT_REQUEST_MULTIPART_PARSE_FAILED',
        correlationId,
        context: {
          userId: user.id,
          contentType: req.headers['content-type'] ?? null
        },
        cause,
        status: 400
      });
    }

    const { files, fields } = parsedUpload;
    if (!files.length) {
      throw createAppError({
        caller: 'imports::importRequest',
        reason: 'At least one file is required.',
        errorKey: 'IMPORT_REQUEST_EMPTY_FILES',
        correlationId,
        status: 400
      });
    }

    const batchId = createOpaqueId();
    const primaryImportKind = files.length === 1
      ? detectImportKind({ fileName: files[0].fileName, buffer: files[0].buffer })
      : null;
    const sourceType = files.length === 1
      ? (
          primaryImportKind === 'archive'
            ? 'archive'
            : (primaryImportKind === 'backup_json' ? 'backup_json' : 'single_file')
        )
      : 'bulk_files';
    const uploadedAt = new Date().toISOString();
    const requestedDatasetName = typeof fields.datasetName === 'string' && fields.datasetName.trim()
      ? fields.datasetName.trim()
      : defaultDatasetName({ files });
    const description = typeof fields.description === 'string' ? fields.description : null;
    const splitBulkArchivesIntoDatasets = parseBooleanField({
      value: fields.splitBulkArchivesIntoDatasets,
      defaultValue: true
    });
    const advancedTrackDeduplication = parseBooleanField({
      value: fields.advancedTrackDeduplication,
      defaultValue: true
    });

    logger.info({
      caller: 'imports::importRequest',
      message: 'Parsed multipart upload.',
      correlationId,
      context: {
        userId: user.id,
        batchId,
        sourceType,
        requestedDatasetName,
        splitBulkArchivesIntoDatasets,
        advancedTrackDeduplication,
        fileCount: files.length,
        fileNames: files.map((file) => file.fileName)
      }
    });

    try {
      const result = await db.withTransaction(async (client) => {
        await client.query(
          `INSERT INTO upload_batches (
            id,
            uploader_user_id,
            source_type,
            original_filename,
            checksum,
            size_bytes,
            uploaded_at,
            status,
            summary_json
          ) VALUES ($1, $2, $3, $4, NULL, NULL, $5, 'processing', '{}'::jsonb)`,
          [batchId, user.id, sourceType, files[0]?.fileName ?? null, uploadedAt]
        );

        let sharedDatasetId = null;
        const sharedFileResults = [];
        const batchFileResults = [];
        const datasetResults = [];

        const ensureSharedDatasetId = async () => {
          if (sharedDatasetId) {
            return sharedDatasetId;
          }

          sharedDatasetId = await createDataset({
            client,
            ownerUserId: user.id,
            datasetName: requestedDatasetName,
            description
          });
          return sharedDatasetId;
        };

        for (const file of files) {
          try {
            const importKind = detectImportKind({ fileName: file.fileName, buffer: file.buffer });
            const shouldSplitIntoDatasets = (
              splitBulkArchivesIntoDatasets
              && importKind === 'archive'
              && isSplitArchiveFileName({ fileName: file.fileName })
            );

            if (shouldSplitIntoDatasets) {
              const splitResult = await processBulkArchiveAsDatasets({
                client,
                batchId,
                ownerUserId: user.id,
                description,
                file,
                advancedTrackDeduplication,
                correlationId
              });
              batchFileResults.push(splitResult.batchFileResult);
              datasetResults.push(...splitResult.datasetResults);
              continue;
            }

            if (importKind === 'backup_json') {
              const backupResult = await processBackupExportAsDatasets({
                client,
                batchId,
                ownerUserId: user.id,
                requestedDatasetName,
                description,
                file,
                advancedTrackDeduplication,
                correlationId
              });
              batchFileResults.push(backupResult.batchFileResult);
              datasetResults.push(...backupResult.datasetResults);
              continue;
            }

            const datasetId = await ensureSharedDatasetId();
            const fileResult = await processUploadedFile({
              client,
              batchId,
              datasetId,
              ownerUserId: user.id,
              file,
              advancedTrackDeduplication,
              correlationId
            });
            batchFileResults.push(fileResult);
            sharedFileResults.push(fileResult);
          } catch (cause) {
            throw toLoggedAppError({
              caller: 'imports::importRequest',
              reason: 'Failed processing uploaded file during import.',
              errorKey: 'IMPORT_REQUEST_FILE_PROCESS_FAILED',
              correlationId,
              context: {
                batchId,
                fileName: file.fileName,
                fileSizeBytes: file.buffer.length
              },
              cause
            });
          }
        }

        if (sharedDatasetId) {
          const sharedDatasetHasTrack = sharedFileResults.some((fileResult) => (
            (fileResult.children ?? []).some((child) => child.parseStatus === 'parsed')
          ));

          if (sharedDatasetHasTrack) {
            datasetResults.unshift(buildDatasetImportResult({
              datasetId: sharedDatasetId,
              datasetName: requestedDatasetName,
              fileResults: sharedFileResults
            }));
          } else {
            await client.query(`DELETE FROM datasets WHERE id = $1`, [sharedDatasetId]);
            sharedDatasetId = null;
          }
        }

        const summary = buildUploadBatchSummary({
          batchFileResults,
          datasetResults,
          splitBulkArchivesIntoDatasets,
          advancedTrackDeduplication
        });
        const status = resolveImportStatus({ summary });

        await client.query(
          `UPDATE upload_batches
           SET status = $2,
               summary_json = $3::jsonb
           WHERE id = $1`,
          [batchId, status, JSON.stringify(summary)]
        );

        try {
          await audit.record({
            actorUserId: user.id,
            eventType: 'import.completed',
            entityType: 'upload_batch',
            entityId: batchId,
            payload: {
              datasetId: summary.datasetId,
              datasetIds: summary.datasetIds,
              datasetCount: summary.datasetCount,
              status,
              summary
            }
          });
        } catch (cause) {
          throw toLoggedAppError({
            caller: 'imports::importRequest',
            reason: 'Failed recording import audit event.',
            errorKey: 'IMPORT_REQUEST_AUDIT_FAILED',
            correlationId,
            context: {
              batchId,
              datasetIds: summary.datasetIds,
              status
            },
            cause
          });
        }

        return {
          batchId,
          datasetId: summary.datasetId,
          datasetIds: summary.datasetIds,
          status,
          summary
        };
      });

      logger.info({
        caller: 'imports::importRequest',
        message: 'Completed import request.',
        correlationId,
        context: {
          userId: user.id,
          batchId: result.batchId,
          datasetId: result.datasetId,
          datasetIds: result.datasetIds,
          status: result.status,
          fileCount: result.summary.fileCount,
          parsedFileCount: result.summary.parsedFileCount,
          failedFileCount: result.summary.failedFileCount
        }
      });

      return result;
    } catch (cause) {
      throw toLoggedAppError({
        caller: 'imports::importRequest',
        reason: 'Failed completing import transaction.',
        errorKey: 'IMPORT_REQUEST_TRANSACTION_FAILED',
        correlationId,
        context: {
          userId: user.id,
          batchId,
          sourceType,
          requestedDatasetName,
          splitBulkArchivesIntoDatasets,
          advancedTrackDeduplication,
          fileCount: files.length
        },
        cause
      });
    }
  };

  const listUploadBatches = async ({ user, failedOnly = false }) => {
    const filters = [];
    const params = [];

    if (user.role !== 'moderator' && user.role !== 'admin') {
      params.push(user.id);
      filters.push(`uploader_user_id = $${params.length}`);
    }

    if (failedOnly) {
      filters.push(`status IN ('failed', 'completed_with_errors')`);
    }

    const whereClause = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const result = await db.query(
      `SELECT *
       FROM upload_batches
       ${whereClause}
       ORDER BY uploaded_at DESC
       LIMIT 100`,
      params
    );

    return result.rows.map((row) => ({
      id: row.id,
      uploaderUserId: row.uploader_user_id,
      sourceType: row.source_type,
      originalFilename: row.original_filename,
      uploadedAt: row.uploaded_at,
      status: row.status,
      summary: row.summary_json
    }));
  };

  const getUploadBatch = async ({ batchId, user, correlationId = null }) => {
    const result = await db.query('SELECT * FROM upload_batches WHERE id = $1', [batchId]);
    const row = result.rows[0] ?? null;
    if (!row) {
      throw createAppError({
        caller: 'imports::getUploadBatch',
        reason: 'Upload batch was not found.',
        errorKey: 'IMPORT_BATCH_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    if (
      row.uploader_user_id !== user.id
      && user.role !== 'moderator'
      && user.role !== 'admin'
    ) {
      throw createAppError({
        caller: 'imports::getUploadBatch',
        reason: 'This upload batch is not visible to the current user.',
        errorKey: 'IMPORT_BATCH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return {
      id: row.id,
      uploaderUserId: row.uploader_user_id,
      sourceType: row.source_type,
      originalFilename: row.original_filename,
      status: row.status,
      uploadedAt: row.uploaded_at,
      summary: row.summary_json
    };
  };

  return {
    importRequest,
    listUploadBatches,
    getUploadBatch
  };
};
