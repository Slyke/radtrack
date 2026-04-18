import Busboy from 'busboy';
import { AppError, createAppError } from '../lib/errors.js';
import { logError } from '../lib/logger.js';
import { createOpaqueId, sha256Hex } from '../utils/ids.js';
import { detectImportKind, extractSupportedArchiveEntries, parseTrackBuffer } from '../utils/track.js';
import { canImport } from './permissions.js';

const batchInsertReadings = async ({ client, trackId, rows, actorCreatedAt }) => {
  const chunkSize = 250;

  for (let startIndex = 0; startIndex < rows.length; startIndex += chunkSize) {
    const slice = rows.slice(startIndex, startIndex + chunkSize);
    const values = [];
    const placeholders = [];

    slice.forEach((row, index) => {
      const base = index * 16;
      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}::jsonb, $${base + 14}::jsonb, $${base + 15}, $${base + 16})`
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
        row.doseRate,
        row.countRate,
        row.comment,
        row.rowNumber,
        JSON.stringify(row.warningFlags),
        JSON.stringify(row.extraJson),
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
        dose_rate,
        count_rate,
        comment,
        row_number,
        warning_flags_json,
        extra_json,
        received_at,
        created_at
      ) VALUES ${placeholders.join(', ')}`,
      values
    );
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

  const findDuplicateRawFile = async ({ client, checksum }) => {
    const result = await client.query(
      `SELECT id FROM raw_files WHERE checksum = $1 ORDER BY created_at ASC LIMIT 1`,
      [checksum]
    );
    return result.rows[0]?.id ?? null;
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
    parsed
  }) => {
    const trackId = createOpaqueId();
    const now = new Date().toISOString();
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
        created_at
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb,
        $12, $13, $14, $15, $16, $17, $18, $19
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
      trackId,
      rows: parsed.rows,
      actorCreatedAt: now
    });

    return trackId;
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
    correlationId = null
  }) => {
    const checksum = sha256Hex({ value: buffer });
    const duplicateOfRawFileId = await findDuplicateRawFile({ client, checksum });
    const duplicateSummary = duplicateOfRawFileId
      ? {
          duplicateOfRawFileId,
          skippedParsing: true
        }
      : {};

    const rawFileId = await persistRawFile({
      client,
      batchId,
      parentRawFileId,
      kind: 'track_file',
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
      await ensureUserDevice({ client, userId: ownerUserId, device: parsed.device, correlationId });
      const trackId = await createTrackFromParsed({
        client,
        datasetId,
        rawFileId,
        ownerUserId,
        parsed
      });

      const summary = {
        trackId,
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
        reason: 'Track payload processing failed.',
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
    const sourceType = files.length === 1
      ? (detectImportKind({ fileName: files[0].fileName, buffer: files[0].buffer }) === 'archive' ? 'archive' : 'single_file')
      : 'bulk_files';
    const uploadedAt = new Date().toISOString();
    const datasetName = typeof fields.datasetName === 'string' && fields.datasetName.trim()
      ? fields.datasetName.trim()
      : defaultDatasetName({ files });

    logger.info({
      caller: 'imports::importRequest',
      message: 'Parsed multipart upload.',
      correlationId,
      context: {
        userId: user.id,
        batchId,
        sourceType,
        datasetName,
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

        const datasetId = await createDataset({
          client,
          ownerUserId: user.id,
          datasetName,
          description: typeof fields.description === 'string' ? fields.description : null
        });

        const fileResults = [];
        for (const file of files) {
          try {
            fileResults.push(await processUploadedFile({
              client,
              batchId,
              datasetId,
              ownerUserId: user.id,
              file,
              correlationId
            }));
          } catch (cause) {
            throw toLoggedAppError({
              caller: 'imports::importRequest',
              reason: 'Failed processing uploaded file during import.',
              errorKey: 'IMPORT_REQUEST_FILE_PROCESS_FAILED',
              correlationId,
              context: {
                batchId,
                datasetId,
                fileName: file.fileName,
                fileSizeBytes: file.buffer.length
              },
              cause
            });
          }
        }

        const flattenedChildren = fileResults.flatMap((fileResult) => fileResult.children ?? []);
        const summary = {
          datasetId,
          datasetName,
          fileCount: files.length,
          parsedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'parsed').length,
          duplicateFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'duplicate_skipped').length,
          failedFileCount: flattenedChildren.filter((entry) => entry.parseStatus === 'failed').length,
          warningCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.warningCount ?? 0), 0),
          errorCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.errorCount ?? (entry.parseStatus === 'failed' ? 1 : 0)), 0),
          skippedRowCount: flattenedChildren.reduce((total, entry) => total + (entry.summary?.skippedRowCount ?? 0), 0),
          files: fileResults
        };
        const status = summary.failedFileCount > 0
          ? (summary.parsedFileCount > 0 ? 'completed_with_errors' : 'failed')
          : (summary.warningCount > 0 || summary.duplicateFileCount > 0 ? 'completed_with_warnings' : 'completed');

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
              datasetId,
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
              datasetId,
              status
            },
            cause
          });
        }

        return {
          batchId,
          datasetId,
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
          datasetName,
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
