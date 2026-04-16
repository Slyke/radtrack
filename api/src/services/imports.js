import Busboy from 'busboy';
import { createAppError } from '../lib/errors.js';
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
      const base = index * 15;
      placeholders.push(
        `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}, $${base + 11}, $${base + 12}, $${base + 13}::jsonb, $${base + 14}::jsonb, $${base + 15})`
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
        created_at
      ) VALUES ${placeholders.join(', ')}`,
      values
    );
  }
};

const parseMultipartUpload = ({ req }) => new Promise((resolve, reject) => {
  const busboy = Busboy({
    headers: req.headers,
    limits: {
      files: 50,
      fileSize: 256 * 1024 * 1024
    }
  });
  const files = [];
  const fields = {};

  busboy.on('file', (fieldName, file, info) => {
    const chunks = [];

    file.on('data', (chunk) => chunks.push(chunk));
    file.on('limit', () => reject(new Error(`File too large: ${info.filename}`)));
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
  busboy.on('error', reject);
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

export const createImportService = ({ db, audit }) => {
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
    provenance
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
      const parsed = parseTrackBuffer({ buffer, fileName });
      await ensureUserDevice({ client, userId: ownerUserId, device: parsed.device });
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
    } catch (error) {
      const summary = {
        error: error.message
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
    file
  }) => {
    const importKind = detectImportKind({ fileName: file.fileName, buffer: file.buffer });

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
        const children = await extractSupportedArchiveEntries({ buffer: file.buffer });
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
            }
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
      } catch (error) {
        await client.query(
          `UPDATE raw_files
           SET parse_status = 'failed',
               parse_summary_json = $2::jsonb
           WHERE id = $1`,
          [rootRawFileId, JSON.stringify({ error: error.message })]
        );

        return {
          rootRawFileId,
          originalFilename: file.fileName,
          importKind,
          checksum: rootChecksum,
          sizeBytes: file.buffer.length,
          children: [],
          error: error.message
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
        mimeType: file.mimeType,
        rootRawFileId
      }
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
        errorKey: 'AUTH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const { files, fields } = await parseMultipartUpload({ req });
    if (!files.length) {
      throw createAppError({
        caller: 'imports::importRequest',
        reason: 'At least one file is required.',
        errorKey: 'IMPORT_INVALID_PAYLOAD',
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

    return db.withTransaction(async (client) => {
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
        fileResults.push(await processUploadedFile({
          client,
          batchId,
          datasetId,
          ownerUserId: user.id,
          file
        }));
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

      return {
        batchId,
        datasetId,
        status,
        summary
      };
    });
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
        errorKey: 'DATASET_NOT_FOUND',
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
        errorKey: 'AUTH_FORBIDDEN',
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
