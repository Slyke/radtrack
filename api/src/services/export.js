import { getBuildInfo } from '../lib/build-info.js';
import { createAppError } from '../lib/errors.js';

export const createExportService = ({ db, queryService, settingsService }) => {
  const isPlainObject = (value) => (
    Boolean(value)
    && typeof value === 'object'
    && !Array.isArray(value)
  );

  const compactValue = (value) => {
    if (Array.isArray(value)) {
      return value.map((entry) => compactValue(entry));
    }

    if (!isPlainObject(value)) {
      return value;
    }

    const compacted = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry === null || entry === undefined) {
        continue;
      }

      compacted[key] = compactValue(entry);
    }

    return compacted;
  };

  const buildCompactPoint = ({ point }) => {
    const extra = isPlainObject(point.extra) && Object.keys(point.extra).length
      ? point.extra
      : undefined;
    const comment = typeof point.comment === 'string'
      ? point.comment.trim()
      : point.comment;

    return compactValue({
      datasetId: point.datasetId,
      datalogId: point.datalogId,
      occurredAt: point.occurredAt,
      latitude: point.latitude,
      longitude: point.longitude,
      accuracy: point.accuracy,
      altitudeMeters: point.altitudeMeters,
      deviceId: point.deviceId,
      deviceName: point.deviceName,
      deviceType: point.deviceType,
      deviceCalibration: point.deviceCalibration,
      firmwareVersion: point.firmwareVersion,
      sourceReadingId: point.sourceReadingId,
      comment: comment || undefined,
      custom: point.custom,
      measurements: point.measurements,
      extra
    });
  };

  const buildTrackMetadataRecord = ({ row, fullExport }) => {
    const base = {
      id: row.id,
      datasetId: row.dataset_id,
      sourceType: row.source_type,
      datalogName: row.datalog_name,
      deviceIdentifierRaw: row.device_identifier_raw,
      rawHeaderLine: row.raw_header_line,
      rawColumns: row.raw_columns_json,
      headerMetadata: row.header_metadata_json,
      supportedFields: row.supported_fields_json,
      rowCount: row.row_count,
      validRowCount: row.valid_row_count,
      warningCount: row.warning_count,
      errorCount: row.error_count,
      skippedRowCount: row.skipped_row_count,
      startedAt: row.started_at,
      endedAt: row.ended_at,
      createdAt: row.created_at
    };

    if (fullExport) {
      return {
        ...base,
        deviceModel: row.device_model,
        deviceSerial: row.device_serial
      };
    }

    return compactValue(base);
  };

  const loadDatasetMetadata = async ({ datasetIds }) => {
    if (!datasetIds.length) {
      return [];
    }

    const result = await db.query(
      `SELECT id, name, description
       FROM datasets
       WHERE id = ANY($1::text[])`,
      [datasetIds]
    );
    const byId = new Map(result.rows.map((row) => [row.id, row]));

    return datasetIds.flatMap((datasetId) => {
      const row = byId.get(datasetId);
      return row
        ? [{
            id: row.id,
            name: row.name,
            description: row.description
          }]
        : [];
    });
  };

  const loadTrackMetadata = async ({ trackIds, fullExport }) => {
    if (!trackIds.length) {
      return [];
    }

    const result = await db.query(
      `SELECT
         id,
         dataset_id,
         source_type,
         datalog_name,
         device_identifier_raw,
         device_model,
         device_serial,
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
         created_at
       FROM datalogs
       WHERE id = ANY($1::text[])`,
      [trackIds]
    );
    const byId = new Map(result.rows.map((row) => [row.id, row]));

    return trackIds.flatMap((trackId) => {
      const row = byId.get(trackId);
      return row
        ? [buildTrackMetadataRecord({
            row,
            fullExport
          })]
        : [];
    });
  };

  const exportJson = async ({ user, input, correlationId = null }) => {
    const exportRaw = Boolean(input?.includeRaw);
    const exportAggregates = Boolean(input?.includeAggregates);
    const fullExport = Boolean(input?.fullExport);
    if (!exportRaw && !exportAggregates) {
      throw createAppError({
        caller: 'export::exportJson',
        reason: 'Export must include raw points, aggregates, or both.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const uiConfig = await settingsService.getUiConfig();
    const buildInfo = getBuildInfo();
    const envelope = {
      title: 'RadTrack Datalog Export',
      type: 'radtrack-datalog-export',
      formatVersion: 3,
      exportTime: new Date().toISOString(),
      build: buildInfo.label,
      fullExport,
      metric: input?.metric ?? uiConfig.defaultMetric,
      filters: {
        datasetIds: input?.datasetIds ?? [],
        combinedDatasetIds: input?.combinedDatasetIds ?? [],
        dateFrom: input?.dateFrom ?? null,
        dateTo: input?.dateTo ?? null,
        applyExcludeAreas: Boolean(input?.applyExcludeAreas)
      },
      datasets: [],
      datalogs: [],
      raw: null,
      aggregates: null
    };

    if (exportRaw) {
      const rawPayload = await queryService.getRawPoints({
        user,
        input: {
          ...input,
          limit: input?.limit ?? Number.MAX_SAFE_INTEGER
        },
        correlationId
      });

      envelope.raw = fullExport
        ? rawPayload
        : compactValue({
            datasetIds: rawPayload.datasetIds,
            metric: rawPayload.metric,
            totalCount: rawPayload.totalCount,
            capped: rawPayload.capped,
            points: rawPayload.points.map((point) => buildCompactPoint({ point }))
          });
    }

    if (exportAggregates) {
      const aggregatePayload = await queryService.getAggregates({
        user,
        input,
        correlationId
      });
      envelope.aggregates = fullExport
        ? aggregatePayload
        : compactValue({
            datasetIds: aggregatePayload.datasetIds,
            metric: aggregatePayload.metric,
            shape: aggregatePayload.shape,
            cellSizeMeters: aggregatePayload.cellSizeMeters,
            cells: aggregatePayload.cells
          });
    }

    const datasetIds = envelope.raw?.datasetIds?.length
      ? envelope.raw.datasetIds
      : (envelope.aggregates?.datasetIds ?? []);
    envelope.datasets = await loadDatasetMetadata({ datasetIds });

    if (envelope.raw?.points?.length) {
      const trackIds = [...new Set(envelope.raw.points
        .map((point) => point.datalogId)
        .filter((trackId) => typeof trackId === 'string' && trackId))];
      envelope.datalogs = await loadTrackMetadata({
        trackIds,
        fullExport
      });
    }

    return fullExport ? envelope : compactValue(envelope);
  };

  return {
    exportJson
  };
};
