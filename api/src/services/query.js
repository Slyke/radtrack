import { createAppError } from '../lib/errors.js';
import { sha256Hex } from '../utils/ids.js';
import { buildAggregateCell, computeAggregateStats, pointInCircle, pointInPolygon } from '../utils/geo.js';

const allowedMetrics = ['dose_rate', 'count_rate', 'accuracy'];

const coerceNumber = ({ value }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const coerceBoolean = ({ value }) => {
  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'string') {
    return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
  }

  return Boolean(value);
};

const toStringArray = ({ value }) => {
  if (Array.isArray(value)) {
    return value.map(String).flatMap((entry) => entry.split(',')).map((entry) => entry.trim()).filter(Boolean);
  }

  if (typeof value === 'string') {
    return value.split(',').map((entry) => entry.trim()).filter(Boolean);
  }

  return [];
};

const normalizeFilterInput = ({ input }) => ({
  datasetIds: toStringArray({ value: input?.datasetIds }),
  combinedDatasetIds: toStringArray({ value: input?.combinedDatasetIds }),
  dateFrom: input?.dateFrom ? String(input.dateFrom) : null,
  dateTo: input?.dateTo ? String(input.dateTo) : null,
  metric: allowedMetrics.includes(String(input?.metric)) ? String(input.metric) : 'dose_rate',
  metricMin: coerceNumber({ value: input?.metricMin }),
  metricMax: coerceNumber({ value: input?.metricMax }),
  minLat: coerceNumber({ value: input?.minLat }),
  maxLat: coerceNumber({ value: input?.maxLat }),
  minLon: coerceNumber({ value: input?.minLon }),
  maxLon: coerceNumber({ value: input?.maxLon }),
  includeHidden: coerceBoolean({ value: input?.includeHidden }),
  applyExcludeAreas: coerceBoolean({ value: input?.applyExcludeAreas }),
  limit: coerceNumber({ value: input?.limit }),
  shape: ['hexagon', 'square', 'circle'].includes(String(input?.shape)) ? String(input.shape) : 'hexagon',
  cellSizeMeters: coerceNumber({ value: input?.cellSizeMeters }) ?? null,
  zoom: coerceNumber({ value: input?.zoom }),
  modeBucketDecimals: coerceNumber({ value: input?.modeBucketDecimals }) ?? null
});

export const createQueryService = ({ db, cache, settingsService, datasetService }) => {
  const resolveDatasetIds = async ({ user, filter, correlationId = null }) => {
    const explicitDatasetIds = new Set(filter.datasetIds);

    for (const combinedDatasetId of filter.combinedDatasetIds) {
      const combined = await datasetService.getCombinedDataset({ combinedDatasetId, user, correlationId });
      for (const member of combined.members) {
        explicitDatasetIds.add(member.datasetId);
      }
    }

    if (!explicitDatasetIds.size) {
      const datasets = await datasetService.listDatasets({ user });
      return datasets.map((dataset) => dataset.id);
    }

    for (const datasetId of explicitDatasetIds) {
      await datasetService.getDatasetAccess({ datasetId, user, correlationId });
    }

    return [...explicitDatasetIds];
  };

  const loadExcludeAreas = async ({ datasetIds }) => {
    if (!datasetIds.length) {
      return [];
    }

    const result = await db.query(
      `SELECT *
       FROM exclude_areas
       WHERE dataset_id = ANY($1::text[])`,
      [datasetIds]
    );

    return result.rows.map((row) => ({
      id: row.id,
      datasetId: row.dataset_id,
      shapeType: row.shape_type,
      geometry: row.geometry_json,
      applyByDefaultOnExport: row.apply_by_default_on_export
    }));
  };

  const applyExcludeAreasToRows = ({ rows, excludeAreas }) => {
    if (!excludeAreas.length) {
      return rows;
    }

    return rows.filter((row) => {
      const point = {
        latitude: row.latitude,
        longitude: row.longitude
      };

      for (const area of excludeAreas) {
        if (area.datasetId !== row.dataset_id) {
          continue;
        }

        if (
          area.shapeType === 'circle'
          && area.geometry?.center
          && typeof area.geometry?.radiusMeters === 'number'
          && pointInCircle({
            point,
            circle: {
              center: area.geometry.center,
              radiusMeters: area.geometry.radiusMeters
            }
          })
        ) {
          return false;
        }

        if (
          area.shapeType === 'polygon'
          && Array.isArray(area.geometry?.points)
          && pointInPolygon({
            point,
            polygon: area.geometry.points
          })
        ) {
          return false;
        }
      }

      return true;
    });
  };

  const fetchFilteredRows = async ({ user, input, correlationId = null }) => {
    const filter = normalizeFilterInput({ input });
    const datasetIds = await resolveDatasetIds({ user, filter, correlationId });
    if (!datasetIds.length) {
      return {
        datasetIds,
        filter,
        rows: [],
        excludeAreas: []
      };
    }

    const params = [datasetIds];
    const where = ['t.dataset_id = ANY($1::text[])'];

    if (!filter.includeHidden) {
      where.push('r.is_hidden = FALSE');
    }

    if (filter.dateFrom) {
      params.push(filter.dateFrom);
      where.push(`(r.occurred_at IS NULL OR r.occurred_at >= $${params.length}::timestamptz)`);
    }

    if (filter.dateTo) {
      params.push(filter.dateTo);
      where.push(`(r.occurred_at IS NULL OR r.occurred_at <= $${params.length}::timestamptz)`);
    }

    if (filter.minLat !== null) {
      params.push(filter.minLat);
      where.push(`r.latitude >= $${params.length}`);
    }

    if (filter.maxLat !== null) {
      params.push(filter.maxLat);
      where.push(`r.latitude <= $${params.length}`);
    }

    if (filter.minLon !== null) {
      params.push(filter.minLon);
      where.push(`r.longitude >= $${params.length}`);
    }

    if (filter.maxLon !== null) {
      params.push(filter.maxLon);
      where.push(`r.longitude <= $${params.length}`);
    }

    if (filter.metricMin !== null) {
      params.push(filter.metricMin);
      where.push(`r.${filter.metric} >= $${params.length}`);
    }

    if (filter.metricMax !== null) {
      params.push(filter.metricMax);
      where.push(`r.${filter.metric} <= $${params.length}`);
    }

    const result = await db.query(
      `SELECT
         r.id,
         r.track_id,
         t.dataset_id,
         t.track_name,
         r.raw_timestamp,
         r.parsed_time_text,
         r.occurred_at,
         r.latitude,
         r.longitude,
         r.accuracy,
         r.dose_rate,
         r.count_rate,
         r.comment,
         r.row_number,
         r.warning_flags_json,
         r.extra_json
       FROM readings r
       JOIN tracks t ON t.id = r.track_id
       WHERE ${where.join(' AND ')}
       ORDER BY r.occurred_at NULLS LAST, r.row_number ASC`,
      params
    );

    const excludeAreas = filter.applyExcludeAreas
      ? await loadExcludeAreas({ datasetIds })
      : [];

    return {
      datasetIds,
      filter,
      excludeAreas,
      rows: applyExcludeAreasToRows({ rows: result.rows, excludeAreas })
    };
  };

  const getRawPoints = async ({ user, input, correlationId = null }) => {
    const uiConfig = await settingsService.getUiConfig();
    const { datasetIds, filter, rows } = await fetchFilteredRows({ user, input, correlationId });
    const limit = filter.limit ?? uiConfig.rawPointCap;

    return {
      datasetIds,
      metric: filter.metric,
      totalCount: rows.length,
      capped: rows.length > limit,
      points: rows.slice(0, limit).map((row) => ({
        id: row.id,
        datasetId: row.dataset_id,
        trackId: row.track_id,
        trackName: row.track_name,
        rawTimestamp: row.raw_timestamp,
        parsedTimeText: row.parsed_time_text,
        occurredAt: row.occurred_at,
        latitude: row.latitude,
        longitude: row.longitude,
        accuracy: row.accuracy,
        doseRate: row.dose_rate,
        countRate: row.count_rate,
        comment: row.comment,
        rowNumber: row.row_number,
        warningFlags: row.warning_flags_json,
        extra: row.extra_json
      }))
    };
  };

  const getAggregates = async ({ user, input, correlationId = null }) => {
    const uiConfig = await settingsService.getUiConfig();
    const { datasetIds, filter, rows } = await fetchFilteredRows({ user, input, correlationId });
    const cellSizeMeters = filter.cellSizeMeters ?? uiConfig.defaultCellSizeMeters;
    const modeBucketDecimals = filter.modeBucketDecimals ?? uiConfig.modeBucketDecimals;
    const ttlSeconds = uiConfig.cacheTtlSeconds;
    const cacheKeyPrefix = `aggregate:datasets=${datasetIds.slice().sort().join(',') || 'none'}`;
    const cacheKey = `${cacheKeyPrefix}:query=${sha256Hex({
      value: JSON.stringify({
        datasetIds,
        filter: {
          ...filter,
          cellSizeMeters,
          modeBucketDecimals
        }
      })
    })}`;

    const cached = await cache.readJson({ key: cacheKey, ttlSeconds });
    if (cached) {
      return {
        ...cached,
        cache: {
          hit: true,
          key: cacheKey
        }
      };
    }

    if (!rows.length) {
      const empty = {
        datasetIds,
        metric: filter.metric,
        shape: filter.shape,
        cellSizeMeters,
        cells: []
      };
      await cache.writeJson({ key: cacheKey, value: empty, ttlSeconds });
      return {
        ...empty,
        cache: {
          hit: false,
          key: cacheKey
        }
      };
    }

    const origin = {
      latitude: rows[0].latitude,
      longitude: rows[0].longitude
    };
    const cells = new Map();
    for (const row of rows) {
      const metricValue = row[filter.metric];
      if (metricValue === null || metricValue === undefined) {
        continue;
      }

      const cell = buildAggregateCell({
        point: {
          latitude: row.latitude,
          longitude: row.longitude
        },
        origin,
        shape: filter.shape,
        cellSizeMeters
      });
      const current = cells.get(cell.id) ?? {
        id: cell.id,
        center: cell.center,
        radiusMeters: cell.radiusMeters,
        metricValues: [],
        pointCount: 0
      };
      current.metricValues.push(metricValue);
      current.pointCount += 1;
      cells.set(cell.id, current);
    }

    const response = {
      datasetIds,
      metric: filter.metric,
      shape: filter.shape,
      cellSizeMeters,
      cells: [...cells.values()].map((cell) => ({
        id: cell.id,
        center: cell.center,
        radiusMeters: cell.radiusMeters,
        stats: computeAggregateStats({
          values: cell.metricValues,
          modeBucketDecimals
        })
      }))
    };

    await cache.writeJson({ key: cacheKey, value: response, ttlSeconds });
    return {
      ...response,
      cache: {
        hit: false,
        key: cacheKey
      }
    };
  };

  const invalidateDatasets = async ({ datasetIds }) => {
    for (const datasetId of datasetIds) {
      await cache.deletePattern({ pattern: `aggregate:*${datasetId}*` });
    }
  };

  return {
    fetchFilteredRows,
    getRawPoints,
    getAggregates,
    invalidateDatasets
  };
};
