import { createAppError } from '../lib/errors.js';
import { sha256Hex } from '../utils/ids.js';
import { buildAggregateCell, computeAggregateStats, pointInCircle, pointInPolygon } from '../utils/geo.js';

const allowedMetrics = ['dose_rate', 'count_rate', 'accuracy'];
const aggregatePopupMetrics = [
  { key: 'doseRate', column: 'dose_rate' },
  { key: 'countRate', column: 'count_rate' },
  { key: 'accuracy', column: 'accuracy' },
  { key: 'temperatureC', column: 'temperature_c' }
];

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
  trackIds: toStringArray({ value: input?.trackIds }),
  trackSelectionMode: ['all', 'include', 'none'].includes(String(input?.trackSelectionMode))
    ? String(input.trackSelectionMode)
    : 'all',
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

const withoutViewportFilter = ({ filter }) => ({
  ...filter,
  minLat: null,
  maxLat: null,
  minLon: null,
  maxLon: null,
  zoom: null
});

const hasViewportFilter = ({ filter }) => (
  filter.minLat !== null
  && filter.maxLat !== null
  && filter.minLon !== null
  && filter.maxLon !== null
);

const buildAggregateCacheViewport = ({ filter }) => {
  if (!hasViewportFilter({ filter })) {
    return null;
  }

  const latSpan = Math.max(filter.maxLat - filter.minLat, 0.01);
  const lonSpan = Math.max(filter.maxLon - filter.minLon, 0.01);
  const latAnchorStep = Math.max(latSpan / 2, 0.005);
  const lonAnchorStep = Math.max(lonSpan / 2, 0.005);
  const latWindowSpan = Math.max(latSpan * 2, 0.02);
  const lonWindowSpan = Math.max(lonSpan * 2, 0.02);

  const snappedMinLat = Math.floor(filter.minLat / latAnchorStep) * latAnchorStep;
  const snappedMinLon = Math.floor(filter.minLon / lonAnchorStep) * lonAnchorStep;

  return {
    minLat: snappedMinLat,
    maxLat: snappedMinLat + latWindowSpan,
    minLon: snappedMinLon,
    maxLon: snappedMinLon + lonWindowSpan
  };
};

const clipAggregateCellsToViewport = ({
  cells,
  viewport
}) => {
  if (
    viewport.minLat === null
    || viewport.maxLat === null
    || viewport.minLon === null
    || viewport.maxLon === null
  ) {
    return cells;
  }

  return cells.filter((cell) => {
    const latOffset = cell.radiusMeters / 111320;
    const lonOffset = cell.radiusMeters / (
      Math.cos((cell.center.latitude * Math.PI) / 180) * 111320 || Number.EPSILON
    );

    return (
      (cell.center.latitude + latOffset) >= viewport.minLat
      && (cell.center.latitude - latOffset) <= viewport.maxLat
      && (cell.center.longitude + lonOffset) >= viewport.minLon
      && (cell.center.longitude - lonOffset) <= viewport.maxLon
    );
  });
};

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
      return [];
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
      effectType: row.effect_type,
      compressMinPoints: row.compress_min_points,
      compressMaxPoints: row.compress_max_points,
      geometry: row.geometry_json,
      applyByDefaultOnExport: row.apply_by_default_on_export
    }));
  };

  const pointIsInsideExcludeArea = ({ area, point }) => {
    if (
      area.shapeType === 'circle'
      && area.geometry?.center
      && typeof area.geometry?.radiusMeters === 'number'
    ) {
      return pointInCircle({
        point,
        circle: {
          center: area.geometry.center,
          radiusMeters: area.geometry.radiusMeters
        }
      });
    }

    if (
      area.shapeType === 'polygon'
      && Array.isArray(area.geometry?.points)
    ) {
      return pointInPolygon({
        point,
        polygon: area.geometry.points
      });
    }

    return false;
  };

  const compressRowsForArea = ({ rows, area }) => {
    const minPoints = Number.isInteger(area.compressMinPoints)
      ? Math.max(0, area.compressMinPoints)
      : 2;
    const maxPoints = Number.isInteger(area.compressMaxPoints)
      ? Math.max(minPoints, area.compressMaxPoints)
      : Math.max(minPoints, 20);
    const matchingRows = rows.filter((row) => (
      row.dataset_id === area.datasetId
      && pointIsInsideExcludeArea({
        area,
        point: {
          latitude: row.latitude,
          longitude: row.longitude
        }
      })
    ));

    if (matchingRows.length <= maxPoints) {
      return rows;
    }

    const rangeSize = Math.max((maxPoints - minPoints) + 1, 1);
    const targetCount = Math.min(
      matchingRows.length,
      minPoints + (Number.parseInt(sha256Hex({ value: `exclude:${area.id}:count` }).slice(0, 12), 16) % rangeSize)
    );

    if (targetCount >= matchingRows.length) {
      return rows;
    }

    const keepIds = new Set(
      [...matchingRows]
        .sort((left, right) => (
          sha256Hex({ value: `exclude:${area.id}:row:${left.id}` }).localeCompare(
            sha256Hex({ value: `exclude:${area.id}:row:${right.id}` })
          )
          || left.id.localeCompare(right.id)
        ))
        .slice(0, targetCount)
        .map((row) => row.id)
    );

    return rows.filter((row) => (
      row.dataset_id !== area.datasetId
      || !pointIsInsideExcludeArea({
        area,
        point: {
          latitude: row.latitude,
          longitude: row.longitude
        }
      })
      || keepIds.has(row.id)
    ));
  };

  const applyExcludeAreasToRows = ({ rows, excludeAreas }) => {
    if (!excludeAreas.length) {
      return rows;
    }

    let nextRows = rows;

    for (const area of excludeAreas.filter((entry) => (entry.effectType ?? 'hard_remove') === 'hard_remove')) {
      nextRows = nextRows.filter((row) => (
        row.dataset_id !== area.datasetId
        || !pointIsInsideExcludeArea({
          area,
          point: {
            latitude: row.latitude,
            longitude: row.longitude
          }
        })
      ));
    }

    for (const area of excludeAreas.filter((entry) => entry.effectType === 'compress')) {
      nextRows = compressRowsForArea({
        rows: nextRows,
        area
      });
    }

    return nextRows;
  };

  const fetchFilteredRows = async ({
    user,
    input,
    correlationId = null,
    includeViewport = true
  }) => {
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

    if (
      filter.trackSelectionMode === 'none'
      || (filter.trackSelectionMode === 'include' && !filter.trackIds.length)
    ) {
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

    if (includeViewport) {
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
    }

    if (filter.metricMin !== null) {
      params.push(filter.metricMin);
      where.push(`r.${filter.metric} >= $${params.length}`);
    }

    if (filter.metricMax !== null) {
      params.push(filter.metricMax);
      where.push(`r.${filter.metric} <= $${params.length}`);
    }

    if (filter.trackSelectionMode === 'include' && filter.trackIds.length) {
      params.push(filter.trackIds);
      where.push(`t.id = ANY($${params.length}::text[])`);
    }

    const result = await db.query(
      `SELECT
         r.id,
         r.track_id,
         t.dataset_id,
         t.source_type,
         t.track_name,
         r.raw_timestamp,
         r.parsed_time_text,
         r.occurred_at,
         r.received_at,
         r.latitude,
         r.longitude,
         r.accuracy,
         r.altitude_meters,
         r.dose_rate,
         r.count_rate,
         r.temperature_c,
         r.humidity_pct,
         r.pressure_hpa,
         r.battery_pct,
         r.device_id,
         r.device_name,
         r.device_type,
         r.device_calibration,
         r.firmware_version,
         r.source_reading_id,
         r.comment,
         r.custom_text,
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
        sourceType: row.source_type,
        trackName: row.track_name,
        rawTimestamp: row.raw_timestamp,
        parsedTimeText: row.parsed_time_text,
        occurredAt: row.occurred_at,
        receivedAt: row.received_at,
        latitude: row.latitude,
        longitude: row.longitude,
        accuracy: row.accuracy,
        altitudeMeters: row.altitude_meters,
        doseRate: row.dose_rate,
        countRate: row.count_rate,
        temperatureC: row.temperature_c,
        humidityPct: row.humidity_pct,
        pressureHpa: row.pressure_hpa,
        batteryPct: row.battery_pct,
        deviceId: row.device_id,
        deviceName: row.device_name,
        deviceType: row.device_type,
        deviceCalibration: row.device_calibration,
        firmwareVersion: row.firmware_version,
        sourceReadingId: row.source_reading_id,
        comment: row.comment,
        custom: row.custom_text,
        rowNumber: row.row_number,
        warningFlags: row.warning_flags_json,
        extra: row.extra_json
      }))
    };
  };

  const getAggregates = async ({ user, input, correlationId = null }) => {
    const uiConfig = await settingsService.getUiConfig();
    const requestFilter = normalizeFilterInput({ input });
    const viewportFilter = {
      minLat: requestFilter.minLat,
      maxLat: requestFilter.maxLat,
      minLon: requestFilter.minLon,
      maxLon: requestFilter.maxLon
    };
    const cacheViewport = buildAggregateCacheViewport({ filter: requestFilter });
    const aggregateInput = cacheViewport
      ? {
          ...input,
          ...cacheViewport
        }
      : input;
    const { datasetIds, filter, rows } = await fetchFilteredRows({
      user,
      input: aggregateInput,
      correlationId,
      includeViewport: Boolean(cacheViewport)
    });
    const cacheableFilter = cacheViewport
      ? filter
      : withoutViewportFilter({ filter });
    const cellSizeMeters = filter.cellSizeMeters ?? uiConfig.defaultCellSizeMeters;
    const modeBucketDecimals = filter.modeBucketDecimals ?? uiConfig.modeBucketDecimals;
    const ttlSeconds = uiConfig.cacheTtlSeconds;
    const cacheKeyPrefix = `aggregate-cells:datasets=${datasetIds.slice().sort().join(',') || 'none'}`;
    const cacheKey = `${cacheKeyPrefix}:query=${sha256Hex({
      value: JSON.stringify({
        datasetIds,
        filter: {
          ...cacheableFilter,
          cellSizeMeters,
          modeBucketDecimals
        }
      })
    })}`;

    const cached = await cache.readJson({ key: cacheKey, ttlSeconds });
    if (cached) {
      return {
        ...cached,
        cells: clipAggregateCellsToViewport({
          cells: cached.cells,
          viewport: viewportFilter
        }),
        cache: {
          hit: true,
          key: cacheKey
        }
      };
    }

    if (!rows.length) {
      const empty = {
        datasetIds,
        metric: cacheableFilter.metric,
        shape: cacheableFilter.shape,
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

    const cells = new Map();
    for (const row of rows) {
      const cell = buildAggregateCell({
        point: {
          latitude: row.latitude,
          longitude: row.longitude
        },
        shape: cacheableFilter.shape,
        cellSizeMeters
      });
      const current = cells.get(cell.id) ?? {
        id: cell.id,
        center: cell.center,
        radiusMeters: cell.radiusMeters,
        metricValues: [],
        pointCount: 0,
        timeRangeStart: null,
        timeRangeEnd: null,
        popupMetricValues: {
          doseRate: [],
          countRate: [],
          accuracy: [],
          temperatureC: []
        }
      };

      const metricValue = row[filter.metric];
      if (metricValue !== null && metricValue !== undefined) {
        current.metricValues.push(metricValue);
      }

      current.pointCount += 1;

      for (const popupMetric of aggregatePopupMetrics) {
        const popupValue = row[popupMetric.column];
        if (popupValue === null || popupValue === undefined) {
          continue;
        }

        current.popupMetricValues[popupMetric.key].push(popupValue);
      }

      const timeValue = row.occurred_at ?? row.received_at;
      if (timeValue) {
        if (!current.timeRangeStart || timeValue < current.timeRangeStart) {
          current.timeRangeStart = timeValue;
        }

        if (!current.timeRangeEnd || timeValue > current.timeRangeEnd) {
          current.timeRangeEnd = timeValue;
        }
      }

      cells.set(cell.id, current);
    }

    const fullResponse = {
      datasetIds,
      metric: cacheableFilter.metric,
      shape: cacheableFilter.shape,
      cellSizeMeters,
      cells: [...cells.values()].map((cell) => ({
        id: cell.id,
        center: cell.center,
        radiusMeters: cell.radiusMeters,
        pointCount: cell.pointCount,
        timeRange: {
          start: cell.timeRangeStart,
          end: cell.timeRangeEnd
        },
        stats: computeAggregateStats({
          values: cell.metricValues,
          modeBucketDecimals
        }),
        metrics: {
          doseRate: computeAggregateStats({
            values: cell.popupMetricValues.doseRate,
            modeBucketDecimals
          }),
          countRate: computeAggregateStats({
            values: cell.popupMetricValues.countRate,
            modeBucketDecimals
          }),
          accuracy: computeAggregateStats({
            values: cell.popupMetricValues.accuracy,
            modeBucketDecimals
          }),
          temperatureC: computeAggregateStats({
            values: cell.popupMetricValues.temperatureC,
            modeBucketDecimals
          })
        }
      }))
    };

    await cache.writeJson({ key: cacheKey, value: fullResponse, ttlSeconds });
    return {
      ...fullResponse,
      cells: clipAggregateCellsToViewport({
        cells: fullResponse.cells,
        viewport: viewportFilter
      }),
      cache: {
        hit: false,
        key: cacheKey
      }
    };
  };

  const invalidateDatasets = async ({ datasetIds }) => {
    for (const datasetId of datasetIds) {
      await cache.deletePattern({ pattern: `aggregate:*${datasetId}*` });
      await cache.deletePattern({ pattern: `aggregate-cells:*${datasetId}*` });
    }
  };

  return {
    fetchFilteredRows,
    getRawPoints,
    getAggregates,
    invalidateDatasets
  };
};
