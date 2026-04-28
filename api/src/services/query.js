import { createAppError } from '../lib/errors.js';
import { logFeature } from '../lib/feature-logging.js';
import {
  getAggregateTimeBasePropKey,
  getCoreMetricColumn,
  isSyntheticPopupField,
  normalizePropKey,
  normalizeSupportedFields
} from '../utils/datalog-fields.js';
import {
  buildStoredComponents,
  resolveStoredComponentValue,
  stripStoredComponentsFromExtra
} from '../utils/datalog-components.js';
import { sha256Hex } from '../utils/ids.js';
import { buildAggregateCell, computeAggregateStats, pointInCircle, pointInPolygon } from '../utils/geo.js';

const worldWidthDegrees = 360;
const canonicalMinLongitude = -180;
const canonicalMaxLongitude = 180;
const rowTimestampSql = 'COALESCE(r.occurred_at, r.received_at)';
const defaultHistoricalQueryCacheTtlSeconds = 5 * 60;
const rawPointQueryCacheVersion = 'v2';
const aggregateQueryCacheVersion = 'v4';
const aggregateCellCacheVersion = 'v5';
const timeSliceSourceCacheVersion = 'v1';
const aggregateCellInvalidationDefaultSizes = [20, 100, 250, 500, 750, 1000, 2500, 5000, 10000, 15000, 20000];
const maxTimeSliceWindowCount = 10000;
const timeSliceUnitMillis = {
  minutes: 60 * 1000,
  hours: 60 * 60 * 1000,
  days: 24 * 60 * 60 * 1000
};

const normalizeMetricKey = ({ value, fallback = 'doseRate' }) => normalizePropKey({ value }) ?? fallback;

const normalizePopupFieldKeys = ({ value }) => [...new Set(
  toStringArray({ value })
    .map((entry) => normalizePropKey({ value: entry }))
    .filter(Boolean)
)].sort();

const aggregatePopupDataOnlyFieldKeys = new Set([
  'comment',
  'custom',
  'deviceCalibration',
  'deviceId',
  'deviceName',
  'deviceType',
  'firmwareVersion',
  'parsedTimeText',
  'rawTimestamp',
  'sourceReadingId'
]);

const getExpectedPopupMetricKeys = ({ filter }) => {
  if (!filter.popupFieldKeysExplicit) {
    return [];
  }

  return [...new Set(filter.popupFieldKeys
    .map((propKey) => getAggregateTimeBasePropKey({ value: propKey }) ?? propKey)
    .filter((propKey) => propKey !== 'occurredAt' && !aggregatePopupDataOnlyFieldKeys.has(propKey)))]
    .sort();
};

const aggregateCellHasAnyMetricStats = ({ cell, metricKeys }) => metricKeys
  .some((metricKey) => Object.prototype.hasOwnProperty.call(cell?.metrics ?? {}, metricKey));

const aggregateCellsLookStaleForPopupMetrics = ({ cells, metricKeys }) => (
  Boolean(metricKeys.length && cells.length)
  && !cells.some((cell) => aggregateCellHasAnyMetricStats({ cell, metricKeys }))
);

const toTimestampMillis = ({ value }) => {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
};

const getRowMetricValue = ({ row, metricKey }) => {
  if (metricKey === 'occurredAt') {
    return toTimestampMillis({ value: row.occurred_at ?? row.received_at });
  }

  const coreColumn = getCoreMetricColumn({ propKey: metricKey });
  if (coreColumn) {
    return row[coreColumn];
  }

  return row.measurements_json?.[metricKey] ?? null;
};

const coercePopupValueString = ({ value }) => {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (typeof value === 'string') {
    return value.trim() || null;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }

  return null;
};

const resolveExtraFieldValue = ({ extraJson, propKey }) => {
  if (!extraJson || typeof extraJson !== 'object' || Array.isArray(extraJson)) {
    return null;
  }

  if (propKey in extraJson) {
    return coercePopupValueString({ value: extraJson[propKey] });
  }

  for (const [rawKey, rawValue] of Object.entries(extraJson)) {
    if (normalizePropKey({ value: rawKey }) === propKey) {
      return coercePopupValueString({ value: rawValue });
    }
  }

  return null;
};

const getRowPopupDataValue = ({ row, propKey }) => {
  switch (propKey) {
    case 'deviceId':
      return coercePopupValueString({ value: row.device_id });
    case 'rawTimestamp':
      return coercePopupValueString({ value: row.raw_timestamp });
    case 'parsedTimeText':
      return coercePopupValueString({ value: row.parsed_time_text });
    default:
      return resolveStoredComponentValue({
        source: row,
        extraJson: row.extra_json,
        propKey
      }) ?? resolveExtraFieldValue({
        extraJson: stripStoredComponentsFromExtra({ extraJson: row.extra_json }),
        propKey
      });
  }
};

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

const coercePositiveInteger = ({ value }) => {
  const parsed = coerceNumber({ value });
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
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

const normalizeTimeFilterMode = ({ input }) => {
  const rawMode = typeof input?.timeFilterMode === 'string'
    ? input.timeFilterMode.trim().toLowerCase()
    : '';
  if (['none', 'absolute', 'relative'].includes(rawMode)) {
    return rawMode;
  }

  return input?.dateFrom || input?.dateTo ? 'absolute' : 'none';
};

const normalizeRelativeTimeUnit = ({ input }) => {
  const rawValue = typeof input?.relativeUnit === 'string'
    ? input.relativeUnit.trim().toLowerCase()
    : '';

  return ['hours', 'days'].includes(rawValue) ? rawValue : null;
};

const asIsoTimestampOrThrow = ({
  value,
  field,
  correlationId
}) => {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw createAppError({
      caller: 'query::asIsoTimestampOrThrow',
      reason: `Invalid ${field} timestamp.`,
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return parsed.toISOString();
};

const resolveEffectiveTimeWindow = ({
  filter,
  correlationId = null,
  now = new Date()
}) => {
  if (filter.timeFilterMode === 'none') {
    return {
      dateFrom: null,
      dateTo: null,
      historicalCacheEligible: true
    };
  }

  if (filter.timeFilterMode === 'relative') {
    const relativeAmount = coercePositiveInteger({ value: filter.relativeAmount });
    if (!relativeAmount || !filter.relativeUnit) {
      throw createAppError({
        caller: 'query::resolveEffectiveTimeWindow',
        reason: 'Relative time filters require a positive whole amount and a supported unit.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const unitMillis = filter.relativeUnit === 'days'
      ? 24 * 60 * 60 * 1000
      : 60 * 60 * 1000;

    return {
      dateFrom: new Date(now.getTime() - (relativeAmount * unitMillis)).toISOString(),
      dateTo: null,
      historicalCacheEligible: false
    };
  }

  const dateFrom = asIsoTimestampOrThrow({
    value: filter.dateFrom,
    field: 'dateFrom',
    correlationId
  });
  const dateTo = asIsoTimestampOrThrow({
    value: filter.dateTo,
    field: 'dateTo',
    correlationId
  });

  if (dateFrom && dateTo && dateFrom > dateTo) {
    throw createAppError({
      caller: 'query::resolveEffectiveTimeWindow',
      reason: 'Time filter start must be before the end.',
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return {
    dateFrom,
    dateTo,
    historicalCacheEligible: Boolean(filter.historicalCacheEligible && dateTo)
  };
};

const toResultFilter = ({ filter }) => ({
  datasetIds: filter.datasetIds,
  combinedDatasetIds: filter.combinedDatasetIds,
  datalogIds: filter.datalogIds,
  datalogSelectionMode: filter.datalogSelectionMode,
  dateFrom: filter.dateFrom,
  dateTo: filter.dateTo,
  metric: filter.metric,
  metricMin: filter.metricMin,
  metricMax: filter.metricMax,
  minLat: filter.minLat,
  maxLat: filter.maxLat,
  minLon: filter.minLon,
  maxLon: filter.maxLon,
  requireCoordinates: filter.requireCoordinates,
  includeHidden: filter.includeHidden,
  applyExcludeAreas: filter.applyExcludeAreas,
  shape: filter.shape,
  cellSizeMeters: filter.cellSizeMeters,
  zoom: filter.zoom,
  modeBucketDecimals: filter.modeBucketDecimals,
  popupFieldKeys: filter.popupFieldKeys,
  popupFieldKeysExplicit: filter.popupFieldKeysExplicit
});

const normalizeFilterInput = ({ input }) => ({
  datasetIds: toStringArray({ value: input?.datasetIds }).sort(),
  combinedDatasetIds: toStringArray({ value: input?.combinedDatasetIds }).sort(),
  datalogIds: toStringArray({ value: input?.datalogIds }).sort(),
  datalogSelectionMode: ['all', 'include', 'none'].includes(String(input?.datalogSelectionMode))
    ? String(input.datalogSelectionMode)
    : 'all',
  timeFilterMode: normalizeTimeFilterMode({ input }),
  dateFrom: input?.dateFrom ? String(input.dateFrom) : null,
  dateTo: input?.dateTo ? String(input.dateTo) : null,
  relativeAmount: coercePositiveInteger({ value: input?.relativeAmount }),
  relativeUnit: normalizeRelativeTimeUnit({ input }),
  historicalCacheEligible: coerceBoolean({ value: input?.historicalCacheEligible }),
  forceRecheck: coerceBoolean({ value: input?.forceRecheck }),
  metric: normalizeMetricKey({ value: input?.metric }),
  metricMin: coerceNumber({ value: input?.metricMin }),
  metricMax: coerceNumber({ value: input?.metricMax }),
  minLat: coerceNumber({ value: input?.minLat }),
  maxLat: coerceNumber({ value: input?.maxLat }),
  minLon: coerceNumber({ value: input?.minLon }),
  maxLon: coerceNumber({ value: input?.maxLon }),
  requireCoordinates: coerceBoolean({ value: input?.requireCoordinates }),
  uncappedRawPoints: coerceBoolean({ value: input?.uncappedRawPoints }),
  includeHidden: coerceBoolean({ value: input?.includeHidden }),
  applyExcludeAreas: coerceBoolean({ value: input?.applyExcludeAreas }),
  limit: coerceNumber({ value: input?.limit }),
  shape: ['hexagon', 'square', 'circle'].includes(String(input?.shape)) ? String(input.shape) : 'hexagon',
  cellSizeMeters: coerceNumber({ value: input?.cellSizeMeters }) ?? null,
  zoom: coerceNumber({ value: input?.zoom }),
  modeBucketDecimals: coerceNumber({ value: input?.modeBucketDecimals }) ?? null,
  popupFieldKeys: normalizePopupFieldKeys({
    value: input?.popupFieldKeys ?? input?.popupFields ?? input?.popupMetricKeys
  }),
  popupFieldKeysExplicit: (
    Object.prototype.hasOwnProperty.call(input ?? {}, 'popupFieldKeys')
    || Object.prototype.hasOwnProperty.call(input ?? {}, 'popupFields')
    || Object.prototype.hasOwnProperty.call(input ?? {}, 'popupMetricKeys')
    || coerceBoolean({ value: input?.popupFieldKeysExplicit })
  )
});

const roundCacheCoordinate = ({ value }) => Number(Number(value).toFixed(4));

const withoutViewportFilter = ({ filter }) => ({
  ...toResultFilter({ filter }),
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

const wrapLongitude = ({ longitude }) => (
  ((((longitude + 180) % worldWidthDegrees) + worldWidthDegrees) % worldWidthDegrees) - 180
);

const buildLongitudeViewport = ({
  minLon,
  maxLon
}) => {
  if (minLon === null || maxLon === null) {
    return null;
  }

  const span = maxLon - minLon;
  if (!Number.isFinite(span)) {
    return null;
  }

  if (span >= worldWidthDegrees) {
    return {
      span,
      spansWorld: true,
      ranges: [
        {
          minLon: canonicalMinLongitude,
          maxLon: canonicalMaxLongitude
        }
      ]
    };
  }

  const normalizedMinLon = wrapLongitude({ longitude: minLon });
  const normalizedMaxLon = normalizedMinLon + span;

  if (normalizedMaxLon <= canonicalMaxLongitude) {
    return {
      span,
      spansWorld: false,
      ranges: [
        {
          minLon: normalizedMinLon,
          maxLon: normalizedMaxLon
        }
      ]
    };
  }

  return {
    span,
    spansWorld: false,
    ranges: [
      {
        minLon: normalizedMinLon,
        maxLon: canonicalMaxLongitude
      },
      {
        minLon: canonicalMinLongitude,
        maxLon: normalizedMaxLon - worldWidthDegrees
      }
    ]
  };
};

const buildAggregateCacheViewport = ({ filter }) => {
  if (!hasViewportFilter({ filter })) {
    return null;
  }

  const latSpan = Math.max(filter.maxLat - filter.minLat, 0.01);
  const longitudeViewport = buildLongitudeViewport({
    minLon: filter.minLon,
    maxLon: filter.maxLon
  });
  const lonSpan = Math.max(longitudeViewport?.span ?? (filter.maxLon - filter.minLon), 0.01);
  const latAnchorStep = Math.max(latSpan / 2, 0.005);
  const lonAnchorStep = Math.max(lonSpan / 2, 0.005);
  const latWindowSpan = Math.max(latSpan * 2, 0.02);
  const lonWindowSpan = Math.max(lonSpan * 2, 0.02);

  const snappedMinLat = Math.floor(filter.minLat / latAnchorStep) * latAnchorStep;
  const cacheMinLon = longitudeViewport?.ranges[0]?.minLon ?? filter.minLon;
  const snappedMinLon = Math.floor(cacheMinLon / lonAnchorStep) * lonAnchorStep;

  return {
    minLat: roundCacheCoordinate({ value: snappedMinLat }),
    maxLat: roundCacheCoordinate({ value: snappedMinLat + latWindowSpan }),
    minLon: roundCacheCoordinate({ value: snappedMinLon }),
    maxLon: roundCacheCoordinate({ value: snappedMinLon + lonWindowSpan })
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

  const longitudeViewport = buildLongitudeViewport({
    minLon: viewport.minLon,
    maxLon: viewport.maxLon
  });

  return cells.filter((cell) => {
    const latOffset = cell.radiusMeters / 111320;
    const lonOffset = cell.radiusMeters / (
      Math.cos((cell.center.latitude * Math.PI) / 180) * 111320 || Number.EPSILON
    );
    const matchesLongitude = longitudeViewport?.spansWorld
      ? true
      : (
        longitudeViewport?.ranges.some((range) => (
          (cell.center.longitude + lonOffset) >= range.minLon
          && (cell.center.longitude - lonOffset) <= range.maxLon
        )) ?? true
      );

    return (
      (cell.center.latitude + latOffset) >= viewport.minLat
      && (cell.center.latitude - latOffset) <= viewport.maxLat
      && matchesLongitude
    );
  });
};

const buildRawPointCacheKey = ({
  userId,
  queryHash,
  cacheKeyDatasets
}) => (
  `raw-points:${rawPointQueryCacheVersion}:user=${userId}`
  + `:query=${queryHash}`
  + `:datasets=${cacheKeyDatasets}`
);

const buildTimeSliceSourceCacheKey = ({
  userId,
  queryHash,
  cacheKeyDatasets
}) => (
  `time-slice-source:${timeSliceSourceCacheVersion}:user=${userId}`
  + `:query=${queryHash}`
  + `:datasets=${cacheKeyDatasets}`
);

const normalizeTimeSliceSettings = ({ input }) => {
  const amount = coercePositiveInteger({ value: input?.sliceAmount ?? input?.timeSliceAmount }) ?? 1;
  const unit = ['days', 'hours', 'minutes'].includes(String(input?.sliceUnit ?? input?.timeSliceUnit))
    ? String(input?.sliceUnit ?? input?.timeSliceUnit)
    : 'days';

  return {
    amount,
    unit
  };
};

const getTimeSliceIntervalMillis = ({ settings }) => settings.amount * timeSliceUnitMillis[settings.unit];

const buildTimeSliceWindowsForFilter = ({
  filter,
  settings
}) => {
  if (filter.timeFilterMode !== 'absolute' || !filter.dateFrom || !filter.dateTo) {
    return [];
  }

  const startMs = toTimestampMillis({ value: filter.dateFrom });
  const endMs = toTimestampMillis({ value: filter.dateTo });
  const intervalMs = getTimeSliceIntervalMillis({ settings });
  if (
    startMs === null
    || endMs === null
    || startMs > endMs
    || !Number.isFinite(intervalMs)
    || intervalMs <= 0
  ) {
    return [];
  }

  const windows = [];
  for (
    let windowStartMs = startMs, index = 0;
    windowStartMs <= endMs && index < maxTimeSliceWindowCount;
    windowStartMs += intervalMs, index += 1
  ) {
    const windowEndMs = Math.min(windowStartMs + intervalMs, endMs);
    windows.push({
      index,
      startIso: new Date(windowStartMs).toISOString(),
      endIso: new Date(windowEndMs).toISOString(),
      startMs: windowStartMs,
      endMs: windowEndMs,
      isLast: windowEndMs >= endMs
    });

    if (windowEndMs >= endMs) {
      break;
    }
  }

  return windows;
};

const getRowTimestampMillis = ({ row }) => toTimestampMillis({
  value: row.occurred_at ?? row.received_at
});

const buildTimeSlicePreparedWindows = ({
  rows,
  windows
}) => {
  let cursor = 0;
  const timestamps = rows.map((row) => getRowTimestampMillis({ row }));

  return windows.map((window) => {
    while (cursor < timestamps.length && (timestamps[cursor] ?? Number.POSITIVE_INFINITY) < window.startMs) {
      cursor += 1;
    }

    const currentStartIndex = cursor;
    while (
      cursor < timestamps.length
      && timestamps[cursor] !== null
      && (
        window.isLast
          ? timestamps[cursor] <= window.endMs
          : timestamps[cursor] < window.endMs
      )
    ) {
      cursor += 1;
    }

    return {
      ...window,
      currentStartIndex,
      currentEndIndex: cursor,
      cumulativeStartIndex: 0,
      cumulativeEndIndex: cursor
    };
  });
};

const buildAggregateCellCacheScopeHash = ({
  datasetIds,
  filter,
  cellSizeMeters,
  modeBucketDecimals
}) => sha256Hex({
  value: JSON.stringify({
    datasetIds,
    filter: {
      ...withoutViewportFilter({ filter }),
      cellSizeMeters,
      modeBucketDecimals
    }
  })
});

const buildAggregateCellCacheKey = ({
  userId,
  scopeHash,
  shape,
  cellSizeMeters,
  cellId,
  cacheKeyDatasets
}) => (
  `aggregate-cell:${aggregateCellCacheVersion}:user=${userId}`
  + `:scope=${scopeHash}`
  + `:shape=${shape}`
  + `:size=${cellSizeMeters}`
  + `:cell=${cellId}`
  + `:datasets=${cacheKeyDatasets}`
);

const buildAggregateQueryCacheKey = ({
  userId,
  queryHash,
  cacheKeyDatasets
}) => (
  `aggregate-cells:${aggregateQueryCacheVersion}:user=${userId}`
  + `:query=${queryHash}`
  + `:datasets=${cacheKeyDatasets}`
);

const stripAggregateCellCacheInfo = ({ cell }) => {
  const { cache: _cache, ...rest } = cell;
  return rest;
};

const getRowPopupMetricValue = ({ row, metricKey }) => {
  switch (metricKey) {
    case 'occurredAt':
      return toTimestampMillis({ value: row.occurred_at ?? row.received_at });
    case 'latitude':
      return row.latitude;
    case 'longitude':
      return row.longitude;
    case 'altitudeMeters':
      return row.altitude_meters;
    case 'accuracy':
      return row.accuracy;
    default:
      return row.measurements_json?.[metricKey];
  }
};

const buildAggregateCellPayload = ({
  cell,
  rows,
  selectedMetric,
  popupFieldKeys,
  popupFieldKeysExplicit,
  popupStringFieldKeys,
  modeBucketDecimals
}) => {
  const metricValues = [];
  const popupMetricValues = {};
  const popupDataValues = {};
  const selectedPopupMetricKeys = popupFieldKeysExplicit || popupFieldKeys.length
    ? new Set()
    : null;
  if (selectedPopupMetricKeys) {
    for (const propKey of popupFieldKeys) {
      const aggregateTimeBasePropKey = getAggregateTimeBasePropKey({ value: propKey });
      if (aggregateTimeBasePropKey) {
        selectedPopupMetricKeys.add(aggregateTimeBasePropKey);
      } else if (propKey !== 'occurredAt') {
        selectedPopupMetricKeys.add(propKey);
      }
    }
  }
  let timeRangeStart = null;
  let timeRangeEnd = null;

  for (const row of rows) {
    const metricValue = getRowMetricValue({
      row,
      metricKey: selectedMetric
    });
    if (metricValue !== null && metricValue !== undefined) {
      metricValues.push(metricValue);
    }

    const popupMetricKeys = selectedPopupMetricKeys
      ? [...selectedPopupMetricKeys]
      : [
          'occurredAt',
          'latitude',
          'longitude',
          'altitudeMeters',
          'accuracy',
          ...Object.keys(row.measurements_json ?? {})
        ];
    for (const metricKey of popupMetricKeys) {
      const popupValue = getRowPopupMetricValue({ row, metricKey });
      if (popupValue === null || popupValue === undefined) {
        continue;
      }

      const values = popupMetricValues[metricKey] ?? [];
      values.push(popupValue);
      popupMetricValues[metricKey] = values;
    }

    for (const propKey of popupStringFieldKeys) {
      const popupValue = getRowPopupDataValue({ row, propKey });
      if (popupValue === null || popupValue === undefined) {
        continue;
      }

      const values = popupDataValues[propKey] ?? [];
      values.push(popupValue);
      popupDataValues[propKey] = values;
    }

    const timeValue = row.occurred_at ?? row.received_at;
    if (timeValue) {
      if (!timeRangeStart || timeValue < timeRangeStart) {
        timeRangeStart = timeValue;
      }

      if (!timeRangeEnd || timeValue > timeRangeEnd) {
        timeRangeEnd = timeValue;
      }
    }
  }

  return {
    id: cell.id,
    center: cell.center,
    radiusMeters: cell.radiusMeters,
    pointCount: rows.length,
    timeRange: {
      start: timeRangeStart,
      end: timeRangeEnd
    },
    stats: computeAggregateStats({
      values: metricValues,
      modeBucketDecimals
    }),
    metrics: Object.fromEntries(
      Object.entries(popupMetricValues).map(([metricKey, values]) => [
        metricKey,
        computeAggregateStats({
          values,
          modeBucketDecimals
        })
      ])
    ),
    dataValues: Object.fromEntries(
      Object.entries(popupDataValues).map(([propKey, values]) => [
        propKey,
        [...new Set(values)]
      ])
    )
  };
};

export const createQueryService = ({ db, cache, logger, runtimeConfig, settingsService, datasetService }) => {
  const getHistoricalQueryCacheTtlSeconds = ({ uiConfig }) => {
    const ttlSeconds = Number(uiConfig.cacheTtlSeconds);
    return Number.isFinite(ttlSeconds) && ttlSeconds > 0
      ? Math.trunc(ttlSeconds)
      : defaultHistoricalQueryCacheTtlSeconds;
  };

  const getCacheEligibilityReason = ({ prepared }) => {
    if (prepared.historicalCacheEligible) {
      return 'eligible';
    }

    if (prepared.filter.timeFilterMode === 'relative') {
      return 'relative-time-filter';
    }

    if (!prepared.filter.dateTo) {
      return 'missing-historical-end-time';
    }

    if (!prepared.filter.historicalCacheEligible) {
      return 'request-not-marked-historical';
    }

    return 'not-eligible';
  };

  const logQueryFeature = ({
    caller,
    context = null,
    correlationId = null,
    level = 'debug',
    message
  }) => logFeature({
    caller,
    context,
    correlationId,
    feature: 'query',
    level,
    logger,
    message,
    runtimeConfig
  });

  const logCacheFeature = ({
    caller,
    context = null,
    correlationId = null,
    level = 'debug',
    message
  }) => logFeature({
    caller,
    context,
    correlationId,
    feature: 'cache',
    level,
    logger,
    message,
    runtimeConfig
  });

  const buildAggregateCellCacheContext = ({
    user,
    datasetIds,
    filter,
    cellSizeMeters,
    modeBucketDecimals
  }) => {
    const cacheKeyDatasets = datasetIds.slice().sort().join(',') || 'none';
    const scopeHash = buildAggregateCellCacheScopeHash({
      datasetIds,
      filter,
      cellSizeMeters,
      modeBucketDecimals
    });

    return {
      cacheKeyDatasets,
      scopeHash,
      invalidatePattern: (
        `aggregate-cell:${aggregateCellCacheVersion}:user=${user.id}`
        + `:scope=${scopeHash}`
        + `:shape=${filter.shape}`
        + `:size=${cellSizeMeters}`
        + `:*:datasets=${cacheKeyDatasets}`
      ),
      buildKey: ({ cellId }) => buildAggregateCellCacheKey({
        userId: user.id,
        scopeHash,
        shape: filter.shape,
        cellSizeMeters,
        cellId,
        cacheKeyDatasets
      })
    };
  };

  const buildCacheMeta = ({
    key = null,
    hit,
    reason = null,
    source = null,
    ttlSecondsRemaining = null
  }) => ({
    hit,
    key,
    reason,
    source: source ?? (key ? 'computed' : 'disabled'),
    ttlSecondsRemaining
  });

  const mapRowToRawPoint = ({ row }) => ({
    id: row.id,
    datasetId: row.dataset_id,
    datalogId: row.datalog_id,
    sourceType: row.source_type,
    datalogName: row.datalog_name,
    rawTimestamp: row.raw_timestamp,
    parsedTimeText: row.parsed_time_text,
    occurredAt: row.occurred_at,
    receivedAt: row.received_at,
    latitude: row.latitude,
    longitude: row.longitude,
    accuracy: row.accuracy,
    altitudeMeters: row.altitude_meters,
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
    measurements: row.measurements_json ?? {},
    components: buildStoredComponents({
      source: row,
      extraJson: row.extra_json
    }),
    extra: stripStoredComponentsFromExtra({ extraJson: row.extra_json })
  });

  const buildRawPointResponse = ({
    datasetIds,
    filter,
    rows,
    limit
  }) => ({
    datasetIds,
    metric: filter.metric,
    totalCount: rows.length,
    capped: limit === null ? false : rows.length > limit,
    points: (limit === null ? rows : rows.slice(0, limit)).map((row) => mapRowToRawPoint({ row }))
  });

  const resolveAggregateCellWithCacheInfo = async ({
    baseCell,
    cellCacheKey,
    expectedPopupMetricKeys = [],
    refreshTtlOnRead = false,
    ttlSeconds,
    sourceOnWrite
  }) => {
    const cachedCell = await cache.readJson({
      key: cellCacheKey,
      ttlSeconds,
      includeMeta: true,
      refreshTtlOnRead
    });
    if (cachedCell) {
      if (
        !aggregateCellsLookStaleForPopupMetrics({
          cells: [cachedCell.value],
          metricKeys: expectedPopupMetricKeys
        })
      ) {
        return {
          ...cachedCell.value,
          cache: {
            hit: true,
            key: cellCacheKey,
            source: 'cache',
            ttlSecondsRemaining: cachedCell.ttlSecondsRemaining
          }
        };
      }

      await cache.deleteKey({ key: cellCacheKey });
    }

    await cache.writeJson({
      key: cellCacheKey,
      value: baseCell,
      ttlSeconds
    });
    const ttlSecondsRemaining = await cache.getTtlSeconds({ key: cellCacheKey });

    return {
      ...baseCell,
      cache: {
        hit: sourceOnWrite === 'cache',
        key: cellCacheKey,
        source: sourceOnWrite,
        ttlSecondsRemaining
      }
    };
  };

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

    return [...explicitDatasetIds].sort();
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

  const getLiveUpdateMetricValue = ({ point, metricKey }) => {
    if (metricKey === 'occurredAt') {
      return toTimestampMillis({ value: point.occurredAt ?? point.receivedAt });
    }

    switch (metricKey) {
      case 'latitude':
        return point.latitude;
      case 'longitude':
        return point.longitude;
      case 'accuracy':
        return point.accuracy;
      case 'altitudeMeters':
        return point.altitudeMeters;
      default:
        return point.measurements?.[metricKey] ?? null;
    }
  };

  const pointMatchesLiveUpdateFilter = ({
    datasetIds,
    filter,
    point,
    hardRemoveExcludeAreas
  }) => {
    if (!point || typeof point !== 'object') {
      return false;
    }

    if (!datasetIds.includes(point.datasetId)) {
      return false;
    }

    if (
      filter.datalogSelectionMode === 'include'
      && !filter.datalogIds.includes(point.datalogId)
    ) {
      return false;
    }

    const hasCoordinates = Number.isFinite(point.latitude) && Number.isFinite(point.longitude);
    if (filter.requireCoordinates && !hasCoordinates) {
      return false;
    }

    const pointTimestamp = point.occurredAt ?? point.receivedAt ?? null;
    if (filter.dateFrom && (!pointTimestamp || pointTimestamp < filter.dateFrom)) {
      return false;
    }

    if (filter.dateTo && (!pointTimestamp || pointTimestamp > filter.dateTo)) {
      return false;
    }

    const metricValue = getLiveUpdateMetricValue({
      point,
      metricKey: filter.metric
    });
    if (filter.metricMin !== null && (metricValue === null || metricValue < filter.metricMin)) {
      return false;
    }

    if (filter.metricMax !== null && (metricValue === null || metricValue > filter.metricMax)) {
      return false;
    }

    if (
      (
        filter.minLat !== null
        || filter.maxLat !== null
        || filter.minLon !== null
        || filter.maxLon !== null
        || hardRemoveExcludeAreas.length
      )
      && !hasCoordinates
    ) {
      return false;
    }

    if (filter.minLat !== null && point.latitude < filter.minLat) {
      return false;
    }

    if (filter.maxLat !== null && point.latitude > filter.maxLat) {
      return false;
    }

    if (filter.minLon !== null && filter.maxLon !== null) {
      const longitudeViewport = buildLongitudeViewport({
        minLon: filter.minLon,
        maxLon: filter.maxLon
      });
      if (
        longitudeViewport
        && !longitudeViewport.spansWorld
        && !longitudeViewport.ranges.some((range) => (
          point.longitude >= range.minLon
          && point.longitude <= range.maxLon
        ))
      ) {
        return false;
      }
    }

    return !hardRemoveExcludeAreas.some((area) => (
      area.datasetId === point.datasetId
      && pointIsInsideExcludeArea({
        area,
        point: {
          latitude: point.latitude,
          longitude: point.longitude
        }
      })
    ));
  };

  const prepareResolvedFilter = async ({
    user,
    input,
    correlationId = null
  }) => {
    const normalizedFilter = normalizeFilterInput({ input });
    const resolvedTimeWindow = resolveEffectiveTimeWindow({
      filter: normalizedFilter,
      correlationId
    });
    const filter = {
      ...normalizedFilter,
      dateFrom: resolvedTimeWindow.dateFrom,
      dateTo: resolvedTimeWindow.dateTo
    };
    const datasetIds = await resolveDatasetIds({ user, filter, correlationId });

    return {
      datasetIds,
      filter,
      historicalCacheEligible: resolvedTimeWindow.historicalCacheEligible,
      shortCircuit: (
        !datasetIds.length
        || filter.datalogSelectionMode === 'none'
        || (filter.datalogSelectionMode === 'include' && !filter.datalogIds.length)
      )
    };
  };

  const fetchRowsForPreparedFilter = async ({
    datasetIds,
    filter,
    includeViewport = true
  }) => {
    if (
      !datasetIds.length
      || filter.datalogSelectionMode === 'none'
      || (filter.datalogSelectionMode === 'include' && !filter.datalogIds.length)
    ) {
      return {
        excludeAreas: [],
        rows: []
      };
    }

    const params = [datasetIds];
    const where = ['dlog.dataset_id = ANY($1::text[])'];
    const joins = ['JOIN datalogs dlog ON dlog.id = r.datalog_id'];
    const longitudeViewport = includeViewport
      ? buildLongitudeViewport({
          minLon: filter.minLon,
          maxLon: filter.maxLon
        })
      : null;

    if (!filter.includeHidden) {
      where.push('r.is_hidden = FALSE');
    }

    if (filter.requireCoordinates) {
      where.push('r.latitude IS NOT NULL');
      where.push('r.longitude IS NOT NULL');
    }

    if (filter.dateFrom) {
      params.push(filter.dateFrom);
      where.push(`${rowTimestampSql} >= $${params.length}::timestamptz`);
    }

    if (filter.dateTo) {
      params.push(filter.dateTo);
      where.push(`${rowTimestampSql} <= $${params.length}::timestamptz`);
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

      if (longitudeViewport && !longitudeViewport.spansWorld) {
        const longitudeClauses = longitudeViewport.ranges.map((range) => {
          params.push(range.minLon);
          const minParamIndex = params.length;
          params.push(range.maxLon);
          const maxParamIndex = params.length;

          return `(r.longitude >= $${minParamIndex} AND r.longitude <= $${maxParamIndex})`;
        });

        where.push(`(${longitudeClauses.join(' OR ')})`);
      }
    }

    const coreMetricColumn = getCoreMetricColumn({ propKey: filter.metric });
    const metricExpression = filter.metric === 'occurredAt'
      ? `EXTRACT(EPOCH FROM ${rowTimestampSql}) * 1000.0`
      : coreMetricColumn
        ? `r.${coreMetricColumn}`
        : (() => {
            params.push(filter.metric);
            joins.push(
              `LEFT JOIN reading_numeric_values metric_value
               ON metric_value.reading_id = r.id
              AND metric_value.prop_key = $${params.length}`
            );
            return 'metric_value.numeric_value';
          })();

    if (filter.metricMin !== null) {
      params.push(filter.metricMin);
      where.push(`${metricExpression} >= $${params.length}`);
    }

    if (filter.metricMax !== null) {
      params.push(filter.metricMax);
      where.push(`${metricExpression} <= $${params.length}`);
    }

    if (filter.datalogSelectionMode === 'include' && filter.datalogIds.length) {
      params.push(filter.datalogIds);
      where.push(`dlog.id = ANY($${params.length}::text[])`);
    }

    const result = await db.query(
      `SELECT
         r.id,
         r.datalog_id,
         dlog.dataset_id,
         dlog.source_type,
         dlog.datalog_name,
         dlog.supported_fields_json,
         r.raw_timestamp,
         r.parsed_time_text,
         r.occurred_at,
         r.received_at,
         r.latitude,
         r.longitude,
         r.accuracy,
         r.altitude_meters,
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
         r.extra_json,
         COALESCE(measurement_map.measurements_json, '{}'::jsonb) AS measurements_json
       FROM readings r
       ${joins.join('\n')}
       LEFT JOIN LATERAL (
         SELECT jsonb_object_agg(v.prop_key, v.numeric_value) AS measurements_json
         FROM reading_numeric_values v
         WHERE v.reading_id = r.id
       ) measurement_map ON TRUE
       WHERE ${where.join(' AND ')}
       ORDER BY ${rowTimestampSql} NULLS LAST, r.row_number ASC`,
      params
    );

    const excludeAreas = filter.applyExcludeAreas
      ? await loadExcludeAreas({ datasetIds })
      : [];

    return {
      excludeAreas,
      rows: applyExcludeAreasToRows({ rows: result.rows, excludeAreas })
    };
  };

  const countRowsForPreparedFilter = async ({
    datasetIds,
    filter,
    includeViewport = true
  }) => {
    if (
      !datasetIds.length
      || filter.datalogSelectionMode === 'none'
      || (filter.datalogSelectionMode === 'include' && !filter.datalogIds.length)
    ) {
      return 0;
    }

    const params = [datasetIds];
    const where = ['dlog.dataset_id = ANY($1::text[])'];
    const joins = ['JOIN datalogs dlog ON dlog.id = r.datalog_id'];
    const longitudeViewport = includeViewport
      ? buildLongitudeViewport({
          minLon: filter.minLon,
          maxLon: filter.maxLon
        })
      : null;

    if (!filter.includeHidden) {
      where.push('r.is_hidden = FALSE');
    }

    if (filter.requireCoordinates) {
      where.push('r.latitude IS NOT NULL');
      where.push('r.longitude IS NOT NULL');
    }

    if (filter.dateFrom) {
      params.push(filter.dateFrom);
      where.push(`${rowTimestampSql} >= $${params.length}::timestamptz`);
    }

    if (filter.dateTo) {
      params.push(filter.dateTo);
      where.push(`${rowTimestampSql} <= $${params.length}::timestamptz`);
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

      if (longitudeViewport && !longitudeViewport.spansWorld) {
        const longitudeClauses = longitudeViewport.ranges.map((range) => {
          params.push(range.minLon);
          const minParamIndex = params.length;
          params.push(range.maxLon);
          const maxParamIndex = params.length;

          return `(r.longitude >= $${minParamIndex} AND r.longitude <= $${maxParamIndex})`;
        });

        where.push(`(${longitudeClauses.join(' OR ')})`);
      }
    }

    if (filter.metricMin !== null || filter.metricMax !== null) {
      const coreMetricColumn = getCoreMetricColumn({ propKey: filter.metric });
      const metricExpression = filter.metric === 'occurredAt'
        ? `EXTRACT(EPOCH FROM ${rowTimestampSql}) * 1000.0`
        : coreMetricColumn
          ? `r.${coreMetricColumn}`
          : (() => {
              params.push(filter.metric);
              joins.push(
                `LEFT JOIN reading_numeric_values metric_value
                 ON metric_value.reading_id = r.id
                AND metric_value.prop_key = $${params.length}`
              );
              return 'metric_value.numeric_value';
            })();

      if (filter.metricMin !== null) {
        params.push(filter.metricMin);
        where.push(`${metricExpression} >= $${params.length}`);
      }

      if (filter.metricMax !== null) {
        params.push(filter.metricMax);
        where.push(`${metricExpression} <= $${params.length}`);
      }
    }

    if (filter.datalogSelectionMode === 'include' && filter.datalogIds.length) {
      params.push(filter.datalogIds);
      where.push(`dlog.id = ANY($${params.length}::text[])`);
    }

    const result = await db.query(
      `SELECT COUNT(*)::integer AS total_count
       FROM readings r
       ${joins.join('\n')}
       WHERE ${where.join(' AND ')}`,
      params
    );

    return Number(result.rows[0]?.total_count ?? 0);
  };

  const fetchFilteredRows = async ({
    user,
    input,
    correlationId = null,
    includeViewport = true
  }) => {
    const prepared = await prepareResolvedFilter({
      user,
      input,
      correlationId
    });
    if (prepared.shortCircuit) {
      return {
        datasetIds: prepared.datasetIds,
        filter: prepared.filter,
        historicalCacheEligible: prepared.historicalCacheEligible,
        rows: [],
        excludeAreas: []
      };
    }

    const result = await fetchRowsForPreparedFilter({
      datasetIds: prepared.datasetIds,
      filter: prepared.filter,
      includeViewport
    });

    return {
      datasetIds: prepared.datasetIds,
      filter: prepared.filter,
      historicalCacheEligible: prepared.historicalCacheEligible,
      ...result
    };
  };

  const getRawPoints = async ({ user, input, correlationId = null }) => {
    const uiConfig = await settingsService.getUiConfig();
    const historicalQueryCacheTtlSeconds = getHistoricalQueryCacheTtlSeconds({ uiConfig });
    const prepared = await prepareResolvedFilter({
      user,
      input,
      correlationId
    });
    const requestedLimit = Number.isInteger(prepared.filter.limit) && prepared.filter.limit > 0
      ? prepared.filter.limit
      : null;
    const limit = prepared.filter.uncappedRawPoints
      ? null
      : requestedLimit ?? uiConfig.rawPointCap;
    const cacheEnabled = prepared.historicalCacheEligible && !prepared.filter.uncappedRawPoints;
    const cacheEligibilityReason = prepared.filter.uncappedRawPoints
      ? 'uncapped raw point query'
      : getCacheEligibilityReason({ prepared });
    const cacheKeyDatasets = prepared.datasetIds.slice().sort().join(',') || 'none';
    const cacheKey = cacheEnabled
      ? buildRawPointCacheKey({
          userId: user.id,
          queryHash: sha256Hex({
            value: JSON.stringify({
              datasetIds: prepared.datasetIds,
              filter: {
                ...toResultFilter({ filter: prepared.filter }),
                limit
              }
            })
          }),
          cacheKeyDatasets
        })
      : null;

    logQueryFeature({
      caller: 'query::getRawPoints',
      correlationId,
      level: 'debug',
      message: 'Raw point query prepared.',
      context: {
        cacheEnabled,
        cacheEligibilityReason,
        datasetCount: prepared.datasetIds.length,
        dateFrom: prepared.filter.dateFrom,
        dateTo: prepared.filter.dateTo,
        forceRecheck: prepared.filter.forceRecheck,
        limit,
        timeFilterMode: prepared.filter.timeFilterMode
      }
    });

    if (cacheEnabled && prepared.filter.forceRecheck) {
      await cache.deleteKey({ key: cacheKey });
    }

    if (cacheEnabled) {
      const cached = await cache.readJson({
        key: cacheKey,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        includeMeta: true
      });
      if (cached) {
        logCacheFeature({
          caller: 'query::getRawPoints.cache',
          correlationId,
          level: 'info',
          message: 'Raw point query cache hit.',
          context: {
            cacheKey,
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          }
        });
        return {
          ...cached.value,
          cache: buildCacheMeta({
            key: cacheKey,
            hit: true,
            source: 'cache',
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          })
        };
      }
    }

    const rowResult = prepared.shortCircuit
      ? { rows: [] }
      : await fetchRowsForPreparedFilter({
          datasetIds: prepared.datasetIds,
          filter: prepared.filter
        });
    const response = buildRawPointResponse({
      datasetIds: prepared.datasetIds,
      filter: prepared.filter,
      rows: rowResult.rows,
      limit
    });

    if (!cacheEnabled) {
      logCacheFeature({
        caller: 'query::getRawPoints.cache',
        correlationId,
        level: 'debug',
        message: 'Raw point query cache skipped.',
        context: {
          cacheEligibilityReason
        }
      });
      return {
        ...response,
        cache: buildCacheMeta({
          hit: false,
          reason: cacheEligibilityReason
        })
      };
    }

    await cache.writeJson({
      key: cacheKey,
      value: response,
      ttlSeconds: historicalQueryCacheTtlSeconds
    });
    const ttlSecondsRemaining = await cache.getTtlSeconds({ key: cacheKey });
    logCacheFeature({
      caller: 'query::getRawPoints.cache',
      correlationId,
      level: 'info',
      message: 'Raw point query cache written.',
      context: {
        cacheKey,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        ttlSecondsRemaining
      }
    });
    return {
      ...response,
      cache: buildCacheMeta({
        key: cacheKey,
        hit: false,
        source: 'computed',
        ttlSecondsRemaining
      })
    };
  };

  const getPointCount = async ({ user, input, correlationId = null, includeViewport = true }) => {
    const prepared = await prepareResolvedFilter({
      user,
      input,
      correlationId
    });

    return {
      datasetIds: prepared.datasetIds,
      totalCount: prepared.shortCircuit
        ? 0
        : await countRowsForPreparedFilter({
            datasetIds: prepared.datasetIds,
            filter: prepared.filter,
            includeViewport
        })
    };
  };

  const getTimeSliceSource = async ({ user, input, correlationId = null }) => {
    const uiConfig = await settingsService.getUiConfig();
    const historicalQueryCacheTtlSeconds = getHistoricalQueryCacheTtlSeconds({ uiConfig });
    const settings = normalizeTimeSliceSettings({ input });
    const prepared = await prepareResolvedFilter({
      user,
      input: {
        ...input,
        requireCoordinates: true
      },
      correlationId
    });
    const windows = buildTimeSliceWindowsForFilter({
      filter: prepared.filter,
      settings
    });
    const cacheEnabled = prepared.historicalCacheEligible;
    const cacheEligibilityReason = getCacheEligibilityReason({ prepared });
    const cacheKeyDatasets = prepared.datasetIds.slice().sort().join(',') || 'none';
    const cacheableFilter = withoutViewportFilter({ filter: prepared.filter });
    const cacheKey = cacheEnabled
      ? buildTimeSliceSourceCacheKey({
          userId: user.id,
          queryHash: sha256Hex({
            value: JSON.stringify({
              datasetIds: prepared.datasetIds,
              filter: cacheableFilter,
              settings
            })
          }),
          cacheKeyDatasets
        })
      : null;

    logQueryFeature({
      caller: 'query::getTimeSliceSource',
      correlationId,
      level: 'debug',
      message: 'Time slice source query prepared.',
      context: {
        cacheEnabled,
        cacheEligibilityReason,
        datasetCount: prepared.datasetIds.length,
        dateFrom: prepared.filter.dateFrom,
        dateTo: prepared.filter.dateTo,
        forceRecheck: prepared.filter.forceRecheck,
        intervalAmount: settings.amount,
        intervalUnit: settings.unit,
        timeFilterMode: prepared.filter.timeFilterMode,
        windowCount: windows.length
      }
    });

    if (cacheEnabled && prepared.filter.forceRecheck) {
      await cache.deleteKey({ key: cacheKey });
    }

    if (cacheEnabled) {
      const cached = await cache.readJson({
        key: cacheKey,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        includeMeta: true
      });
      if (cached) {
        logCacheFeature({
          caller: 'query::getTimeSliceSource.cache',
          correlationId,
          level: 'info',
          message: 'Time slice source cache hit.',
          context: {
            cacheKey,
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          }
        });
        return {
          ...cached.value,
          cache: buildCacheMeta({
            key: cacheKey,
            hit: true,
            source: 'cache',
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          })
        };
      }
    }

    const rowResult = prepared.shortCircuit
      ? { rows: [] }
      : await fetchRowsForPreparedFilter({
          datasetIds: prepared.datasetIds,
          filter: prepared.filter,
          includeViewport: false
        });
    const response = {
      datasetIds: prepared.datasetIds,
      metric: prepared.filter.metric,
      sliceSettings: settings,
      totalCount: rowResult.rows.length,
      points: rowResult.rows.map((row) => mapRowToRawPoint({ row })),
      windows: buildTimeSlicePreparedWindows({
        rows: rowResult.rows,
        windows
      })
    };

    if (!cacheEnabled) {
      logCacheFeature({
        caller: 'query::getTimeSliceSource.cache',
        correlationId,
        level: 'debug',
        message: 'Time slice source cache skipped.',
        context: {
          cacheEligibilityReason
        }
      });
      return {
        ...response,
        cache: buildCacheMeta({
          hit: false,
          reason: cacheEligibilityReason
        })
      };
    }

    await cache.writeJson({
      key: cacheKey,
      value: response,
      ttlSeconds: historicalQueryCacheTtlSeconds
    });
    const ttlSecondsRemaining = await cache.getTtlSeconds({ key: cacheKey });
    logCacheFeature({
      caller: 'query::getTimeSliceSource.cache',
      correlationId,
      level: 'info',
      message: 'Time slice source cache written.',
      context: {
        cacheKey,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        ttlSecondsRemaining
      }
    });
    return {
      ...response,
      cache: buildCacheMeta({
        key: cacheKey,
        hit: false,
        source: 'computed',
        ttlSecondsRemaining
      })
    };
  };

  const getTimeBounds = async ({ user, input, correlationId = null }) => {
    const prepared = await prepareResolvedFilter({
      user,
      input: {
        ...input,
        dateFrom: null,
        dateTo: null,
        timeFilterMode: 'none',
        metricMin: null,
        metricMax: null,
        minLat: null,
        maxLat: null,
        minLon: null,
        maxLon: null
      },
      correlationId
    });

    if (prepared.shortCircuit) {
      return {
        datasetIds: prepared.datasetIds,
        start: null,
        end: null,
        totalCount: 0
      };
    }

    const params = [prepared.datasetIds];
    const where = [
      'dlog.dataset_id = ANY($1::text[])',
      `${rowTimestampSql} IS NOT NULL`
    ];

    if (!prepared.filter.includeHidden) {
      where.push('r.is_hidden = FALSE');
    }

    if (prepared.filter.datalogSelectionMode === 'include' && prepared.filter.datalogIds.length) {
      params.push(prepared.filter.datalogIds);
      where.push(`dlog.id = ANY($${params.length}::text[])`);
    }

    const result = await db.query(
      `SELECT
         MIN(${rowTimestampSql}) AS start_time,
         MAX(${rowTimestampSql}) AS end_time,
         COUNT(*)::integer AS total_count
       FROM readings r
       JOIN datalogs dlog ON dlog.id = r.datalog_id
       WHERE ${where.join(' AND ')}`,
      params
    );
    const row = result.rows[0] ?? {};

    return {
      datasetIds: prepared.datasetIds,
      start: row.start_time ?? null,
      end: row.end_time ?? null,
      totalCount: Number(row.total_count ?? 0)
    };
  };

  const getAggregateCellPoints = async ({ user, input, correlationId = null }) => {
    const normalizedInput = normalizeFilterInput({ input });
    const cellId = typeof input?.cellId === 'string'
      ? input.cellId.trim()
      : '';

    if (!cellId) {
      throw createAppError({
        caller: 'query::getAggregateCellPoints',
        reason: 'Aggregate cell points require a cell id.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const cellSizeMeters = normalizedInput.cellSizeMeters;
    if (!Number.isFinite(cellSizeMeters) || cellSizeMeters <= 0) {
      throw createAppError({
        caller: 'query::getAggregateCellPoints',
        reason: 'Aggregate cell points require a positive cell size.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const rowResult = await fetchFilteredRows({
      user,
      input: {
        ...input,
        minLat: null,
        maxLat: null,
        minLon: null,
        maxLon: null
      },
      correlationId,
      includeViewport: false
    });

    const points = rowResult.rows
      .filter((row) => (
        typeof row.latitude === 'number'
        && typeof row.longitude === 'number'
        && buildAggregateCell({
          point: {
            latitude: row.latitude,
            longitude: row.longitude
          },
          shape: normalizedInput.shape,
          cellSizeMeters
        }).id === cellId
      ))
      .map((row) => mapRowToRawPoint({ row }));

    return {
      datasetIds: rowResult.datasetIds,
      metric: rowResult.filter.metric,
      shape: normalizedInput.shape,
      cellId,
      cellSizeMeters,
      totalCount: points.length,
      points
    };
  };

  const getAggregates = async ({ user, input, correlationId = null }) => {
    const [uiConfig, userSettings] = await Promise.all([
      settingsService.getUiConfig(),
      settingsService.getUserSettings({ userId: user.id })
    ]);
    const historicalQueryCacheTtlSeconds = getHistoricalQueryCacheTtlSeconds({ uiConfig });
    const cellCacheRefreshTtlOnRead = userSettings.cellCacheRefreshTtlOnRead === true;
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
    const prepared = await prepareResolvedFilter({
      user,
      input: aggregateInput,
      correlationId
    });
    const cacheableFilter = cacheViewport
      ? prepared.filter
      : withoutViewportFilter({ filter: prepared.filter });
    const cellSizeMeters = prepared.filter.cellSizeMeters ?? uiConfig.defaultCellSizeMeters;
    const modeBucketDecimals = prepared.filter.modeBucketDecimals ?? uiConfig.modeBucketDecimals;
    const cacheEnabled = prepared.historicalCacheEligible;
    const cacheEligibilityReason = getCacheEligibilityReason({ prepared });
    const cacheKeyDatasets = prepared.datasetIds.slice().sort().join(',') || 'none';
    const expectedPopupMetricKeys = getExpectedPopupMetricKeys({ filter: prepared.filter });
    const cellCacheContext = cacheEnabled
      ? buildAggregateCellCacheContext({
          user,
          datasetIds: prepared.datasetIds,
          filter: cacheableFilter,
          cellSizeMeters,
          modeBucketDecimals
        })
      : null;
    const cacheKey = cacheEnabled
      ? buildAggregateQueryCacheKey({
          userId: user.id,
          queryHash: sha256Hex({
            value: JSON.stringify({
              datasetIds: prepared.datasetIds,
              filter: {
                ...cacheableFilter,
                cellSizeMeters,
                modeBucketDecimals
              }
            })
          }),
          cacheKeyDatasets
        })
      : null;

    logQueryFeature({
      caller: 'query::getAggregates',
      correlationId,
      level: 'debug',
      message: 'Aggregate query prepared.',
      context: {
        cacheEnabled,
        cacheEligibilityReason,
        cacheViewportEnabled: Boolean(cacheViewport),
        cellSizeMeters,
        datasetCount: prepared.datasetIds.length,
        dateFrom: prepared.filter.dateFrom,
        dateTo: prepared.filter.dateTo,
        forceRecheck: prepared.filter.forceRecheck,
        modeBucketDecimals,
        cellCacheRefreshTtlOnRead,
        shape: prepared.filter.shape,
        timeFilterMode: prepared.filter.timeFilterMode,
        zoom: requestFilter.zoom
      }
    });

    if (cacheEnabled && prepared.filter.forceRecheck) {
      await cache.deleteKey({ key: cacheKey });
      await cache.deletePattern({ pattern: cellCacheContext.invalidatePattern });
    }

    if (cacheEnabled) {
      const cached = await cache.readJson({
        key: cacheKey,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        includeMeta: true
      });
      if (cached) {
        const visibleCells = clipAggregateCellsToViewport({
          cells: cached.value.cells,
          viewport: viewportFilter
        });
        if (
          aggregateCellsLookStaleForPopupMetrics({
            cells: cached.value.cells,
            metricKeys: expectedPopupMetricKeys
          })
        ) {
          await cache.deleteKey({ key: cacheKey });
          await cache.deletePattern({ pattern: cellCacheContext.invalidatePattern });
        } else {
        logCacheFeature({
          caller: 'query::getAggregates.cache',
          correlationId,
          level: 'info',
          message: 'Aggregate query cache hit.',
          context: {
            cacheKey,
            cachedCellCount: cached.value.cells.length,
            cellCacheRefreshTtlOnRead,
            visibleCellCount: visibleCells.length,
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          }
        });

        return {
          ...cached.value,
          cells: await Promise.all(visibleCells.map((cell) => resolveAggregateCellWithCacheInfo({
            baseCell: cell,
            cellCacheKey: cellCacheContext.buildKey({ cellId: cell.id }),
            expectedPopupMetricKeys,
            refreshTtlOnRead: cellCacheRefreshTtlOnRead,
            ttlSeconds: historicalQueryCacheTtlSeconds,
            sourceOnWrite: 'cache'
          }))),
          cache: buildCacheMeta({
            key: cacheKey,
            hit: true,
            source: 'cache',
            ttlSecondsRemaining: cached.ttlSecondsRemaining
          })
        };
        }
      }
    }

    const rowResult = prepared.shortCircuit
      ? { rows: [] }
      : await fetchRowsForPreparedFilter({
          datasetIds: prepared.datasetIds,
          filter: prepared.filter,
          includeViewport: Boolean(cacheViewport)
        });
    const rows = rowResult.rows;

    if (!rows.length) {
      const empty = {
        datasetIds: prepared.datasetIds,
        metric: cacheableFilter.metric,
        shape: cacheableFilter.shape,
        cellSizeMeters,
        cells: []
      };

      if (!cacheEnabled) {
        logCacheFeature({
          caller: 'query::getAggregates.cache',
          correlationId,
          level: 'debug',
          message: 'Empty aggregate query cache skipped.',
          context: {
            cacheEligibilityReason
          }
        });
        return {
          ...empty,
          cache: buildCacheMeta({
            hit: false,
            reason: cacheEligibilityReason
          })
        };
      }

      await cache.writeJson({
        key: cacheKey,
        value: empty,
        ttlSeconds: historicalQueryCacheTtlSeconds
      });
      const ttlSecondsRemaining = await cache.getTtlSeconds({ key: cacheKey });
      logCacheFeature({
        caller: 'query::getAggregates.cache',
        correlationId,
        level: 'info',
        message: 'Empty aggregate query cache written.',
        context: {
          cacheKey,
          ttlSeconds: historicalQueryCacheTtlSeconds,
          ttlSecondsRemaining
        }
      });
      return {
        ...empty,
        cache: buildCacheMeta({
          key: cacheKey,
          hit: false,
          source: 'computed',
          ttlSecondsRemaining
        })
      };
    }

    const requestedPopupFieldKeys = new Set(prepared.filter.popupFieldKeys);
    const popupStringFieldKeys = [...new Set(
      rows.flatMap((row) => normalizeSupportedFields({ value: row.supported_fields_json })
        .filter((field) => field.valueType === 'string' && !isSyntheticPopupField({ propKey: field.propKey }))
        .filter((field) => (
          (!prepared.filter.popupFieldKeysExplicit && !requestedPopupFieldKeys.size)
          || requestedPopupFieldKeys.has(field.propKey)
        ))
        .map((field) => field.propKey))
    )];

    const cellGroups = new Map();
    for (const row of rows) {
      const cell = buildAggregateCell({
        point: {
          latitude: row.latitude,
          longitude: row.longitude
        },
        shape: cacheableFilter.shape,
        cellSizeMeters
      });
      const current = cellGroups.get(cell.id) ?? {
        cell,
        rows: []
      };

      current.rows.push(row);
      cellGroups.set(cell.id, current);
    }

    const resolvedCells = await Promise.all(
      [...cellGroups.values()].map(async ({ cell, rows: cellRows }) => {
        const baseCell = buildAggregateCellPayload({
          cell,
          rows: cellRows,
          selectedMetric: prepared.filter.metric,
          popupFieldKeys: prepared.filter.popupFieldKeys,
          popupFieldKeysExplicit: prepared.filter.popupFieldKeysExplicit,
          popupStringFieldKeys,
          modeBucketDecimals
        });

        if (!cacheEnabled) {
          return baseCell;
        }

        return resolveAggregateCellWithCacheInfo({
          baseCell,
          cellCacheKey: cellCacheContext.buildKey({ cellId: cell.id }),
          expectedPopupMetricKeys,
          refreshTtlOnRead: cellCacheRefreshTtlOnRead,
          ttlSeconds: historicalQueryCacheTtlSeconds,
          sourceOnWrite: 'computed'
        });
      })
    );
    logCacheFeature({
      caller: 'query::getAggregates.cache',
      correlationId,
      level: 'debug',
      message: cacheEnabled ? 'Aggregate cell cache resolution completed.' : 'Aggregate cell cache skipped.',
      context: {
        cacheEnabled,
        cacheEligibilityReason,
        cellCount: resolvedCells.length,
        cellsWithCacheMeta: resolvedCells.filter((cell) => Boolean(cell.cache)).length,
        cellsWithCacheKey: resolvedCells.filter((cell) => Boolean(cell.cache?.key)).length,
        cellsWithTtl: resolvedCells.filter((cell) => cell.cache?.ttlSecondsRemaining !== null && cell.cache?.ttlSecondsRemaining !== undefined).length,
        cellCacheRefreshTtlOnRead,
        sampleCache: resolvedCells.find((cell) => cell.cache)?.cache ?? null
      }
    });

    const fullResponse = {
      datasetIds: prepared.datasetIds,
      metric: cacheableFilter.metric,
      shape: cacheableFilter.shape,
      cellSizeMeters,
      cells: cacheEnabled
        ? resolvedCells.map((cell) => stripAggregateCellCacheInfo({ cell }))
        : resolvedCells
    };

    if (!cacheEnabled) {
      logCacheFeature({
        caller: 'query::getAggregates.cache',
        correlationId,
        level: 'debug',
        message: 'Aggregate query cache skipped.',
        context: {
          cacheEligibilityReason,
          responseCellCount: resolvedCells.length
        }
      });
      return {
        ...fullResponse,
        cells: clipAggregateCellsToViewport({
          cells: resolvedCells,
          viewport: viewportFilter
        }),
        cache: buildCacheMeta({
          hit: false,
          reason: cacheEligibilityReason
        })
      };
    }

    await cache.writeJson({
      key: cacheKey,
      value: fullResponse,
      ttlSeconds: historicalQueryCacheTtlSeconds
    });
    const ttlSecondsRemaining = await cache.getTtlSeconds({ key: cacheKey });
    logCacheFeature({
      caller: 'query::getAggregates.cache',
      correlationId,
      level: 'info',
      message: 'Aggregate query cache written.',
      context: {
        cacheKey,
        cellCacheRefreshTtlOnRead,
        responseCellCount: fullResponse.cells.length,
        returnedCellCount: resolvedCells.length,
        ttlSeconds: historicalQueryCacheTtlSeconds,
        ttlSecondsRemaining
      }
    });
    return {
      ...fullResponse,
      cells: clipAggregateCellsToViewport({
        cells: resolvedCells,
        viewport: viewportFilter
      }),
      cache: buildCacheMeta({
        key: cacheKey,
        hit: false,
        source: 'computed',
        ttlSecondsRemaining
      })
    };
  };

  const invalidateDatasets = async ({ datasetIds }) => {
    for (const datasetId of datasetIds) {
      await cache.deletePattern({ pattern: `raw-points:*${datasetId}*` });
      await cache.deletePattern({ pattern: `time-slice-source:*${datasetId}*` });
      await cache.deletePattern({ pattern: `aggregate:*${datasetId}*` });
      await cache.deletePattern({ pattern: `aggregate-cells:*${datasetId}*` });
      await cache.deletePattern({ pattern: `aggregate-cell:${aggregateCellCacheVersion}:*${datasetId}*` });
    }
  };

  const invalidateAggregateCellsForPoint = async ({
    datasetIds,
    point,
    cellSizeMetersValues = [],
    shapes = ['hexagon', 'square', 'circle']
  }) => {
    if (!Array.isArray(datasetIds) || !datasetIds.length || !point) {
      return;
    }

    const uiConfig = await settingsService.getUiConfig();
    const normalizedCellSizes = [...new Set(
      [
        uiConfig.defaultCellSizeMeters,
        ...aggregateCellInvalidationDefaultSizes,
        ...cellSizeMetersValues
      ]
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value) && value > 0)
    )].sort((left, right) => left - right);
    const normalizedShapes = [...new Set(
      shapes.filter((shape) => ['hexagon', 'square', 'circle'].includes(shape))
    )];

    for (const datasetId of datasetIds) {
      await cache.deletePattern({ pattern: `aggregate-cells:*${datasetId}*` });
    }

    for (const shape of normalizedShapes) {
      for (const cellSizeMetersValue of normalizedCellSizes) {
        const cell = buildAggregateCell({
          point,
          shape,
          cellSizeMeters: cellSizeMetersValue
        });

        for (const datasetId of datasetIds) {
          await cache.deletePattern({
            pattern: (
              `aggregate-cell:${aggregateCellCacheVersion}:*:shape=${shape}`
              + `:size=${cellSizeMetersValue}`
              + `:cell=${cell.id}:*${datasetId}*`
            )
          });
        }
      }
    }
  };

  const invalidateLivePoint = async ({
    datasetIds,
    point
  }) => {
    if (!Array.isArray(datasetIds) || !datasetIds.length || !point) {
      return;
    }

    for (const datasetId of datasetIds) {
      await cache.deletePattern({ pattern: `raw-points:*${datasetId}*` });
      await cache.deletePattern({ pattern: `time-slice-source:*${datasetId}*` });
      await cache.deletePattern({ pattern: `aggregate:*${datasetId}*` });
    }

    await invalidateAggregateCellsForPoint({
      datasetIds,
      point
    });
  };

  const createLiveUpdateMatcher = async ({
    user,
    input,
    correlationId = null
  }) => {
    const prepared = await prepareResolvedFilter({
      user,
      input,
      correlationId
    });
    const hardRemoveExcludeAreas = prepared.filter.applyExcludeAreas
      ? (await loadExcludeAreas({ datasetIds: prepared.datasetIds }))
          .filter((entry) => (entry.effectType ?? 'hard_remove') === 'hard_remove')
      : [];

    return {
      matches: ({ point }) => {
        if (prepared.shortCircuit) {
          return false;
        }

        return pointMatchesLiveUpdateFilter({
          datasetIds: prepared.datasetIds,
          filter: prepared.filter,
          point,
          hardRemoveExcludeAreas
        });
      }
    };
  };

  return {
    createLiveUpdateMatcher,
    fetchFilteredRows,
    getPointCount,
    getRawPoints,
    getTimeSliceSource,
    getTimeBounds,
    getAggregateCellPoints,
    getAggregates,
    invalidateDatasets,
    invalidateAggregateCellsForPoint,
    invalidateLivePoint
  };
};
