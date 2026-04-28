<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { onDestroy, onMount, untrack } from 'svelte';
  import LeafletMap from '$lib/components/LeafletMap.svelte';
  import MapAggregatePointsModal from '$lib/components/MapAggregatePointsModal.svelte';
  import { apiFetch, resolveApiWebSocketPath } from '$lib/api/client';
  import {
    aggregateDataCountPropKey,
    getAggregateTimeBasePropKey,
    getMetricOptionLabels,
    humanizePropKey,
    isAggregateTimePropKey,
    mergePopupFields,
    normalizePropKey,
    normalizeSupportedFields,
    type MetricField
  } from '$lib/datalog-fields';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';
  import {
    isMapDatasetDefaultEnabled,
    loadMapDatasetDefaults,
    loadMapDatasetOrder,
    orderMapDatasets,
    type MapDatasetDefaultRecord
  } from '$lib/map-dataset-defaults';
  import {
    isMapFieldVisible,
    loadMapFieldOrder,
    loadMapFieldVisibility,
    orderMapFields,
    type MapFieldOrder,
    type MapFieldVisibilityRecord
  } from '$lib/map-field-visibility';
  import { sessionStore } from '$lib/stores/session';

  type AggregateStats = {
    min: number | null;
    max: number | null;
    mean: number | null;
    median: number | null;
    mode: number | null;
    count: number;
  };

  type AggregateCache = {
    hit: boolean;
    reason?: string | null;
    key: string | null;
    source: 'cache' | 'computed' | 'disabled';
    ttlSecondsRemaining: number | null;
  };

  type AggregateCell = {
    id: string;
    center: {
      latitude: number;
      longitude: number;
    };
    radiusMeters: number;
    pointCount: number;
    timeRange: {
      start: string | null;
      end: string | null;
    };
    stats: AggregateStats;
    metrics: Record<string, AggregateStats>;
    dataValues: Record<string, string[]>;
    cache?: AggregateCache;
  };

  type MapPoint = {
    id: string;
    datasetId: string;
    datalogId: string;
    datalogName?: string | null;
    rawTimestamp?: string | null;
    parsedTimeText?: string | null;
    latitude: number;
    longitude: number;
    occurredAt: string | null;
    receivedAt?: string | null;
    altitudeMeters: number | null;
    accuracy: number | null;
    measurements: Record<string, number>;
    deviceId?: string | null;
    deviceName?: string | null;
    deviceType?: string | null;
    deviceCalibration?: string | null;
    firmwareVersion?: string | null;
    sourceReadingId?: string | null;
    custom?: string | null;
    comment: string | null;
    components?: Record<string, string>;
    extra?: Record<string, unknown>;
  };

  type AggregateCellModalState = {
    cellId: string;
    queryKey: string;
    shape: AppliedMapFilters['shape'];
    cellSizeMeters: number;
    cell: AggregateCell;
  };

  type AggregateCellModalColumn = {
    propKey: string;
    label: string;
    valueType: 'number' | 'time' | 'string';
  };

  type AggregateCellModalValue = string | number | boolean | null;

  type AggregateCellModalRow = {
    id: string;
    values: Record<string, AggregateCellModalValue>;
  };

  type AggregateCellModalExportContext = {
    build: string | null;
    fileNameBase: string;
    filters: Record<string, unknown>;
  };

  type DatasetOption = {
    id: string;
    name: string;
    trackCount: number;
    readingCount: number;
    updatedAt: string | null;
  };

  type CombinedDatasetOption = {
    id: string;
    name: string;
    memberCount: number;
    updatedAt: string | null;
  };

  type TrackOption = {
    id: string;
    datasetId: string;
    datasetName: string;
    trackName: string | null;
    rowCount: number;
    supportedFields: MetricField[];
    metricFields: MetricField[];
  };

  type BasemapKey = 'default' | 'satellite' | 'light' | 'topo';

  type BasemapOption = {
    key: BasemapKey;
    label: string;
    tileUrlTemplate: string;
    attribution: string;
  };

  type MapViewport = {
    centerLat: number;
    centerLon: number;
    minLat: number;
    maxLat: number;
    minLon: number;
    maxLon: number;
    zoom: number;
  };

  type MapFocus = {
    latitude: number;
    longitude: number;
    zoom: number;
    key: string;
  };

  type TimeFilterMode = 'none' | 'absolute' | 'relative';
  type TimeFilterPrecision = 'date' | 'datetime';
  type TimeFilterRelativeUnit = 'hours' | 'days';
  type TimeSliceUnit = 'minutes' | 'hours' | 'days';
  type SidebarSize = 'small' | 'large';
  type UrlSelectionMode = 'explicit' | 'none' | 'all' | 'default';

  type MapFilters = {
    datasetIds: string[];
    combinedDatasetIds: string[];
    trackIds: string[];
    trackSelectionMode: 'all' | 'include' | 'none';
    metric: string;
    mode: 'aggregate' | 'raw';
    shape: 'hexagon' | 'square' | 'circle';
    aggregateStat: 'min' | 'max' | 'mean' | 'median' | 'mode' | 'count';
    cellSizeMeters: number;
    autoCellSize: boolean;
    applyExcludeAreas: boolean;
    timeFilterMode: TimeFilterMode;
    timeFilterPrecision: TimeFilterPrecision;
    timeFilterStart: string;
    timeFilterEnd: string;
    timeFilterRelativeAmount: number;
    timeFilterRelativeUnit: TimeFilterRelativeUnit;
    forceRecheck: boolean;
  };

  type AppliedMapFilters = MapFilters & {
    appliedCellSizeMeters: number;
  };

  type MapQuerySnapshot = {
    filters: AppliedMapFilters;
    viewport: MapViewport;
    key: string;
  };

  type PopupFields = {
    metrics: Record<string, boolean>;
  };

  type PopupStateSnapshot = {
    metrics: Record<string, boolean>;
    touched: Record<string, boolean>;
  };

  type ColorScaleSettings = {
    auto: boolean;
    low: number;
    mid: number;
    high: number;
  };

  type ColorScaleThresholds = {
    low: number;
    mid: number;
    high: number;
  };

  type LiveUpdateEnvelope = {
    type: 'ready' | 'subscribed' | 'updates-available' | 'error';
    currentCursor?: number;
    currentPublishedAt?: string;
    latestCursor?: number;
    latestPublishedAt?: string;
    updateCount?: number;
    reason?: string;
  };

  type LiveUpdatePointDetail = {
    cursor: number;
    publishedAt: string;
    id: string;
    datasetId: string;
    datalogId: string;
    occurredAt: string | null;
    receivedAt: string | null;
    latitude: number | null;
    longitude: number | null;
    altitudeMeters: number | null;
    accuracy: number | null;
    measurements: Record<string, number>;
  };

  type LiveUpdateStatus = {
    hasUpdates: boolean;
    updateCount: number;
    latestCursor?: number;
    latestPublishedAt?: string;
    currentCursor?: number;
    currentPublishedAt?: string;
    points?: LiveUpdatePointDetail[];
  };

  type LiveUpdateLogEntry = {
    key: string;
    cursor: number;
    publishedAt: string;
    point: LiveUpdatePointDetail;
  };

  type LiveUpdateMarker = {
    id: string;
    latitude: number;
    longitude: number;
    occurredAt: string | null;
    publishedAt: string;
  };

  type TimeSliceSettings = {
    amount: number;
    unit: TimeSliceUnit;
  };

  type TimeSliceWindow = {
    index: number;
    startIso: string;
    endIso: string;
    startMs: number;
    endMs: number;
    isLast: boolean;
  };

  type PreparedTimeSliceWindow = TimeSliceWindow & {
    currentStartIndex: number;
    currentEndIndex: number;
    cumulativeStartIndex: number;
    cumulativeEndIndex: number;
  };

  const autoUpdateDebounceMs = 450;
  const autoUpdateStorageKeyPrefix = 'radtrack.map.auto-update';
  const autoCellSizeStorageKeyPrefix = 'radtrack.map.auto-cell-size';
  const basemapStorageKeyPrefix = 'radtrack.map.basemap';
  const filterUrlParamName = 'filters';
  const liveUpdatesReconnectDelayMs = 5000;
  const liveUpdatesStorageKeyPrefix = 'radtrack.map.live-updates';
  const liveUpdateMarkerSweepMs = 5000;
  const liveUpdateMarkerMinimumVisibleMs = 5000;
  const liveUpdatePointLimit = 500;
  const maxLiveUpdateLogEntries = 100;
  const mapLatUrlParamName = 'lat';
  const mapLonUrlParamName = 'lon';
  const mapZoomUrlParamName = 'z';
  const sidebarSizeStorageKeyPrefix = 'radtrack.map.sidebar-size';
  const timeSliceAmountUrlParamName = 'sliceAmount';
  const timeSliceLargePointWarningThreshold = 100000;
  const timeSlicePlaybackBaseMs = 1000;
  const timeSlicePlaybackSpeeds = [1, 2, 5, 10];
  const timeSliceUnitUrlParamName = 'sliceUnit';
  const userLocationZoom = 11;
  const firstDataPointZoom = 13;
  const projectedEarthRadiusMeters = 6378137;
  const projectedMaxLatitude = 85.05112878;
  const sidebarSizeOptions: {
    key: SidebarSize;
    width: string;
    titleKey: string;
  }[] = [
    {
      key: 'small',
      width: '27.5rem',
      titleKey: 'radtrack-map_sidebar_size_small-label'
    },
    {
      key: 'large',
      width: '35rem',
      titleKey: 'radtrack-map_sidebar_size_large-label'
    }
  ];
  const autoCellSizeByZoom = [
    { zoom: 5, size: 20000 },
    { zoom: 6, size: 15000 },
    { zoom: 7, size: 10000 },
    { zoom: 8, size: 5000 },
    { zoom: 9, size: 2500 },
    { zoom: 10, size: 1000 },
    { zoom: 11, size: 750 },
    { zoom: 12, size: 500 },
    { zoom: 13, size: 250 },
    { zoom: 14, size: 100 },
    { zoom: 15, size: 20 }
  ];

  const createInitialViewport = (): MapViewport => ({
    centerLat: 41.9,
    centerLon: -87.7,
    minLat: 41.7,
    maxLat: 42.1,
    minLon: -88.0,
    maxLon: -87.5,
    zoom: 9
  });

  const createInitialFilters = (): MapFilters => ({
    datasetIds: [],
    combinedDatasetIds: [],
    trackIds: [],
    trackSelectionMode: 'all',
    metric: 'doseRate',
    mode: 'aggregate',
    shape: 'hexagon',
    aggregateStat: 'mean',
    cellSizeMeters: 250,
    autoCellSize: true,
    applyExcludeAreas: true,
    timeFilterMode: 'none',
    timeFilterPrecision: 'datetime',
    timeFilterStart: '',
    timeFilterEnd: '',
    timeFilterRelativeAmount: 24,
    timeFilterRelativeUnit: 'hours',
    forceRecheck: false
  });

  const createInitialPopupFields = (): PopupFields => ({
    metrics: {}
  });

  const createInitialPopupStateSnapshot = (): PopupStateSnapshot => ({
    metrics: {},
    touched: {}
  });

  const createInitialColorScaleSettings = (): ColorScaleSettings => ({
    auto: true,
    low: 0,
    mid: 0,
    high: 0
  });

  const cloneViewport = (source: MapViewport): MapViewport => ({
    centerLat: source.centerLat,
    centerLon: source.centerLon,
    minLat: source.minLat,
    maxLat: source.maxLat,
    minLon: source.minLon,
    maxLon: source.maxLon,
    zoom: source.zoom
  });

  const pointIsInsideViewport = ({
    point,
    sourceViewport
  }: {
    point: MapPoint;
    sourceViewport: MapViewport;
  }) => {
    if (point.latitude < sourceViewport.minLat || point.latitude > sourceViewport.maxLat) {
      return false;
    }

    if ((sourceViewport.maxLon - sourceViewport.minLon) >= 360) {
      return true;
    }

    if (sourceViewport.minLon <= sourceViewport.maxLon) {
      return point.longitude >= sourceViewport.minLon && point.longitude <= sourceViewport.maxLon;
    }

    return point.longitude >= sourceViewport.minLon || point.longitude <= sourceViewport.maxLon;
  };

  const cloneFilters = (
    source: MapFilters | AppliedMapFilters,
    { clearOneShotControls = false }: { clearOneShotControls?: boolean } = {}
  ): MapFilters => ({
    datasetIds: [...source.datasetIds],
    combinedDatasetIds: [...source.combinedDatasetIds],
    trackIds: [...source.trackIds],
    trackSelectionMode: source.trackSelectionMode,
    metric: source.metric,
    mode: source.mode,
    shape: source.shape,
    aggregateStat: source.aggregateStat,
    cellSizeMeters: Number(source.cellSizeMeters),
    autoCellSize: source.autoCellSize,
    applyExcludeAreas: source.applyExcludeAreas,
    timeFilterMode: source.timeFilterMode,
    timeFilterPrecision: source.timeFilterPrecision,
    timeFilterStart: source.timeFilterStart,
    timeFilterEnd: source.timeFilterEnd,
    timeFilterRelativeAmount: Number(source.timeFilterRelativeAmount),
    timeFilterRelativeUnit: source.timeFilterRelativeUnit,
    forceRecheck: clearOneShotControls ? false : source.forceRecheck
  });

  const clonePopupStateSnapshot = (source: PopupStateSnapshot): PopupStateSnapshot => ({
    metrics: { ...source.metrics },
    touched: { ...source.touched }
  });

  const cloneAppliedFilters = (
    source: AppliedMapFilters,
    { clearOneShotControls = false }: { clearOneShotControls?: boolean } = {}
  ): AppliedMapFilters => ({
    ...cloneFilters(source, { clearOneShotControls }),
    appliedCellSizeMeters: Number(source.appliedCellSizeMeters)
  });

  const toFiniteNumber = (value: unknown) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const toLiveUpdateCursor = (value: unknown) => {
    const parsed = toFiniteNumber(value);
    return Number.isInteger(parsed) && parsed !== null && parsed >= 0 ? parsed : null;
  };

  const maxLiveUpdateCursor = (...values: Array<number | null>) => values.reduce<number | null>((highest, current) => {
    if (current === null) {
      return highest;
    }

    if (highest === null || current > highest) {
      return current;
    }

    return highest;
  }, null);

  const padDatePart = (value: number) => String(value).padStart(2, '0');

  const formatLocalDateInputValue = ({ value }: { value: Date }) => [
    value.getFullYear(),
    padDatePart(value.getMonth() + 1),
    padDatePart(value.getDate())
  ].join('-');

  const formatLocalDateTimeInputValue = ({ value }: { value: Date }) => (
    `${formatLocalDateInputValue({ value })}T${padDatePart(value.getHours())}:${padDatePart(value.getMinutes())}`
  );

  const createDefaultTimeSliceSettings = (): TimeSliceSettings => ({
    amount: 1,
    unit: 'days'
  });

  const normalizeTimeSliceSettings = ({ settings }: { settings: TimeSliceSettings }) => {
    const amount = Math.max(1, Math.trunc(toFiniteNumber(settings.amount) ?? 1));
    return {
      amount,
      unit: ['minutes', 'hours', 'days'].includes(settings.unit)
        ? settings.unit
        : 'days'
    } as TimeSliceSettings;
  };

  const getTimeSliceUnitMillis = ({ unit }: { unit: TimeSliceUnit }) => {
    switch (unit) {
      case 'minutes':
        return 60 * 1000;
      case 'days':
        return 24 * 60 * 60 * 1000;
      case 'hours':
      default:
        return 60 * 60 * 1000;
    }
  };

  const getTimeSliceIntervalMs = ({ settings }: { settings: TimeSliceSettings }) => {
    const normalized = normalizeTimeSliceSettings({ settings });
    return normalized.amount * getTimeSliceUnitMillis({ unit: normalized.unit });
  };

  const getTimeSliceUnitLabelKey = ({
    amount,
    unit
  }: {
    amount: number;
    unit: TimeSliceUnit;
  }) => {
    if (unit === 'minutes') {
      return amount === 1
        ? 'radtrack-map_time_slice_minute-label'
        : 'radtrack-map_time_slice_minutes-label';
    }

    if (unit === 'days') {
      return amount === 1
        ? 'radtrack-map_time_filter_day-label'
        : 'radtrack-map_time_filter_days-label';
    }

    return amount === 1
      ? 'radtrack-map_time_filter_hour-label'
      : 'radtrack-map_time_filter_hours-label';
  };

  const toRadians = (value: number) => (value * Math.PI) / 180;
  const toDegrees = (value: number) => (value * 180) / Math.PI;
  const clampProjectedLatitude = ({ latitude }: { latitude: number }) => Math.max(
    -projectedMaxLatitude,
    Math.min(projectedMaxLatitude, latitude)
  );
  const toProjectedMeters = ({
    latitude,
    longitude
  }: {
    latitude: number;
    longitude: number;
  }) => ({
    x: projectedEarthRadiusMeters * toRadians(longitude),
    y: projectedEarthRadiusMeters * Math.log(
      Math.tan((Math.PI / 4) + (toRadians(clampProjectedLatitude({ latitude })) / 2))
    )
  });
  const fromProjectedMeters = ({
    x,
    y
  }: {
    x: number;
    y: number;
  }) => ({
    latitude: toDegrees((2 * Math.atan(Math.exp(y / projectedEarthRadiusMeters))) - (Math.PI / 2)),
    longitude: toDegrees(x / projectedEarthRadiusMeters)
  });

  const buildFrontendAggregateCell = ({
    cellSizeMeters,
    point,
    shape
  }: {
    cellSizeMeters: number;
    point: { latitude: number; longitude: number };
    shape: AppliedMapFilters['shape'];
  }) => {
    const meters = toProjectedMeters(point);

    if (shape === 'hexagon') {
      const size = cellSizeMeters;
      const q = ((Math.sqrt(3) / 3) * meters.x - (1 / 3) * meters.y) / size;
      const r = ((2 / 3) * meters.y) / size;
      const s = -q - r;
      let roundedQ = Math.round(q);
      let roundedR = Math.round(r);
      let roundedS = Math.round(s);

      const qDiff = Math.abs(roundedQ - q);
      const rDiff = Math.abs(roundedR - r);
      const sDiff = Math.abs(roundedS - s);

      if (qDiff > rDiff && qDiff > sDiff) {
        roundedQ = -roundedR - roundedS;
      } else if (rDiff > sDiff) {
        roundedR = -roundedQ - roundedS;
      } else {
        roundedS = -roundedQ - roundedR;
      }

      return {
        id: `hex:${roundedQ}:${roundedR}`,
        center: fromProjectedMeters({
          x: size * Math.sqrt(3) * (roundedQ + (roundedR / 2)),
          y: size * 1.5 * roundedR
        }),
        radiusMeters: size
      };
    }

    const step = cellSizeMeters;
    const cellX = Math.round(meters.x / step);
    const cellY = Math.round(meters.y / step);
    return {
      id: `${shape}:${cellX}:${cellY}`,
      center: fromProjectedMeters({
        x: cellX * step,
        y: cellY * step
      }),
      radiusMeters: shape === 'circle' ? cellSizeMeters / 2 : cellSizeMeters
    };
  };

  const computeFrontendAggregateStats = ({
    values
  }: {
    values: number[];
  }): AggregateStats => {
    if (!values.length) {
      return {
        min: null,
        max: null,
        mean: null,
        median: null,
        mode: null,
        count: 0
      };
    }

    const sorted = [...values].sort((left, right) => left - right);
    const modeBucketDecimals = Math.max(0, Math.trunc(toFiniteNumber($sessionStore.ui?.modeBucketDecimals) ?? 2));
    const bucketScale = 10 ** modeBucketDecimals;
    const modeCounts = new Map<number, number>();
    for (const value of sorted) {
      const bucket = Math.round(value * bucketScale) / bucketScale;
      modeCounts.set(bucket, (modeCounts.get(bucket) ?? 0) + 1);
    }

    let mode: number | null = null;
    let modeCount = -1;
    for (const [bucket, count] of modeCounts.entries()) {
      if (count > modeCount) {
        mode = bucket;
        modeCount = count;
      }
    }

    const midpoint = Math.floor(sorted.length / 2);
    return {
      min: sorted[0],
      max: sorted[sorted.length - 1],
      mean: sorted.reduce((total, value) => total + value, 0) / sorted.length,
      median: sorted.length % 2 === 0
        ? (sorted[midpoint - 1] + sorted[midpoint]) / 2
        : sorted[midpoint],
      mode,
      count: sorted.length
    };
  };

  const getDatePortion = ({ value }: { value: string }) => {
    const trimmedValue = String(value ?? '').trim();
    return /^\d{4}-\d{2}-\d{2}/.test(trimmedValue) ? trimmedValue.slice(0, 10) : '';
  };

  const getDefaultAbsoluteTimeRange = ({
    precision
  }: {
    precision: TimeFilterPrecision;
  }) => {
    const now = new Date();
    if (precision === 'date') {
      const today = formatLocalDateInputValue({ value: now });
      return {
        start: today,
        end: today
      };
    }

    const oneHourAgo = new Date(now.getTime() - (60 * 60 * 1000));
    return {
      start: formatLocalDateTimeInputValue({ value: oneHourAgo }),
      end: formatLocalDateTimeInputValue({ value: now })
    };
  };

  const localDateToIso = ({
    value,
    boundary
  }: {
    value: string;
    boundary: 'start' | 'end';
  }) => {
    const [yearValue, monthValue, dayValue] = value.split('-').map((entry) => Number(entry));
    if (![yearValue, monthValue, dayValue].every((entry) => Number.isInteger(entry))) {
      return null;
    }

    const date = boundary === 'start'
      ? new Date(yearValue, monthValue - 1, dayValue, 0, 0, 0, 0)
      : new Date(yearValue, monthValue - 1, dayValue + 1, 0, 0, 0, -1);

    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  };

  const localDateTimeToIso = ({ value }: { value: string }) => {
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
  };

  const resolveAbsoluteTimeWindow = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => {
    if (sourceFilters.timeFilterMode !== 'absolute') {
      return null;
    }

    if (!sourceFilters.timeFilterStart || !sourceFilters.timeFilterEnd) {
      return null;
    }

    const dateFrom = sourceFilters.timeFilterPrecision === 'date'
      ? localDateToIso({
          value: sourceFilters.timeFilterStart,
          boundary: 'start'
        })
      : localDateTimeToIso({ value: sourceFilters.timeFilterStart });
    const dateTo = sourceFilters.timeFilterPrecision === 'date'
      ? localDateToIso({
          value: sourceFilters.timeFilterEnd,
          boundary: 'end'
        })
      : localDateTimeToIso({ value: sourceFilters.timeFilterEnd });

    if (!dateFrom || !dateTo) {
      return null;
    }

    return {
      dateFrom,
      dateTo,
      endLocalDate: getDatePortion({ value: sourceFilters.timeFilterEnd })
    };
  };

  const resolveAbsoluteTimeWindowMillis = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => {
    const resolved = resolveAbsoluteTimeWindow({ sourceFilters });
    if (!resolved) {
      return null;
    }

    const startMs = Date.parse(resolved.dateFrom);
    const endMs = Date.parse(resolved.dateTo);
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || startMs > endMs) {
      return null;
    }

    return {
      ...resolved,
      startMs,
      endMs
    };
  };

  const buildTimeSliceWindows = ({
    sourceFilters,
    settings
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
    settings: TimeSliceSettings;
  }): TimeSliceWindow[] => {
    const range = resolveAbsoluteTimeWindowMillis({ sourceFilters });
    if (!range) {
      return [];
    }

    const intervalMs = getTimeSliceIntervalMs({ settings });
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      return [];
    }

    const windows: TimeSliceWindow[] = [];
    for (
      let startMs = range.startMs, index = 0;
      startMs <= range.endMs && index < 10000;
      startMs += intervalMs, index += 1
    ) {
      const endMs = Math.min(startMs + intervalMs, range.endMs);
      windows.push({
        index,
        startIso: new Date(startMs).toISOString(),
        endIso: new Date(endMs).toISOString(),
        startMs,
        endMs,
        isLast: endMs >= range.endMs
      });

      if (endMs >= range.endMs) {
        break;
      }
    }

    return windows;
  };

  const getPointTimestampMs = ({ point }: { point: MapPoint | LiveUpdatePointDetail }) => {
    const timestamp = point.occurredAt ?? point.receivedAt;
    if (!timestamp) {
      return null;
    }

    const parsed = Date.parse(timestamp);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const normalizePlaybackPoint = ({ point }: { point: MapPoint }): MapPoint | null => {
    const latitude = toFiniteNumber(point.latitude);
    const longitude = toFiniteNumber(point.longitude);
    if (latitude === null || longitude === null) {
      return null;
    }

    return {
      ...point,
      latitude,
      longitude,
      altitudeMeters: toFiniteNumber(point.altitudeMeters),
      accuracy: toFiniteNumber(point.accuracy),
      measurements: Object.fromEntries(
        Object.entries(point.measurements ?? {}).flatMap(([key, value]) => {
          const numericValue = toFiniteNumber(value);
          return numericValue === null ? [] : [[key, numericValue]];
        })
      )
    };
  };

  const normalizePreparedTimeSliceWindow = ({ value }: { value: unknown }): PreparedTimeSliceWindow | null => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null;
    }

    const record = value as Record<string, unknown>;
    const index = toFiniteNumber(record.index);
    const startMs = toFiniteNumber(record.startMs);
    const endMs = toFiniteNumber(record.endMs);
    const currentStartIndex = toFiniteNumber(record.currentStartIndex);
    const currentEndIndex = toFiniteNumber(record.currentEndIndex);
    const cumulativeStartIndex = toFiniteNumber(record.cumulativeStartIndex);
    const cumulativeEndIndex = toFiniteNumber(record.cumulativeEndIndex);
    if (
      index === null
      || startMs === null
      || endMs === null
      || currentStartIndex === null
      || currentEndIndex === null
      || cumulativeStartIndex === null
      || cumulativeEndIndex === null
      || typeof record.startIso !== 'string'
      || typeof record.endIso !== 'string'
    ) {
      return null;
    }

    return {
      index: Math.max(0, Math.trunc(index)),
      startIso: record.startIso,
      endIso: record.endIso,
      startMs,
      endMs,
      isLast: record.isLast === true,
      currentStartIndex: Math.max(0, Math.trunc(currentStartIndex)),
      currentEndIndex: Math.max(0, Math.trunc(currentEndIndex)),
      cumulativeStartIndex: Math.max(0, Math.trunc(cumulativeStartIndex)),
      cumulativeEndIndex: Math.max(0, Math.trunc(cumulativeEndIndex))
    };
  };

  const getPreparedTimeSlicePointRange = ({
    currentOnly,
    pointCount,
    window
  }: {
    currentOnly: boolean;
    pointCount: number;
    window: PreparedTimeSliceWindow;
  }) => {
    const rawStart = currentOnly ? window.currentStartIndex : window.cumulativeStartIndex;
    const rawEnd = currentOnly ? window.currentEndIndex : window.cumulativeEndIndex;
    const start = Math.max(0, Math.min(pointCount, rawStart));
    const end = Math.max(start, Math.min(pointCount, rawEnd));

    return { start, end };
  };

  const getPreparedTimeSlicePointsInViewport = ({
    currentOnly,
    points: sourcePoints,
    sourceViewport,
    window
  }: {
    currentOnly: boolean;
    points: MapPoint[];
    sourceViewport: MapViewport;
    window: PreparedTimeSliceWindow;
  }) => {
    const range = getPreparedTimeSlicePointRange({
      currentOnly,
      pointCount: sourcePoints.length,
      window
    });
    const visiblePoints: MapPoint[] = [];

    for (let index = range.start; index < range.end; index += 1) {
      const point = sourcePoints[index];
      if (point && pointIsInsideViewport({ point, sourceViewport })) {
        visiblePoints.push(point);
      }
    }

    return visiblePoints;
  };

  const getTimeFilterValidationKey = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => {
    if (sourceFilters.timeFilterMode === 'none') {
      return null;
    }

    if (sourceFilters.timeFilterMode === 'relative') {
      const relativeAmount = toFiniteNumber(sourceFilters.timeFilterRelativeAmount);
      return Number.isInteger(relativeAmount) && relativeAmount !== null && relativeAmount > 0
        ? null
        : 'radtrack-map_time_filter_relative_invalid';
    }

    if (!sourceFilters.timeFilterStart || !sourceFilters.timeFilterEnd) {
      return 'radtrack-map_time_filter_absolute_required';
    }

    const resolved = resolveAbsoluteTimeWindow({ sourceFilters });
    if (!resolved) {
      return 'radtrack-map_time_filter_invalid';
    }

    return resolved.dateFrom <= resolved.dateTo
      ? null
      : 'radtrack-map_time_filter_range_invalid';
  };

  const buildTimeFilterQuery = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => {
    const query: Record<string, unknown> = {};

    if (sourceFilters.timeFilterMode === 'absolute') {
      const resolved = resolveAbsoluteTimeWindow({ sourceFilters });
      const currentLocalDate = formatLocalDateInputValue({ value: new Date() });
      query.timeFilterMode = 'absolute';
      query.dateFrom = resolved?.dateFrom ?? null;
      query.dateTo = resolved?.dateTo ?? null;
      query.historicalCacheEligible = Boolean(
        resolved?.endLocalDate
        && resolved.endLocalDate < currentLocalDate
      );
    } else if (sourceFilters.timeFilterMode === 'relative') {
      query.timeFilterMode = 'relative';
      query.relativeAmount = Math.max(1, Math.trunc(toFiniteNumber(sourceFilters.timeFilterRelativeAmount) ?? 0));
      query.relativeUnit = sourceFilters.timeFilterRelativeUnit;
    }

    if (sourceFilters.forceRecheck) {
      query.forceRecheck = true;
    }

    return query;
  };

  const normalizeTimeFilterInputsForPrecision = ({
    sourceFilters,
    precision
  }: {
    sourceFilters: MapFilters;
    precision: TimeFilterPrecision;
  }) => {
    const defaults = getDefaultAbsoluteTimeRange({ precision });
    if (precision === 'date') {
      return {
        start: getDatePortion({ value: sourceFilters.timeFilterStart }) || defaults.start,
        end: getDatePortion({ value: sourceFilters.timeFilterEnd }) || defaults.end
      };
    }

    const startDatePortion = getDatePortion({ value: sourceFilters.timeFilterStart });
    const endDatePortion = getDatePortion({ value: sourceFilters.timeFilterEnd });
    return {
      start: sourceFilters.timeFilterStart.includes('T')
        ? sourceFilters.timeFilterStart
        : (startDatePortion ? `${startDatePortion}T00:00` : defaults.start),
      end: sourceFilters.timeFilterEnd.includes('T')
        ? sourceFilters.timeFilterEnd
        : (endDatePortion ? `${endDatePortion}T23:59` : defaults.end)
    };
  };

  const booleanRecordChanged = ({
    current,
    next
  }: {
    current: Record<string, boolean>;
    next: Record<string, boolean>;
  }) => {
    const currentKeys = Object.keys(current);
    const nextKeys = Object.keys(next);
    if (currentKeys.length !== nextKeys.length) {
      return true;
    }

    return nextKeys.some((key) => current[key] !== next[key]);
  };

  const getAppliedCellSizeMeters = ({
    filters: sourceFilters,
    viewport: sourceViewport
  }: {
    filters: MapFilters | AppliedMapFilters;
    viewport: MapViewport;
  }) => {
    if (!sourceFilters.autoCellSize) {
      return Math.max(10, toFiniteNumber(sourceFilters.cellSizeMeters) ?? 250);
    }

    return autoCellSizeByZoom.reduce((closest, current) => {
      const currentDistance = Math.abs(current.zoom - sourceViewport.zoom);
      const closestDistance = Math.abs(closest.zoom - sourceViewport.zoom);

      if (currentDistance < closestDistance) {
        return current;
      }

      if (currentDistance === closestDistance && current.zoom > closest.zoom) {
        return current;
      }

      return closest;
    }).size;
  };

  const createAppliedFilters = ({
    filters: sourceFilters,
    viewport: sourceViewport
  }: {
    filters: MapFilters | AppliedMapFilters;
    viewport: MapViewport;
  }): AppliedMapFilters => ({
    ...cloneFilters(sourceFilters),
    appliedCellSizeMeters: getAppliedCellSizeMeters({
      filters: sourceFilters,
      viewport: sourceViewport
    })
  });

  const createPlaybackAppliedFilters = ({
    filters: sourceFilters,
    viewport: sourceViewport
  }: {
    filters: AppliedMapFilters;
    viewport: MapViewport;
  }): AppliedMapFilters => ({
    ...sourceFilters,
    appliedCellSizeMeters: getAppliedCellSizeMeters({
      filters: sourceFilters,
      viewport: sourceViewport
    })
  });

  const serializeFilters = ({
    filters: sourceFilters
  }: {
    filters: MapFilters | AppliedMapFilters;
  }) => JSON.stringify({
    datasetIds: sourceFilters.datasetIds,
    combinedDatasetIds: sourceFilters.combinedDatasetIds,
    trackIds: sourceFilters.trackIds,
    trackSelectionMode: sourceFilters.trackSelectionMode,
    metric: sourceFilters.metric,
    mode: sourceFilters.mode,
    shape: sourceFilters.shape,
    aggregateStat: sourceFilters.aggregateStat,
    cellSizeMeters: sourceFilters.cellSizeMeters,
    autoCellSize: sourceFilters.autoCellSize,
    applyExcludeAreas: sourceFilters.applyExcludeAreas,
    timeFilterMode: sourceFilters.timeFilterMode,
    timeFilterPrecision: sourceFilters.timeFilterPrecision,
    timeFilterStart: sourceFilters.timeFilterStart,
    timeFilterEnd: sourceFilters.timeFilterEnd,
    timeFilterRelativeAmount: sourceFilters.timeFilterRelativeAmount,
    timeFilterRelativeUnit: sourceFilters.timeFilterRelativeUnit,
    forceRecheck: sourceFilters.forceRecheck
  });

  const serializeQuery = ({
    filters: sourceFilters,
    viewport: sourceViewport
  }: {
    filters: AppliedMapFilters;
    viewport: MapViewport;
  }) => JSON.stringify({
    datasetIds: sourceFilters.datasetIds,
    combinedDatasetIds: sourceFilters.combinedDatasetIds,
    trackIds: sourceFilters.trackIds,
    trackSelectionMode: sourceFilters.trackSelectionMode,
    metric: sourceFilters.metric,
    mode: sourceFilters.mode,
    shape: sourceFilters.shape,
    aggregateStat: sourceFilters.aggregateStat,
    cellSizeMeters: sourceFilters.appliedCellSizeMeters,
    autoCellSize: sourceFilters.autoCellSize,
    applyExcludeAreas: sourceFilters.applyExcludeAreas,
    timeFilterMode: sourceFilters.timeFilterMode,
    timeFilterPrecision: sourceFilters.timeFilterPrecision,
    timeFilterStart: sourceFilters.timeFilterStart,
    timeFilterEnd: sourceFilters.timeFilterEnd,
    timeFilterRelativeAmount: sourceFilters.timeFilterRelativeAmount,
    timeFilterRelativeUnit: sourceFilters.timeFilterRelativeUnit,
    minLat: sourceViewport.minLat,
    maxLat: sourceViewport.maxLat,
    minLon: sourceViewport.minLon,
    maxLon: sourceViewport.maxLon,
    zoom: sourceViewport.zoom
  });

  const serializeTimeSlicePointSourceQuery = ({
    filters: sourceFilters
  }: {
    filters: AppliedMapFilters;
  }) => JSON.stringify({
    datasetIds: sourceFilters.datasetIds,
    combinedDatasetIds: sourceFilters.combinedDatasetIds,
    trackIds: sourceFilters.trackIds,
    trackSelectionMode: sourceFilters.trackSelectionMode,
    applyExcludeAreas: sourceFilters.applyExcludeAreas,
    timeFilterMode: sourceFilters.timeFilterMode,
    timeFilterPrecision: sourceFilters.timeFilterPrecision,
    timeFilterStart: sourceFilters.timeFilterStart,
    timeFilterEnd: sourceFilters.timeFilterEnd
  });

  const serializeTimeSliceSourceQuery = ({
    filters: sourceFilters,
    settings
  }: {
    filters: AppliedMapFilters;
    settings: TimeSliceSettings;
  }) => {
    const normalized = normalizeTimeSliceSettings({ settings });

    return JSON.stringify({
      pointSource: JSON.parse(serializeTimeSlicePointSourceQuery({ filters: sourceFilters })),
      sliceAmount: normalized.amount,
      sliceUnit: normalized.unit
    });
  };

  const createQuerySnapshot = ({
    filters: sourceFilters,
    viewport: sourceViewport
  }: {
    filters: MapFilters | AppliedMapFilters;
    viewport: MapViewport;
  }): MapQuerySnapshot => {
    const snapshotViewport = cloneViewport(sourceViewport);
    const snapshotFilters = createAppliedFilters({
      filters: sourceFilters,
      viewport: snapshotViewport
    });

    return {
      filters: snapshotFilters,
      viewport: snapshotViewport,
      key: serializeQuery({
        filters: snapshotFilters,
        viewport: snapshotViewport
      })
    };
  };

  const getMedian = ({ values }: { values: number[] }) => {
    if (!values.length) {
      return null;
    }

    const midpoint = Math.floor(values.length / 2);
    return values.length % 2 === 0
      ? (values[midpoint - 1] + values[midpoint]) / 2
      : values[midpoint];
  };

  const normalizeColorScale = ({
    low,
    mid,
    high
  }: {
    low: number | null;
    mid: number | null;
    high: number | null;
  }): ColorScaleThresholds | null => {
    if (low === null || mid === null || high === null) {
      return null;
    }

    const sorted = [low, mid, high].sort((left, right) => left - right);
    return {
      low: sorted[0],
      mid: sorted[1],
      high: sorted[2]
    };
  };

  const getActiveAggregateValues = ({
    cells,
    aggregateStat
  }: {
    cells: AggregateCell[];
    aggregateStat: AppliedMapFilters['aggregateStat'];
  }) => cells
    .map((cell) => cell.stats?.[aggregateStat])
    .filter((value): value is number => typeof value === 'number')
    .sort((left, right) => left - right);

  const getAutoColorScale = ({
    cells,
    aggregateStat
  }: {
    cells: AggregateCell[];
    aggregateStat: AppliedMapFilters['aggregateStat'];
  }) => {
    const values = getActiveAggregateValues({ cells, aggregateStat });
    if (!values.length) {
      return null;
    }

    return normalizeColorScale({
      low: values[0],
      mid: getMedian({ values }),
      high: values[values.length - 1]
    });
  };

  const getEffectiveColorScale = ({
    cells,
    aggregateStat,
    settings
  }: {
    cells: AggregateCell[];
    aggregateStat: AppliedMapFilters['aggregateStat'];
    settings: ColorScaleSettings;
  }) => {
    const autoScale = getAutoColorScale({ cells, aggregateStat });
    if (settings.auto) {
      return autoScale;
    }

    return normalizeColorScale({
      low: toFiniteNumber(settings.low) ?? autoScale?.low ?? null,
      mid: toFiniteNumber(settings.mid) ?? autoScale?.mid ?? null,
      high: toFiniteNumber(settings.high) ?? autoScale?.high ?? null
    });
  };

  let datasets = $state<DatasetOption[]>([]);
  let combinedDatasets = $state<CombinedDatasetOption[]>([]);
  let datasetTracksByDatasetId = $state<Record<string, TrackOption[]>>({});
  let points = $state<MapPoint[]>([]);
  let aggregates = $state<AggregateCell[]>([]);
  let errorMessage = $state<string | null>(null);
  let loading = $state(false);
  let lookupsReady = $state(false);
  let autoUpdateMap = $state(true);
  let liveUpdatesEnabled = $state(true);
  let liveUpdateCursor = $state<number | null>(null);
  let liveUpdatePendingCursor = $state<number | null>(null);
  let liveUpdateTransport = $state<'idle' | 'polling' | 'websocket'>('idle');
  let liveUpdateLog = $state<LiveUpdateLogEntry[]>([]);
  let liveUpdateMarkers = $state<LiveUpdateMarker[]>([]);
  let liveUpdateMarkersSetAt = 0;
  let selectedPoint = $state<MapPoint | null>(null);
  let aggregateCellModalState = $state<AggregateCellModalState | null>(null);
  let aggregateCellModalPoints = $state<MapPoint[]>([]);
  let aggregateCellModalErrorMessage = $state<string | null>(null);
  let aggregateCellModalLoading = $state(false);
  let aggregateCellModalRequestVersion = 0;
  let popupMaxHeightPx = $state(260);
  let viewport = $state(createInitialViewport());
  let filters = $state(createInitialFilters());
  let filterUrlSyncReady = $state(false);
  let restoredUrlFilters = false;
  let restoredUrlMapLocation = false;
  let lastUrlStateKey = $state('');
  let popupFields = $state(createInitialPopupFields());
  let popupFieldTouched = $state<Record<string, boolean>>({});
  let appliedPopupState = $state<PopupStateSnapshot>(createInitialPopupStateSnapshot());
  let colorScaleSettings = $state(createInitialColorScaleSettings());
  let activeQuery = $state<MapQuerySnapshot | null>(null);
  let timeSliceEnabled = $state(false);
  let timeSliceCurrentOnly = $state(false);
  let timeSliceSettings = $state<TimeSliceSettings>(createDefaultTimeSliceSettings());
  let timeSliceDraftSettings = $state<TimeSliceSettings>(createDefaultTimeSliceSettings());
  let timeSliceConfigOpen = $state(false);
  let timeSliceIndex = $state(0);
  let timeSlicePlaying = $state(false);
  let timeSlicePlaybackSpeed = $state(1);
  let timeSlicePlaybackStartedAt = 0;
  let timeSlicePlaybackStartIndex = 0;
  let timeSliceSourcePoints = $state<MapPoint[]>([]);
  let timeSlicePreparedWindows = $state<PreparedTimeSliceWindow[]>([]);
  let timeSliceSourceLoading = $state(false);
  let loadedTimeSliceSourceKey = $state('');
  let timeSliceSourceErrorKey = $state('');
  let timeSlicePointCount = $state<number | null>(null);
  let timeSlicePointCountLoading = $state(false);
  let loadedTimeSlicePointCountKey = $state('');
  let timeSlicePointCountErrorKey = $state('');
  let timeSliceLargeLoadConfirmedKey = $state('');
  let timeBoundsLoading = $state(false);
  let rawPointTotalCount = $state(0);
  let mapFocus = $state<MapFocus | null>(null);
  let loadingTrackDatasetIds = $state<string[]>([]);
  let basemapKey = $state<BasemapKey>('default');
  let mapDatasetDefaults = $state<MapDatasetDefaultRecord>({});
  let mapDatasetOrder = $state<string[]>([]);
  let mapFieldOrder = $state<MapFieldOrder>([]);
  let mapFieldVisibility = $state<MapFieldVisibilityRecord>({});
  let sidebarSize = $state<SidebarSize>('large');
  let sidebarUpdateTimer: ReturnType<typeof setTimeout> | null = null;
  let mapStageElement: HTMLDivElement;
  let mapStageResizeObserver: ResizeObserver | null = null;
  let liveUpdateSocket: WebSocket | null = null;
  let liveUpdateSocketSubscriptionKey = '';
  let liveUpdateSocketReady = false;
  let liveUpdateReconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let liveUpdateTimer: ReturnType<typeof setInterval> | null = null;
  let liveUpdateMarkerTimer: ReturnType<typeof setInterval> | null = null;
  let timeSlicePlaybackTimer: ReturnType<typeof setInterval> | null = null;
  let liveUpdateRefreshInFlight = false;
  let liveUpdateRefreshQueued = false;
  let timeSlicePointCountRequestVersion = 0;
  let timeSliceSourceRequestVersion = 0;
  let queuedSidebarSnapshot: {
    query: MapQuerySnapshot;
    popupState: PopupStateSnapshot;
  } | null = null;
  let requestVersion = 0;
  let restoredUrlDatasetIdsMode: UrlSelectionMode = 'explicit';
  let restoredUrlCombinedDatasetIdsMode: UrlSelectionMode = 'explicit';
  let restoredUrlDatalogIdsMode: UrlSelectionMode = 'explicit';

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radtrack-common_none');

  const formatNumber = (value: number | null | undefined) => {
    if (value === null || value === undefined) {
      return t('radtrack-common_na-label');
    }

    return new Intl.NumberFormat($localeStore.language, {
      useGrouping: false,
      maximumFractionDigits: 5
    }).format(value);
  };

  const formatCount = (value: number) => new Intl.NumberFormat($localeStore.language).format(value);
  const formatMetricFieldValue = ({
    field,
    value
  }: {
    field: MetricField;
    value: number | null | undefined;
  }) => field.valueType === 'time' && typeof value === 'number'
    ? formatTime(new Date(value).toISOString())
    : formatNumber(value);

  const shortId = (value: string | null | undefined) => {
    if (!value) {
      return t('radtrack-common_none');
    }

    return value.slice(-8);
  };

  const formatAggregateTimeRange = ({
    timeRange
  }: {
    timeRange: AggregateCell['timeRange'];
  }) => {
    if (!timeRange.start && !timeRange.end) {
      return null;
    }

    if (timeRange.start && timeRange.end && timeRange.start !== timeRange.end) {
      return `${formatTime(timeRange.start)} -> ${formatTime(timeRange.end)}`;
    }

    return formatTime(timeRange.start ?? timeRange.end);
  };

  const updatePopupMaxHeight = () => {
    if (!mapStageElement) {
      return;
    }

    popupMaxHeightPx = Math.max(
      260,
      Math.floor(mapStageElement.clientHeight / 2) + 20
    );
  };

  const resolveExtraValue = ({
    extra,
    propKey
  }: {
    extra: Record<string, unknown> | null | undefined;
    propKey: string;
  }): string | number | boolean | null => {
    const coerceValue = (value: unknown) => (
      typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean'
        ? value
        : null
    );

    if (!extra) {
      return null;
    }

    if (propKey in extra) {
      return coerceValue(extra[propKey]);
    }

    for (const [rawKey, rawValue] of Object.entries(extra)) {
      if (
        rawKey === propKey
        || rawKey.toLowerCase() === propKey.toLowerCase()
        || normalizePropKey(rawKey) === propKey
      ) {
        return coerceValue(rawValue);
      }
    }

    return null;
  };

  const resolveAggregateCellSyntheticValue = ({
    cell,
    propKey
  }: {
    cell: AggregateCell | null;
    propKey: string;
  }): AggregateCellModalValue => {
    if (!cell) {
      return null;
    }

    switch (propKey) {
      case 'radtrackCacheKey':
        return cell.cache?.key ?? null;
      case 'radtrackCacheSource':
        return cell.cache?.source
          ? (cell.cache.reason ? `${cell.cache.source} (${cell.cache.reason})` : cell.cache.source)
          : null;
      case 'radtrackCacheTtlSeconds':
        return cell.cache?.ttlSecondsRemaining ?? null;
      default:
        return null;
    }
  };

  const getPointPopupFieldValue = ({
    point,
    propKey,
    cell
  }: {
    point: MapPoint;
    propKey: string;
    cell: AggregateCell | null;
  }): AggregateCellModalValue => {
    switch (propKey) {
      case 'occurredAt': {
        const timestamp = point.occurredAt ?? point.receivedAt;
        return timestamp ? Date.parse(timestamp) : null;
      }
      case 'latitude':
        return point.latitude;
      case 'longitude':
        return point.longitude;
      case 'altitudeMeters':
        return point.altitudeMeters;
      case 'accuracy':
        return point.accuracy;
      case 'deviceId':
        return point.deviceId ?? null;
      case 'deviceName':
        return point.deviceName ?? null;
      case 'deviceType':
        return point.deviceType ?? null;
      case 'deviceCalibration':
        return point.deviceCalibration ?? null;
      case 'firmwareVersion':
        return point.firmwareVersion ?? null;
      case 'sourceReadingId':
        return point.sourceReadingId ?? null;
      case 'comment':
        return point.comment ?? null;
      case 'custom':
        return point.custom ?? null;
      case 'rawTimestamp':
        return point.rawTimestamp ?? null;
      case 'parsedTimeText':
        return point.parsedTimeText ?? null;
      case 'radtrackCacheKey':
      case 'radtrackCacheSource':
      case 'radtrackCacheTtlSeconds':
        return resolveAggregateCellSyntheticValue({ cell, propKey });
      default:
        return point.measurements?.[propKey]
          ?? point.components?.[propKey]
          ?? resolveExtraValue({
            extra: point.extra,
            propKey
          });
    }
  };

  const getPointMetricNumberValue = ({
    metricKey,
    point
  }: {
    metricKey: string;
    point: MapPoint;
  }) => {
    switch (metricKey) {
      case 'occurredAt': {
        const timestampMs = getPointTimestampMs({ point });
        return timestampMs === null ? null : timestampMs;
      }
      case 'latitude':
        return point.latitude;
      case 'longitude':
        return point.longitude;
      case 'altitudeMeters':
        return point.altitudeMeters;
      case 'accuracy':
        return point.accuracy;
      default: {
        const value = point.measurements?.[metricKey];
        return typeof value === 'number' && Number.isFinite(value) ? value : null;
      }
    }
  };

  const getAggregateCellForPoint = ({
    point,
    sourceFilters
  }: {
    point: MapPoint;
    sourceFilters: AppliedMapFilters;
  }) => {
    if (!Number.isFinite(point.latitude) || !Number.isFinite(point.longitude)) {
      return null;
    }

    return buildFrontendAggregateCell({
      cellSizeMeters: sourceFilters.appliedCellSizeMeters,
      point: {
        latitude: point.latitude,
        longitude: point.longitude
      },
      shape: sourceFilters.shape
    });
  };

  const buildAggregateCellsForPoints = ({
    popupFieldsForCells,
    sourceFilters,
    sourcePoints
  }: {
    popupFieldsForCells: MetricField[];
    sourceFilters: AppliedMapFilters;
    sourcePoints: MapPoint[];
  }): AggregateCell[] => {
    const cellGroups = new Map<string, {
      cell: ReturnType<typeof buildFrontendAggregateCell>;
      points: MapPoint[];
    }>();

    for (const point of sourcePoints) {
      const cell = getAggregateCellForPoint({
        point,
        sourceFilters
      });
      if (!cell) {
        continue;
      }

      const current = cellGroups.get(cell.id) ?? {
        cell,
        points: []
      };
      current.points.push(point);
      cellGroups.set(cell.id, current);
    }

    return [...cellGroups.values()].map(({ cell, points: cellPoints }) => {
      let timeRangeStart: string | null = null;
      let timeRangeEnd: string | null = null;
      const selectedMetricValues: number[] = [];
      const popupMetricValues = new Map<string, number[]>();
      const popupDataValues = new Map<string, string[]>();

      for (const point of cellPoints) {
        const selectedMetricValue = getPointMetricNumberValue({
          metricKey: sourceFilters.metric,
          point
        });
        if (selectedMetricValue !== null) {
          selectedMetricValues.push(selectedMetricValue);
        }

        const timestamp = point.occurredAt ?? point.receivedAt;
        if (timestamp) {
          if (!timeRangeStart || timestamp < timeRangeStart) {
            timeRangeStart = timestamp;
          }

          if (!timeRangeEnd || timestamp > timeRangeEnd) {
            timeRangeEnd = timestamp;
          }
        }

        for (const field of popupFieldsForCells) {
          if (field.propKey === aggregateDataCountPropKey || field.source === 'synthetic') {
            continue;
          }

          const aggregateBasePropKey = getAggregateTimeBasePropKey(field.propKey);
          const propKey = aggregateBasePropKey ?? field.propKey;
          if (field.valueType === 'string' && !aggregateBasePropKey) {
            const rawValue = getPointPopupFieldValue({
              point,
              propKey,
              cell: null
            });
            if (rawValue === null || rawValue === undefined || rawValue === '') {
              continue;
            }

            const values = popupDataValues.get(propKey) ?? [];
            values.push(String(rawValue));
            popupDataValues.set(propKey, values);
            continue;
          }

          const numericValue = getPointMetricNumberValue({
            metricKey: propKey,
            point
          });
          if (numericValue === null) {
            continue;
          }

          const values = popupMetricValues.get(propKey) ?? [];
          values.push(numericValue);
          popupMetricValues.set(propKey, values);
        }
      }

      return {
        id: cell.id,
        center: cell.center,
        radiusMeters: cell.radiusMeters,
        pointCount: cellPoints.length,
        timeRange: {
          start: timeRangeStart,
          end: timeRangeEnd
        },
        stats: computeFrontendAggregateStats({
          values: selectedMetricValues
        }),
        metrics: Object.fromEntries([...popupMetricValues.entries()].map(([metricKey, values]) => [
          metricKey,
          computeFrontendAggregateStats({ values })
        ])),
        dataValues: Object.fromEntries([...popupDataValues.entries()].map(([propKey, values]) => [
          propKey,
          [...new Set(values)]
        ]))
      };
    });
  };

  const toUrlNumber = (value: string | null) => (
    value === null || !value.trim() ? null : toFiniteNumber(value)
  );
  const roundUrlCoordinate = ({ value }: { value: number }) => Number(value.toFixed(4));
  const timeSliceUnitOptions: TimeSliceUnit[] = ['days', 'hours', 'minutes'];

  const availableBasemapKeys: BasemapKey[] = ['default', 'satellite', 'light', 'topo'];
  const availableSidebarSizes: SidebarSize[] = ['small', 'large'];

  const getScopedStorageKey = ({ prefix }: { prefix: string }) => `${prefix}.${$sessionStore.user?.id ?? 'default'}`;

  const loadStoredBoolean = ({
    prefix,
    fallbackValue
  }: {
    prefix: string;
    fallbackValue: boolean;
  }) => {
    if (!browser) {
      return fallbackValue;
    }

    const storedValue = window.localStorage.getItem(getScopedStorageKey({ prefix }));
    if (storedValue === null) {
      return fallbackValue;
    }

    return storedValue !== 'false';
  };

  const persistBoolean = ({
    prefix,
    value
  }: {
    prefix: string;
    value: boolean;
  }) => {
    if (!browser) {
      return;
    }

    window.localStorage.setItem(getScopedStorageKey({ prefix }), String(value));
  };

  const loadStoredString = ({
    prefix,
    fallbackValue
  }: {
    prefix: string;
    fallbackValue: string;
  }) => {
    if (!browser) {
      return fallbackValue;
    }

    return window.localStorage.getItem(getScopedStorageKey({ prefix })) ?? fallbackValue;
  };

  const persistString = ({
    prefix,
    value
  }: {
    prefix: string;
    value: string;
  }) => {
    if (!browser) {
      return;
    }

    window.localStorage.setItem(getScopedStorageKey({ prefix }), value);
  };

  const toStoredStringArray = ({ value }: { value: unknown }) => (
    Array.isArray(value)
      ? value.filter((entry): entry is string => typeof entry === 'string')
      : []
  );
  const hasOwn = ({ source, key }: { source: Record<string, unknown>; key: string }) => (
    Object.prototype.hasOwnProperty.call(source, key)
  );

  const getUrlSelectionMode = ({
    allowDefault = false,
    value
  }: {
    allowDefault?: boolean;
    value: unknown;
  }): UrlSelectionMode => {
    if (value === 'none' || value === 'all' || (allowDefault && value === 'default')) {
      return value;
    }

    return 'explicit';
  };

  const normalizeStoredFilters = ({ value }: { value: unknown }): MapFilters | null => {
    if (!value || typeof value !== 'object') {
      return null;
    }

    const defaults = createInitialFilters();
    const source = value as Record<string, unknown>;
    const datalogIdsValue = source.datalogIds ?? source.trackIds;
    const datalogIdsMode = getUrlSelectionMode({
      allowDefault: true,
      value: datalogIdsValue
    });
    const restoredTrackIds = toStoredStringArray({ value: datalogIdsValue });
    let trackSelectionMode: MapFilters['trackSelectionMode'] = defaults.trackSelectionMode;
    if (datalogIdsMode === 'none') {
      trackSelectionMode = 'none';
    } else if (datalogIdsMode === 'all' || datalogIdsMode === 'default') {
      trackSelectionMode = defaults.trackSelectionMode;
    } else if (restoredTrackIds.length) {
      trackSelectionMode = 'include';
    } else if (['all', 'include', 'none'].includes(String(source.trackSelectionMode))) {
      trackSelectionMode = source.trackSelectionMode as MapFilters['trackSelectionMode'];
    }
    const mode = ['aggregate', 'raw'].includes(String(source.mode))
      ? source.mode as MapFilters['mode']
      : defaults.mode;
    const shape = ['hexagon', 'square', 'circle'].includes(String(source.shape))
      ? source.shape as MapFilters['shape']
      : defaults.shape;
    const aggregateStat = ['min', 'max', 'mean', 'median', 'mode', 'count'].includes(String(source.aggregateStat))
      ? source.aggregateStat as MapFilters['aggregateStat']
      : defaults.aggregateStat;
    const timeFilterMode = ['none', 'absolute', 'relative'].includes(String(source.timeFilterMode))
      ? source.timeFilterMode as TimeFilterMode
      : defaults.timeFilterMode;
    const timeFilterPrecision = ['date', 'datetime'].includes(String(source.timeFilterPrecision))
      ? source.timeFilterPrecision as TimeFilterPrecision
      : defaults.timeFilterPrecision;
    const timeFilterRelativeUnit = ['hours', 'days'].includes(String(source.timeFilterRelativeUnit))
      ? source.timeFilterRelativeUnit as TimeFilterRelativeUnit
      : defaults.timeFilterRelativeUnit;
    const cellSizeMeters = toFiniteNumber(source.cellSizeMeters);
    const timeFilterRelativeAmount = toFiniteNumber(source.timeFilterRelativeAmount);

    return {
      datasetIds: toStoredStringArray({ value: source.datasetIds }),
      combinedDatasetIds: toStoredStringArray({ value: source.combinedDatasetIds }),
      trackIds: restoredTrackIds,
      trackSelectionMode,
      metric: typeof source.metric === 'string' && source.metric ? source.metric : defaults.metric,
      mode,
      shape,
      aggregateStat,
      cellSizeMeters: cellSizeMeters && cellSizeMeters >= 10 ? cellSizeMeters : defaults.cellSizeMeters,
      autoCellSize: typeof source.autoCellSize === 'boolean' ? source.autoCellSize : defaults.autoCellSize,
      applyExcludeAreas: typeof source.applyExcludeAreas === 'boolean'
        ? source.applyExcludeAreas
        : defaults.applyExcludeAreas,
      timeFilterMode,
      timeFilterPrecision,
      timeFilterStart: typeof source.timeFilterStart === 'string' ? source.timeFilterStart : defaults.timeFilterStart,
      timeFilterEnd: typeof source.timeFilterEnd === 'string' ? source.timeFilterEnd : defaults.timeFilterEnd,
      timeFilterRelativeAmount: timeFilterRelativeAmount && timeFilterRelativeAmount > 0
        ? Math.trunc(timeFilterRelativeAmount)
        : defaults.timeFilterRelativeAmount,
      timeFilterRelativeUnit,
      forceRecheck: false
    };
  };

  const normalizeAggregateCache = ({ value }: { value: unknown }): AggregateCache | null => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null;
    }

    const record = value as Record<string, unknown>;
    const ttlSecondsRemaining = record.ttlSecondsRemaining === null || record.ttlSecondsRemaining === undefined
      ? null
      : toFiniteNumber(record.ttlSecondsRemaining);
    return {
      hit: record.hit === true,
      reason: typeof record.reason === 'string' && record.reason ? record.reason : null,
      key: typeof record.key === 'string' && record.key ? record.key : null,
      source: record.source === 'cache'
        ? 'cache'
        : (record.source === 'disabled' ? 'disabled' : 'computed'),
      ttlSecondsRemaining: ttlSecondsRemaining === null ? null : ttlSecondsRemaining
    };
  };

  const resolveAggregateCellCache = ({
    cellCache,
    responseCache
  }: {
    cellCache: unknown;
    responseCache: AggregateCache | null;
  }) => {
    const normalizedCellCache = normalizeAggregateCache({ value: cellCache });
    if (!normalizedCellCache) {
      return responseCache ?? undefined;
    }

    const responseCacheTtlSecondsRemaining = responseCache?.ttlSecondsRemaining ?? null;
    return normalizedCellCache.ttlSecondsRemaining === null && responseCacheTtlSecondsRemaining !== null
      ? {
          ...normalizedCellCache,
          ttlSecondsRemaining: responseCacheTtlSecondsRemaining
        }
      : normalizedCellCache;
  };

  const parseUrlFilterValue = ({ rawValue }: { rawValue: string }) => {
    try {
      return JSON.parse(rawValue);
    } catch {
      try {
        return JSON.parse(decodeURIComponent(rawValue));
      } catch {
        return null;
      }
    }
  };

  const loadUrlTimeSliceSettings = (): TimeSliceSettings | null => {
    if (!browser) {
      return null;
    }

    const url = new URL(window.location.href);
    const amount = toFiniteNumber(url.searchParams.get(timeSliceAmountUrlParamName));
    const rawUnit = url.searchParams.get(timeSliceUnitUrlParamName);
    if (amount === null && rawUnit === null) {
      return null;
    }

    return normalizeTimeSliceSettings({
      settings: {
        amount: amount === null ? createDefaultTimeSliceSettings().amount : amount,
        unit: timeSliceUnitOptions.includes(rawUnit as TimeSliceUnit)
          ? rawUnit as TimeSliceUnit
          : createDefaultTimeSliceSettings().unit
      }
    });
  };

  const loadUrlFilters = (): MapFilters | null => {
    if (!browser) {
      return null;
    }

    const rawValue = new URL(window.location.href).searchParams.get(filterUrlParamName);
    if (!rawValue) {
      return null;
    }

    const parsedValue = parseUrlFilterValue({ rawValue });
    if (!parsedValue || typeof parsedValue !== 'object') {
      return null;
    }

    const parsedRecord = parsedValue as Record<string, unknown>;
    restoredUrlDatasetIdsMode = getUrlSelectionMode({
      allowDefault: true,
      value: hasOwn({ source: parsedRecord, key: 'datasetIds' }) ? parsedRecord.datasetIds : 'default'
    });
    restoredUrlCombinedDatasetIdsMode = getUrlSelectionMode({
      allowDefault: true,
      value: hasOwn({ source: parsedRecord, key: 'combinedDatasetIds' })
        ? parsedRecord.combinedDatasetIds
        : 'default'
    });
    restoredUrlDatalogIdsMode = getUrlSelectionMode({
      allowDefault: true,
      value: hasOwn({ source: parsedRecord, key: 'datalogIds' })
        ? parsedRecord.datalogIds
        : (hasOwn({ source: parsedRecord, key: 'trackIds' }) ? parsedRecord.trackIds : 'default')
    });

    return normalizeStoredFilters({ value: parsedValue });
  };

  const sameStringSet = ({ left, right }: { left: string[]; right: string[] }) => (
    left.length === right.length
    && left.every((value) => right.includes(value))
  );

  const getUrlSelectionValue = ({
    allIds,
    defaultIds,
    selectedIds
  }: {
    allIds: string[];
    defaultIds: string[];
    selectedIds: string[];
  }) => {
    if (sameStringSet({ left: selectedIds, right: defaultIds })) {
      return 'default';
    }

    if (!selectedIds.length) {
      return 'none';
    }

    if (allIds.length && sameStringSet({ left: selectedIds, right: allIds })) {
      return 'all';
    }

    return selectedIds;
  };

  const getDefaultTrackSelectionMode = ({ sourceFilters }: { sourceFilters: MapFilters }) => (
    sourceFilters.datasetIds.length || sourceFilters.combinedDatasetIds.length ? 'all' : 'none'
  );

  const getDatalogUrlSelectionValue = ({ sourceFilters }: { sourceFilters: MapFilters }) => {
    const defaultTrackSelectionMode = getDefaultTrackSelectionMode({ sourceFilters });
    if (sourceFilters.trackSelectionMode === defaultTrackSelectionMode && !sourceFilters.trackIds.length) {
      return 'default';
    }

    if (sourceFilters.trackSelectionMode === 'none') {
      return 'none';
    }

    if (sourceFilters.trackSelectionMode === 'all') {
      return 'all';
    }

    return sourceFilters.trackIds.length ? sourceFilters.trackIds : 'none';
  };

  const getDefaultUrlMetric = () => $sessionStore.ui?.defaultMetric || createInitialFilters().metric;

  const buildCompactUrlFilters = ({ nextFilters }: { nextFilters: MapFilters }) => {
    const clonedFilters = cloneFilters(nextFilters, { clearOneShotControls: true });
    const defaults = createInitialFilters();
    const compactFilters: Record<string, unknown> = {};
    const datasetIdsValue = getUrlSelectionValue({
      allIds: datasets.map((dataset) => dataset.id),
      defaultIds: getDefaultDatasetIds(),
      selectedIds: clonedFilters.datasetIds
    });
    const combinedDatasetIdsValue = getUrlSelectionValue({
      allIds: combinedDatasets.map((combinedDataset) => combinedDataset.id),
      defaultIds: [],
      selectedIds: clonedFilters.combinedDatasetIds
    });
    const datalogIdsValue = getDatalogUrlSelectionValue({ sourceFilters: clonedFilters });

    if (datasetIdsValue !== 'default') {
      compactFilters.datasetIds = datasetIdsValue;
    }

    if (combinedDatasetIdsValue !== 'default') {
      compactFilters.combinedDatasetIds = combinedDatasetIdsValue;
    }

    if (datalogIdsValue !== 'default') {
      compactFilters.datalogIds = datalogIdsValue;
    }

    if (clonedFilters.metric !== getDefaultUrlMetric()) {
      compactFilters.metric = clonedFilters.metric;
    }

    if (clonedFilters.mode !== defaults.mode) {
      compactFilters.mode = clonedFilters.mode;
    }

    if (clonedFilters.mode === 'aggregate') {
      if (clonedFilters.shape !== defaults.shape) {
        compactFilters.shape = clonedFilters.shape;
      }

      if (clonedFilters.aggregateStat !== defaults.aggregateStat) {
        compactFilters.aggregateStat = clonedFilters.aggregateStat;
      }
    }

    if (clonedFilters.autoCellSize !== defaults.autoCellSize) {
      compactFilters.autoCellSize = clonedFilters.autoCellSize;
    }

    if (!clonedFilters.autoCellSize && clonedFilters.cellSizeMeters !== defaults.cellSizeMeters) {
      compactFilters.cellSizeMeters = clonedFilters.cellSizeMeters;
    }

    if (clonedFilters.applyExcludeAreas !== defaults.applyExcludeAreas) {
      compactFilters.applyExcludeAreas = clonedFilters.applyExcludeAreas;
    }

    if (clonedFilters.timeFilterMode !== defaults.timeFilterMode) {
      compactFilters.timeFilterMode = clonedFilters.timeFilterMode;
      if (clonedFilters.timeFilterMode === 'absolute') {
        compactFilters.timeFilterPrecision = clonedFilters.timeFilterPrecision;
        compactFilters.timeFilterStart = clonedFilters.timeFilterStart;
        compactFilters.timeFilterEnd = clonedFilters.timeFilterEnd;
      } else if (clonedFilters.timeFilterMode === 'relative') {
        compactFilters.timeFilterRelativeAmount = clonedFilters.timeFilterRelativeAmount;
        compactFilters.timeFilterRelativeUnit = clonedFilters.timeFilterRelativeUnit;
      }
    }

    return compactFilters;
  };

  const loadUrlMapFocus = () => {
    if (!browser) {
      return null;
    }

    const url = new URL(window.location.href);
    const latitude = toUrlNumber(url.searchParams.get(mapLatUrlParamName));
    const longitude = toUrlNumber(url.searchParams.get(mapLonUrlParamName));
    const zoom = toUrlNumber(url.searchParams.get(mapZoomUrlParamName));
    if (
      latitude === null
      || longitude === null
      || latitude < -90
      || latitude > 90
      || longitude < -180
      || longitude > 180
    ) {
      return null;
    }

    restoredUrlMapLocation = true;
    return createMapFocus({
      latitude,
      longitude,
      source: 'url',
      zoom: zoom === null
        ? viewport.zoom
        : Math.max(1, Math.min(18, Math.round(zoom)))
    });
  };

  const getDefaultDatasetIds = () => datasets
    .filter((dataset) => isMapDatasetDefaultEnabled({
      defaults: mapDatasetDefaults,
      datasetId: dataset.id
    }))
    .map((dataset) => dataset.id);

  const createResetFilters = (): MapFilters => {
    const defaultDatasetIds = getDefaultDatasetIds();
    return {
      ...createInitialFilters(),
      autoCellSize: loadStoredBoolean({
        prefix: autoCellSizeStorageKeyPrefix,
        fallbackValue: true
      }),
      datasetIds: defaultDatasetIds,
      trackIds: [],
      trackSelectionMode: defaultDatasetIds.length ? 'all' : 'none'
    };
  };

  const serializeUrlMapState = ({
    nextFilters,
    nextViewport
  }: {
    nextFilters: MapFilters;
    nextViewport: MapViewport;
  }) => JSON.stringify({
    filters: buildCompactUrlFilters({ nextFilters }),
    latitude: roundUrlCoordinate({ value: nextViewport.centerLat }),
    longitude: roundUrlCoordinate({ value: nextViewport.centerLon }),
    sliceAmount: normalizeTimeSliceSettings({ settings: timeSliceSettings }).amount,
    sliceUnit: normalizeTimeSliceSettings({ settings: timeSliceSettings }).unit,
    zoom: Math.round(nextViewport.zoom)
  });

  const replaceUrlMapState = ({
    nextFilters,
    nextViewport
  }: {
    nextFilters: MapFilters;
    nextViewport: MapViewport;
  }) => {
    if (!browser) {
      return;
    }

    const nextUrlStateKey = serializeUrlMapState({
      nextFilters,
      nextViewport
    });
    const url = new URL(window.location.href);
    const compactFilters = buildCompactUrlFilters({ nextFilters });
    if (Object.keys(compactFilters).length) {
      url.searchParams.set(filterUrlParamName, JSON.stringify(compactFilters));
    } else {
      url.searchParams.delete(filterUrlParamName);
    }
    url.searchParams.set(mapLatUrlParamName, roundUrlCoordinate({ value: nextViewport.centerLat }).toFixed(4));
    url.searchParams.set(mapLonUrlParamName, roundUrlCoordinate({ value: nextViewport.centerLon }).toFixed(4));
    url.searchParams.set(mapZoomUrlParamName, String(Math.round(nextViewport.zoom)));
    const normalizedTimeSliceSettings = normalizeTimeSliceSettings({ settings: timeSliceSettings });
    url.searchParams.set(timeSliceAmountUrlParamName, String(normalizedTimeSliceSettings.amount));
    url.searchParams.set(timeSliceUnitUrlParamName, normalizedTimeSliceSettings.unit);
    window.history.replaceState(window.history.state, '', `${url.pathname}${url.search}${url.hash}`);
    lastUrlStateKey = nextUrlStateKey;
  };

  const loadMapPreferences = () => {
    autoUpdateMap = loadStoredBoolean({
      prefix: autoUpdateStorageKeyPrefix,
      fallbackValue: true
    });
    liveUpdatesEnabled = loadStoredBoolean({
      prefix: liveUpdatesStorageKeyPrefix,
      fallbackValue: true
    });
    const encodedUrlFilters = loadUrlFilters();
    restoredUrlFilters = Boolean(encodedUrlFilters);
    filters = encodedUrlFilters ?? {
      ...filters,
      autoCellSize: loadStoredBoolean({
        prefix: autoCellSizeStorageKeyPrefix,
        fallbackValue: true
      })
    };
    timeSliceSettings = loadUrlTimeSliceSettings() ?? timeSliceSettings;
    timeSliceDraftSettings = { ...timeSliceSettings };

    const storedBasemapKey = loadStoredString({
      prefix: basemapStorageKeyPrefix,
      fallbackValue: 'default'
    });
    basemapKey = availableBasemapKeys.includes(storedBasemapKey as BasemapKey)
      ? storedBasemapKey as BasemapKey
      : 'default';
    const storedSidebarSize = loadStoredString({
      prefix: sidebarSizeStorageKeyPrefix,
      fallbackValue: 'large'
    });
    sidebarSize = availableSidebarSizes.includes(storedSidebarSize as SidebarSize)
      ? storedSidebarSize as SidebarSize
      : 'large';
    mapDatasetDefaults = loadMapDatasetDefaults({
      userId: $sessionStore.user?.id ?? null
    });
    mapDatasetOrder = loadMapDatasetOrder({
      userId: $sessionStore.user?.id ?? null
    });
    mapFieldOrder = loadMapFieldOrder({
      userId: $sessionStore.user?.id ?? null
    });
    mapFieldVisibility = loadMapFieldVisibility({
      userId: $sessionStore.user?.id ?? null
    });
  };

  const toggleSelectedValue = ({
    values,
    value
  }: {
    values: string[];
    value: string;
  }) => values.includes(value)
    ? values.filter((entry) => entry !== value)
    : [...values, value];

  const handleDatasetToggle = (datasetId: string) => {
    const nextDatasetIds = toggleSelectedValue({
      values: filters.datasetIds,
      value: datasetId
    });
    const removedDatasetIds = filters.datasetIds.filter((value) => !nextDatasetIds.includes(value));
    const removedTrackIds = new Set(
      removedDatasetIds.flatMap((value) => datasetTracksByDatasetId[value]?.map((track) => track.id) ?? [])
    );

    const nextTrackIds = nextDatasetIds.length
      ? filters.trackIds.filter((trackId) => !removedTrackIds.has(trackId))
      : [];

    filters = {
      ...filters,
      datasetIds: nextDatasetIds,
      trackIds: nextTrackIds,
      trackSelectionMode: !nextDatasetIds.length
        ? 'none'
        : (
          filters.trackSelectionMode === 'none'
            ? 'all'
            : (
              filters.trackSelectionMode === 'include' && !nextTrackIds.length
                ? 'all'
                : filters.trackSelectionMode
            )
        )
    };
    handleFilterChange();
  };

  const handleDatasetSelectAll = () => {
    filters = {
      ...filters,
      datasetIds: datasets.map((dataset) => dataset.id),
      trackIds: [],
      trackSelectionMode: 'all'
    };
    handleFilterChange();
  };

  const handleDatasetClearAll = () => {
    filters = {
      ...filters,
      datasetIds: [],
      trackIds: [],
      trackSelectionMode: 'none'
    };
    handleFilterChange();
  };

  const handleCombinedDatasetSelectAll = () => {
    filters = {
      ...filters,
      combinedDatasetIds: combinedDatasets.map((combinedDataset) => combinedDataset.id)
    };
    handleFilterChange();
  };

  const handleCombinedDatasetClearAll = () => {
    filters = {
      ...filters,
      combinedDatasetIds: []
    };
    handleFilterChange();
  };

  const handleCombinedDatasetToggle = (combinedDatasetId: string) => {
    filters = {
      ...filters,
      combinedDatasetIds: toggleSelectedValue({
        values: filters.combinedDatasetIds,
        value: combinedDatasetId
      })
    };
    handleFilterChange();
  };

  const getSelectableTrackIds = () => selectedTrackGroups.flatMap((group) => group.tracks.map((track) => track.id));

  const isTrackChecked = ({ trackId }: { trackId: string }) => (
    filters.trackSelectionMode === 'all'
      || (filters.trackSelectionMode === 'include' && filters.trackIds.includes(trackId))
  );

  const handleTrackSelectAll = () => {
    filters = {
      ...filters,
      trackIds: [],
      trackSelectionMode: 'all'
    };
    handleFilterChange();
  };

  const handleTrackClearAll = () => {
    filters = {
      ...filters,
      trackIds: [],
      trackSelectionMode: 'none'
    };
    handleFilterChange();
  };

  const handleTrackToggle = (trackId: string) => {
    const selectableTrackIds = getSelectableTrackIds();
    let nextExplicitTrackIds: string[] = [];

    if (filters.trackSelectionMode === 'all') {
      nextExplicitTrackIds = selectableTrackIds.filter((value) => value !== trackId);
    } else if (filters.trackSelectionMode === 'none') {
      nextExplicitTrackIds = [trackId];
    } else {
      nextExplicitTrackIds = toggleSelectedValue({
        values: filters.trackIds,
        value: trackId
      });
    }

    const allTrackIdsSelected = selectableTrackIds.length > 0
      && nextExplicitTrackIds.length === selectableTrackIds.length
      && selectableTrackIds.every((value) => nextExplicitTrackIds.includes(value));

    filters = {
      ...filters,
      trackIds: allTrackIdsSelected ? [] : nextExplicitTrackIds,
      trackSelectionMode: allTrackIdsSelected
        ? 'all'
        : (
          nextExplicitTrackIds.length ? 'include' : 'none'
        )
    };
    handleFilterChange();
  };

  const formatDatasetOptionSummary = (dataset: DatasetOption) => [
    `${dataset.trackCount} ${t('radtrack-common_tracks-label')}`,
    `${dataset.readingCount} ${t('radtrack-common_readings-label')}`
  ].join(' / ');

  const formatCombinedDatasetOptionSummary = (combinedDataset: CombinedDatasetOption) => [
    t('radtrack-map_combined_member_count-label', { count: combinedDataset.memberCount })
  ].join(' / ');

  const formatTrackOptionSummary = (track: TrackOption) => [
    track.datasetName,
    `${track.rowCount} ${t('radtrack-common_readings-label')}`
  ].join(' / ');

  const resolveFieldLabel = ({
    labels,
    propKey
  }: {
    labels: Map<string, string>;
    propKey: string;
  }) => labels.get(propKey) ?? humanizePropKey(propKey) ?? propKey;

  const getAggregateStatLabel = (aggregateStat: MapFilters['aggregateStat']) => {
    switch (aggregateStat) {
      case 'min':
        return t('radtrack-common_min-label');
      case 'max':
        return t('radtrack-common_max-label');
      case 'median':
        return t('radtrack-common_median-label');
      case 'mode':
        return t('radtrack-common_mode-label');
      case 'count':
        return t('radtrack-common_count-label');
      case 'mean':
      default:
        return t('radtrack-common_mean-label');
    }
  };

  const selectionChipClass = ({ count }: { count: number }) => count ? 'chip start' : 'chip subtle';

  const sortDatasets = ({ values }: { values: DatasetOption[] }) => orderMapDatasets({
    datasets: values,
    order: mapDatasetOrder
  });

  const sortTracks = ({ values }: { values: TrackOption[] }) => [...values].sort((left, right) => (
    (left.trackName ?? '').localeCompare(right.trackName ?? '')
    || left.datasetName.localeCompare(right.datasetName)
    || (right.rowCount - left.rowCount)
    || left.id.localeCompare(right.id)
  ));

  const getTrackGroupsForFilters = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => sourceFilters.datasetIds
    .map((datasetId) => {
      const dataset = datasets.find((entry) => entry.id === datasetId);
      const tracks = sortTracks({
        values: datasetTracksByDatasetId[datasetId] ?? []
      });

      return {
        datasetId,
        datasetName: dataset?.name ?? tracks[0]?.datasetName ?? shortId(datasetId),
        tracks
      };
    })
    .filter((group) => group.tracks.length);

  const getTrackOptionsForFilters = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => {
    const trackGroups = getTrackGroupsForFilters({ sourceFilters });
    if (!trackGroups.length) {
      return [] as TrackOption[];
    }

    if (sourceFilters.trackSelectionMode === 'all') {
      return trackGroups.flatMap((group) => group.tracks);
    }

    if (sourceFilters.trackSelectionMode === 'include') {
      return trackGroups.flatMap((group) => group.tracks)
        .filter((track) => sourceFilters.trackIds.includes(track.id));
    }

    return [] as TrackOption[];
  };

  const collectVisibleFields = ({
    tracks,
    fieldSelector,
    respectVisibility = true
  }: {
    tracks: TrackOption[];
    fieldSelector: (track: TrackOption) => MetricField[];
    respectVisibility?: boolean;
  }) => {
    const merged = new Map<string, MetricField>();

    for (const track of tracks) {
      for (const field of fieldSelector(track)) {
        if (respectVisibility && !isMapFieldVisible({ visibility: mapFieldVisibility, propKey: field.propKey })) {
          continue;
        }

        if (!merged.has(field.propKey)) {
          merged.set(field.propKey, field);
        }
      }
    }

    return orderMapFields({
      fields: [...merged.values()],
      order: mapFieldOrder
    });
  };

  const getPopupFieldDefaults = ({ tracks }: { tracks: TrackOption[] }) => {
    const selectableFields = collectVisibleFields({
      tracks,
      fieldSelector: (track) => mergePopupFields(track.supportedFields ?? []),
      respectVisibility: false
    });
    const states = new Map(selectableFields.map((field) => [field.propKey, {
      hasEnabled: false,
      hasExplicitFalse: false
    }]));

    for (const track of tracks) {
      for (const field of mergePopupFields(track.supportedFields ?? [])) {
        const state = states.get(field.propKey);
        if (!state) {
          continue;
        }

        if (field.popupDefaultEnabled === false) {
          state.hasExplicitFalse = true;
        } else {
          state.hasEnabled = true;
        }
      }
    }

    return Object.fromEntries(
      [...states.entries()].map(([propKey, state]) => [
        propKey,
        state.hasEnabled || !state.hasExplicitFalse
      ])
    );
  };

  const resolvePopupMetricState = ({
    metrics,
    propKey,
    touched,
    defaults
  }: {
    metrics: Record<string, boolean>;
    propKey: string;
    touched: Record<string, boolean>;
    defaults: Record<string, boolean>;
  }) => (
    touched[propKey]
      ? (metrics[propKey] ?? defaults[propKey] ?? true)
      : (defaults[propKey] ?? true)
  );

  const getPopupMetricStates = ({
    metrics,
    fields,
    defaults,
    touched
  }: {
    metrics: Record<string, boolean>;
    fields: MetricField[];
    defaults: Record<string, boolean>;
    touched: Record<string, boolean>;
  }) => Object.fromEntries(
    fields.map((field) => [
      field.propKey,
      resolvePopupMetricState({
        metrics,
        propKey: field.propKey,
        touched,
        defaults
      })
    ])
  );

  const serializePopupMetricStates = ({ states }: { states: Record<string, boolean> }) => JSON.stringify(
    Object.entries(states).sort(([left], [right]) => left.localeCompare(right))
  );

  const effectiveCellSizeMeters = $derived(getAppliedCellSizeMeters({
    filters,
    viewport
  }));

  const activeMetric = $derived(activeQuery?.filters.metric ?? filters.metric);
  const activeMode = $derived(activeQuery?.filters.mode ?? filters.mode);
  const activeShape = $derived(activeQuery?.filters.shape ?? filters.shape);
  const activeAggregateStat = $derived(activeQuery?.filters.aggregateStat ?? filters.aggregateStat);
  const pendingTrackOptions = $derived.by(() => getTrackOptionsForFilters({ sourceFilters: filters }));
  const appliedTrackOptions = $derived.by(() => getTrackOptionsForFilters({
    sourceFilters: activeQuery?.filters ?? filters
  }));
  const pendingAvailableMetricFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: pendingTrackOptions,
    fieldSelector: (track) => track.metricFields ?? [],
    respectVisibility: false
  }));
  const appliedAvailableMetricFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: appliedTrackOptions,
    fieldSelector: (track) => track.metricFields ?? [],
    respectVisibility: false
  }));
  const pendingAvailablePopupFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: pendingTrackOptions,
    fieldSelector: (track) => mergePopupFields(track.supportedFields ?? []),
    respectVisibility: false
  }));
  const appliedAvailablePopupFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: appliedTrackOptions,
    fieldSelector: (track) => mergePopupFields(track.supportedFields ?? []),
    respectVisibility: false
  }));
  const popupSelectableFields = $derived.by<MetricField[]>(() => pendingAvailablePopupFields);
  const pendingPopupFieldDefaults = $derived.by<Record<string, boolean>>(() => getPopupFieldDefaults({
    tracks: pendingTrackOptions
  }));
  const appliedPopupFieldDefaults = $derived.by<Record<string, boolean>>(() => getPopupFieldDefaults({
    tracks: appliedTrackOptions
  }));
  const pendingMetricOptionLabels = $derived.by(() => getMetricOptionLabels(pendingAvailableMetricFields));
  const appliedMetricOptionLabels = $derived.by(() => getMetricOptionLabels(appliedAvailableMetricFields));
  const pendingPopupOptionLabels = $derived.by(() => getMetricOptionLabels(pendingAvailablePopupFields));
  const appliedPopupOptionLabels = $derived.by(() => getMetricOptionLabels(appliedAvailablePopupFields));
  const getPendingMetricLabel = (metric: string) => resolveFieldLabel({
    labels: pendingMetricOptionLabels,
    propKey: metric
  });
  const getAppliedMetricLabel = (metric: string) => resolveFieldLabel({
    labels: appliedMetricOptionLabels,
    propKey: metric
  });
  const getPendingPopupFieldLabel = (propKey: string) => resolveFieldLabel({
    labels: pendingPopupOptionLabels,
    propKey
  });
  const getAppliedPopupFieldLabel = (propKey: string) => resolveFieldLabel({
    labels: appliedPopupOptionLabels,
    propKey
  });
  const activeLegendSummary = $derived.by(() => t('radtrack-map_legend_summary-label', {
    metric: getAppliedMetricLabel(activeMetric),
    stat: getAggregateStatLabel(activeAggregateStat)
  }));
  const activeAppliedCellSizeMeters = $derived(activeQuery?.filters.appliedCellSizeMeters ?? effectiveCellSizeMeters);
  const activePlaybackFilters = $derived.by<AppliedMapFilters | null>(() => activeQuery
    ? createPlaybackAppliedFilters({
        filters: activeQuery.filters,
        viewport
      })
    : null);
  const activeSidebarWidth = $derived(
    sidebarSizeOptions.find((option) => option.key === sidebarSize)?.width ?? '35rem'
  );
  const activeTimeSliceWindows = $derived.by(() => activeQuery?.filters.timeFilterMode === 'absolute'
    ? buildTimeSliceWindows({
        sourceFilters: activeQuery.filters,
        settings: timeSliceSettings
      })
    : []);
  const activeTimeSlicePointSourceKey = $derived(activeQuery
    ? serializeTimeSlicePointSourceQuery({ filters: activeQuery.filters })
    : '');
  const activeTimeSliceSourceKey = $derived(activeQuery
    ? serializeTimeSliceSourceQuery({
        filters: activeQuery.filters,
        settings: timeSliceSettings
      })
    : '');
  const activeTimeSliceSelectedPointCount = $derived(
    loadedTimeSlicePointCountKey === activeTimeSlicePointSourceKey ? timeSlicePointCount : null
  );
  const visibleTimeSlicePointCount = $derived.by(() => (
    activeMode === 'aggregate'
      ? aggregates.reduce((total, cell) => total + cell.pointCount, 0)
      : rawPointTotalCount || points.length
  ));
  const timeSliceDisplayPointCount = $derived(activeTimeSliceSelectedPointCount ?? visibleTimeSlicePointCount);
  const timeSliceLargeLoadConfirmationKey = $derived.by(() => activeQuery && timeSliceEnabled
    ? `${activeTimeSlicePointSourceKey}:${timeSliceDisplayPointCount}`
    : '');
  const timeSliceNeedsLargeLoadConfirmation = $derived.by(() => Boolean(
    timeSliceEnabled
    && activeTimeSliceWindows.length
    && activeTimeSliceSelectedPointCount !== null
    && activeTimeSliceSelectedPointCount > timeSliceLargePointWarningThreshold
    && timeSliceLargeLoadConfirmedKey !== timeSliceLargeLoadConfirmationKey
  ));
  const activeTimeSlicePreparedWindows = $derived.by<PreparedTimeSliceWindow[]>(() => (
    loadedTimeSliceSourceKey === activeTimeSliceSourceKey ? timeSlicePreparedWindows : []
  ));
  const activeTimeSliceWindow = $derived.by<PreparedTimeSliceWindow | null>(() => {
    if (
      !timeSliceEnabled
      || timeSliceNeedsLargeLoadConfirmation
      || loadedTimeSliceSourceKey !== activeTimeSliceSourceKey
    ) {
      return null;
    }

    return activeTimeSlicePreparedWindows[
      Math.max(0, Math.min(timeSliceIndex, activeTimeSlicePreparedWindows.length - 1))
    ] ?? null;
  });
  const activeTimeSliceDisplayStartMs = $derived.by(() => (
    timeSliceCurrentOnly
      ? activeTimeSliceWindow?.startMs ?? null
      : activeTimeSliceWindows[0]?.startMs ?? activeTimeSliceWindow?.startMs ?? null
  ));
  const activeTimeSliceDisplayStartIso = $derived.by(() => (
    activeTimeSliceDisplayStartMs === null
      ? null
      : new Date(activeTimeSliceDisplayStartMs).toISOString()
  ));
  const timeSliceIntervalLabel = $derived.by(() => {
    const normalized = normalizeTimeSliceSettings({ settings: timeSliceSettings });
    return t('radtrack-map_time_slice_interval_summary-label', {
      amount: formatCount(normalized.amount),
      unit: t(getTimeSliceUnitLabelKey({
        amount: normalized.amount,
        unit: normalized.unit
      }))
    });
  });
  const timeSliceSummary = $derived.by(() => {
    if (!timeSliceEnabled) {
      return t('radtrack-map_time_slice_all_data-label');
    }

    if (timeSliceNeedsLargeLoadConfirmation) {
      return t('radtrack-map_time_slice_large_load_warning-label', {
        count: formatCount(timeSliceDisplayPointCount)
      });
    }

    if (!activeTimeSliceWindows.length) {
      return t('radtrack-map_time_slice_unavailable-label');
    }

    if (
      timeSliceSourceErrorKey === activeTimeSliceSourceKey
      || timeSlicePointCountErrorKey === activeTimeSlicePointSourceKey
    ) {
      return t('radtrack-map_time_slice_source_failed-label');
    }

    if (
      timeSlicePointCountLoading
      || activeTimeSliceSelectedPointCount === null
      || loadedTimeSliceSourceKey !== activeTimeSliceSourceKey
    ) {
      return t('radtrack-map_time_slice_source_loading-label');
    }

    return activeTimeSliceWindow
      ? t('radtrack-map_time_slice_window_summary-label', {
        current: formatCount(activeTimeSliceWindow.index + 1),
        total: formatCount(activeTimeSliceWindows.length),
        start: formatTime(activeTimeSliceDisplayStartIso ?? activeTimeSliceWindow.startIso),
        end: formatTime(activeTimeSliceWindow.endIso)
      })
      : t('radtrack-map_time_slice_unavailable-label');
  });
  const timeSlicePerformanceWarning = $derived.by(() => {
    const sourceCount = timeSliceSourcePoints.length;
    if (!timeSliceEnabled || !activeTimeSliceWindow || sourceCount < 50000) {
      return null;
    }

    return t('radtrack-map_time_slice_performance_warning-label', {
      count: formatCount(sourceCount)
    });
  });
  const displayedPoints = $derived.by<MapPoint[]>(() => {
    if (activeMode !== 'raw') {
      return [];
    }

    return activeTimeSliceWindow
      ? getPreparedTimeSlicePointsInViewport({
          currentOnly: timeSliceCurrentOnly,
          points: timeSliceSourcePoints,
          sourceViewport: viewport,
          window: activeTimeSliceWindow
        })
      : points;
  });
  const selectedTrackGroups = $derived.by(() => getTrackGroupsForFilters({ sourceFilters: filters }));
  const pendingPopupMetricStates = $derived.by<Record<string, boolean>>(() => getPopupMetricStates({
    metrics: popupFields.metrics,
    fields: popupSelectableFields,
    defaults: pendingPopupFieldDefaults,
    touched: popupFieldTouched
  }));
  const appliedPopupMetricStates = $derived.by<Record<string, boolean>>(() => getPopupMetricStates({
    metrics: appliedPopupState.metrics,
    fields: appliedAvailablePopupFields,
    defaults: appliedPopupFieldDefaults,
    touched: appliedPopupState.touched
  }));
  const appliedPopupFields = $derived.by<PopupFields>(() => ({
    metrics: appliedPopupMetricStates
  }));
  const popupLabelsRecord = $derived.by(() => Object.fromEntries(appliedPopupOptionLabels.entries()));
  const fieldValueTypesRecord = $derived.by(() => Object.fromEntries(
    appliedAvailablePopupFields.map((field) => [field.propKey, field.valueType])
  ));
  const visiblePopupFields = $derived.by(() => popupSelectableFields
    .filter((field) => pendingPopupMetricStates[field.propKey]));
  const appliedVisiblePopupFields = $derived.by(() => appliedAvailablePopupFields
    .filter((field) => appliedPopupMetricStates[field.propKey]));
  const displayedAggregateSourcePoints = $derived.by<MapPoint[]>(() => (
    activeMode === 'aggregate' && activeTimeSliceWindow
      ? getPreparedTimeSlicePointsInViewport({
          currentOnly: timeSliceCurrentOnly,
          points: timeSliceSourcePoints,
          sourceViewport: viewport,
          window: activeTimeSliceWindow
        })
      : []
  ));
  const displayedAggregates = $derived.by<AggregateCell[]>(() => {
    if (activeMode !== 'aggregate') {
      return [];
    }

    if (!activeTimeSliceWindow) {
      return aggregates;
    }

    if (!activePlaybackFilters || timeSliceSourceLoading) {
      return [];
    }

    return buildAggregateCellsForPoints({
      popupFieldsForCells: appliedVisiblePopupFields,
      sourceFilters: activePlaybackFilters,
      sourcePoints: displayedAggregateSourcePoints
    });
  });
  const visibleRawPointCount = $derived.by(() => {
    if (activeMode !== 'aggregate') {
      return displayedPoints.length;
    }

    return displayedAggregates.reduce((total, cell) => total + cell.pointCount, 0);
  });
  const visibleCellCount = $derived.by(() => (
    activeMode === 'aggregate' ? displayedAggregates.length : 0
  ));
  const activeColorScale = $derived.by(() => getEffectiveColorScale({
    cells: displayedAggregates,
    aggregateStat: activeAggregateStat,
    settings: colorScaleSettings
  }));
  const activeLegendScaleValues = $derived.by(() => ({
    low: activeColorScale ? formatNumber(activeColorScale.low) : null,
    mid: activeColorScale ? formatNumber(activeColorScale.mid) : null,
    high: activeColorScale ? formatNumber(activeColorScale.high) : null
  }));
  const legendNeedsWide = $derived.by(() => {
    const labels = [
      `${t('radtrack-common_low-label')} ${activeLegendScaleValues.low ?? ''}`.trim(),
      `${t('radtrack-common_mid-label')} ${activeLegendScaleValues.mid ?? ''}`.trim(),
      `${t('radtrack-common_high-label')} ${activeLegendScaleValues.high ?? ''}`.trim()
    ];

    return labels.some((label) => label.length > 18)
      || labels.join('').length > 48
      || activeLegendSummary.length > 20;
  });
  const aggregateCellModalColumns = $derived.by<AggregateCellModalColumn[]>(() => {
    const columns = new Map<string, AggregateCellModalColumn>();

    for (const field of appliedVisiblePopupFields) {
      if (field.propKey === aggregateDataCountPropKey) {
        continue;
      }

      const aggregateBasePropKey = getAggregateTimeBasePropKey(field.propKey);
      const propKey = aggregateBasePropKey ?? field.propKey;
      if (columns.has(propKey)) {
        continue;
      }

      columns.set(propKey, {
        propKey,
        label: aggregateBasePropKey ? humanizePropKey(propKey) : field.displayName,
        valueType: aggregateBasePropKey ? 'time' : field.valueType
      });
    }

    if (!columns.size) {
      for (const field of normalizeSupportedFields([
        { propKey: 'occurredAt' },
        { propKey: 'latitude' },
        { propKey: 'longitude' }
      ])) {
        columns.set(field.propKey, {
          propKey: field.propKey,
          label: field.displayName,
          valueType: field.valueType
        });
      }
    }

    return [...columns.values()];
  });
  const appliedVisiblePointMetricFields = $derived.by(() => appliedVisiblePopupFields
    .filter((field) => (
      field.valueType !== 'string'
      && field.source !== 'synthetic'
      && !isAggregateTimePropKey(field.propKey)
      && !['occurredAt', 'latitude', 'longitude'].includes(field.propKey)
    )));
  const enabledPopupFieldCount = $derived.by(() => visiblePopupFields.length);
  const aggregateCellModalCurrentCell = $derived.by(() => {
    const modalState = aggregateCellModalState;
    if (!modalState || !activeQuery || activeQuery.key !== modalState.queryKey) {
      return null;
    }

    if (
      activeMode !== 'aggregate'
      || activeShape !== modalState.shape
      || getAppliedCellSizeMeters({
        filters: activeQuery.filters,
        viewport: activeQuery.viewport
      }) !== modalState.cellSizeMeters
    ) {
      return null;
    }

    return displayedAggregates.find((cell) => cell.id === modalState.cellId) ?? null;
  });
  const aggregateCellModalDisplayCell = $derived.by(() => (
    aggregateCellModalCurrentCell
    ?? aggregateCellModalState?.cell
    ?? null
  ));
  const aggregateCellModalSubtitle = $derived.by(() => aggregateCellModalDisplayCell
    ? formatAggregateTimeRange({ timeRange: aggregateCellModalDisplayCell.timeRange })
    : null);
  const aggregateCellModalVisiblePoints = $derived.by<MapPoint[]>(() => {
    const snapshot = activeQuery;
    if (
      aggregateCellModalCurrentCell
      && activeTimeSliceWindow
      && activeMode === 'aggregate'
      && snapshot
      && activePlaybackFilters
    ) {
      return displayedAggregateSourcePoints.filter((point) => (
        getAggregateCellForPoint({
          point,
          sourceFilters: activePlaybackFilters
        })?.id === aggregateCellModalCurrentCell.id
      ));
    }

    return aggregateCellModalPoints;
  });
  const aggregateCellModalRows = $derived.by<AggregateCellModalRow[]>(() => aggregateCellModalVisiblePoints.map((point) => ({
    id: point.id,
    values: Object.fromEntries(aggregateCellModalColumns.map((column) => [
      column.propKey,
      getPointPopupFieldValue({
        point,
        propKey: column.propKey,
        cell: aggregateCellModalDisplayCell
      })
    ]))
  })));
  const aggregateCellModalExportContext = $derived.by<AggregateCellModalExportContext | null>(() => {
    if (!aggregateCellModalState || !activeQuery || activeQuery.key !== aggregateCellModalState.queryKey) {
      return null;
    }

    const cell = aggregateCellModalDisplayCell;
    const absoluteTimeWindow = resolveAbsoluteTimeWindow({
      sourceFilters: activeQuery.filters
    });
    const timeFilter = activeQuery.filters.timeFilterMode === 'absolute'
      ? {
          mode: 'absolute',
          precision: activeQuery.filters.timeFilterPrecision,
          dateFrom: activeTimeSliceWindow?.startIso ?? absoluteTimeWindow?.dateFrom ?? null,
          dateTo: activeTimeSliceWindow?.endIso ?? absoluteTimeWindow?.dateTo ?? null,
          fullDateFrom: absoluteTimeWindow?.dateFrom ?? null,
          fullDateTo: absoluteTimeWindow?.dateTo ?? null,
          sliceIndex: activeTimeSliceWindow?.index ?? null
        }
      : activeQuery.filters.timeFilterMode === 'relative'
        ? {
            mode: 'relative',
            amount: Math.max(1, Math.trunc(toFiniteNumber(activeQuery.filters.timeFilterRelativeAmount) ?? 0)),
            unit: activeQuery.filters.timeFilterRelativeUnit
          }
        : {
            mode: 'none'
          };

    return {
      build: $sessionStore.build?.label ?? null,
      fileNameBase: `radtrack-map-cell-${aggregateCellModalState.cellId}`,
      filters: {
        mode: activeQuery.filters.mode,
        metric: activeQuery.filters.metric,
        metricLabel: getAppliedMetricLabel(activeQuery.filters.metric),
        aggregateStat: activeQuery.filters.aggregateStat,
        aggregateStatLabel: getAggregateStatLabel(activeQuery.filters.aggregateStat),
        shape: aggregateCellModalState.shape,
        cellSizeMeters: aggregateCellModalState.cellSizeMeters,
        cellId: aggregateCellModalState.cellId,
        cellCenter: cell
          ? {
              latitude: cell.center.latitude,
              longitude: cell.center.longitude
            }
          : null,
        cellRadiusMeters: cell?.radiusMeters ?? null,
        cellTimeRange: cell?.timeRange ?? null,
        datasetIds: [...activeQuery.filters.datasetIds],
        combinedDatasetIds: [...activeQuery.filters.combinedDatasetIds],
        datalogIds: [...activeQuery.filters.trackIds],
        datalogSelectionMode: activeQuery.filters.trackSelectionMode,
        applyExcludeAreas: activeQuery.filters.applyExcludeAreas,
        timeFilter,
        viewport: {
          centerLat: activeQuery.viewport.centerLat,
          centerLon: activeQuery.viewport.centerLon,
          minLat: activeQuery.viewport.minLat,
          maxLat: activeQuery.viewport.maxLat,
          minLon: activeQuery.viewport.minLon,
          maxLon: activeQuery.viewport.maxLon,
          zoom: activeQuery.viewport.zoom
        }
      }
    };
  });
  const trackSelectionSummary = $derived.by(() => {
    if (!filters.datasetIds.length || filters.trackSelectionMode === 'none') {
      return t('radtrack-common_none');
    }

    if (filters.trackSelectionMode === 'all') {
      return t('radtrack-common_all-label');
    }

    return t('radtrack-map_selected_count-label', { count: filters.trackIds.length });
  });
  const timeFilterValidationMessage = $derived.by(() => {
    const key = getTimeFilterValidationKey({ sourceFilters: filters });
    return key ? t(key) : null;
  });
  const timeFilterSummary = $derived.by(() => {
    if (filters.timeFilterMode === 'none') {
      return t('radtrack-common_none');
    }

    if (filters.timeFilterMode === 'relative') {
      const amount = Math.max(1, Math.trunc(toFiniteNumber(filters.timeFilterRelativeAmount) ?? 0));
      const unitKey = filters.timeFilterRelativeUnit === 'days'
        ? (amount === 1 ? 'radtrack-map_time_filter_day-label' : 'radtrack-map_time_filter_days-label')
        : (amount === 1 ? 'radtrack-map_time_filter_hour-label' : 'radtrack-map_time_filter_hours-label');
      return t('radtrack-map_time_filter_since_summary-label', {
        amount: formatCount(amount),
        unit: t(unitKey)
      });
    }

    if (!filters.timeFilterStart || !filters.timeFilterEnd || timeFilterValidationMessage) {
      return t('radtrack-map_time_filter_incomplete-label');
    }

    const startValue = filters.timeFilterStart.replace('T', ' ');
    const endValue = filters.timeFilterEnd.replace('T', ' ');
    return `${startValue} -> ${endValue}`;
  });
  const basemapOptions = $derived.by<BasemapOption[]>(() => [
    {
      key: 'default',
      label: t('radtrack-map_basemap_default-label'),
      tileUrlTemplate: $sessionStore.ui?.tileUrlTemplate ?? '',
      attribution: $sessionStore.ui?.attribution ?? ''
    },
    {
      key: 'satellite',
      label: t('radtrack-map_basemap_satellite-label'),
      tileUrlTemplate: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
      attribution: 'Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
    },
    {
      key: 'light',
      label: t('radtrack-map_basemap_light-label'),
      tileUrlTemplate: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
      attribution: '© OpenStreetMap contributors © CARTO'
    },
    {
      key: 'topo',
      label: t('radtrack-map_basemap_topo-label'),
      tileUrlTemplate: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
      attribution: 'Map data: © OpenStreetMap contributors, SRTM | Map style: © OpenTopoMap (CC-BY-SA)'
    }
  ]);
  const activeBasemap = $derived.by(() => basemapOptions.find((option) => option.key === basemapKey) ?? basemapOptions[0]);

  const getCurrentQuerySnapshot = ({
    filters: nextFilters = filters,
    viewport: nextViewport = viewport
  }: {
    filters?: MapFilters | AppliedMapFilters;
    viewport?: MapViewport;
  } = {}) => createQuerySnapshot({
    filters: nextFilters,
    viewport: nextViewport
  });

  const getAppliedFilters = () => cloneFilters(activeQuery?.filters ?? filters);
  const getPendingPopupStateSnapshot = (): PopupStateSnapshot => ({
    metrics: { ...popupFields.metrics },
    touched: { ...popupFieldTouched }
  });
  const getAppliedPopupStateSnapshot = (): PopupStateSnapshot => clonePopupStateSnapshot(appliedPopupState);

  const hasPendingSidebarChanges = () => (
    serializeFilters({ filters }) !== serializeFilters({ filters: activeQuery?.filters ?? filters })
    || serializePopupMetricStates({ states: pendingPopupMetricStates }) !== serializePopupMetricStates({
      states: appliedPopupMetricStates
    })
  );
  const hasPendingUrlState = $derived.by(() => (
    filterUrlSyncReady
    && serializeUrlMapState({
      nextFilters: filters,
      nextViewport: viewport
    }) !== lastUrlStateKey
  ));

  const canUpdateMap = $derived.by(() => (
    !loading
    && !timeFilterValidationMessage
    && (
      hasPendingSidebarChanges()
      || (!autoUpdateMap && hasPendingUrlState)
      || filters.timeFilterMode === 'relative'
      || filters.forceRecheck
    )
  ));
  const liveUpdatePollingIntervalMs = $derived.by(() => Math.max(
    5000,
    (Math.max(1, Math.trunc(toFiniteNumber($sessionStore.ui?.liveUpdatePollingIntervalSeconds) ?? 15))) * 1000
  ));
  const liveUpdatesAllowed = $derived.by(() => filters.timeFilterMode !== 'absolute');
  const liveUpdatesActive = $derived.by(() => (
    browser
    && lookupsReady
    && Boolean(activeQuery)
    && liveUpdatesEnabled
    && liveUpdatesAllowed
  ));
  const liveUpdateStatusLabel = $derived.by(() => {
    if (!liveUpdatesAllowed) {
      return t('radtrack-map_live_updates_disabled_absolute-label');
    }

    if (!liveUpdatesEnabled || !liveUpdatesActive) {
      return t('radtrack-map_live_updates_off-label');
    }

    return liveUpdateTransport === 'polling'
      ? t('radtrack-map_live_updates_polling-label')
      : t('radtrack-map_live_updates_websocket-label');
  });

  const clearScheduledSidebarUpdate = () => {
    if (!sidebarUpdateTimer) {
      return;
    }

    clearTimeout(sidebarUpdateTimer);
    sidebarUpdateTimer = null;
  };

  const requestMapData = ({
    popupState,
    snapshot
  }: {
    popupState: PopupStateSnapshot;
    snapshot: MapQuerySnapshot;
  }) => {
    queuedSidebarSnapshot = {
      query: snapshot,
      popupState: clonePopupStateSnapshot(popupState)
    };

    if (loading) {
      return;
    }

    const nextSnapshot = queuedSidebarSnapshot;
    queuedSidebarSnapshot = null;

    if (!nextSnapshot) {
      return;
    }

    void loadMapData({
      popupState: nextSnapshot.popupState,
      snapshot: nextSnapshot.query
    });
  };

  const applySidebarFilters = () => {
    clearScheduledSidebarUpdate();
    if (timeFilterValidationMessage) {
      return;
    }

    const nextPopupState = getPendingPopupStateSnapshot();
    const nextSnapshot = getCurrentQuerySnapshot();

    if (nextSnapshot.key === activeQuery?.key && !nextSnapshot.filters.forceRecheck) {
      appliedPopupState = nextPopupState;
      return;
    }

    requestMapData({
      popupState: nextPopupState,
      snapshot: nextSnapshot
    });
  };

  const scheduleSidebarAutoUpdate = () => {
    clearScheduledSidebarUpdate();

    if (!autoUpdateMap || !lookupsReady || timeFilterValidationMessage || !hasPendingSidebarChanges()) {
      return;
    }

    sidebarUpdateTimer = setTimeout(() => {
      sidebarUpdateTimer = null;
      applySidebarFilters();
    }, autoUpdateDebounceMs);
  };

  const buildBaseQuery = ({
    includeViewport = true,
    snapshot
  }: {
    includeViewport?: boolean;
    snapshot: MapQuerySnapshot;
  }) => ({
    datasetIds: snapshot.filters.datasetIds,
    combinedDatasetIds: snapshot.filters.combinedDatasetIds,
    datalogIds: snapshot.filters.trackIds,
    datalogSelectionMode: snapshot.filters.trackSelectionMode,
    metric: snapshot.filters.metric,
    ...(includeViewport
      ? {
          minLat: snapshot.viewport.minLat,
          maxLat: snapshot.viewport.maxLat,
          minLon: snapshot.viewport.minLon,
          maxLon: snapshot.viewport.maxLon
        }
      : {}),
    applyExcludeAreas: snapshot.filters.applyExcludeAreas,
    ...buildTimeFilterQuery({ sourceFilters: snapshot.filters })
  });

  const buildLiveUpdateQuery = ({
    snapshot,
    sinceCursor
  }: {
    snapshot: MapQuerySnapshot;
    sinceCursor: number | null;
  }) => ({
    ...buildBaseQuery({ snapshot }),
    sinceCursor
  });

  const buildTimeBoundsQuery = ({
    sourceFilters
  }: {
    sourceFilters: MapFilters | AppliedMapFilters;
  }) => ({
    datasetIds: sourceFilters.datasetIds,
    combinedDatasetIds: sourceFilters.combinedDatasetIds,
    datalogIds: sourceFilters.trackIds,
    datalogSelectionMode: sourceFilters.trackSelectionMode,
    applyExcludeAreas: sourceFilters.applyExcludeAreas
  });

  const buildLiveUpdateStatusQuery = ({
    includePoints = false,
    sinceCursor,
    snapshot
  }: {
    includePoints?: boolean;
    sinceCursor: number | null;
    snapshot: MapQuerySnapshot;
  }) => ({
    ...buildLiveUpdateQuery({
      snapshot,
      sinceCursor
    }),
    includePoints,
    pointLimit: includePoints ? liveUpdatePointLimit : undefined
  });

  const normalizeLiveUpdatePointDetail = ({ value }: { value: unknown }): LiveUpdatePointDetail | null => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
      return null;
    }

    const record = value as Record<string, unknown>;
    const cursor = toLiveUpdateCursor(record.cursor);
    const latitude = toFiniteNumber(record.latitude);
    const longitude = toFiniteNumber(record.longitude);
    if (
      cursor === null
      || typeof record.id !== 'string'
      || typeof record.datasetId !== 'string'
      || typeof record.datalogId !== 'string'
    ) {
      return null;
    }

    return {
      cursor,
      publishedAt: typeof record.publishedAt === 'string' ? record.publishedAt : new Date().toISOString(),
      id: record.id,
      datasetId: record.datasetId,
      datalogId: record.datalogId,
      occurredAt: typeof record.occurredAt === 'string' ? record.occurredAt : null,
      receivedAt: typeof record.receivedAt === 'string' ? record.receivedAt : null,
      latitude,
      longitude,
      altitudeMeters: toFiniteNumber(record.altitudeMeters),
      accuracy: toFiniteNumber(record.accuracy),
      measurements: record.measurements && typeof record.measurements === 'object' && !Array.isArray(record.measurements)
        ? Object.fromEntries(Object.entries(record.measurements).flatMap(([key, rawValue]) => {
            const numericValue = toFiniteNumber(rawValue);
            return numericValue === null ? [] : [[key, numericValue]];
          }))
        : {}
    };
  };

  const normalizeLiveUpdateStatus = ({ value }: { value: unknown }): LiveUpdateStatus => {
    const record = value && typeof value === 'object' && !Array.isArray(value)
      ? value as Record<string, unknown>
      : {};

    return {
      hasUpdates: record.hasUpdates === true,
      updateCount: Math.max(0, Math.trunc(toFiniteNumber(record.updateCount) ?? 0)),
      latestCursor: toLiveUpdateCursor(record.latestCursor) ?? undefined,
      latestPublishedAt: typeof record.latestPublishedAt === 'string' ? record.latestPublishedAt : undefined,
      currentCursor: toLiveUpdateCursor(record.currentCursor) ?? undefined,
      currentPublishedAt: typeof record.currentPublishedAt === 'string' ? record.currentPublishedAt : undefined,
      points: Array.isArray(record.points)
        ? record.points.map((point) => normalizeLiveUpdatePointDetail({ value: point })).filter((point): point is LiveUpdatePointDetail => Boolean(point))
        : undefined
    };
  };

  const clearLiveUpdateDisplay = ({ clearLog = false }: { clearLog?: boolean } = {}) => {
    liveUpdateMarkers = [];
    liveUpdateMarkersSetAt = 0;
    if (clearLog) {
      liveUpdateLog = [];
    }
  };

  const appendLiveUpdatePoints = ({
    points: nextPoints
  }: {
    points: LiveUpdatePointDetail[];
  }) => {
    if (!nextPoints.length) {
      return;
    }

    const existingKeys = new Set(liveUpdateLog.map((entry) => entry.key));
    const nextEntries = nextPoints
      .map((point) => ({
        key: `${point.cursor}:${point.id}`,
        cursor: point.cursor,
        publishedAt: point.publishedAt,
        point
      }))
      .filter((entry) => !existingKeys.has(entry.key));

    if (nextEntries.length) {
      liveUpdateLog = [...nextEntries, ...liveUpdateLog].slice(0, maxLiveUpdateLogEntries);
    }

    const nextMarkers = nextPoints
      .filter((point) => Number.isFinite(point.latitude) && Number.isFinite(point.longitude))
      .map((point) => ({
        id: `${point.cursor}:${point.id}`,
        latitude: Number(point.latitude),
        longitude: Number(point.longitude),
        occurredAt: point.occurredAt ?? point.receivedAt,
        publishedAt: point.publishedAt
      }));

    if (nextMarkers.length) {
      liveUpdateMarkers = nextMarkers;
      liveUpdateMarkersSetAt = Date.now();
    }
  };

  const handleLiveUpdateStatus = async ({ status }: { status: LiveUpdateStatus }) => {
    const currentCursor = toLiveUpdateCursor(status.currentCursor);
    if (status.points?.length) {
      appendLiveUpdatePoints({
        points: status.points
      });
    }

    if (status.hasUpdates) {
      liveUpdatePendingCursor = maxLiveUpdateCursor(
        liveUpdatePendingCursor,
        toLiveUpdateCursor(status.latestCursor),
        currentCursor
      );
      await triggerLiveUpdateRefresh();
      return;
    }

    liveUpdateCursor = maxLiveUpdateCursor(liveUpdateCursor, currentCursor);
  };

  const fetchLiveUpdateDetailsAndRefresh = async () => {
    if (!activeQuery || !liveUpdatesActive) {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: '/api/map/live-updates',
        query: buildLiveUpdateStatusQuery({
          includePoints: true,
          snapshot: activeQuery,
          sinceCursor: liveUpdateCursor
        })
      });
      await handleLiveUpdateStatus({
        status: normalizeLiveUpdateStatus({ value: response.result })
      });
    } catch {
      liveUpdateTransport = liveUpdatesActive ? 'polling' : 'idle';
    }
  };

  const clearLiveUpdateReconnectTimer = () => {
    if (!liveUpdateReconnectTimer) {
      return;
    }

    clearTimeout(liveUpdateReconnectTimer);
    liveUpdateReconnectTimer = null;
  };

  const clearLiveUpdateTimer = () => {
    if (!liveUpdateTimer) {
      return;
    }

    clearInterval(liveUpdateTimer);
    liveUpdateTimer = null;
  };

  const clearLiveUpdateMarkerTimer = () => {
    if (!liveUpdateMarkerTimer) {
      return;
    }

    clearInterval(liveUpdateMarkerTimer);
    liveUpdateMarkerTimer = null;
  };

  const clearTimeSlicePlaybackTimer = () => {
    if (!timeSlicePlaybackTimer) {
      return;
    }

    clearInterval(timeSlicePlaybackTimer);
    timeSlicePlaybackTimer = null;
  };

  const closeLiveUpdateSocket = ({ resetTransport = true }: { resetTransport?: boolean } = {}) => {
    liveUpdateSocketReady = false;
    liveUpdateSocketSubscriptionKey = '';

    if (!liveUpdateSocket) {
      if (resetTransport) {
        liveUpdateTransport = 'idle';
      }
      return;
    }

    const socket = liveUpdateSocket;
    liveUpdateSocket = null;
    socket.onopen = null;
    socket.onmessage = null;
    socket.onerror = null;
    socket.onclose = null;
    if (socket.readyState === WebSocket.CONNECTING || socket.readyState === WebSocket.OPEN) {
      socket.close();
    }

    if (resetTransport) {
      liveUpdateTransport = 'idle';
    }
  };

  const teardownLiveUpdates = () => {
    clearLiveUpdateReconnectTimer();
    clearLiveUpdateTimer();
    closeLiveUpdateSocket();
    liveUpdatePendingCursor = null;
    liveUpdateRefreshQueued = false;
    liveUpdateRefreshInFlight = false;
    clearLiveUpdateDisplay({ clearLog: true });
  };

  const scheduleLiveUpdateReconnect = () => {
    if (!liveUpdatesActive || liveUpdateReconnectTimer || liveUpdateSocket) {
      return;
    }

    liveUpdateReconnectTimer = setTimeout(() => {
      liveUpdateReconnectTimer = null;
      openLiveUpdateSocket();
    }, liveUpdatesReconnectDelayMs);
  };

  const sendLiveUpdateSubscription = ({ force = false }: { force?: boolean } = {}) => {
    if (
      !liveUpdateSocket
      || liveUpdateSocket.readyState !== WebSocket.OPEN
      || !liveUpdateSocketReady
      || !activeQuery
      || !liveUpdatesActive
    ) {
      return;
    }

    if (!force && liveUpdateSocketSubscriptionKey === activeQuery.key) {
      return;
    }

    liveUpdateSocket.send(JSON.stringify({
      type: 'subscribe',
      sinceCursor: liveUpdateCursor,
      filters: buildBaseQuery({ snapshot: activeQuery })
    }));
    liveUpdateSocketSubscriptionKey = activeQuery.key;
  };

  const createLiveRefreshSnapshot = () => {
    if (!activeQuery) {
      return null;
    }

    return getCurrentQuerySnapshot({
      filters: {
        ...cloneFilters(activeQuery.filters, { clearOneShotControls: true }),
        forceRecheck: true
      },
      viewport: activeQuery.viewport
    });
  };

  const triggerLiveUpdateRefresh = async () => {
    if (!activeQuery || !liveUpdatesActive) {
      return;
    }

    if (loading || liveUpdateRefreshInFlight) {
      liveUpdateRefreshQueued = true;
      return;
    }

    liveUpdateRefreshInFlight = true;
    try {
      do {
        liveUpdateRefreshQueued = false;
        const refreshSnapshot = createLiveRefreshSnapshot();
        if (!refreshSnapshot) {
          return;
        }

        const refreshedCursor = await loadMapData({
          popupState: getAppliedPopupStateSnapshot(),
          snapshot: refreshSnapshot,
          preserveSelectedPoint: true
        });
        if (refreshedCursor === null) {
          return;
        }

        liveUpdateCursor = refreshedCursor;
        if (liveUpdatePendingCursor !== null && liveUpdatePendingCursor <= refreshedCursor) {
          liveUpdatePendingCursor = null;
        }
      } while (
        liveUpdateRefreshQueued
        || (liveUpdatePendingCursor !== null && liveUpdateCursor !== null && liveUpdatePendingCursor > liveUpdateCursor)
      );
    } finally {
      liveUpdateRefreshInFlight = false;
    }
  };

  const handleLiveUpdateMessage = ({ payload }: { payload: LiveUpdateEnvelope }) => {
    if (payload.type === 'ready' || payload.type === 'subscribed') {
      const currentCursor = toLiveUpdateCursor(payload.currentCursor);
      if (liveUpdateCursor === null && currentCursor !== null) {
        liveUpdateCursor = currentCursor;
      }

      if (payload.type === 'ready') {
        liveUpdateSocketReady = true;
        sendLiveUpdateSubscription({ force: true });
      }
      return;
    }

    if (payload.type === 'updates-available') {
      liveUpdatePendingCursor = maxLiveUpdateCursor(
        liveUpdatePendingCursor,
        toLiveUpdateCursor(payload.latestCursor)
      );
      void fetchLiveUpdateDetailsAndRefresh();
    }
  };

  const openLiveUpdateSocket = () => {
    if (!browser || !liveUpdatesActive || liveUpdateSocket) {
      return;
    }

    try {
      const socket = new WebSocket(resolveApiWebSocketPath({ path: '/api/map/live-updates/ws' }));
      liveUpdateSocket = socket;
      liveUpdateTransport = 'idle';

      socket.onopen = () => {
        if (liveUpdateSocket !== socket) {
          return;
        }

        clearLiveUpdateReconnectTimer();
        liveUpdateTransport = 'websocket';
      };

      socket.onmessage = (event) => {
        if (liveUpdateSocket !== socket) {
          return;
        }

        try {
          handleLiveUpdateMessage({
            payload: JSON.parse(String(event.data))
          });
        } catch {
          liveUpdateTransport = 'polling';
        }
      };

      socket.onerror = () => {
        if (liveUpdateSocket !== socket) {
          return;
        }

        liveUpdateTransport = 'polling';
      };

      socket.onclose = () => {
        if (liveUpdateSocket !== socket) {
          return;
        }

        liveUpdateSocket = null;
        liveUpdateSocketReady = false;
        liveUpdateSocketSubscriptionKey = '';
        if (liveUpdatesActive) {
          liveUpdateTransport = 'polling';
          scheduleLiveUpdateReconnect();
          return;
        }

        liveUpdateTransport = 'idle';
      };
    } catch {
      liveUpdateTransport = 'polling';
      scheduleLiveUpdateReconnect();
    }
  };

  const pollLiveUpdates = async () => {
    if (!activeQuery || !liveUpdatesActive) {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: '/api/map/live-updates',
        query: buildLiveUpdateStatusQuery({
          includePoints: true,
          snapshot: activeQuery,
          sinceCursor: liveUpdateCursor
        })
      });
      await handleLiveUpdateStatus({
        status: normalizeLiveUpdateStatus({ value: response.result })
      });
    } catch {
      liveUpdateTransport = liveUpdatesActive ? 'polling' : 'idle';
    }
  };

  const closeAggregateCellModal = () => {
    aggregateCellModalRequestVersion += 1;
    aggregateCellModalState = null;
    aggregateCellModalPoints = [];
    aggregateCellModalErrorMessage = null;
    aggregateCellModalLoading = false;
  };

  const openAggregateCellModal = ({ cell }: { cell: AggregateCell }) => {
    if (!activeQuery) {
      return;
    }

    aggregateCellModalState = {
      cellId: cell.id,
      queryKey: activeQuery.key,
      shape: activeQuery.filters.shape,
      cellSizeMeters: activeQuery.filters.appliedCellSizeMeters,
      cell
    };
    aggregateCellModalErrorMessage = null;
    aggregateCellModalPoints = [];
  };

  const loadAggregateCellPoints = async ({
    modalState,
    snapshot
  }: {
    modalState: AggregateCellModalState;
    snapshot: MapQuerySnapshot;
  }) => {
    const requestId = ++aggregateCellModalRequestVersion;
    aggregateCellModalLoading = true;
    aggregateCellModalErrorMessage = null;

    try {
      const response = await apiFetch<any>({
        path: '/api/map/aggregate-cell-points',
        query: {
          ...buildBaseQuery({
            includeViewport: false,
            snapshot
          }),
          shape: modalState.shape,
          cellSizeMeters: modalState.cellSizeMeters,
          cellId: modalState.cellId
        }
      });

      if (
        requestId !== aggregateCellModalRequestVersion
        || !aggregateCellModalState
        || aggregateCellModalState.cellId !== modalState.cellId
      ) {
        return;
      }

      aggregateCellModalPoints = response.result.points;
    } catch (error) {
      if (requestId !== aggregateCellModalRequestVersion) {
        return;
      }

      aggregateCellModalErrorMessage = error instanceof Error
        ? error.message
        : t('radtrack-map_failed');
    } finally {
      if (requestId !== aggregateCellModalRequestVersion) {
        return;
      }

      aggregateCellModalLoading = false;
    }
  };

  const createMapFocus = ({
    latitude,
    longitude,
    source,
    zoom
  }: {
    latitude: number;
    longitude: number;
    source: string;
    zoom: number;
  }): MapFocus => ({
    latitude,
    longitude,
    zoom,
    key: `${source}:${Date.now()}:${latitude.toFixed(5)}:${longitude.toFixed(5)}:${zoom}`
  });

  const getBrowserLocationFocus = () => new Promise<MapFocus | null>((resolve) => {
    if (!browser || !navigator.geolocation) {
      resolve(null);
      return;
    }

    let settled = false;
    const finish = (value: MapFocus | null) => {
      if (settled) {
        return;
      }

      settled = true;
      resolve(value);
    };

    navigator.geolocation.getCurrentPosition(
      (position) => {
        finish(createMapFocus({
          latitude: position.coords.latitude,
          longitude: position.coords.longitude,
          source: 'geolocation',
          zoom: userLocationZoom
        }));
      },
      () => {
        finish(null);
      },
      {
        enableHighAccuracy: false,
        maximumAge: 10 * 60 * 1000,
        timeout: 2500
      }
    );

    window.setTimeout(() => {
      finish(null);
    }, 3000);
  });

  const getFirstDataPointFocus = async () => {
    const hasSelectedDataSource = filters.datasetIds.length || filters.combinedDatasetIds.length;
    const focusFilters: MapFilters = hasSelectedDataSource
      ? filters
      : {
          ...filters,
          datasetIds: datasets.map((dataset) => dataset.id),
          trackIds: [],
          trackSelectionMode: datasets.length ? 'all' as const : 'none' as const
        };

    if (!focusFilters.datasetIds.length && !focusFilters.combinedDatasetIds.length) {
      return null;
    }

    const snapshot = getCurrentQuerySnapshot({
      filters: focusFilters
    });
    const response = await apiFetch<any>({
      path: '/api/map/points',
      query: {
        ...buildBaseQuery({
          includeViewport: false,
          snapshot
        }),
        requireCoordinates: true,
        limit: 1
      }
    });
    const point = response.result?.points?.[0];
    if (
      !point
      || !Number.isFinite(point.latitude)
      || !Number.isFinite(point.longitude)
    ) {
      return null;
    }

    return createMapFocus({
      latitude: point.latitude,
      longitude: point.longitude,
      source: 'first-data-point',
      zoom: firstDataPointZoom
    });
  };

  const initializeMapFocus = async () => {
    const applyInitialFocus = ({ focus }: { focus: MapFocus }) => {
      viewport = {
        ...viewport,
        centerLat: focus.latitude,
        centerLon: focus.longitude,
        zoom: focus.zoom
      };
      mapFocus = focus;
    };

    const urlMapFocus = loadUrlMapFocus();
    if (urlMapFocus) {
      applyInitialFocus({ focus: urlMapFocus });
      return true;
    }

    const browserLocationFocus = await getBrowserLocationFocus();
    if (browserLocationFocus) {
      applyInitialFocus({ focus: browserLocationFocus });
      return true;
    }

    try {
      const firstDataPointFocus = await getFirstDataPointFocus();
      if (firstDataPointFocus) {
        applyInitialFocus({ focus: firstDataPointFocus });
        return true;
      }
    } catch {
      return false;
    }

    return false;
  };

  const loadLookups = async () => {
    try {
      const [datasetResponse, combinedResponse] = await Promise.all([
        apiFetch<any>({ path: '/api/datasets' }),
        apiFetch<any>({ path: '/api/combined-datasets' })
      ]);
      const normalizedDatasets = datasetResponse.datasets.map((dataset: any) => ({
        ...dataset,
        trackCount: Number(dataset.trackCount ?? dataset.datalogCount ?? 0)
      }));
      const sortedDatasets = sortDatasets({ values: normalizedDatasets });
      datasets = sortedDatasets;
      combinedDatasets = combinedResponse.combinedDatasets;

      let nextDatasetIds = filters.datasetIds;
      let nextCombinedDatasetIds = filters.combinedDatasetIds;
      let nextTrackIds = filters.trackIds;
      let nextTrackSelectionMode = filters.trackSelectionMode;

      if (
        restoredUrlDatasetIdsMode !== 'explicit'
        || restoredUrlCombinedDatasetIdsMode !== 'explicit'
        || restoredUrlDatalogIdsMode !== 'explicit'
      ) {
        nextDatasetIds = restoredUrlDatasetIdsMode === 'all'
          ? sortedDatasets.map((dataset) => dataset.id)
          : (
            restoredUrlDatasetIdsMode === 'default'
              ? getDefaultDatasetIds()
              : (
                restoredUrlDatasetIdsMode === 'none'
                  ? []
                  : nextDatasetIds
              )
          );
        nextCombinedDatasetIds = restoredUrlCombinedDatasetIdsMode === 'all'
          ? combinedResponse.combinedDatasets.map((combinedDataset: CombinedDatasetOption) => combinedDataset.id)
          : (
            restoredUrlCombinedDatasetIdsMode === 'default' || restoredUrlCombinedDatasetIdsMode === 'none'
              ? []
              : nextCombinedDatasetIds
          );

        if (restoredUrlDatalogIdsMode === 'default') {
          nextTrackIds = [];
          nextTrackSelectionMode = nextDatasetIds.length || nextCombinedDatasetIds.length ? 'all' : 'none';
        } else if (restoredUrlDatalogIdsMode === 'all') {
          nextTrackIds = [];
          nextTrackSelectionMode = 'all';
        } else if (restoredUrlDatalogIdsMode === 'none') {
          nextTrackIds = [];
          nextTrackSelectionMode = 'none';
        }

        filters = {
          ...filters,
          datasetIds: nextDatasetIds,
          combinedDatasetIds: nextCombinedDatasetIds,
          trackIds: nextTrackIds,
          trackSelectionMode: nextTrackSelectionMode
        };
      }

      if (!restoredUrlFilters && !filters.datasetIds.length && !filters.combinedDatasetIds.length) {
        const defaultDatasetIds = getDefaultDatasetIds();
        filters = {
          ...filters,
          datasetIds: defaultDatasetIds,
          trackIds: [],
          trackSelectionMode: defaultDatasetIds.length ? 'all' : 'none'
        };
      }
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed_selectors');
    } finally {
      lookupsReady = true;
    }
  };

  const loadMapData = async ({
    popupState = getAppliedPopupStateSnapshot(),
    snapshot = getCurrentQuerySnapshot(),
    preserveSelectedPoint = false
  }: {
    popupState?: PopupStateSnapshot;
    snapshot?: MapQuerySnapshot;
    preserveSelectedPoint?: boolean;
  } = {}) => {
    const requestId = ++requestVersion;
    let completedSuccessfully = false;
    let responseLiveUpdateCursor: number | null = null;
    const preservedSelectedPointId = preserveSelectedPoint ? selectedPoint?.id ?? null : null;
    const query = buildBaseQuery({ snapshot });
    const nextTimeSlicePointSourceKey = serializeTimeSlicePointSourceQuery({ filters: snapshot.filters });

    loading = true;
    errorMessage = null;
    rawPointTotalCount = 0;
    if (activeTimeSlicePointSourceKey && activeTimeSlicePointSourceKey !== nextTimeSlicePointSourceKey) {
      clearTimeSlicePlaybackData();
    }
    if (!preserveSelectedPoint) {
      selectedPoint = null;
    }

    try {
      if (snapshot.filters.mode === 'raw') {
        const response = await apiFetch<any>({
          path: '/api/map/points',
          query
        });

        if (requestId !== requestVersion) {
          return null;
        }

        responseLiveUpdateCursor = toLiveUpdateCursor(response.result?.liveUpdates?.currentCursor);
        points = response.result.points;
        rawPointTotalCount = Number(response.result.totalCount ?? response.result.points.length);
        aggregates = [];
        if (preservedSelectedPointId) {
          selectedPoint = response.result.points.find((point: MapPoint) => point.id === preservedSelectedPointId) ?? null;
        }
        completedSuccessfully = true;
        return responseLiveUpdateCursor;
      }

      const response = await apiFetch<any>({
        path: '/api/map/aggregates',
        query: {
          ...query,
          shape: snapshot.filters.shape,
          cellSizeMeters: snapshot.filters.appliedCellSizeMeters,
          zoom: snapshot.viewport.zoom
        }
      });

      if (requestId !== requestVersion) {
        return null;
      }

      responseLiveUpdateCursor = toLiveUpdateCursor(response.result?.liveUpdates?.currentCursor);
      points = [];
      selectedPoint = null;
      rawPointTotalCount = 0;
      const responseCache = normalizeAggregateCache({ value: response.result.cache });
      aggregates = response.result.cells.map((cell: AggregateCell) => ({
        ...cell,
        cache: resolveAggregateCellCache({
          cellCache: cell.cache,
          responseCache
        })
      }));
      completedSuccessfully = true;
    } catch (error) {
      if (requestId !== requestVersion) {
        return null;
      }

      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed');
    } finally {
      if (requestId !== requestVersion) {
        return null;
      }

      if (completedSuccessfully) {
        if (responseLiveUpdateCursor !== null) {
          liveUpdateCursor = responseLiveUpdateCursor;
        }
        activeQuery = {
          ...snapshot,
          filters: cloneAppliedFilters(snapshot.filters, { clearOneShotControls: true })
        };
        appliedPopupState = clonePopupStateSnapshot(popupState);
        if (snapshot.filters.forceRecheck && filters.forceRecheck) {
          filters = {
            ...filters,
            forceRecheck: false
          };
        }
      }

      loading = false;

      if (queuedSidebarSnapshot?.query.key === snapshot.key) {
        queuedSidebarSnapshot = null;
      }

      if (queuedSidebarSnapshot) {
        const nextSnapshot = queuedSidebarSnapshot;
        queuedSidebarSnapshot = null;
        void loadMapData({
          popupState: nextSnapshot.popupState,
          snapshot: nextSnapshot.query
        });
      }
    }

    return completedSuccessfully ? responseLiveUpdateCursor : null;
  };

  const loadTracksForDatasets = async ({ datasetIds }: { datasetIds: string[] }) => {
    const missingDatasetIds = datasetIds.filter((datasetId) => !datasetTracksByDatasetId[datasetId]);
    if (!missingDatasetIds.length) {
      return;
    }

    loadingTrackDatasetIds = [...new Set([...loadingTrackDatasetIds, ...missingDatasetIds])];

    try {
      const responses = await Promise.all(
        missingDatasetIds.map(async (datasetId) => ({
          datasetId,
          response: await apiFetch<any>({ path: `/api/datasets/${datasetId}` })
        }))
      );

      let nextTracksByDatasetId = { ...datasetTracksByDatasetId };
      for (const { datasetId, response } of responses) {
        nextTracksByDatasetId = {
          ...nextTracksByDatasetId,
          [datasetId]: sortTracks({
            values: response.dataset.datalogs.map((track: any) => ({
              id: track.id,
              datasetId,
              datasetName: response.dataset.name,
              trackName: track.datalogName ?? track.trackName,
              rowCount: Number(track.validRowCount ?? track.rowCount ?? 0),
              supportedFields: normalizeSupportedFields(track.supportedFields ?? []),
              metricFields: track.metricFields ?? []
            }))
          })
        };
      }
      datasetTracksByDatasetId = nextTracksByDatasetId;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed_selectors');
    } finally {
      loadingTrackDatasetIds = loadingTrackDatasetIds.filter((datasetId) => !missingDatasetIds.includes(datasetId));
    }
  };

  const updateMap = () => {
    if (!canUpdateMap) {
      return;
    }

    if (!autoUpdateMap) {
      replaceUrlMapState({
        nextFilters: filters,
        nextViewport: viewport
      });
    }

    const nextPopupState = getPendingPopupStateSnapshot();
    const nextSnapshot = getCurrentQuerySnapshot();
    if (nextSnapshot.key === activeQuery?.key && nextSnapshot.filters.timeFilterMode === 'relative') {
      requestMapData({
        popupState: nextPopupState,
        snapshot: nextSnapshot
      });
      return;
    }

    applySidebarFilters();
  };

  const handleTimeFilterModeChange = () => {
    if (filters.timeFilterMode === 'absolute') {
      const nextRange = normalizeTimeFilterInputsForPrecision({
        sourceFilters: filters,
        precision: filters.timeFilterPrecision
      });
      filters = {
        ...filters,
        timeFilterStart: nextRange.start,
        timeFilterEnd: nextRange.end
      };
    }

    if (filters.timeFilterMode === 'relative' && getTimeFilterValidationKey({ sourceFilters: filters })) {
      filters = {
        ...filters,
        timeFilterRelativeAmount: 24,
        timeFilterRelativeUnit: 'hours'
      };
    }

    handleFilterChange();
  };

  const handleTimeFilterPrecisionChange = () => {
    const nextRange = normalizeTimeFilterInputsForPrecision({
      sourceFilters: filters,
      precision: filters.timeFilterPrecision
    });
    filters = {
      ...filters,
      timeFilterStart: nextRange.start,
      timeFilterEnd: nextRange.end
    };
    handleFilterChange();
  };

  const formatIsoForTimeFilterInput = ({
    isoValue,
    precision
  }: {
    isoValue: string | null | undefined;
    precision: TimeFilterPrecision;
  }) => {
    if (!isoValue) {
      return null;
    }

    const date = new Date(isoValue);
    if (Number.isNaN(date.getTime())) {
      return null;
    }

    return precision === 'date'
      ? formatLocalDateInputValue({ value: date })
      : formatLocalDateTimeInputValue({ value: date });
  };

  const loadSelectedTimeBounds = async () => {
    if (timeBoundsLoading) {
      return null;
    }

    timeBoundsLoading = true;
    try {
      const response = await apiFetch<any>({
        path: '/api/map/time-bounds',
        query: buildTimeBoundsQuery({
          sourceFilters: filters
        })
      });
      const result = response.result ?? {};
      return {
        start: typeof result.start === 'string' ? result.start : null,
        end: typeof result.end === 'string' ? result.end : null
      };
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed');
      return null;
    } finally {
      timeBoundsLoading = false;
    }
  };

  const setAbsoluteTimeFilterStartToEarliest = async () => {
    const bounds = await loadSelectedTimeBounds();
    const start = formatIsoForTimeFilterInput({
      isoValue: bounds?.start,
      precision: filters.timeFilterPrecision
    });
    if (!start) {
      return;
    }

    filters = {
      ...filters,
      timeFilterStart: start
    };
    handleFilterChange();
  };

  const setAbsoluteTimeFilterEndToLatest = async () => {
    const bounds = await loadSelectedTimeBounds();
    const end = formatIsoForTimeFilterInput({
      isoValue: bounds?.end,
      precision: filters.timeFilterPrecision
    });
    if (!end) {
      return;
    }

    filters = {
      ...filters,
      timeFilterEnd: end
    };
    handleFilterChange();
  };

  const setAbsoluteTimeFilterEndToNow = () => {
    const now = new Date();
    filters = {
      ...filters,
      timeFilterEnd: filters.timeFilterPrecision === 'date'
        ? formatLocalDateInputValue({ value: now })
        : formatLocalDateTimeInputValue({ value: now })
    };
    handleFilterChange();
  };

  const resetTimeSlicePlayback = () => {
    timeSliceIndex = 0;
    timeSlicePlaying = false;
    clearTimeSlicePlaybackTimer();
  };

  const clearTimeSliceSource = () => {
    timeSliceSourceRequestVersion += 1;
    timeSliceSourceLoading = false;
    timeSliceSourcePoints = [];
    timeSlicePreparedWindows = [];
    loadedTimeSliceSourceKey = '';
    timeSliceSourceErrorKey = '';
  };

  const clearTimeSlicePointCount = () => {
    timeSlicePointCountRequestVersion += 1;
    timeSlicePointCount = null;
    timeSlicePointCountLoading = false;
    loadedTimeSlicePointCountKey = '';
    timeSlicePointCountErrorKey = '';
  };

  const clearTimeSlicePlaybackData = () => {
    clearTimeSliceSource();
    clearTimeSlicePointCount();
  };

  const handleTimeSliceEnabledChange = (event: Event) => {
    timeSliceEnabled = (event.currentTarget as HTMLInputElement).checked;
    resetTimeSlicePlayback();
    if (!timeSliceEnabled) {
      timeSliceConfigOpen = false;
      timeSliceLargeLoadConfirmedKey = '';
      clearTimeSlicePlaybackData();
    }
  };

  const confirmTimeSliceLargeLoad = () => {
    if (!timeSliceLargeLoadConfirmationKey) {
      return;
    }

    timeSliceLargeLoadConfirmedKey = timeSliceLargeLoadConfirmationKey;
    timeSliceSourceErrorKey = '';

    if (activeQuery && activeTimeSliceSourceKey && activeTimeSliceSelectedPointCount !== null) {
      void loadTimeSliceSourceForSnapshot({
        snapshot: activeQuery,
        sourceKey: activeTimeSliceSourceKey
      });
    }
  };

  const handleTimeSliceCurrentOnlyChange = (event: Event) => {
    timeSliceCurrentOnly = (event.currentTarget as HTMLInputElement).checked;
  };

  const loadTimeSlicePointCountForSnapshot = async ({
    snapshot,
    sourceKey
  }: {
    snapshot: MapQuerySnapshot;
    sourceKey: string;
  }) => {
    const countRequestId = ++timeSlicePointCountRequestVersion;
    timeSlicePointCountLoading = true;
    timeSlicePointCount = null;
    loadedTimeSlicePointCountKey = '';
    timeSlicePointCountErrorKey = '';

    try {
      const response = await apiFetch<any>({
        path: '/api/map/point-count',
        query: {
          ...buildBaseQuery({
            includeViewport: false,
            snapshot
          }),
          requireCoordinates: true,
          historicalCacheEligible: false
        }
      });

      if (countRequestId !== timeSlicePointCountRequestVersion || activeTimeSlicePointSourceKey !== sourceKey) {
        return;
      }

      timeSlicePointCount = Math.max(0, Math.trunc(toFiniteNumber(response.result?.totalCount) ?? 0));
      loadedTimeSlicePointCountKey = sourceKey;
    } catch (error) {
      if (countRequestId !== timeSlicePointCountRequestVersion || activeTimeSlicePointSourceKey !== sourceKey) {
        return;
      }

      timeSlicePointCountErrorKey = sourceKey;
      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed');
    } finally {
      if (countRequestId === timeSlicePointCountRequestVersion) {
        timeSlicePointCountLoading = false;
      }
    }
  };

  const loadTimeSliceSourceForSnapshot = async ({
    snapshot,
    sourceKey
  }: {
    snapshot: MapQuerySnapshot;
    sourceKey: string;
  }) => {
    const sourceRequestId = ++timeSliceSourceRequestVersion;
    timeSliceSourceLoading = true;
    timeSliceSourcePoints = [];
    loadedTimeSliceSourceKey = '';
    timeSliceSourceErrorKey = '';

    try {
      const response = await apiFetch<any>({
        path: '/api/map/time-slice-source',
        query: {
          ...buildBaseQuery({
            includeViewport: false,
            snapshot
          }),
          sliceAmount: normalizeTimeSliceSettings({ settings: timeSliceSettings }).amount,
          sliceUnit: normalizeTimeSliceSettings({ settings: timeSliceSettings }).unit,
          requireCoordinates: true
        }
      });

      if (sourceRequestId !== timeSliceSourceRequestVersion || activeTimeSliceSourceKey !== sourceKey) {
        return;
      }

      const responsePoints = Array.isArray(response.result?.points)
        ? response.result.points
        : [];
      timeSliceSourcePoints = responsePoints
        .map((point: MapPoint) => normalizePlaybackPoint({ point }))
        .filter((point: MapPoint | null): point is MapPoint => Boolean(point));
      timeSlicePreparedWindows = Array.isArray(response.result?.windows)
        ? response.result.windows
            .map((window: unknown) => normalizePreparedTimeSliceWindow({ value: window }))
            .filter((window: PreparedTimeSliceWindow | null): window is PreparedTimeSliceWindow => Boolean(window))
        : [];
      loadedTimeSliceSourceKey = sourceKey;
    } catch (error) {
      if (sourceRequestId !== timeSliceSourceRequestVersion || activeTimeSliceSourceKey !== sourceKey) {
        return;
      }

      timeSliceSourceErrorKey = sourceKey;
      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed');
    } finally {
      if (sourceRequestId === timeSliceSourceRequestVersion) {
        timeSliceSourceLoading = false;
      }
    }
  };

  const openTimeSliceConfig = () => {
    timeSliceDraftSettings = { ...timeSliceSettings };
    timeSliceConfigOpen = true;
  };

  const closeTimeSliceConfig = () => {
    timeSliceConfigOpen = false;
  };

  const applyTimeSliceConfig = () => {
    const nextTimeSliceSettings = normalizeTimeSliceSettings({
      settings: timeSliceDraftSettings
    });
    timeSliceSettings = nextTimeSliceSettings;
    timeSliceIndex = 0;
    timeSlicePlaying = false;
    timeSliceConfigOpen = false;

    if (filterUrlSyncReady) {
      replaceUrlMapState({
        nextFilters: autoUpdateMap ? filters : getAppliedFilters(),
        nextViewport: viewport
      });
    }
  };

  const setTimeSlicePosition = ({ index }: { index: number }) => {
    if (!activeTimeSliceWindows.length) {
      timeSliceIndex = 0;
      timeSlicePlaying = false;
      return;
    }

    timeSliceIndex = Math.max(0, Math.min(activeTimeSliceWindows.length - 1, Math.trunc(index)));
    timeSlicePlaying = false;
  };

  const moveTimeSlice = ({ delta }: { delta: number }) => {
    setTimeSlicePosition({
      index: timeSliceIndex + delta
    });
  };

  const toggleTimeSlicePlayback = () => {
    if (!activeTimeSliceWindow || activeTimeSliceWindows.length < 2) {
      timeSlicePlaying = false;
      return;
    }

    if (timeSlicePlaying) {
      timeSlicePlaying = false;
      return;
    }

    if (timeSliceIndex >= activeTimeSliceWindows.length - 1) {
      timeSliceIndex = 0;
    }
    timeSlicePlaying = true;
  };

  const handleFilterChange = () => {
    clearLiveUpdateDisplay({ clearLog: true });
    timeSliceLargeLoadConfirmedKey = '';
    resetTimeSlicePlayback();
    clearTimeSlicePlaybackData();
    if (!autoUpdateMap) {
      clearScheduledSidebarUpdate();
      return;
    }

    scheduleSidebarAutoUpdate();
  };

  const handleViewportChange = (event: CustomEvent<MapViewport>) => {
    viewport = event.detail;

    if (filterUrlSyncReady) {
      replaceUrlMapState({
        nextFilters: autoUpdateMap ? filters : getAppliedFilters(),
        nextViewport: event.detail
      });
    }

    if (!lookupsReady) {
      return;
    }

    if (
      timeSliceEnabled
      && activeQuery?.filters.timeFilterMode === 'absolute'
      && activeTimeSliceWindows.length
    ) {
      return;
    }

    requestMapData({
      popupState: getAppliedPopupStateSnapshot(),
      snapshot: getCurrentQuerySnapshot({
        filters: getAppliedFilters(),
        viewport: event.detail
      })
    });
  };

  const handleAutoUpdateToggle = () => {
    persistBoolean({
      prefix: autoUpdateStorageKeyPrefix,
      value: autoUpdateMap
    });

    if (!autoUpdateMap) {
      clearScheduledSidebarUpdate();
      return;
    }

    scheduleSidebarAutoUpdate();
  };

  const handleLiveUpdatesToggle = () => {
    if (!liveUpdatesAllowed && liveUpdatesEnabled) {
      liveUpdatesEnabled = false;
    }

    persistBoolean({
      prefix: liveUpdatesStorageKeyPrefix,
      value: liveUpdatesEnabled
    });

    if (!liveUpdatesEnabled) {
      teardownLiveUpdates();
      return;
    }

    liveUpdateTransport = liveUpdateSocket?.readyState === WebSocket.OPEN ? 'websocket' : 'idle';
  };

  const handleAutoCellSizeToggle = () => {
    if (!filters.autoCellSize) {
      filters = {
        ...filters,
        cellSizeMeters: effectiveCellSizeMeters
      };
    }

    persistBoolean({
      prefix: autoCellSizeStorageKeyPrefix,
      value: filters.autoCellSize
    });

    handleFilterChange();
  };

  const handleAutoColorScaleToggle = () => {
    if (!colorScaleSettings.auto && activeColorScale) {
      colorScaleSettings = {
        ...colorScaleSettings,
        low: activeColorScale.low,
        mid: activeColorScale.mid,
        high: activeColorScale.high
      };
    }
  };

  const handleBasemapChange = () => {
    persistString({
      prefix: basemapStorageKeyPrefix,
      value: basemapKey
    });
  };

  const handleSidebarSizeChange = ({ nextSize }: { nextSize: SidebarSize }) => {
    sidebarSize = nextSize;
    persistString({
      prefix: sidebarSizeStorageKeyPrefix,
      value: nextSize
    });
  };

  const handleFilterReset = () => {
    clearScheduledSidebarUpdate();
    restoredUrlFilters = false;
    filters = createResetFilters();
    handleFilterChange();
  };

  const hideSelectedPoint = async () => {
    if (!$sessionStore.csrf || !selectedPoint) {
      return;
    }

    await apiFetch({
      path: '/api/readings/hide',
      method: 'POST',
      body: {
        readingIds: [selectedPoint.id],
        datasetIds: [selectedPoint.datasetId],
        reason: t('radtrack-map_hidden_reason')
      },
      csrf: $sessionStore.csrf
    });

    await loadMapData({
      popupState: getAppliedPopupStateSnapshot(),
      snapshot: getCurrentQuerySnapshot({
        filters: getAppliedFilters()
      })
    });
  };

  onMount(async () => {
    if (mapStageElement) {
      updatePopupMaxHeight();
      mapStageResizeObserver = new ResizeObserver(() => {
        updatePopupMaxHeight();
      });
      mapStageResizeObserver.observe(mapStageElement);
    }

    liveUpdateMarkerTimer = setInterval(() => {
      if (!liveUpdateMarkers.length || !liveUpdateMarkersSetAt) {
        return;
      }

      if ((Date.now() - liveUpdateMarkersSetAt) >= liveUpdateMarkerMinimumVisibleMs) {
        clearLiveUpdateDisplay();
      }
    }, liveUpdateMarkerSweepMs);

    loadMapPreferences();
    await loadLookups();
    const focusedMap = await initializeMapFocus();
    if (restoredUrlFilters && restoredUrlMapLocation) {
      lastUrlStateKey = serializeUrlMapState({
        nextFilters: filters,
        nextViewport: viewport
      });
    }
    filterUrlSyncReady = true;
    if (!focusedMap) {
      await loadMapData();
    }
  });

  onDestroy(() => {
    mapStageResizeObserver?.disconnect();
    clearScheduledSidebarUpdate();
    clearLiveUpdateMarkerTimer();
    clearTimeSlicePlaybackTimer();
    teardownLiveUpdates();
  });

  $effect(() => {
    const modalState = aggregateCellModalState;
    const snapshot = activeQuery;

    if (!modalState) {
      return;
    }

    if (
      !snapshot
      || activeMode !== 'aggregate'
      || snapshot.key !== modalState.queryKey
      || activeShape !== modalState.shape
      || snapshot.filters.appliedCellSizeMeters !== modalState.cellSizeMeters
      || !aggregateCellModalCurrentCell
    ) {
      closeAggregateCellModal();
      return;
    }

    if (activeTimeSliceWindow) {
      aggregateCellModalRequestVersion += 1;
      aggregateCellModalPoints = [];
      aggregateCellModalErrorMessage = null;
      aggregateCellModalLoading = false;
      return;
    }

    void loadAggregateCellPoints({
      modalState,
      snapshot
    });
  });

  $effect(() => {
    const nextFilters = cloneFilters(filters, { clearOneShotControls: true });
    const nextViewport = cloneViewport(viewport);
    if (!filterUrlSyncReady || !autoUpdateMap) {
      return;
    }

    untrack(() => {
      replaceUrlMapState({
        nextFilters,
        nextViewport
      });
    });
  });

  $effect(() => {
    const totalSlices = activeTimeSliceWindows.length;
    if (!totalSlices) {
      if (timeSliceIndex !== 0) {
        timeSliceIndex = 0;
      }
      if (timeSlicePlaying) {
        timeSlicePlaying = false;
      }
      return;
    }

    if (timeSliceIndex > totalSlices - 1) {
      timeSliceIndex = totalSlices - 1;
    }
  });

  $effect(() => {
    const snapshot = activeQuery;
    const sourceKey = activeTimeSlicePointSourceKey;
    if (loading) {
      return;
    }

    const shouldUsePlaybackSource = Boolean(
      timeSliceEnabled
      && snapshot
      && snapshot.filters.timeFilterMode === 'absolute'
      && activeTimeSliceWindows.length
    );

    if (!shouldUsePlaybackSource) {
      if (
        timeSliceSourceLoading
        || timeSliceSourcePoints.length
        || loadedTimeSliceSourceKey
        || timeSliceSourceErrorKey
        || timeSlicePointCountLoading
        || timeSlicePointCount !== null
        || loadedTimeSlicePointCountKey
        || timeSlicePointCountErrorKey
      ) {
        clearTimeSlicePlaybackData();
      }
      return;
    }

    if (
      !snapshot
      || loadedTimeSlicePointCountKey === sourceKey
      || timeSlicePointCountLoading
      || timeSlicePointCountErrorKey === sourceKey
    ) {
      return;
    }

    void loadTimeSlicePointCountForSnapshot({
      snapshot,
      sourceKey
    });
  });

  $effect(() => {
    const snapshot = activeQuery;
    const sourceKey = activeTimeSliceSourceKey;
    if (loading) {
      return;
    }

    const shouldLoadSource = Boolean(
      timeSliceEnabled
      && snapshot
      && snapshot.filters.timeFilterMode === 'absolute'
      && activeTimeSliceWindows.length
      && activeTimeSliceSelectedPointCount !== null
      && !timeSliceNeedsLargeLoadConfirmation
    );

    if (!shouldLoadSource) {
      return;
    }

    if (
      !snapshot
      || loadedTimeSliceSourceKey === sourceKey
      || timeSliceSourceLoading
      || timeSliceSourceErrorKey === sourceKey
    ) {
      return;
    }

    void loadTimeSliceSourceForSnapshot({
      snapshot,
      sourceKey
    });
  });

  $effect(() => {
    if (!selectedPoint || activeMode !== 'raw' || !activeTimeSliceWindow) {
      return;
    }

    if (!displayedPoints.some((point) => point.id === selectedPoint?.id)) {
      selectedPoint = null;
    }
  });

  $effect(() => {
    clearTimeSlicePlaybackTimer();

    const totalSlices = activeTimeSliceWindows.length;
    const speed = timeSlicePlaybackSpeed;
    if (!timeSlicePlaying || totalSlices < 2) {
      return;
    }

    timeSlicePlaybackStartedAt = Date.now();
    timeSlicePlaybackStartIndex = Math.max(0, Math.min(timeSliceIndex, totalSlices - 1));
    timeSlicePlaybackTimer = setInterval(() => {
      const frameMs = timeSlicePlaybackBaseMs / Math.max(1, speed);
      const elapsedFrames = Math.floor((Date.now() - timeSlicePlaybackStartedAt) / frameMs);
      if (elapsedFrames <= 0) {
        return;
      }

      const nextIndex = timeSlicePlaybackStartIndex + elapsedFrames;
      if (nextIndex >= totalSlices - 1) {
        timeSliceIndex = totalSlices - 1;
        timeSlicePlaying = false;
        clearTimeSlicePlaybackTimer();
        return;
      }

      timeSliceIndex = nextIndex;
    }, 100);

    return () => {
      clearTimeSlicePlaybackTimer();
    };
  });

  $effect(() => {
    const availableMetricKeys = pendingAvailableMetricFields.map((field) => field.propKey);
    const popupFieldKeys = popupSelectableFields.map((field) => field.propKey);
    if (availableMetricKeys.length && !availableMetricKeys.includes(filters.metric)) {
      filters = {
        ...filters,
        metric: $sessionStore.ui?.defaultMetric && availableMetricKeys.includes($sessionStore.ui.defaultMetric)
          ? $sessionStore.ui.defaultMetric
          : availableMetricKeys[0]
      };
    }

    const nextPopupMetrics: Record<string, boolean> = { ...popupFields.metrics };
    for (const propKey of popupFieldKeys) {
      if (popupFieldTouched[propKey]) {
        nextPopupMetrics[propKey] = popupFields.metrics[propKey] ?? pendingPopupFieldDefaults[propKey] ?? true;
      } else {
        nextPopupMetrics[propKey] = pendingPopupFieldDefaults[propKey] ?? true;
      }
    }

    if (booleanRecordChanged({
      current: popupFields.metrics,
      next: nextPopupMetrics
    })) {
      popupFields = {
        ...popupFields,
        metrics: nextPopupMetrics
      };
    }
  });

  $effect(() => {
    if (filters.timeFilterMode !== 'absolute' || !liveUpdatesEnabled) {
      return;
    }

    liveUpdatesEnabled = false;
    persistBoolean({
      prefix: liveUpdatesStorageKeyPrefix,
      value: false
    });
  });

  $effect(() => {
    const active = liveUpdatesActive;
    const snapshotKey = activeQuery?.key ?? '';
    const intervalMs = liveUpdatePollingIntervalMs;

    clearLiveUpdateTimer();

    if (!browser || !active || !snapshotKey) {
      teardownLiveUpdates();
      return;
    }

    if (!liveUpdateSocket && !liveUpdateReconnectTimer) {
      openLiveUpdateSocket();
    } else if (liveUpdateSocket?.readyState === WebSocket.OPEN && liveUpdateSocketReady) {
      sendLiveUpdateSubscription();
    }

    if (liveUpdateSocket?.readyState === WebSocket.OPEN) {
      liveUpdateTransport = 'websocket';
    } else if (liveUpdateTransport !== 'polling') {
      liveUpdateTransport = 'idle';
    }
    liveUpdateTimer = setInterval(() => {
      if (!activeQuery || !liveUpdatesActive) {
        return;
      }

      if (activeQuery.filters.timeFilterMode === 'relative') {
        if (!liveUpdateSocket || liveUpdateSocket.readyState !== WebSocket.OPEN) {
          void pollLiveUpdates();
        }
        void triggerLiveUpdateRefresh();
        return;
      }

      if (!liveUpdateSocket || liveUpdateSocket.readyState !== WebSocket.OPEN) {
        void pollLiveUpdates();
      }
    }, intervalMs);

    return () => {
      clearLiveUpdateTimer();
    };
  });

  $effect(() => {
    if (loading || !liveUpdatesActive || !liveUpdateRefreshQueued || liveUpdateRefreshInFlight) {
      return;
    }

    void triggerLiveUpdateRefresh();
  });

  $effect(() => {
    const selectedDatasetIds = [...filters.datasetIds];
    if (!selectedDatasetIds.length) {
      return;
    }

    untrack(() => {
      void loadTracksForDatasets({ datasetIds: selectedDatasetIds });
    });
  });
</script>

<div class="map-page">
  <div class="page-header">
    <div>
      <h1>{t('radtrack-map_title')}</h1>
    </div>
    <div class="chip-row">
      {#if hasPendingSidebarChanges()}
        <span class="chip mid">{t('radtrack-map_pending_changes-label')}</span>
      {/if}
      {#if loading}
        <span class="chip warning">{t('radtrack-common_loading-label')}</span>
      {/if}
    </div>
  </div>

  <div class="map-layout" style={`--map-sidebar-width: ${activeSidebarWidth};`}>
    <aside class="map-sidebar" class:compact={sidebarSize === 'small'}>
      <section class="panel">
        <div class="map-panel-header">
          <h2>{t('radtrack-common_filters-label')}</h2>
          <div class="map-panel-actions">
            <div class="sidebar-size-toggle" aria-label={t('radtrack-map_sidebar_size-label')}>
              {#each sidebarSizeOptions as option}
                <button
                  aria-pressed={sidebarSize === option.key}
                  class:active={sidebarSize === option.key}
                  onclick={() => {
                    handleSidebarSizeChange({ nextSize: option.key });
                  }}
                  aria-label={t(option.titleKey)}
                  title={t(option.titleKey)}
                  type="button"
                >
                  <span aria-hidden="true" class={`sidebar-size-icon sidebar-size-icon-${option.key}`}></span>
                </button>
              {/each}
            </div>
            <button onclick={handleFilterReset} type="button">
              {t('radtrack-map_reset_filters-button')}
            </button>
            {#if !autoUpdateMap}
              <button class="primary" disabled={!canUpdateMap} onclick={updateMap}>
                {t('radtrack-map_update-button')}
              </button>
            {/if}
          </div>
        </div>

        <label class="checkbox-field map-auto-update">
          <input bind:checked={autoUpdateMap} onchange={handleAutoUpdateToggle} type="checkbox" />
          <span>{t('radtrack-map_auto_update-label')}</span>
        </label>

        <label class="checkbox-field map-live-updates">
          <input
            bind:checked={liveUpdatesEnabled}
            disabled={!liveUpdatesAllowed}
            onchange={handleLiveUpdatesToggle}
            type="checkbox"
          />
          <span>{t('radtrack-map_live_updates-label')}</span>
          <span class="chip subtle">{liveUpdateStatusLabel}</span>
        </label>
        {#if !liveUpdatesAllowed}
          <p class="muted map-live-updates-note">{t('radtrack-map_live_updates_absolute_note-label')}</p>
        {/if}

        {#if liveUpdatesEnabled && liveUpdatesAllowed}
          <details class="selector-accordion live-update-log-accordion">
            <summary>
              <span>{t('radtrack-map_live_update_log-title')}</span>
              <span class="settings-accordion-meta">
                <span class={liveUpdateLog.length ? 'chip start' : 'chip subtle'}>
                  {formatCount(liveUpdateLog.length)}
                </span>
                <span aria-hidden="true" class="settings-accordion-icon"></span>
              </span>
            </summary>

            {#if liveUpdateLog.length}
              <div class="live-update-log-list">
                {#each liveUpdateLog as entry}
                  <div class="live-update-log-item">
                    <div class="live-update-log-title">
                      <span>{formatTime(entry.point.occurredAt ?? entry.point.receivedAt)}</span>
                      <span class="chip subtle">{t('radtrack-map_live_update_cursor-label', { cursor: entry.cursor })}</span>
                    </div>
                    <div class="live-update-log-meta">
                      <span>{t('radtrack-common_latitude-label')} {formatNumber(entry.point.latitude)}</span>
                      <span>{t('radtrack-common_longitude-label')} {formatNumber(entry.point.longitude)}</span>
                    </div>
                    <div class="live-update-log-meta">
                      <span>{t('radtrack-map_id_short-label', { id: shortId(entry.point.id) })}</span>
                      <span>{formatTime(entry.publishedAt)}</span>
                    </div>
                  </div>
                {/each}
              </div>
            {:else}
              <div class="selection-empty muted">{t('radtrack-map_live_update_log_empty')}</div>
            {/if}
          </details>
        {/if}

        <div class="form-grid">
          <div class="selector-accordion-stack">
            <details class="selector-accordion map-data-accordion">
              <summary>
                <span>{t('radtrack-map_data-title')}</span>
                <span class="settings-accordion-meta">
                  <span aria-hidden="true" class="settings-accordion-icon"></span>
                </span>
              </summary>

              <div class="map-data-accordion-body">
                <details class="selector-accordion">
                  <summary>
                    <span>{t('radtrack-datasets_title')}</span>
                    <span class="settings-accordion-meta">
                      <span class={selectionChipClass({ count: filters.datasetIds.length })}>
                        {filters.datasetIds.length
                          ? t('radtrack-map_selected_count-label', { count: filters.datasetIds.length })
                          : t('radtrack-common_none')}
                      </span>
                      <span aria-hidden="true" class="settings-accordion-icon"></span>
                    </span>
                  </summary>

                  <div class="selection-stack">
                    <div class="actions">
                      <button onclick={handleDatasetSelectAll} disabled={!datasets.length || filters.datasetIds.length === datasets.length}>
                        {t('radtrack-map_tracks_all-button')}
                      </button>
                      <button onclick={handleDatasetClearAll} disabled={!filters.datasetIds.length}>
                        {t('radtrack-map_tracks_none-button')}
                      </button>
                    </div>

                    <div class="selection-list selection-list-tall">
                      {#each datasets as dataset}
                        <label class="selection-item">
                          <input
                            checked={filters.datasetIds.includes(dataset.id)}
                            onchange={() => {
                              handleDatasetToggle(dataset.id);
                            }}
                            type="checkbox"
                          />
                          <span class="selection-copy">
                            <span class="selection-title" title={dataset.name}>{dataset.name}</span>
                            <span class="selection-meta-row">{formatDatasetOptionSummary(dataset)}</span>
                            <span class="selection-meta-id">{t('radtrack-map_id_short-label', { id: shortId(dataset.id) })}</span>
                          </span>
                        </label>
                      {/each}
                    </div>
                  </div>
                </details>

                <details class="selector-accordion">
                  <summary>
                    <span>{t('radtrack-common_combined_datasets-label')}</span>
                    <span class="settings-accordion-meta">
                      <span class={selectionChipClass({ count: filters.combinedDatasetIds.length })}>
                        {filters.combinedDatasetIds.length
                          ? t('radtrack-map_selected_count-label', { count: filters.combinedDatasetIds.length })
                          : t('radtrack-common_none')}
                      </span>
                      <span aria-hidden="true" class="settings-accordion-icon"></span>
                    </span>
                  </summary>

                  <div class="selection-stack">
                    <div class="selection-help muted">{t('radtrack-map_combined_datasets-help')}</div>

                    {#if combinedDatasets.length}
                      <div class="actions">
                        <button
                          onclick={handleCombinedDatasetSelectAll}
                          disabled={filters.combinedDatasetIds.length === combinedDatasets.length}
                        >
                          {t('radtrack-map_tracks_all-button')}
                        </button>
                        <button onclick={handleCombinedDatasetClearAll} disabled={!filters.combinedDatasetIds.length}>
                          {t('radtrack-map_tracks_none-button')}
                        </button>
                      </div>

                      <div class="selection-list selection-list-tall">
                        {#each combinedDatasets as combinedDataset}
                          <label class="selection-item">
                            <input
                              checked={filters.combinedDatasetIds.includes(combinedDataset.id)}
                              onchange={() => {
                                handleCombinedDatasetToggle(combinedDataset.id);
                              }}
                              type="checkbox"
                            />
                            <span class="selection-copy">
                              <span class="selection-title" title={combinedDataset.name}>{combinedDataset.name}</span>
                              <span class="selection-meta-row">{formatCombinedDatasetOptionSummary(combinedDataset)}</span>
                              <span class="selection-meta-id">{t('radtrack-map_id_short-label', { id: shortId(combinedDataset.id) })}</span>
                            </span>
                          </label>
                        {/each}
                      </div>
                    {:else}
                      <div class="selection-empty muted">{t('radtrack-map_combined_datasets-empty')}</div>
                    {/if}
                  </div>
                </details>

                <details class="selector-accordion">
                  <summary>
                    <span>{t('radtrack-common_tracks-label')}</span>
                    <span class="settings-accordion-meta">
                      <span class={filters.trackSelectionMode === 'all' ? 'chip start' : selectionChipClass({ count: filters.trackIds.length })}>
                        {trackSelectionSummary}
                      </span>
                      <span aria-hidden="true" class="settings-accordion-icon"></span>
                    </span>
                  </summary>

                  <div class="selection-stack">
                    {#if filters.datasetIds.length}
                      <div class="actions">
                        <button onclick={handleTrackSelectAll} disabled={filters.trackSelectionMode === 'all' || !selectedTrackGroups.length}>
                          {t('radtrack-map_tracks_all-button')}
                        </button>
                        <button onclick={handleTrackClearAll} disabled={filters.trackSelectionMode === 'none'}>
                          {t('radtrack-map_tracks_none-button')}
                        </button>
                      </div>

                      {#if selectedTrackGroups.length}
                        <div class="selection-list selection-list-tall">
                          {#each selectedTrackGroups as group}
                            <div class="track-group">
                              <div class="track-group-title" title={group.datasetName}>{group.datasetName}</div>

                              {#each group.tracks as track}
                                <label class="selection-item selection-item-compact">
                                  <input
                                    checked={isTrackChecked({ trackId: track.id })}
                                    onchange={() => {
                                      handleTrackToggle(track.id);
                                    }}
                                    type="checkbox"
                                  />
                                  <span class="selection-copy">
                                    <span class="selection-title" title={track.trackName ?? `${t('radtrack-layout_track_page-label')} ${shortId(track.id)}`}>
                                      {track.trackName ?? `${t('radtrack-layout_track_page-label')} ${shortId(track.id)}`}
                                    </span>
                                    <span class="selection-meta-row">{formatTrackOptionSummary(track)}</span>
                                    <span class="selection-meta-id">{t('radtrack-map_id_short-label', { id: shortId(track.id) })}</span>
                                  </span>
                                </label>
                              {/each}
                            </div>
                          {/each}
                        </div>
                      {:else if loadingTrackDatasetIds.length}
                        <div class="selection-empty muted">{t('radtrack-common_loading-label')}</div>
                      {:else}
                        <div class="selection-empty muted">{t('radtrack-map_tracks_empty')}</div>
                      {/if}
                    {:else}
                      <div class="selection-empty muted">{t('radtrack-map_tracks_require_dataset')}</div>
                    {/if}
                  </div>
                </details>

                <hr class="map-data-divider" />
              </div>
            </details>

          <details class="selector-accordion">
            <summary>
              <span>{t('radtrack-map_time_filter-title')}</span>
              <span class="settings-accordion-meta">
                {#if filters.forceRecheck}
                  <span class="chip warning">{t('radtrack-map_force_recheck-chip')}</span>
                {/if}
                <span class={filters.timeFilterMode === 'none' ? 'chip subtle' : 'chip start'}>
                  {timeFilterSummary}
                </span>
                <span aria-hidden="true" class="settings-accordion-icon"></span>
              </span>
            </summary>

            <div class="selection-stack time-filter-stack">
              <label>
                <div class="muted">{t('radtrack-map_time_filter_mode-label')}</div>
                <select bind:value={filters.timeFilterMode} onchange={handleTimeFilterModeChange}>
                  <option value="none">{t('radtrack-map_time_filter_mode_none-label')}</option>
                  <option value="absolute">{t('radtrack-map_time_filter_mode_absolute-label')}</option>
                  <option value="relative">{t('radtrack-map_time_filter_mode_relative-label')}</option>
                </select>
              </label>

              {#if filters.timeFilterMode === 'absolute'}
                <label>
                  <div class="muted">{t('radtrack-map_time_filter_precision-label')}</div>
                  <select bind:value={filters.timeFilterPrecision} onchange={handleTimeFilterPrecisionChange}>
                    <option value="datetime">{t('radtrack-map_time_filter_precision_datetime-label')}</option>
                    <option value="date">{t('radtrack-map_time_filter_precision_date-label')}</option>
                  </select>
                </label>

                <div class="grid cols-2 time-filter-range-grid">
                  <label>
                    <div class="muted">{t('radtrack-map_time_filter_start-label')}</div>
                    {#if filters.timeFilterPrecision === 'date'}
                      <input bind:value={filters.timeFilterStart} oninput={handleFilterChange} type="date" />
                    {:else}
                      <input bind:value={filters.timeFilterStart} oninput={handleFilterChange} type="datetime-local" />
                    {/if}
                    <div class="time-filter-actions-row">
                      <button
                        aria-label={t('radtrack-map_time_filter_earliest-title')}
                        disabled={timeBoundsLoading}
                        onclick={setAbsoluteTimeFilterStartToEarliest}
                        title={t('radtrack-map_time_filter_earliest-title')}
                        type="button"
                      >
                        {t('radtrack-map_time_filter_earliest-button')}
                      </button>
                    </div>
                  </label>

                  <label>
                    <div class="muted">{t('radtrack-map_time_filter_end-label')}</div>
                    {#if filters.timeFilterPrecision === 'date'}
                      <input bind:value={filters.timeFilterEnd} oninput={handleFilterChange} type="date" />
                    {:else}
                      <input bind:value={filters.timeFilterEnd} oninput={handleFilterChange} type="datetime-local" />
                    {/if}
                    <div class="time-filter-actions-row">
                      <button
                        aria-label={t('radtrack-map_time_filter_latest-title')}
                        disabled={timeBoundsLoading}
                        onclick={setAbsoluteTimeFilterEndToLatest}
                        title={t('radtrack-map_time_filter_latest-title')}
                        type="button"
                      >
                        {t('radtrack-map_time_filter_latest-button')}
                      </button>
                      <button
                        aria-label={t('radtrack-map_time_filter_now-title')}
                        onclick={setAbsoluteTimeFilterEndToNow}
                        title={t('radtrack-map_time_filter_now-title')}
                        type="button"
                      >
                        {t('radtrack-map_time_filter_now-button')}
                      </button>
                    </div>
                  </label>
                </div>

                <div class="time-slice-panel">
                  <div class="time-slice-header">
                    <label class="checkbox-field time-slice-toggle" title={t('radtrack-map_time_slice_playback-title')}>
                      <input
                        checked={timeSliceEnabled}
                        onchange={handleTimeSliceEnabledChange}
                        type="checkbox"
                      />
                      <span>{t('radtrack-map_time_slice_playback-label')}</span>
                    </label>
                    {#if timeSliceEnabled}
                      <button onclick={openTimeSliceConfig} type="button">
                        {t('radtrack-map_time_slice_configure-button')}
                      </button>
                    {/if}
                  </div>

                  {#if timeSliceEnabled}
                    {#if timeSliceNeedsLargeLoadConfirmation}
                      <div class="time-slice-confirm-row">
                        <span class="warning-text">{timeSliceSummary}</span>
                        <button onclick={confirmTimeSliceLargeLoad} type="button">
                          {t('radtrack-map_time_slice_continue-button')}
                        </button>
                      </div>
                    {:else}
                      <div class="time-slice-summary">{timeSliceSummary}</div>

                      <div class="time-slice-controls">
                        <button
                          aria-label={t('radtrack-common_previous-label')}
                          disabled={!activeTimeSliceWindow || timeSliceIndex <= 0 || timeSliceSourceLoading}
                          onclick={() => moveTimeSlice({ delta: -1 })}
                          type="button"
                        >
                          &lt;
                        </button>
                        <button
                          aria-label={timeSlicePlaying ? t('radtrack-map_time_slice_pause-button') : t('radtrack-map_time_slice_play-button')}
                          disabled={!activeTimeSliceWindow || activeTimeSliceWindows.length < 2 || timeSliceSourceLoading}
                          onclick={toggleTimeSlicePlayback}
                          type="button"
                        >
                          {timeSlicePlaying ? t('radtrack-map_time_slice_pause-button') : t('radtrack-map_time_slice_play-button')}
                        </button>
                        <button
                          aria-label={t('radtrack-common_next-label')}
                          disabled={!activeTimeSliceWindow || timeSliceIndex >= activeTimeSliceWindows.length - 1 || timeSliceSourceLoading}
                          onclick={() => moveTimeSlice({ delta: 1 })}
                          type="button"
                        >
                          &gt;
                        </button>
                        <select
                          aria-label={t('radtrack-map_time_slice_speed-label')}
                          bind:value={timeSlicePlaybackSpeed}
                          disabled={!activeTimeSliceWindow || activeTimeSliceWindows.length < 2 || timeSliceSourceLoading}
                        >
                          {#each timeSlicePlaybackSpeeds as speed}
                            <option value={speed}>{speed}x</option>
                          {/each}
                        </select>
                      </div>

                      <input
                        aria-label={t('radtrack-map_time_slice-title')}
                        disabled={!activeTimeSliceWindow || timeSliceSourceLoading}
                        max={Math.max(0, activeTimeSliceWindows.length - 1)}
                        min="0"
                        oninput={(event) => setTimeSlicePosition({
                          index: Number((event.currentTarget as HTMLInputElement).value)
                        })}
                        step="1"
                        type="range"
                        value={Math.max(0, Math.min(timeSliceIndex, Math.max(0, activeTimeSliceWindows.length - 1)))}
                      />

                      <div class="time-slice-meta-row">
                        <label class="checkbox-field time-slice-current-toggle" title={t('radtrack-map_time_slice_current_only-title')}>
                          <input
                            checked={timeSliceCurrentOnly}
                            onchange={handleTimeSliceCurrentOnlyChange}
                            type="checkbox"
                          />
                          <span>{t('radtrack-map_time_slice_current_only-label')}</span>
                        </label>
                        <span class="chip subtle">{timeSliceIntervalLabel}</span>
                        {#if timeSliceSourceLoading}
                          <span class="chip warning">{t('radtrack-map_time_slice_source_loading-label')}</span>
                        {/if}
                      </div>

                      {#if timeSlicePerformanceWarning}
                        <div class="selection-empty warning-text">{timeSlicePerformanceWarning}</div>
                      {/if}
                    {/if}
                  {/if}
                </div>
              {:else if filters.timeFilterMode === 'relative'}
                <div class="grid cols-2 time-filter-range-grid">
                  <label>
                    <div class="muted">{t('radtrack-map_time_filter_since_amount-label')}</div>
                    <input
                      bind:value={filters.timeFilterRelativeAmount}
                      min="1"
                      oninput={handleFilterChange}
                      step="1"
                      type="number"
                    />
                  </label>

                  <label>
                    <div class="muted">{t('radtrack-map_time_filter_since_unit-label')}</div>
                    <select bind:value={filters.timeFilterRelativeUnit} onchange={handleFilterChange}>
                      <option value="hours">{t('radtrack-map_time_filter_hours-label')}</option>
                      <option value="days">{t('radtrack-map_time_filter_days-label')}</option>
                    </select>
                  </label>
                </div>

              {/if}

              <label class="checkbox-field">
                <input bind:checked={filters.forceRecheck} onchange={handleFilterChange} type="checkbox" />
                <span>{t('radtrack-map_force_recheck-label')}</span>
              </label>

              {#if timeFilterValidationMessage}
                <div class="selection-empty warning-text">{timeFilterValidationMessage}</div>
              {/if}

            </div>
          </details>

          <hr class="map-data-divider time-filter-divider" />
          </div>

          <label>
            <div class="muted">{t('radtrack-common_metric-label')}</div>
            <select bind:value={filters.metric} onchange={handleFilterChange}>
              {#each pendingAvailableMetricFields as field}
                <option value={field.propKey}>{getPendingMetricLabel(field.propKey)}</option>
              {/each}
            </select>
          </label>

          <label>
            <div class="muted">{t('radtrack-common_mode-label')}</div>
            <select bind:value={filters.mode} onchange={handleFilterChange}>
              <option value="aggregate">{t('radtrack-common_aggregates-label')}</option>
              <option value="raw">{t('radtrack-common_raw_points-label')}</option>
            </select>
          </label>

          {#if filters.mode === 'aggregate'}
            <label>
              <div class="muted">{t('radtrack-common_shape-label')}</div>
              <select bind:value={filters.shape} onchange={handleFilterChange}>
                <option value="hexagon">{t('radtrack-common_hexagon-label')}</option>
                <option value="square">{t('radtrack-common_square-label')}</option>
                <option value="circle">{t('radtrack-common_circle-label')}</option>
              </select>
            </label>
          {/if}

          {#if filters.mode === 'aggregate'}
            <label>
              <div class="muted">{t('radtrack-map_aggregate_stat-label')}</div>
              <select bind:value={filters.aggregateStat} onchange={handleFilterChange}>
                <option value="min">{t('radtrack-common_min-label')}</option>
                <option value="max">{t('radtrack-common_max-label')}</option>
                <option value="mean">{t('radtrack-common_mean-label')}</option>
                <option value="median">{t('radtrack-common_median-label')}</option>
                <option value="mode">{t('radtrack-common_mode-label')}</option>
                <option value="count">{t('radtrack-common_count-label')}</option>
              </select>
            </label>
          {/if}

          {#if filters.mode === 'aggregate'}
            <div class="form-grid field-group">
              <label class="checkbox-field">
                <input bind:checked={filters.autoCellSize} onchange={handleAutoCellSizeToggle} type="checkbox" />
                <span>{t('radtrack-map_auto_cell_size-label')}</span>
              </label>

              {#if !filters.autoCellSize}
                <label>
                  <div class="muted">{t('radtrack-map_cell_size_manual-label')}</div>
                  <input
                    bind:value={filters.cellSizeMeters}
                    min="10"
                    oninput={handleFilterChange}
                    type="number"
                  />
                </label>
              {/if}

              <span class="chip start">
                {t('radtrack-map_active_cell_size-label', {
                  size: formatNumber(activeAppliedCellSizeMeters),
                  zoom: viewport.zoom
                })}
              </span>
            </div>
          {/if}

          <label class="checkbox-field">
            <input bind:checked={filters.applyExcludeAreas} onchange={handleFilterChange} type="checkbox" />
            <span>{t('radtrack-map_apply_exclude_areas-label')}</span>
          </label>
        </div>
      </section>

      <section class="panel">
        <h2>{t('radtrack-map_display_options-title')}</h2>

        {#if activeMode === 'aggregate'}
          <div class="form-grid">
            <label class="checkbox-field">
              <input bind:checked={colorScaleSettings.auto} onchange={handleAutoColorScaleToggle} type="checkbox" />
              <span>{t('radtrack-map_auto_scale-label')}</span>
            </label>

            <div class="grid cols-3 scale-grid">
              <label>
                <div class="muted">{t('radtrack-map_scale_low-label')}</div>
                <input bind:value={colorScaleSettings.low} disabled={colorScaleSettings.auto} step="any" type="number" />
              </label>

              <label>
                <div class="muted">{t('radtrack-map_scale_mid-label')}</div>
                <input bind:value={colorScaleSettings.mid} disabled={colorScaleSettings.auto} step="any" type="number" />
              </label>

              <label>
                <div class="muted">{t('radtrack-map_scale_high-label')}</div>
                <input bind:value={colorScaleSettings.high} disabled={colorScaleSettings.auto} step="any" type="number" />
              </label>
            </div>
          </div>
        {/if}

        <details class="settings-accordion">
          <summary>
            <span>{t('radtrack-map_popup_fields-title')}</span>
              <span class="settings-accordion-meta">
              <span class="chip subtle">{enabledPopupFieldCount}/{popupSelectableFields.length}</span>
              <span aria-hidden="true" class="settings-accordion-icon"></span>
            </span>
          </summary>

          <div class="popup-field-grid">
            {#each popupSelectableFields as field}
              <label class="checkbox-field">
                <input
                  checked={Boolean(pendingPopupMetricStates[field.propKey])}
                  onchange={(event) => {
                    const checked = (event.currentTarget as HTMLInputElement).checked;
                    popupFieldTouched = {
                      ...popupFieldTouched,
                      [field.propKey]: true
                    };
                    popupFields = {
                      ...popupFields,
                      metrics: {
                        ...popupFields.metrics,
                        [field.propKey]: checked
                      }
                    };
                    handleFilterChange();
                  }}
                  type="checkbox"
                />
                <span>{getPendingPopupFieldLabel(field.propKey)}</span>
              </label>
            {/each}
          </div>
        </details>
      </section>

      <section class="panel">
        <h2>{t('radtrack-common_viewport-label')}</h2>
        <div class="grid">
          <span class="chip start">{t('radtrack-common_lat_range-label', { min: viewport.minLat.toFixed(4), max: viewport.maxLat.toFixed(4) })}</span>
          <span class="chip start">{t('radtrack-common_lon_range-label', { min: viewport.minLon.toFixed(4), max: viewport.maxLon.toFixed(4) })}</span>
          <span class="chip mid">{t('radtrack-common_zoom-label', { zoom: viewport.zoom })}</span>
          <span class="chip mid">{t('radtrack-common_raw_points-label')} {formatCount(visibleRawPointCount)}</span>
          {#if activeMode === 'aggregate'}
            <span class="chip subtle">{t('radtrack-common_cells-label')} {formatCount(visibleCellCount)}</span>
          {/if}
        </div>
      </section>

      {#if selectedPoint}
        <section class="panel">
          <h2>{t('radtrack-common_selected_reading-label')}</h2>
          <div class="grid">
            <span class="chip start">{formatTime(selectedPoint.occurredAt)}</span>
            <span class="chip start">{formatTime(selectedPoint.receivedAt)}</span>
            <span class="chip subtle">{t('radtrack-common_latitude-label')} {formatNumber(selectedPoint.latitude)}</span>
            <span class="chip subtle">{t('radtrack-common_longitude-label')} {formatNumber(selectedPoint.longitude)}</span>
            {#each appliedVisiblePointMetricFields as field}
              <span class="chip mid">{getAppliedPopupFieldLabel(field.propKey)} {formatMetricFieldValue({
                field,
                value: field.propKey === 'altitudeMeters'
                  ? selectedPoint.altitudeMeters
                  : (
                    field.propKey === 'accuracy'
                      ? selectedPoint.accuracy
                      : selectedPoint.measurements?.[field.propKey]
                  )
              })}</span>
            {/each}
            {#if selectedPoint.comment}
              <p class="muted">{selectedPoint.comment}</p>
            {/if}
            {#if $sessionStore.user?.role !== 'view_only'}
              <button class="danger" onclick={hideSelectedPoint}>{t('radtrack-common_hide_point-button')}</button>
            {/if}
          </div>
        </section>
      {/if}

    </aside>

    <section class="map-canvas">
      {#if errorMessage}
        <article class="panel map-error">
          <p class="muted">{errorMessage}</p>
        </article>
      {/if}

        <div
          bind:this={mapStageElement}
          class="map-stage"
          style:--map-popup-max-height={`${popupMaxHeightPx}px`}
        >
        <LeafletMap
          aggregateStat={activeAggregateStat}
          aggregates={displayedAggregates}
          attribution={activeBasemap?.attribution ?? ''}
          colorScale={activeMode === 'aggregate' ? activeColorScale : null}
          focus={mapFocus}
          liveUpdateMarkers={liveUpdateMarkers}
          metricLabels={popupLabelsRecord}
          metricValueTypes={fieldValueTypesRecord}
          mode={activeMode}
          on:openaggregatecellpoints={(event) => {
            openAggregateCellModal({
              cell: event.detail
            });
          }}
          on:selectpoint={(event) => {
            selectedPoint = event.detail;
          }}
          on:viewportchange={handleViewportChange}
          points={displayedPoints}
          popupFields={appliedPopupFields}
          shape={activeShape}
          tileUrlTemplate={activeBasemap?.tileUrlTemplate ?? ''}
        />

        {#if aggregateCellModalState}
          <MapAggregatePointsModal
            columns={aggregateCellModalColumns}
            errorMessage={aggregateCellModalErrorMessage}
            exportContext={aggregateCellModalExportContext}
            loading={aggregateCellModalLoading}
            maxHeightPx={popupMaxHeightPx}
            on:close={closeAggregateCellModal}
            pointCount={aggregateCellModalDisplayCell?.pointCount ?? aggregateCellModalPoints.length}
            rows={aggregateCellModalRows}
            subtitle={aggregateCellModalSubtitle}
          />
        {/if}

        {#if timeSliceConfigOpen}
          <div class="map-config-modal-shell" role="presentation">
            <button
              aria-label={t('radtrack-common_close-button')}
              class="map-config-modal-backdrop"
              onclick={closeTimeSliceConfig}
              type="button"
            ></button>

            <section
              aria-labelledby="time-slice-config-title"
              aria-modal="true"
              class="panel map-config-modal"
              role="dialog"
            >
              <header class="map-config-modal-header">
                <h2 id="time-slice-config-title">{t('radtrack-map_time_slice_configure-title')}</h2>
                <button onclick={closeTimeSliceConfig} type="button">{t('radtrack-common_close-button')}</button>
              </header>

              <div class="grid cols-2 time-filter-range-grid">
                <label>
                  <div class="muted">{t('radtrack-map_time_slice_interval_amount-label')}</div>
                  <input bind:value={timeSliceDraftSettings.amount} min="1" step="1" type="number" />
                </label>

                <label>
                  <div class="muted">{t('radtrack-map_time_slice_interval_unit-label')}</div>
                  <select bind:value={timeSliceDraftSettings.unit}>
                    <option value="days">{t('radtrack-map_time_filter_days-label')}</option>
                    <option value="hours">{t('radtrack-map_time_filter_hours-label')}</option>
                    <option value="minutes">{t('radtrack-map_time_slice_minutes-label')}</option>
                  </select>
                </label>
              </div>

              <div class="actions">
                <button onclick={closeTimeSliceConfig} type="button">{t('radtrack-common_cancel-button')}</button>
                <button class="primary" onclick={applyTimeSliceConfig} type="button">
                  {t('radtrack-common_save-button')}
                </button>
              </div>
            </section>
          </div>
        {/if}

        <div class="map-overlay map-overlay-basemap">
          <section class="panel map-overlay-card map-overlay-basemap-card">
            <label class="form-grid">
              <div class="muted">{t('radtrack-map_basemap-label')}</div>
              <select bind:value={basemapKey} onchange={handleBasemapChange}>
                {#each basemapOptions as basemap}
                  <option value={basemap.key}>{basemap.label}</option>
                {/each}
              </select>
            </label>
          </section>
        </div>

        {#if activeMode === 'aggregate'}
          <div class:is-wide={legendNeedsWide} class="map-overlay map-overlay-legend">
            <details class="panel map-overlay-card" open>
              <summary>
                <span>{t('radtrack-map_legend-title')}</span>
                <span class="chip subtle">{activeLegendSummary}</span>
              </summary>

              <div class="legend-bar"></div>

              <div class="legend-scale-row">
                <span class="chip start">
                  {t('radtrack-common_low-label')}
                  {#if activeLegendScaleValues.low}
                    {' '}
                    {activeLegendScaleValues.low}
                  {/if}
                </span>
                <span class="chip warning">
                  {t('radtrack-common_mid-label')}
                  {#if activeLegendScaleValues.mid}
                    {' '}
                    {activeLegendScaleValues.mid}
                  {/if}
                </span>
                <span class="chip danger">
                  {t('radtrack-common_high-label')}
                  {#if activeLegendScaleValues.high}
                    {' '}
                    {activeLegendScaleValues.high}
                  {/if}
                </span>
              </div>
            </details>
          </div>
        {/if}
      </div>
    </section>
  </div>
</div>

<style>
  .map-page {
    height: calc(100dvh - (var(--space-5) * 2));
    display: grid;
    grid-template-rows: auto minmax(0, 1fr);
    gap: var(--space-4);
    min-height: 0;
    min-width: 0;
    width: 100%;
  }

  .map-layout {
    display: grid;
    grid-template-columns: minmax(22rem, var(--map-sidebar-width, 35rem)) minmax(0, 1fr);
    gap: var(--space-4);
    align-items: stretch;
    width: 100%;
    height: 100%;
    min-height: 0;
  }

  .map-sidebar {
    min-height: 0;
    overflow-y: auto;
    padding-right: var(--space-2);
    display: grid;
    gap: var(--space-4);
    align-content: start;
  }

  .map-canvas {
    min-height: 0;
    min-width: 0;
    width: 100%;
    display: grid;
    grid-template-rows: auto minmax(0, 1fr);
    gap: var(--space-4);
  }

  .map-error {
    grid-row: 1;
  }

  .map-stage {
    grid-row: 2;
    min-height: 0;
    min-width: 0;
    width: 100%;
    height: 100%;
    position: relative;
  }

  .map-config-modal-shell {
    position: absolute;
    inset: 0;
    z-index: 720;
    display: grid;
    place-items: start center;
    padding: var(--space-4);
  }

  .map-config-modal-backdrop {
    position: absolute;
    inset: 0;
    min-height: 0;
    padding: 0;
    border: none;
    border-radius: 0;
    background: color-mix(in srgb, var(--color-bg) 52%, transparent);
    box-shadow: none;
    transform: none;
  }

  .map-config-modal-backdrop:hover {
    border: none;
    transform: none;
  }

  .map-config-modal {
    position: relative;
    z-index: 1;
    width: min(30rem, calc(100% - (var(--space-4) * 2)));
    display: grid;
    gap: var(--space-4);
  }

  .map-config-modal-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--space-3);
  }

  .map-config-modal-header h2 {
    margin: 0;
  }

  .map-panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: var(--space-3);
    flex-wrap: wrap;
  }

  .map-panel-actions {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
    margin-left: auto;
  }

  .sidebar-size-toggle {
    display: inline-grid;
    grid-template-columns: repeat(2, 2.1rem);
    gap: 1px;
    padding: 0.18rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel);
  }

  .sidebar-size-toggle button {
    min-width: 0;
    width: 2.1rem;
    height: 2rem;
    padding: 0;
    border-color: transparent;
    background: transparent;
    color: var(--color-text-muted);
  }

  .sidebar-size-toggle button:hover,
  .sidebar-size-toggle button.active {
    border-color: var(--color-accent);
    background: rgba(0, 182, 255, 0.12);
    color: var(--color-text);
  }

  .sidebar-size-icon {
    position: relative;
    display: inline-block;
    height: 1rem;
    border: 1px solid currentColor;
    border-radius: 0.2rem;
  }

  .sidebar-size-icon::before {
    content: '';
    position: absolute;
    inset-block: 0.18rem;
    left: 0.2rem;
    width: 0.22rem;
    border-radius: 999px;
    background: currentColor;
    opacity: 0.75;
  }

  .sidebar-size-icon-small {
    width: 0.82rem;
  }

  .sidebar-size-icon-large {
    width: 1.28rem;
  }

  .checkbox-field {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: center;
    gap: var(--space-3);
  }

  .checkbox-field input {
    width: auto;
    margin: 0;
  }

  .map-auto-update {
    margin-bottom: var(--space-4);
  }

  .map-live-updates {
    grid-template-columns: auto 1fr auto;
    margin-bottom: var(--space-1);
  }

  .map-live-updates-note {
    margin: 0 0 var(--space-4);
  }

  .live-update-log-accordion {
    margin-bottom: var(--space-4);
  }

  .live-update-log-list {
    display: grid;
    gap: var(--space-2);
    max-height: 18rem;
    overflow-y: auto;
    padding: var(--space-2);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel);
  }

  .live-update-log-item {
    display: grid;
    gap: var(--space-1);
    padding: 0.55rem 0.65rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .live-update-log-title,
  .live-update-log-meta {
    min-width: 0;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-2);
  }

  .live-update-log-meta {
    color: var(--color-text-muted);
    font-size: 0.9em;
  }

  .field-group {
    padding-top: var(--space-3);
    border-top: 1px solid var(--color-border);
  }

  .selection-stack {
    display: grid;
    gap: var(--space-3);
  }

  .selector-accordion-stack {
    display: grid;
    gap: var(--space-2);
  }

  .map-data-accordion-body {
    display: grid;
    gap: var(--space-2);
  }

  .map-data-divider {
    width: 100%;
    height: 1px;
    margin: var(--space-3) 0 0;
    border: 0;
    background: var(--color-border);
  }

  .time-filter-divider {
    height: 2px;
    margin: var(--space-3) 0 var(--space-2);
    background: var(--color-border-strong);
  }

  .selector-accordion {
    display: grid;
    gap: var(--space-2);
  }

  .selector-accordion summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
    padding: 0.7rem 0.8rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .selector-accordion summary::-webkit-details-marker {
    display: none;
  }

  .selector-accordion summary:hover {
    border-color: var(--color-border-strong);
  }

  .selector-accordion[open] summary {
    margin-bottom: 0;
  }

  .selection-list {
    display: grid;
    gap: var(--space-2);
    max-height: 22rem;
    overflow-y: auto;
    padding: var(--space-2);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel);
  }

  .selection-list-tall {
    max-height: 28rem;
  }

  .selection-item {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: var(--space-3);
    align-items: center;
    padding: 0.55rem 0.7rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .selection-item-compact {
    padding-block: 0.45rem;
  }

  .selection-item input {
    width: auto;
    margin: 0.15rem 0 0;
  }

  .selection-copy {
    display: grid;
    gap: 0.14rem;
    min-width: 0;
  }

  .selection-title {
    display: block;
    min-width: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .selection-meta-row,
  .selection-meta-id {
    color: var(--color-text-muted);
    font-size: 0.92em;
    min-width: 0;
  }

  .selection-meta-row {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .selection-meta-id {
    color: var(--color-text-faint);
  }

  .selection-help,
  .selection-empty {
    font-size: 0.92em;
  }

  .time-filter-stack {
    padding: var(--space-2);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel);
  }

  .time-filter-range-grid {
    gap: var(--space-3);
  }

  .time-filter-actions-row {
    display: flex;
    gap: var(--space-2);
    justify-content: flex-end;
    flex-wrap: wrap;
    margin-top: var(--space-2);
  }

  .time-slice-panel {
    display: grid;
    gap: var(--space-3);
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .time-slice-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--space-3);
  }

  .time-slice-summary {
    margin-top: 0.15rem;
    color: var(--color-text);
    font-size: 0.92em;
  }

  .time-slice-toggle {
    min-height: 2.4rem;
  }

  .time-slice-controls,
  .time-slice-meta-row,
  .time-slice-confirm-row {
    display: flex;
    align-items: center;
    gap: var(--space-2);
    flex-wrap: wrap;
  }

  .time-slice-confirm-row {
    justify-content: space-between;
  }

  .time-slice-controls button {
    min-width: 2.4rem;
  }

  .time-slice-controls select {
    width: auto;
    min-width: 5rem;
  }

  .time-slice-panel input[type='range'] {
    width: 100%;
  }

  .warning-text {
    color: var(--color-warning);
  }

  .track-group {
    display: grid;
    gap: var(--space-2);
  }

  .track-group + .track-group {
    margin-top: var(--space-2);
    padding-top: var(--space-2);
    border-top: 1px solid var(--color-border);
  }

  .track-group-title {
    color: var(--color-text-muted);
    font-size: 0.88em;
    letter-spacing: 0.03em;
    text-transform: uppercase;
    padding-inline: 0.2rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .settings-accordion {
    display: grid;
    gap: var(--space-3);
    margin-top: var(--space-4);
    padding-top: var(--space-4);
    border-top: 1px solid var(--color-border);
  }

  .settings-accordion summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
    padding: 0.8rem 0.9rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .settings-accordion summary::-webkit-details-marker {
    display: none;
  }

  .settings-accordion summary:hover {
    border-color: var(--color-border-strong);
  }

  .settings-accordion[open] summary {
    margin-bottom: var(--space-2);
  }

  .settings-accordion-meta {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
  }

  .settings-accordion-icon::before {
    content: '>';
    display: inline-block;
    color: var(--color-text-muted);
    transition: transform var(--transition), color var(--transition);
  }

  .selector-accordion[open] > summary .settings-accordion-icon::before,
  .settings-accordion[open] > summary .settings-accordion-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .popup-field-grid {
    display: grid;
    gap: var(--space-3);
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .map-sidebar.compact .time-filter-range-grid,
  .map-sidebar.compact .scale-grid,
  .map-sidebar.compact .popup-field-grid,
  .map-sidebar.compact .field-group {
    grid-template-columns: 1fr;
  }

  .map-overlay {
    position: absolute;
    top: var(--space-4);
    right: var(--space-4);
    z-index: 500;
    max-width: min(32rem, calc(100% - (var(--space-4) * 2)));
    pointer-events: none;
  }

  .map-overlay-card {
    pointer-events: auto;
    display: grid;
    gap: var(--space-3);
    padding: var(--space-3);
  }

  .map-overlay-card summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
  }

  .map-overlay-card summary::-webkit-details-marker {
    display: none;
  }

  .map-overlay-basemap {
    left: var(--space-4);
    top: auto;
    bottom: var(--space-4);
    right: auto;
    max-width: min(16rem, calc(100% - (var(--space-4) * 2)));
  }

  .map-overlay-basemap-card {
    width: min(16rem, calc(100vw - (var(--space-4) * 2)));
  }

  .map-overlay-legend.is-wide {
    max-width: min(52rem, calc(100% - (var(--space-4) * 2)));
  }

  .map-overlay-legend.is-wide .map-overlay-card {
    width: min(52rem, calc(100vw - (var(--space-4) * 2)));
  }

  .legend-bar {
    margin-bottom: var(--space-1);
  }

  .legend-scale-row {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: var(--space-2);
  }

  .legend-scale-row .chip {
    width: 100%;
    justify-content: center;
    white-space: nowrap;
  }

  @media (max-width: 1024px) {
    .map-page {
      height: auto;
      min-height: 0;
    }

    .map-layout {
      grid-template-columns: 1fr;
      height: auto;
    }

    .map-sidebar {
      overflow: visible;
      padding-right: 0;
    }

    .popup-field-grid,
    .scale-grid,
    .time-filter-range-grid {
      grid-template-columns: 1fr;
    }

    .map-overlay {
      left: var(--space-3);
      right: var(--space-3);
      max-width: none;
    }

    .map-overlay-basemap {
      top: auto;
      bottom: var(--space-3);
    }

    .map-overlay-legend {
      top: auto;
      bottom: var(--space-3);
    }

    .legend-scale-row {
      grid-template-columns: 1fr;
    }
  }
</style>
