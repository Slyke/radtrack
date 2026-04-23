<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { onDestroy, onMount, untrack } from 'svelte';
  import LeafletMap from '$lib/components/LeafletMap.svelte';
  import { apiFetch } from '$lib/api/client';
  import {
    getMetricOptionLabels,
    humanizePropKey,
    isAggregateTimePropKey,
    mergePopupFields,
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
    cache?: {
      hit: boolean;
      key: string;
      source: 'cache' | 'computed';
      ttlSecondsRemaining: number | null;
    };
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
    extra?: Record<string, unknown>;
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
    minLat: number;
    maxLat: number;
    minLon: number;
    maxLon: number;
    zoom: number;
  };

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

  const autoUpdateDebounceMs = 450;
  const autoUpdateStorageKeyPrefix = 'radtrack.map.auto-update';
  const autoCellSizeStorageKeyPrefix = 'radtrack.map.auto-cell-size';
  const basemapStorageKeyPrefix = 'radtrack.map.basemap';
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
    applyExcludeAreas: true
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
    minLat: source.minLat,
    maxLat: source.maxLat,
    minLon: source.minLon,
    maxLon: source.maxLon,
    zoom: source.zoom
  });

  const cloneFilters = (source: MapFilters | AppliedMapFilters): MapFilters => ({
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
    applyExcludeAreas: source.applyExcludeAreas
  });

  const clonePopupStateSnapshot = (source: PopupStateSnapshot): PopupStateSnapshot => ({
    metrics: { ...source.metrics },
    touched: { ...source.touched }
  });

  const cloneAppliedFilters = (source: AppliedMapFilters): AppliedMapFilters => ({
    ...cloneFilters(source),
    appliedCellSizeMeters: Number(source.appliedCellSizeMeters)
  });

  const toFiniteNumber = (value: unknown) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
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
    applyExcludeAreas: sourceFilters.applyExcludeAreas
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
    minLat: sourceViewport.minLat,
    maxLat: sourceViewport.maxLat,
    minLon: sourceViewport.minLon,
    maxLon: sourceViewport.maxLon,
    zoom: sourceViewport.zoom
  });

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
  let selectedPoint = $state<MapPoint | null>(null);
  let viewport = $state(createInitialViewport());
  let filters = $state(createInitialFilters());
  let popupFields = $state(createInitialPopupFields());
  let popupFieldTouched = $state<Record<string, boolean>>({});
  let appliedPopupState = $state<PopupStateSnapshot>(createInitialPopupStateSnapshot());
  let colorScaleSettings = $state(createInitialColorScaleSettings());
  let activeQuery = $state<MapQuerySnapshot | null>(null);
  let loadingTrackDatasetIds = $state<string[]>([]);
  let basemapKey = $state<BasemapKey>('default');
  let mapDatasetDefaults = $state<MapDatasetDefaultRecord>({});
  let mapDatasetOrder = $state<string[]>([]);
  let mapFieldOrder = $state<MapFieldOrder>([]);
  let mapFieldVisibility = $state<MapFieldVisibilityRecord>({});
  let sidebarUpdateTimer: ReturnType<typeof setTimeout> | null = null;
  let queuedSidebarSnapshot: {
    query: MapQuerySnapshot;
    popupState: PopupStateSnapshot;
  } | null = null;
  let requestVersion = 0;

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

  const availableBasemapKeys: BasemapKey[] = ['default', 'satellite', 'light', 'topo'];

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

  const loadMapPreferences = () => {
    autoUpdateMap = loadStoredBoolean({
      prefix: autoUpdateStorageKeyPrefix,
      fallbackValue: true
    });
    filters = {
      ...filters,
      autoCellSize: loadStoredBoolean({
        prefix: autoCellSizeStorageKeyPrefix,
        fallbackValue: true
      })
    };

    const storedBasemapKey = loadStoredString({
      prefix: basemapStorageKeyPrefix,
      fallbackValue: 'default'
    });
    basemapKey = availableBasemapKeys.includes(storedBasemapKey as BasemapKey)
      ? storedBasemapKey as BasemapKey
      : 'default';
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
    datasets: [...values].sort((left, right) => (
      (right.readingCount - left.readingCount)
      || (right.trackCount - left.trackCount)
      || left.name.localeCompare(right.name)
    )),
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
    fieldSelector
  }: {
    tracks: TrackOption[];
    fieldSelector: (track: TrackOption) => MetricField[];
  }) => {
    const merged = new Map<string, MetricField>();

    for (const track of tracks) {
      for (const field of fieldSelector(track)) {
        if (!isMapFieldVisible({ visibility: mapFieldVisibility, propKey: field.propKey })) {
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
      fieldSelector: (track) => mergePopupFields(track.supportedFields ?? [])
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
    fieldSelector: (track) => track.metricFields ?? []
  }));
  const appliedAvailableMetricFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: appliedTrackOptions,
    fieldSelector: (track) => track.metricFields ?? []
  }));
  const pendingAvailablePopupFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: pendingTrackOptions,
    fieldSelector: (track) => mergePopupFields(track.supportedFields ?? [])
  }));
  const appliedAvailablePopupFields = $derived.by<MetricField[]>(() => collectVisibleFields({
    tracks: appliedTrackOptions,
    fieldSelector: (track) => mergePopupFields(track.supportedFields ?? [])
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
  const visibleRawPointCount = $derived.by(() => {
    if (activeMode !== 'aggregate') {
      return points.length;
    }

    return aggregates.reduce((total, cell) => total + cell.pointCount, 0);
  });
  const visibleCellCount = $derived.by(() => (
    activeMode === 'aggregate' ? aggregates.length : 0
  ));
  const activeColorScale = $derived.by(() => getEffectiveColorScale({
    cells: aggregates,
    aggregateStat: activeAggregateStat,
    settings: colorScaleSettings
  }));
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
  const appliedVisiblePointMetricFields = $derived.by(() => appliedVisiblePopupFields
    .filter((field) => (
      field.valueType !== 'string'
      && field.source !== 'synthetic'
      && !isAggregateTimePropKey(field.propKey)
      && !['occurredAt', 'latitude', 'longitude'].includes(field.propKey)
    )));
  const enabledPopupFieldCount = $derived.by(() => visiblePopupFields.length);
  const trackSelectionSummary = $derived.by(() => {
    if (!filters.datasetIds.length || filters.trackSelectionMode === 'none') {
      return t('radtrack-common_none');
    }

    if (filters.trackSelectionMode === 'all') {
      return t('radtrack-common_all-label');
    }

    return t('radtrack-map_selected_count-label', { count: filters.trackIds.length });
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
    const nextPopupState = getPendingPopupStateSnapshot();
    const nextSnapshot = getCurrentQuerySnapshot();

    if (nextSnapshot.key === activeQuery?.key) {
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

    if (!autoUpdateMap || !lookupsReady || !hasPendingSidebarChanges()) {
      return;
    }

    sidebarUpdateTimer = setTimeout(() => {
      sidebarUpdateTimer = null;
      applySidebarFilters();
    }, autoUpdateDebounceMs);
  };

  const buildBaseQuery = ({ snapshot }: { snapshot: MapQuerySnapshot }) => ({
    datasetIds: snapshot.filters.datasetIds,
    combinedDatasetIds: snapshot.filters.combinedDatasetIds,
    datalogIds: snapshot.filters.trackIds,
    datalogSelectionMode: snapshot.filters.trackSelectionMode,
    metric: snapshot.filters.metric,
    minLat: snapshot.viewport.minLat,
    maxLat: snapshot.viewport.maxLat,
    minLon: snapshot.viewport.minLon,
    maxLon: snapshot.viewport.maxLon,
    applyExcludeAreas: snapshot.filters.applyExcludeAreas
  });

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

      if (!filters.datasetIds.length && !filters.combinedDatasetIds.length) {
        const defaultDatasetIds = sortedDatasets
          .filter((dataset) => isMapDatasetDefaultEnabled({
            defaults: mapDatasetDefaults,
            datasetId: dataset.id
          }))
          .map((dataset) => dataset.id);
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
    snapshot = getCurrentQuerySnapshot()
  }: {
    popupState?: PopupStateSnapshot;
    snapshot?: MapQuerySnapshot;
  } = {}) => {
    const requestId = ++requestVersion;
    let completedSuccessfully = false;
    const query = buildBaseQuery({ snapshot });

    loading = true;
    errorMessage = null;
    selectedPoint = null;

    try {
      if (snapshot.filters.mode === 'raw') {
        const response = await apiFetch<any>({
          path: '/api/map/points',
          query
        });

        if (requestId !== requestVersion) {
          return;
        }

        points = response.result.points;
        aggregates = [];
        completedSuccessfully = true;
        return;
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
        return;
      }

      points = [];
      aggregates = response.result.cells;
      completedSuccessfully = true;
    } catch (error) {
      if (requestId !== requestVersion) {
        return;
      }

      errorMessage = error instanceof Error ? error.message : t('radtrack-map_failed');
    } finally {
      if (requestId !== requestVersion) {
        return;
      }

      if (completedSuccessfully) {
        activeQuery = {
          ...snapshot,
          filters: cloneAppliedFilters(snapshot.filters)
        };
        appliedPopupState = clonePopupStateSnapshot(popupState);
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
    if (loading) {
      return;
    }

    applySidebarFilters();
  };

  const handleFilterChange = () => {
    if (!autoUpdateMap) {
      clearScheduledSidebarUpdate();
      return;
    }

    scheduleSidebarAutoUpdate();
  };

  const handleViewportChange = (event: CustomEvent<MapViewport>) => {
    viewport = event.detail;

    if (!lookupsReady) {
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
    loadMapPreferences();
    await loadLookups();
    await loadMapData();
  });

  onDestroy(() => {
    clearScheduledSidebarUpdate();
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

  <div class="map-layout">
    <aside class="map-sidebar">
      <section class="panel">
        <div class="map-panel-header">
          <h2>{t('radtrack-common_filters-label')}</h2>
          {#if !autoUpdateMap}
            <button class="primary" disabled={loading || !hasPendingSidebarChanges()} onclick={updateMap}>
              {t('radtrack-map_update-button')}
            </button>
          {/if}
        </div>

        <label class="checkbox-field map-auto-update">
          <input bind:checked={autoUpdateMap} onchange={handleAutoUpdateToggle} type="checkbox" />
          <span>{t('radtrack-map_auto_update-label')}</span>
        </label>

        <div class="form-grid">
          <div class="selector-accordion-stack">
          <details class="selector-accordion" open>
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

        <div class="map-stage">
        <LeafletMap
          aggregateStat={activeAggregateStat}
          aggregates={aggregates}
          attribution={activeBasemap?.attribution ?? ''}
          colorScale={activeMode === 'aggregate' ? activeColorScale : null}
          metricLabels={popupLabelsRecord}
          metricValueTypes={fieldValueTypesRecord}
          mode={activeMode}
          on:selectpoint={(event) => {
            selectedPoint = event.detail;
          }}
          on:viewportchange={handleViewportChange}
          points={points}
          popupFields={appliedPopupFields}
          shape={activeShape}
          tileUrlTemplate={activeBasemap?.tileUrlTemplate ?? ''}
        />

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
          <div class="map-overlay map-overlay-legend">
            <details class="panel map-overlay-card" open>
              <summary>
                <span>{t('radtrack-map_legend-title')}</span>
                <span class="chip subtle">{activeLegendSummary}</span>
              </summary>

              <div class="legend-bar"></div>

              <div class="legend-scale-row">
                <span class="chip start">
                  {t('radtrack-common_low-label')}
                  {#if activeColorScale}
                    {' '}
                    {formatNumber(activeColorScale.low)}
                  {/if}
                </span>
                <span class="chip warning">
                  {t('radtrack-common_mid-label')}
                  {#if activeColorScale}
                    {' '}
                    {formatNumber(activeColorScale.mid)}
                  {/if}
                </span>
                <span class="chip danger">
                  {t('radtrack-common_high-label')}
                  {#if activeColorScale}
                    {' '}
                    {formatNumber(activeColorScale.high)}
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
    grid-template-columns: minmax(22rem, 30rem) minmax(0, 1fr);
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

  .map-panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    margin-bottom: var(--space-3);
    flex-wrap: wrap;
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

  .settings-accordion[open] .settings-accordion-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .popup-field-grid {
    display: grid;
    gap: var(--space-3);
    grid-template-columns: repeat(2, minmax(0, 1fr));
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
    .scale-grid {
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
