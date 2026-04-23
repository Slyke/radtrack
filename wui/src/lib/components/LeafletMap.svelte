<svelte:options runes={true} />

<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import 'leaflet/dist/leaflet.css';
  import {
    aggregateDataCountPropKey,
    getAggregateTimeBasePropKey,
    isAggregateTimePropKey,
    normalizePropKey
  } from '$lib/datalog-fields';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';

  type AggregateStats = {
    min: number | null;
    max: number | null;
    mean: number | null;
    median: number | null;
    mode: number | null;
    count: number;
  };

  type PopupFields = {
    metrics: Record<string, boolean>;
  };

  type ColorScale = {
    low: number;
    mid: number;
    high: number;
  };

  type MapPoint = {
    id: string;
    datasetId: string;
    datalogId: string;
    rawTimestamp?: string | null;
    parsedTimeText?: string | null;
    latitude: number;
    longitude: number;
    altitudeMeters: number | null;
    accuracy: number | null;
    measurements: Record<string, number>;
    deviceId?: string | null;
    deviceName?: string | null;
    deviceType?: string | null;
    deviceCalibration?: string | null;
    firmwareVersion?: string | null;
    sourceReadingId?: string | null;
    comment: string | null;
    custom?: string | null;
    occurredAt: string | null;
    receivedAt?: string | null;
    extra?: Record<string, unknown>;
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

  type ActivePopup = {
    mode: 'raw' | 'aggregate';
    id: string;
  };

  interface Props {
    points?: MapPoint[];
    aggregates?: AggregateCell[];
    mode?: 'raw' | 'aggregate';
    shape?: 'hexagon' | 'square' | 'circle';
    aggregateStat?: string;
    colorScale?: ColorScale | null;
    popupFields?: PopupFields;
    metricLabels?: Record<string, string>;
    metricValueTypes?: Record<string, 'number' | 'time' | 'string'>;
    tileUrlTemplate?: string;
    attribution?: string;
  }

  let {
    points = [],
    aggregates = [],
    mode = 'aggregate',
    shape = 'hexagon',
    aggregateStat = 'mean',
    colorScale = null,
    popupFields = {
      metrics: {}
    },
    metricLabels = {},
    metricValueTypes = {},
    tileUrlTemplate = '',
    attribution = ''
  }: Props = $props();

  const dispatch = createEventDispatcher<{
    viewportchange: {
      minLat: number;
      maxLat: number;
      minLon: number;
      maxLon: number;
      zoom: number;
    };
    selectpoint: MapPoint;
  }>();

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const earthRadiusMeters = 6378137;
  const mercatorMaxLatitude = 85.05112878;
  const worldWidthDegrees = 360;
  const resetToPrimaryWorldMaxZoom = 2;
  const numberFormatter = () => new Intl.NumberFormat($localeStore.language, {
    useGrouping: false,
    maximumFractionDigits: 5
  });

  const toRadians = (value: number) => (value * Math.PI) / 180;
  const toDegrees = (value: number) => (value * 180) / Math.PI;
  const clampLatitude = (latitude: number) => Math.max(
    -mercatorMaxLatitude,
    Math.min(mercatorMaxLatitude, latitude)
  );
  const toProjectedMeters = ({
    latitude,
    longitude
  }: {
    latitude: number;
    longitude: number;
  }) => ({
    x: earthRadiusMeters * toRadians(longitude),
    y: earthRadiusMeters * Math.log(Math.tan((Math.PI / 4) + (toRadians(clampLatitude(latitude)) / 2)))
  });
  const fromProjectedMeters = ({
    x,
    y
  }: {
    x: number;
    y: number;
  }) => ({
    latitude: toDegrees((2 * Math.atan(Math.exp(y / earthRadiusMeters))) - (Math.PI / 2)),
    longitude: toDegrees(x / earthRadiusMeters)
  });
  const wrapLongitude = (longitude: number) => (
    ((((longitude + 180) % worldWidthDegrees) + worldWidthDegrees) % worldWidthDegrees) - 180
  );
  const withLongitudeOffset = ({
    latitude,
    longitude
  }: {
    latitude: number;
    longitude: number;
  }, longitudeOffset: number) => ({
    latitude,
    longitude: longitude + longitudeOffset
  });

  let container: HTMLDivElement;
  let map: import('leaflet').Map | null = null;
  let leaflet: typeof import('leaflet') | null = null;
  let overlayLayer: import('leaflet').LayerGroup | null = null;
  let tileLayer: import('leaflet').TileLayer | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let activeTileLayerSignature = '';
  let activePopup: ActivePopup | null = null;
  let preservePopupState = false;
  const overlayPane = 'data-overlay';

  const updatePopupLayoutVars = () => {
    if (!container) {
      return;
    }

    container.style.setProperty(
      '--map-popup-max-height',
      `${Math.max(240, Math.floor(container.clientHeight / 2))}px`
    );
  };

  const escapeHtml = (value: string) => value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const formatTimestamp = (value: string | null | undefined) => {
    return formatDateTime({
      value,
      language: $localeStore.language
    }) ?? t('radtrack-common_none');
  };

  const formatNumber = (value: number | null | undefined) => {
    if (value === null || value === undefined) {
      return t('radtrack-common_na-label');
    }

    return numberFormatter().format(value);
  };

  const formatTimestampValue = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) {
      return t('radtrack-common_na-label');
    }

    if (typeof value === 'number') {
      return formatTimestamp(new Date(value).toISOString());
    }

    return formatTimestamp(value);
  };

  const formatMetricValue = ({
    metricKey,
    value
  }: {
    metricKey: string;
    value: number | string | null | undefined;
  }) => (
    metricValueTypes[metricKey] === 'time'
      ? formatTimestampValue(value)
      : (
        metricValueTypes[metricKey] === 'string'
          ? (value === null || value === undefined || value === '' ? t('radtrack-common_na-label') : String(value))
          : formatNumber(typeof value === 'string' ? Number(value) : value)
      )
  );

  const getMetricLabel = (metricKey: string) => metricLabels[metricKey] ?? metricKey;

  const coercePopupString = (value: unknown) => {
    if (value === null || value === undefined || value === '') {
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

  const resolveExtraValue = ({
    extra,
    metricKey
  }: {
    extra: Record<string, unknown> | null | undefined;
    metricKey: string;
  }) => {
    if (!extra) {
      return null;
    }

    if (metricKey in extra) {
      return extra[metricKey];
    }

    for (const [rawKey, rawValue] of Object.entries(extra)) {
      if (
        rawKey === metricKey
        || rawKey.toLowerCase() === metricKey.toLowerCase()
        || normalizePropKey(rawKey) === metricKey
      ) {
        return rawValue;
      }
    }

    return null;
  };

  const getPointMetricValue = ({ point, metricKey }: { point: MapPoint; metricKey: string }) => {
    switch (metricKey) {
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
      default:
        return point.measurements?.[metricKey] ?? resolveExtraValue({
          extra: point.extra,
          metricKey
        }) ?? null;
    }
  };

  const getAggregateSyntheticValues = ({
    metricKey,
    cell
  }: {
    metricKey: string;
    cell: AggregateCell;
  }) => {
    switch (metricKey) {
      case 'radtrackCacheKey':
        return cell.cache?.key ? [cell.cache.key] : [];
      case 'radtrackCacheSource':
        return cell.cache?.source ? [cell.cache.source] : [];
      case 'radtrackCacheTtlSeconds':
        return cell.cache?.ttlSecondsRemaining === null || cell.cache?.ttlSecondsRemaining === undefined
          ? []
          : [String(cell.cache.ttlSecondsRemaining)];
      default:
        return [];
    }
  };

  const mixHexColors = ({
    start,
    end,
    ratio
  }: {
    start: string;
    end: string;
    ratio: number;
  }) => {
    const clampedRatio = Math.max(0, Math.min(1, ratio));
    const channels = [0, 2, 4].map((offset) => ({
      start: Number.parseInt(start.slice(offset + 1, offset + 3), 16),
      end: Number.parseInt(end.slice(offset + 1, offset + 3), 16)
    }));

    return `#${channels
      .map(({ start: startChannel, end: endChannel }) => Math.round(
        startChannel + ((endChannel - startChannel) * clampedRatio)
      ).toString(16).padStart(2, '0'))
      .join('')}`;
  };

  const resolveAggregateFill = ({
    value,
    scale
  }: {
    value: number;
    scale: ColorScale | null;
  }) => {
    if (!scale) {
      return '#00b6ff';
    }

    const ordered = [scale.low, scale.mid, scale.high].sort((left, right) => left - right);
    const [low, mid, high] = ordered;

    if (high <= low) {
      return '#00b6ff';
    }

    if (value <= low) {
      return '#00b6ff';
    }

    if (value >= high) {
      return '#ff1f5a';
    }

    if (value <= mid) {
      return mixHexColors({
        start: '#00b6ff',
        end: '#ffbc3a',
        ratio: (value - low) / Math.max(mid - low, Number.EPSILON)
      });
    }

    return mixHexColors({
      start: '#ffbc3a',
      end: '#ff1f5a',
      ratio: (value - mid) / Math.max(high - mid, Number.EPSILON)
    });
  };

  const buildHexPoints = ({
    center,
    radiusMeters
  }: {
    center: { latitude: number; longitude: number };
    radiusMeters: number;
  }) => {
    const projectedCenter = toProjectedMeters(center);
    const points = [];
    for (let index = 0; index < 6; index += 1) {
      const angle = ((60 * index) - 30) * (Math.PI / 180);
      const dx = Math.cos(angle) * radiusMeters;
      const dy = Math.sin(angle) * radiusMeters;
      const projectedPoint = fromProjectedMeters({
        x: projectedCenter.x + dx,
        y: projectedCenter.y + dy
      });
      points.push([
        projectedPoint.latitude,
        projectedPoint.longitude
      ]);
    }
    return points as [number, number][];
  };

  const buildSquareBounds = ({
    center,
    radiusMeters
  }: {
    center: { latitude: number; longitude: number };
    radiusMeters: number;
  }) => {
    const projectedCenter = toProjectedMeters(center);
    const northWest = fromProjectedMeters({
      x: projectedCenter.x - radiusMeters,
      y: projectedCenter.y + radiusMeters
    });
    const southEast = fromProjectedMeters({
      x: projectedCenter.x + radiusMeters,
      y: projectedCenter.y - radiusMeters
    });

    return [
      [southEast.latitude, northWest.longitude],
      [northWest.latitude, southEast.longitude]
    ] as [[number, number], [number, number]];
  };

  const buildAggregateMetricRows = ({
    metricKey,
    stats
  }: {
    metricKey: string;
    stats: AggregateStats | null | undefined;
  }) => {
    if (!stats?.count) {
      return null;
    }

    return [
      { label: t('radtrack-common_mean-label'), value: formatMetricValue({ metricKey, value: stats.mean }) },
      { label: t('radtrack-common_min-label'), value: formatMetricValue({ metricKey, value: stats.min }) },
      { label: t('radtrack-common_max-label'), value: formatMetricValue({ metricKey, value: stats.max }) },
      { label: t('radtrack-common_median-label'), value: formatMetricValue({ metricKey, value: stats.median }) },
      { label: t('radtrack-common_mode-label'), value: formatMetricValue({ metricKey, value: stats.mode }) }
    ];
  };

  const buildAggregateTimeValueRows = ({
    metricKey,
    cell
  }: {
    metricKey: string;
    cell: AggregateCell;
  }) => {
    const metricStats = cell.metrics[metricKey];
    const startValue = metricKey === 'occurredAt'
      ? (cell.timeRange.start ?? metricStats?.min ?? null)
      : (metricStats?.min ?? null);
    const endValue = metricKey === 'occurredAt'
      ? (cell.timeRange.end ?? metricStats?.max ?? null)
      : (metricStats?.max ?? null);

    if (startValue === null && endValue === null) {
      return null;
    }

    if (startValue !== null && endValue !== null && String(startValue) !== String(endValue)) {
      return [
        { label: t('radtrack-common_min-label'), value: formatMetricValue({ metricKey, value: startValue }) },
        { label: t('radtrack-common_max-label'), value: formatMetricValue({ metricKey, value: endValue }) }
      ];
    }

    return [{
      label: t('radtrack-common_value-label'),
      value: formatMetricValue({ metricKey, value: startValue ?? endValue })
    }];
  };

  const buildAggregateTimeRange = ({
    timeRange
  }: {
    timeRange: AggregateCell['timeRange'];
  }) => {
    if (!timeRange.start && !timeRange.end) {
      return null;
    }

    if (timeRange.start && timeRange.end && timeRange.start !== timeRange.end) {
      return `${formatTimestamp(timeRange.start)} -> ${formatTimestamp(timeRange.end)}`;
    }

    return formatTimestamp(timeRange.start ?? timeRange.end);
  };

  const buildPopupDataRowHtml = ({
    label,
    value
  }: {
    label: string;
    value: string;
  }) => `
    <div class="map-popup-row">
      <span class="map-popup-row-label">${escapeHtml(label)}</span>
      <strong class="map-popup-row-value">${escapeHtml(value)}</strong>
    </div>
  `;

  const buildPopupMetricTableHtml = ({
    metricKey,
    rows
  }: {
    metricKey: string;
    rows: Array<{ label: string; value: string }>;
  }) => `
    <table class="map-popup-table${metricValueTypes[metricKey] === 'time' ? ' is-timestamp-table' : ''}">
      <thead>
        <tr>
          ${rows.map((row) => `<th>${escapeHtml(row.label)}</th>`).join('')}
        </tr>
      </thead>
      <tbody>
        <tr>
          ${rows.map((row) => (
            metricValueTypes[metricKey] === 'time'
              ? `<td><div class="map-popup-cell-scroll" tabindex="0" title="${escapeHtml(row.value)}">${escapeHtml(row.value)}</div></td>`
              : `<td>${escapeHtml(row.value)}</td>`
          )).join('')}
        </tr>
      </tbody>
    </table>
  `;

  const buildPopupValueListHtml = ({
    values
  }: {
    values: string[];
  }) => `
    <div class="map-popup-value-list">
      ${values.map((value) => `<code class="map-popup-value-chip" title="${escapeHtml(value)}">${escapeHtml(value)}</code>`).join('')}
    </div>
  `;

  const buildPopupSectionHtml = ({
    title,
    body
  }: {
    title: string;
    body: string;
  }) => {
    if (!body) {
      return '';
    }

    return `
      <section class="map-popup-section">
        <div class="map-popup-section-title">${escapeHtml(title)}</div>
        ${body}
      </section>
    `;
  };

  const buildPopupHeaderHtml = ({
    title,
    subtitle = null,
    badge = null
  }: {
    title: string;
    subtitle?: string | null;
    badge?: string | null;
  }) => `
    <div class="map-popup-header">
      <div class="map-popup-heading">
        <div class="map-popup-title">${escapeHtml(title)}</div>
        ${subtitle ? `<div class="map-popup-subtitle">${escapeHtml(subtitle)}</div>` : ''}
      </div>
      ${badge ? `<span class="map-popup-badge">${escapeHtml(badge)}</span>` : ''}
    </div>
  `;

  const getAggregatePopupSectionPriority = ({ metricKey }: { metricKey: string }) => (
    metricValueTypes[metricKey] === 'time' || isAggregateTimePropKey(metricKey)
      ? 0
      : 1
  );

  const buildRawPopupHtml = ({ point }: { point: MapPoint }) => {
    const rows: string[] = [];

    for (const [metricKey, enabled] of Object.entries(popupFields.metrics)) {
      if (!enabled) {
        continue;
      }

      const metricValue = getPointMetricValue({ point, metricKey });
      if (metricValue === null || metricValue === undefined) {
        continue;
      }

      rows.push(buildPopupDataRowHtml({
        label: getMetricLabel(metricKey),
        value: metricValueTypes[metricKey] === 'string'
          ? (coercePopupString(metricValue) ?? t('radtrack-common_na-label'))
          : formatMetricValue({ metricKey, value: metricValue })
      }));
    }

    return `
      <div class="map-popup">
        ${buildPopupHeaderHtml({
          title: t('radtrack-map_popup_reading-title')
        })}
        ${(rows.length || point.comment) ? `
          <div class="map-popup-scroll">
            ${rows.length ? `<div class="map-popup-data-list">${rows.join('')}</div>` : ''}
            ${point.comment ? `
              <section class="map-popup-section">
                <div class="map-popup-section-title">${escapeHtml(t('radtrack-common_comment-label'))}</div>
                <div class="map-popup-note">${escapeHtml(point.comment)}</div>
              </section>
            ` : ''}
          </div>
        ` : ''}
      </div>
    `;
  };

  const buildAggregatePopupHtml = ({ cell }: { cell: AggregateCell }) => {
    const sections: Array<{ html: string; priority: number }> = [];
    const timeRange = popupFields.metrics.occurredAt
      ? buildAggregateTimeRange({ timeRange: cell.timeRange })
      : null;
    const badge = popupFields.metrics[aggregateDataCountPropKey]
      ? `${formatNumber(cell.pointCount)} ${t('radtrack-map_popup_points-label')}`
      : null;

    for (const [metricKey, enabled] of Object.entries(popupFields.metrics)) {
      if (!enabled || metricKey === aggregateDataCountPropKey || metricKey === 'occurredAt') {
        continue;
      }

      const aggregateTimeBasePropKey = getAggregateTimeBasePropKey(metricKey);
      if (aggregateTimeBasePropKey) {
        const rows = buildAggregateMetricRows({
          metricKey: aggregateTimeBasePropKey,
          stats: cell.metrics[aggregateTimeBasePropKey]
        });
        if (rows) {
          sections.push({
            html: buildPopupSectionHtml({
              title: getMetricLabel(metricKey),
              body: buildPopupMetricTableHtml({
                metricKey,
                rows
              })
            }),
            priority: getAggregatePopupSectionPriority({ metricKey })
          });
        }
        continue;
      }

      if (metricValueTypes[metricKey] === 'string') {
        const values = [
          ...(cell.dataValues?.[metricKey] ?? []),
          ...getAggregateSyntheticValues({ metricKey, cell })
        ];
        if (values.length) {
          sections.push({
            html: buildPopupSectionHtml({
              title: getMetricLabel(metricKey),
              body: buildPopupValueListHtml({
                values: [...new Set(values)]
              })
            }),
            priority: getAggregatePopupSectionPriority({ metricKey })
          });
        }
        continue;
      }

      if (metricValueTypes[metricKey] === 'time' && !isAggregateTimePropKey(metricKey)) {
        const rows = buildAggregateTimeValueRows({
          metricKey,
          cell
        });
        if (rows) {
          sections.push({
            html: buildPopupSectionHtml({
              title: getMetricLabel(metricKey),
              body: buildPopupMetricTableHtml({
                metricKey,
                rows
              })
            }),
            priority: getAggregatePopupSectionPriority({ metricKey })
          });
        }
        continue;
      }

      const rows = buildAggregateMetricRows({
        metricKey,
        stats: cell.metrics[metricKey]
      });
      if (rows) {
        sections.push({
          html: buildPopupSectionHtml({
            title: getMetricLabel(metricKey),
            body: buildPopupMetricTableHtml({
              metricKey,
              rows
            })
          }),
          priority: getAggregatePopupSectionPriority({ metricKey })
        });
      }
    }

    const orderedSections = sections
      .sort((left, right) => left.priority - right.priority)
      .map((section) => section.html);

    return `
      <div class="map-popup">
        ${buildPopupHeaderHtml({
          title: t('radtrack-map_popup_aggregate-title'),
          subtitle: timeRange,
          badge
        })}
        ${orderedSections.length ? `<div class="map-popup-scroll">${orderedSections.join('')}</div>` : ''}
      </div>
    `;
  };

  const emitViewport = () => {
    if (!map) {
      return;
    }

    const bounds = map.getBounds();
    dispatch('viewportchange', {
      minLat: bounds.getSouth(),
      maxLat: bounds.getNorth(),
      minLon: bounds.getWest(),
      maxLon: bounds.getEast(),
      zoom: map.getZoom()
    });
  };

  const getVisibleLongitudeOffsets = () => {
    if (!map || map.getZoom() <= resetToPrimaryWorldMaxZoom) {
      return [0];
    }

    const bounds = map.getBounds();
    const firstWorldCopy = Math.ceil((bounds.getWest() - 180) / worldWidthDegrees);
    const lastWorldCopy = Math.floor((bounds.getEast() + 180) / worldWidthDegrees);
    const offsets: number[] = [];

    for (let worldCopy = firstWorldCopy; worldCopy <= lastWorldCopy; worldCopy += 1) {
      offsets.push(worldCopy * worldWidthDegrees);
    }

    return offsets.length ? offsets : [0];
  };

  const trackPopupLayer = ({
    layer,
    mode,
    id
  }: {
    layer: {
      on: (event: string, handler: () => void) => void;
      openPopup: () => void;
    };
    mode: 'raw' | 'aggregate';
    id: string;
  }) => {
    layer.on('popupopen', () => {
      activePopup = { mode, id };
    });

    layer.on('popupclose', () => {
      if (preservePopupState) {
        return;
      }

      if (activePopup?.mode === mode && activePopup.id === id) {
        activePopup = null;
      }
    });
  };

  const rerenderCurrentView = () => render({
    currentPoints: points,
    currentAggregates: aggregates,
    currentMode: mode,
    currentShape: shape,
    currentColorScale: colorScale
  });

  const maybeResetToPrimaryWorld = () => {
    if (!map || map.getZoom() > resetToPrimaryWorldMaxZoom) {
      return false;
    }

    const center = map.getCenter();
    const normalizedLongitude = wrapLongitude(center.lng);
    if (Math.abs(center.lng - normalizedLongitude) < 1e-7) {
      return false;
    }

    map.setView([center.lat, normalizedLongitude], map.getZoom(), {
      animate: false
    });
    return true;
  };

  const handleMoveEnd = () => {
    if (maybeResetToPrimaryWorld()) {
      return;
    }

    emitViewport();
    rerenderCurrentView();
  };

  const render = ({
    currentPoints,
    currentAggregates,
    currentMode,
    currentShape,
    currentColorScale
  }: {
    currentPoints: MapPoint[];
    currentAggregates: AggregateCell[];
    currentMode: 'raw' | 'aggregate';
    currentShape: 'hexagon' | 'square' | 'circle';
    currentColorScale: ColorScale | null;
  }) => {
    if (!map || !leaflet || !overlayLayer) {
      return;
    }

    const popupToRestore = activePopup;
    let restoreLayer: { openPopup: () => void } | null = null;
    let restoreLayerDistance = Number.POSITIVE_INFINITY;

    preservePopupState = true;
    overlayLayer.clearLayers();
    preservePopupState = false;
    const popupOptions = {
      autoPan: false,
      closeButton: true,
      maxWidth: 760,
      minWidth: 560
    };

    if (currentMode === 'raw') {
      const longitudeOffsets = getVisibleLongitudeOffsets();
      for (const point of currentPoints) {
        for (const longitudeOffset of longitudeOffsets) {
          const marker = leaflet.circleMarker([
            point.latitude,
            point.longitude + longitudeOffset
          ], {
            pane: overlayPane,
            radius: 5,
            color: '#00b6ff',
            weight: 1,
            fillColor: '#00b6ff',
            fillOpacity: 0.55
          });
          marker.bindPopup(buildRawPopupHtml({ point }), popupOptions);
          trackPopupLayer({
            layer: marker,
            mode: 'raw',
            id: point.id
          });
          marker.on('click', () => {
            dispatch('selectpoint', point);
          });
          if (popupToRestore?.mode === 'raw' && popupToRestore.id === point.id) {
            const longitudeDistance = Math.abs((point.longitude + longitudeOffset) - map.getCenter().lng);
            if (longitudeDistance < restoreLayerDistance) {
              restoreLayer = marker;
              restoreLayerDistance = longitudeDistance;
            }
          }
          overlayLayer.addLayer(marker);
        }
      }
      if (popupToRestore && !restoreLayer) {
        activePopup = null;
      }
      restoreLayer?.openPopup();
      return;
    }

    const longitudeOffsets = getVisibleLongitudeOffsets();
    for (const cell of currentAggregates) {
      const metricValue = cell.stats?.[aggregateStat as keyof AggregateStats];
      const fill = typeof metricValue === 'number'
        ? resolveAggregateFill({ value: metricValue, scale: currentColorScale })
        : '#3a4658';
      const popupHtml = buildAggregatePopupHtml({ cell });

      for (const longitudeOffset of longitudeOffsets) {
        const center = withLongitudeOffset(cell.center, longitudeOffset);

        if (currentShape === 'circle') {
          const layer = leaflet.circle([center.latitude, center.longitude], {
            pane: overlayPane,
            radius: cell.radiusMeters,
            color: fill,
            fillColor: fill,
            fillOpacity: 0.42,
            opacity: 0.9,
            weight: 1.5
          });
          layer.bindPopup(popupHtml, popupOptions);
          trackPopupLayer({
            layer,
            mode: 'aggregate',
            id: cell.id
          });
          if (popupToRestore?.mode === 'aggregate' && popupToRestore.id === cell.id) {
            const longitudeDistance = Math.abs(center.longitude - map.getCenter().lng);
            if (longitudeDistance < restoreLayerDistance) {
              restoreLayer = layer;
              restoreLayerDistance = longitudeDistance;
            }
          }
          overlayLayer.addLayer(layer);
          continue;
        }

        if (currentShape === 'square') {
          const layer = leaflet.rectangle(
            buildSquareBounds({
              center,
              radiusMeters: cell.radiusMeters
            }),
            {
              pane: overlayPane,
              color: fill,
              fillColor: fill,
              fillOpacity: 0.35,
              opacity: 0.9,
              weight: 1.5
            }
          );
          layer.bindPopup(popupHtml, popupOptions);
          trackPopupLayer({
            layer,
            mode: 'aggregate',
            id: cell.id
          });
          if (popupToRestore?.mode === 'aggregate' && popupToRestore.id === cell.id) {
            const longitudeDistance = Math.abs(center.longitude - map.getCenter().lng);
            if (longitudeDistance < restoreLayerDistance) {
              restoreLayer = layer;
              restoreLayerDistance = longitudeDistance;
            }
          }
          overlayLayer.addLayer(layer);
          continue;
        }

        const layer = leaflet.polygon(buildHexPoints({
          center,
          radiusMeters: cell.radiusMeters
        }), {
          pane: overlayPane,
          color: fill,
          fillColor: fill,
          fillOpacity: 0.35,
          opacity: 0.9,
          weight: 1.5
        });
        layer.bindPopup(popupHtml, popupOptions);
        trackPopupLayer({
          layer,
          mode: 'aggregate',
          id: cell.id
        });
        if (popupToRestore?.mode === 'aggregate' && popupToRestore.id === cell.id) {
          const longitudeDistance = Math.abs(center.longitude - map.getCenter().lng);
          if (longitudeDistance < restoreLayerDistance) {
            restoreLayer = layer;
            restoreLayerDistance = longitudeDistance;
          }
        }
        overlayLayer.addLayer(layer);
      }
    }

    if (popupToRestore && !restoreLayer) {
      activePopup = null;
    }
    restoreLayer?.openPopup();
  };

  const ensureTileLayer = ({
    urlTemplate,
    attributionText
  }: {
    urlTemplate: string;
    attributionText: string;
  }) => {
    if (!map || !leaflet || !urlTemplate) {
      return;
    }

    const signature = `${urlTemplate}::${attributionText}`;
    if (tileLayer && activeTileLayerSignature === signature) {
      return;
    }

    if (tileLayer) {
      map.removeLayer(tileLayer);
    }

    tileLayer = leaflet.tileLayer(urlTemplate, {
      attribution: attributionText
    }).addTo(map);
    activeTileLayerSignature = signature;
  };

  onMount(async () => {
    leaflet = await import('leaflet');
    map = leaflet.map(container, {
      zoomControl: true
    }).setView([41.9, -87.7], 9);

    if (!map.getPane(overlayPane)) {
      map.createPane(overlayPane);
    }
    const dataPane = map.getPane(overlayPane);
    if (dataPane) {
      dataPane.style.zIndex = '450';
    }

    ensureTileLayer({
      urlTemplate: tileUrlTemplate,
      attributionText: attribution
    });
    overlayLayer = leaflet.layerGroup().addTo(map);
    resizeObserver = new ResizeObserver(() => {
      updatePopupLayoutVars();
      map?.invalidateSize();
    });
    resizeObserver.observe(container);

    map.on('moveend', handleMoveEnd);
    requestAnimationFrame(() => {
      updatePopupLayoutVars();
      map?.invalidateSize();
    });
    handleMoveEnd();
  });

  onDestroy(() => {
    resizeObserver?.disconnect();
    map?.remove();
  });

  $effect(() => {
    const currentPoints = points;
    const currentAggregates = aggregates;
    const currentMode = mode;
    const currentShape = shape;
    const currentTileUrlTemplate = tileUrlTemplate;
    const currentAttribution = attribution;
    const currentColorScale = colorScale;
    popupFields;
    aggregateStat;

    ensureTileLayer({
      urlTemplate: currentTileUrlTemplate,
      attributionText: currentAttribution
    });

    render({
      currentPoints,
      currentAggregates,
      currentMode,
      currentShape,
      currentColorScale
    });
  });
</script>

<div bind:this={container} class="map-container"></div>

<style>
  .map-container :global(.leaflet-popup-content-wrapper) {
    background: var(--color-panel);
    color: var(--color-text);
    border: 1px solid var(--color-border);
    box-shadow: 0 18px 38px rgba(0, 0, 0, 0.3);
  }

  .map-container :global(.leaflet-popup-tip) {
    background: var(--color-panel);
    border: 1px solid var(--color-border);
    box-shadow: none;
  }

  .map-container :global(.leaflet-popup-content) {
    margin: 0;
    min-width: 30rem;
    max-width: 46rem;
    max-height: var(--map-popup-max-height, 50vh);
    overflow: hidden;
    padding: 1rem 1.05rem;
  }

  .map-container :global(.leaflet-container a.leaflet-popup-close-button) {
    color: var(--color-text-muted);
    padding: 0.55rem 0.7rem 0 0;
  }

  .map-container :global(.map-popup) {
    display: grid;
    grid-template-rows: auto minmax(0, 1fr);
    gap: 0.7rem;
    max-height: calc(var(--map-popup-max-height, 50vh) - 2rem);
    min-height: 0;
  }

  .map-container :global(.map-popup-scroll) {
    display: grid;
    gap: 0.7rem;
    min-height: 0;
    overflow-y: auto;
    padding-right: 0.15rem;
    scrollbar-gutter: stable;
  }

  .map-container :global(.map-popup-header) {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 0.75rem;
  }

  .map-container :global(.map-popup-heading) {
    display: grid;
    gap: 0.2rem;
  }

  .map-container :global(.map-popup-title) {
    font-weight: 700;
    letter-spacing: 0.02em;
    font-size: 1.05rem;
  }

  .map-container :global(.map-popup-subtitle) {
    color: var(--color-text-muted);
    line-height: 1.35;
  }

  .map-container :global(.map-popup-badge) {
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    padding: 0.3rem 0.55rem;
    border-radius: 999px;
    border: 1px solid var(--color-start);
    background: var(--color-start-soft);
    color: var(--color-text);
  }

  .map-container :global(.map-popup-section) {
    display: grid;
    gap: 0.35rem;
    padding-top: 0.65rem;
    border-top: 1px solid var(--color-border);
  }

  .map-container :global(.map-popup-section-title) {
    color: var(--color-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-size: 0.78rem;
  }

  .map-container :global(.map-popup-data-list) {
    display: grid;
    gap: 0.4rem;
  }

  .map-container :global(.map-popup-row) {
    display: grid;
    grid-template-columns: 6.5rem minmax(0, 1fr);
    align-items: start;
    gap: 0.7rem;
  }

  .map-container :global(.map-popup-row-label) {
    color: var(--color-text-muted);
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .map-container :global(.map-popup-row-value) {
    font-size: 0.96rem;
    line-height: 1.35;
    text-align: left;
  }

  .map-container :global(.map-popup-table) {
    width: 100%;
    table-layout: fixed;
    border-collapse: separate;
    border-spacing: 0.38rem 0;
  }

  .map-container :global(.map-popup-table th),
  .map-container :global(.map-popup-table td) {
    text-align: left;
    padding: 0;
    border: none;
    width: 20%;
  }

  .map-container :global(.map-popup-table th) {
    padding-bottom: 0.28rem;
    color: var(--color-text-muted);
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .map-container :global(.map-popup-table td) {
    padding: 0.42rem 0.48rem;
    border: 1px solid var(--color-border);
    border-radius: 0.6rem;
    background: var(--color-panel-strong);
    font-size: 0.88rem;
    font-weight: 700;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }

  .map-container :global(.map-popup-table.is-timestamp-table td) {
    padding: 0.34rem 0.4rem;
  }

  .map-container :global(.map-popup-value-list) {
    display: flex;
    flex-wrap: wrap;
    gap: 0.38rem;
  }

  .map-container :global(.map-popup-value-chip) {
    display: inline-block;
    max-width: 100%;
    padding: 0.32rem 0.44rem;
    border-radius: 0.5rem;
    border: 1px solid var(--color-border);
    background: var(--color-panel-strong);
    color: var(--color-text);
    font-size: 0.8rem;
    overflow-wrap: anywhere;
  }

  .map-container :global(.map-popup-cell-scroll) {
    display: block;
    width: 100%;
    max-width: 100%;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    user-select: text;
    cursor: text;
    scrollbar-width: thin;
  }

  .map-container :global(.map-popup-cell-scroll:hover),
  .map-container :global(.map-popup-cell-scroll:focus) {
    overflow-x: auto;
    overflow-y: hidden;
    text-overflow: clip;
    outline: none;
  }

  .map-container :global(.map-popup-note) {
    line-height: 1.45;
    overflow-wrap: anywhere;
  }
</style>
