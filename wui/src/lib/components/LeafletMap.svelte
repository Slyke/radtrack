<svelte:options runes={true} />

<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import 'leaflet/dist/leaflet.css';
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
    time: boolean;
    count: boolean;
    doseRate: boolean;
    countRate: boolean;
    accuracy: boolean;
    temperatureC: boolean;
  };

  type ColorScale = {
    low: number;
    mid: number;
    high: number;
  };

  type MapPoint = {
    id: string;
    datasetId: string;
    latitude: number;
    longitude: number;
    doseRate: number | null;
    countRate: number | null;
    accuracy: number | null;
    temperatureC?: number | null;
    comment: string | null;
    occurredAt: string | null;
    receivedAt?: string | null;
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
    metrics: {
      doseRate: AggregateStats;
      countRate: AggregateStats;
      accuracy: AggregateStats;
      temperatureC: AggregateStats;
    };
  };

  interface Props {
    points?: MapPoint[];
    aggregates?: AggregateCell[];
    mode?: 'raw' | 'aggregate';
    shape?: 'hexagon' | 'square' | 'circle';
    aggregateStat?: string;
    colorScale?: ColorScale | null;
    popupFields?: PopupFields;
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
      time: true,
      count: true,
      doseRate: true,
      countRate: true,
      accuracy: true,
      temperatureC: true
    },
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
    maximumFractionDigits: 4
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
  const overlayPane = 'data-overlay';

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
    stats
  }: {
    stats: AggregateStats;
  }) => {
    if (!stats.count) {
      return null;
    }

    return [
      { label: t('radtrack-common_mean-label'), value: formatNumber(stats.mean) },
      { label: t('radtrack-common_min-label'), value: formatNumber(stats.min) },
      { label: t('radtrack-common_max-label'), value: formatNumber(stats.max) },
      { label: t('radtrack-common_median-label'), value: formatNumber(stats.median) },
      { label: t('radtrack-common_mode-label'), value: formatNumber(stats.mode) }
    ];
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
    rows
  }: {
    rows: Array<{ label: string; value: string }>;
  }) => `
    <table class="map-popup-table">
      <thead>
        <tr>
          ${rows.map((row) => `<th>${escapeHtml(row.label)}</th>`).join('')}
        </tr>
      </thead>
      <tbody>
        <tr>
          ${rows.map((row) => `<td>${escapeHtml(row.value)}</td>`).join('')}
        </tr>
      </tbody>
    </table>
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

  const buildRawPopupHtml = ({ point }: { point: MapPoint }) => {
    const rows: string[] = [];

    if (popupFields.time) {
      rows.push(buildPopupDataRowHtml({
        label: t('radtrack-common_time-label'),
        value: formatTimestamp(point.occurredAt ?? point.receivedAt)
      }));
    }

    if (popupFields.doseRate && point.doseRate !== null && point.doseRate !== undefined) {
      rows.push(buildPopupDataRowHtml({
        label: t('radtrack-common_dose_rate-label'),
        value: formatNumber(point.doseRate)
      }));
    }

    if (popupFields.countRate && point.countRate !== null && point.countRate !== undefined) {
      rows.push(buildPopupDataRowHtml({
        label: t('radtrack-common_count_rate-label'),
        value: formatNumber(point.countRate)
      }));
    }

    if (popupFields.accuracy && point.accuracy !== null && point.accuracy !== undefined) {
      rows.push(buildPopupDataRowHtml({
        label: t('radtrack-common_accuracy-label'),
        value: formatNumber(point.accuracy)
      }));
    }

    if (popupFields.temperatureC && point.temperatureC !== null && point.temperatureC !== undefined) {
      rows.push(buildPopupDataRowHtml({
        label: t('radtrack-common_temperature-label'),
        value: formatNumber(point.temperatureC)
      }));
    }

    return `
      <div class="map-popup">
        ${buildPopupHeaderHtml({
          title: t('radtrack-map_popup_reading-title')
        })}
        ${rows.length ? `<div class="map-popup-data-list">${rows.join('')}</div>` : ''}
        ${point.comment ? `
          <section class="map-popup-section">
            <div class="map-popup-section-title">${escapeHtml(t('radtrack-common_comment-label'))}</div>
            <div class="map-popup-note">${escapeHtml(point.comment)}</div>
          </section>
        ` : ''}
      </div>
    `;
  };

  const buildAggregatePopupHtml = ({ cell }: { cell: AggregateCell }) => {
    const sections: string[] = [];
    const badge = popupFields.count
      ? `${formatNumber(cell.pointCount)} ${t('radtrack-map_popup_points-label')}`
      : null;
    const timeRange = popupFields.time
      ? buildAggregateTimeRange({ timeRange: cell.timeRange })
      : null;

    if (popupFields.doseRate) {
      const rows = buildAggregateMetricRows({ stats: cell.metrics.doseRate });
      if (rows) {
        sections.push(buildPopupSectionHtml({
          title: t('radtrack-common_dose_rate-label'),
          body: buildPopupMetricTableHtml({ rows })
        }));
      }
    }

    if (popupFields.countRate) {
      const rows = buildAggregateMetricRows({ stats: cell.metrics.countRate });
      if (rows) {
        sections.push(buildPopupSectionHtml({
          title: t('radtrack-common_count_rate-label'),
          body: buildPopupMetricTableHtml({ rows })
        }));
      }
    }

    if (popupFields.accuracy) {
      const rows = buildAggregateMetricRows({ stats: cell.metrics.accuracy });
      if (rows) {
        sections.push(buildPopupSectionHtml({
          title: t('radtrack-common_accuracy-label'),
          body: buildPopupMetricTableHtml({ rows })
        }));
      }
    }

    if (popupFields.temperatureC) {
      const rows = buildAggregateMetricRows({ stats: cell.metrics.temperatureC });
      if (rows) {
        sections.push(buildPopupSectionHtml({
          title: t('radtrack-common_temperature-label'),
          body: buildPopupMetricTableHtml({ rows })
        }));
      }
    }

    return `
      <div class="map-popup">
        ${buildPopupHeaderHtml({
          title: t('radtrack-map_popup_aggregate-title'),
          subtitle: timeRange,
          badge
        })}
        ${sections.join('')}
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

    overlayLayer.clearLayers();
    const popupOptions = {
      autoPan: false,
      closeButton: true,
      maxWidth: 520,
      minWidth: 380
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
          marker.on('click', () => {
            dispatch('selectpoint', point);
          });
          overlayLayer.addLayer(marker);
        }
      }
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
        overlayLayer.addLayer(layer);
      }
    }
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
      map?.invalidateSize();
    });
    resizeObserver.observe(container);

    map.on('moveend', handleMoveEnd);
    requestAnimationFrame(() => {
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
    min-width: 22rem;
    max-width: 32rem;
    padding: 1rem 1.05rem;
  }

  .map-container :global(.leaflet-container a.leaflet-popup-close-button) {
    color: var(--color-text-muted);
    padding: 0.55rem 0.7rem 0 0;
  }

  .map-container :global(.map-popup) {
    display: grid;
    gap: 0.7rem;
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
    border-spacing: 0.32rem 0;
  }

  .map-container :global(.map-popup-table th),
  .map-container :global(.map-popup-table td) {
    text-align: left;
    padding: 0;
    border: none;
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
    font-size: 0.92rem;
    font-weight: 700;
  }

  .map-container :global(.map-popup-note) {
    line-height: 1.45;
    overflow-wrap: anywhere;
  }
</style>
