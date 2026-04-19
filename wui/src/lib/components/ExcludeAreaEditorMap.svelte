<svelte:options runes={true} />

<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import 'leaflet/dist/leaflet.css';
  import { localeStore, translateMessage } from '$lib/i18n';

  type Coordinate = {
    latitude: number;
    longitude: number;
  };

  type ExcludeArea = {
    id: string;
    shapeType: 'polygon' | 'circle';
    geometry: {
      points?: Coordinate[];
      center?: Coordinate;
      radiusMeters?: number;
    };
    label: string | null;
    applyByDefaultOnExport: boolean;
  };

  interface Props {
    areas?: ExcludeArea[];
    draftPolygonPoints?: Coordinate[];
    draftCircleCenter?: Coordinate | null;
    draftCircleRadiusMeters?: number;
    fallbackCenter?: Coordinate | null;
    mode?: 'polygon' | 'circle';
    tileUrlTemplate?: string;
    attribution?: string;
  }

  const defaultTileUrlTemplate = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  const defaultAttribution = '&copy; OpenStreetMap contributors';

  let {
    areas = [],
    draftPolygonPoints = [],
    draftCircleCenter = null,
    draftCircleRadiusMeters = 250,
    fallbackCenter = null,
    mode = 'polygon',
    tileUrlTemplate = defaultTileUrlTemplate,
    attribution = defaultAttribution
  }: Props = $props();

  const dispatch = createEventDispatcher<{
    mapclick: Coordinate;
  }>();

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const getShapeLabel = ({ shapeType }: { shapeType: 'polygon' | 'circle' }) => (
    shapeType === 'circle'
      ? t('radtrack-common_circle-label')
      : t('radtrack-common_polygon-label')
  );

  const escapeHtml = (value: string) => value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  let container: HTMLDivElement;
  let map: import('leaflet').Map | null = null;
  let leaflet: typeof import('leaflet') | null = null;
  let tileLayer: import('leaflet').TileLayer | null = null;
  let areaLayer: import('leaflet').LayerGroup | null = null;
  let draftLayer: import('leaflet').LayerGroup | null = null;
  let resizeObserver: ResizeObserver | null = null;
  let activeTileLayerSignature = '';
  let didAutoFit = false;

  const ensureTileLayer = ({
    urlTemplate,
    attributionText
  }: {
    urlTemplate: string;
    attributionText: string;
  }) => {
    if (!map || !leaflet) {
      return;
    }

    const nextUrlTemplate = urlTemplate || defaultTileUrlTemplate;
    const nextAttribution = attributionText || defaultAttribution;
    const signature = `${nextUrlTemplate}::${nextAttribution}`;

    if (tileLayer && activeTileLayerSignature === signature) {
      return;
    }

    if (tileLayer) {
      map.removeLayer(tileLayer);
    }

    tileLayer = leaflet.tileLayer(nextUrlTemplate, {
      attribution: nextAttribution
    }).addTo(map);
    activeTileLayerSignature = signature;
  };

  const buildAreaPopup = ({ area }: { area: ExcludeArea }) => {
    const title = area.label || t('radtrack-common_unnamed-label');
    const exportDefault = area.applyByDefaultOnExport
      ? t('radtrack-common_yes-label')
      : t('radtrack-common_no-label');

    return `
      <div class="exclude-area-popup">
        <strong>${escapeHtml(title)}</strong>
        <div>${escapeHtml(getShapeLabel({ shapeType: area.shapeType }))}</div>
        <div>${escapeHtml(t('radtrack-common_export_default-label'))}: ${escapeHtml(exportDefault)}</div>
      </div>
    `;
  };

  const toLatLngTuple = ({ latitude, longitude }: Coordinate) => [latitude, longitude] as [number, number];

  const collectOverlayPoints = () => {
    const points: Coordinate[] = [];

    for (const area of areas) {
      if (area.shapeType === 'polygon') {
        points.push(...(area.geometry.points ?? []));
        continue;
      }

      if (!area.geometry.center) {
        continue;
      }

      points.push(area.geometry.center);
    }

    points.push(...draftPolygonPoints);

    if (draftCircleCenter) {
      points.push(draftCircleCenter);
    }

    return points;
  };

  const maybeFitBounds = () => {
    if (!map || !leaflet || didAutoFit || !container || container.clientWidth === 0 || container.clientHeight === 0) {
      return;
    }

    const overlayPoints = collectOverlayPoints();
    if (!overlayPoints.length) {
      if (fallbackCenter) {
        map.setView([fallbackCenter.latitude, fallbackCenter.longitude], 13);
        didAutoFit = true;
      }
      return;
    }

    if (overlayPoints.length === 1) {
      const [point] = overlayPoints;
      map.setView([point.latitude, point.longitude], 13);
      didAutoFit = true;
      return;
    }

    const bounds = leaflet.latLngBounds(overlayPoints.map(toLatLngTuple));
    map.fitBounds(bounds.pad(0.2));
    didAutoFit = true;
  };

  const renderAreas = () => {
    if (!leaflet || !areaLayer) {
      return;
    }

    areaLayer.clearLayers();

    for (const area of areas) {
      if (area.shapeType === 'polygon' && area.geometry.points?.length) {
        const layer = leaflet.polygon(area.geometry.points.map(toLatLngTuple), {
          bubblingMouseEvents: false,
          color: '#ffbc3a',
          fillColor: '#ffbc3a',
          fillOpacity: 0.12,
          opacity: 0.95,
          weight: 2
        });
        layer.bindPopup(buildAreaPopup({ area }));
        areaLayer.addLayer(layer);
        continue;
      }

      if (area.shapeType === 'circle' && area.geometry.center && area.geometry.radiusMeters) {
        const layer = leaflet.circle(toLatLngTuple(area.geometry.center), {
          radius: area.geometry.radiusMeters,
          bubblingMouseEvents: false,
          color: '#ffbc3a',
          fillColor: '#ffbc3a',
          fillOpacity: 0.12,
          opacity: 0.95,
          weight: 2
        });
        layer.bindPopup(buildAreaPopup({ area }));
        areaLayer.addLayer(layer);
      }
    }
  };

  const renderDraft = () => {
    if (!leaflet || !draftLayer) {
      return;
    }

    draftLayer.clearLayers();

    for (const point of draftPolygonPoints) {
      draftLayer.addLayer(leaflet.circleMarker(toLatLngTuple(point), {
        bubblingMouseEvents: false,
        radius: 5,
        color: '#00b6ff',
        fillColor: '#00b6ff',
        fillOpacity: 1,
        weight: 2
      }));
    }

    if (draftPolygonPoints.length === 2) {
      draftLayer.addLayer(leaflet.polyline(draftPolygonPoints.map(toLatLngTuple), {
        bubblingMouseEvents: false,
        color: '#00b6ff',
        opacity: 0.95,
        weight: 2,
        dashArray: '6 4'
      }));
    }

    if (draftPolygonPoints.length >= 3) {
      draftLayer.addLayer(leaflet.polygon(draftPolygonPoints.map(toLatLngTuple), {
        bubblingMouseEvents: false,
        color: '#00b6ff',
        fillColor: '#00b6ff',
        fillOpacity: 0.1,
        opacity: 0.95,
        weight: 2,
        dashArray: '6 4'
      }));
    }

    if (draftCircleCenter) {
      draftLayer.addLayer(leaflet.circleMarker(toLatLngTuple(draftCircleCenter), {
        bubblingMouseEvents: false,
        radius: 5,
        color: '#00b6ff',
        fillColor: '#00b6ff',
        fillOpacity: 1,
        weight: 2
      }));

      draftLayer.addLayer(leaflet.circle(toLatLngTuple(draftCircleCenter), {
        radius: Math.max(1, Number(draftCircleRadiusMeters) || 250),
        bubblingMouseEvents: false,
        color: '#00b6ff',
        fillColor: '#00b6ff',
        fillOpacity: 0.1,
        opacity: 0.95,
        weight: 2,
        dashArray: '6 4'
      }));
    }
  };

  const render = () => {
    renderAreas();
    renderDraft();
    maybeFitBounds();
  };

  onMount(async () => {
    leaflet = await import('leaflet');
    map = leaflet.map(container, {
      zoomControl: true
    }).setView([41.9, -87.7], 9);

    ensureTileLayer({
      urlTemplate: tileUrlTemplate,
      attributionText: attribution
    });

    areaLayer = leaflet.layerGroup().addTo(map);
    draftLayer = leaflet.layerGroup().addTo(map);
    resizeObserver = new ResizeObserver(() => {
      map?.invalidateSize();
      maybeFitBounds();
    });
    resizeObserver.observe(container);

    map.on('click', (event) => {
      dispatch('mapclick', {
        latitude: event.latlng.lat,
        longitude: event.latlng.lng
      });
    });

    requestAnimationFrame(() => {
      map?.invalidateSize();
      render();
    });
  });

  onDestroy(() => {
    resizeObserver?.disconnect();
    map?.remove();
  });

  $effect(() => {
    tileUrlTemplate;
    attribution;
    mode;
    areas;
    draftPolygonPoints;
    draftCircleCenter;
    draftCircleRadiusMeters;
    fallbackCenter;

    ensureTileLayer({
      urlTemplate: tileUrlTemplate,
      attributionText: attribution
    });

    render();
  });
</script>

<div bind:this={container} class="exclude-area-map"></div>

<style>
  .exclude-area-map {
    min-height: 24rem;
    height: 100%;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    overflow: hidden;
    background: var(--color-panel);
  }

  .exclude-area-map :global(.leaflet-container) {
    width: 100%;
    height: 100%;
    background: var(--color-panel);
  }

  .exclude-area-map :global(.leaflet-popup-content-wrapper),
  .exclude-area-map :global(.leaflet-popup-tip) {
    background: var(--color-panel);
    color: var(--color-text);
    border: 1px solid var(--color-border);
  }

  .exclude-area-map :global(.leaflet-popup-content) {
    margin: 0;
    padding: 0.8rem 0.9rem;
  }

  .exclude-area-map :global(.leaflet-container a.leaflet-popup-close-button) {
    color: var(--color-text-muted);
  }

  .exclude-area-map :global(.leaflet-container a.leaflet-popup-close-button:hover) {
    color: var(--color-text);
  }

  .exclude-area-map :global(.leaflet-control-attribution) {
    background: color-mix(in srgb, var(--color-panel) 88%, transparent);
    color: var(--color-text-muted);
  }

  .exclude-area-map :global(.leaflet-control-attribution a) {
    color: var(--color-start);
  }

  .exclude-area-map :global(.leaflet-bar a),
  .exclude-area-map :global(.leaflet-bar a:hover) {
    background: var(--color-panel);
    color: var(--color-text);
    border-bottom-color: var(--color-border);
  }

  .exclude-area-map :global(.leaflet-bar) {
    border: 1px solid var(--color-border);
  }

  .exclude-area-map :global(.leaflet-interactive) {
    cursor: crosshair;
  }

  @media (max-width: 1024px) {
    .exclude-area-map {
      min-height: 20rem;
    }
  }
</style>
