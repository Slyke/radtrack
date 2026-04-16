<svelte:options runes={true} />

<script lang="ts">
  import { createEventDispatcher, onDestroy, onMount } from 'svelte';
  import 'leaflet/dist/leaflet.css';

  type MapPoint = {
    id: string;
    datasetId: string;
    latitude: number;
    longitude: number;
    doseRate: number | null;
    countRate: number | null;
    accuracy: number | null;
    comment: string | null;
    occurredAt: string | null;
  };

  type AggregateCell = {
    id: string;
    center: {
      latitude: number;
      longitude: number;
    };
    radiusMeters: number;
    stats: Record<string, number | null>;
  };

  interface Props {
    points?: MapPoint[];
    aggregates?: AggregateCell[];
    mode?: 'raw' | 'aggregate';
    shape?: 'hexagon' | 'square' | 'circle';
    aggregateStat?: string;
    tileUrlTemplate?: string;
    attribution?: string;
  }

  const {
    points = [],
    aggregates = [],
    mode = 'aggregate',
    shape = 'hexagon',
    aggregateStat = 'mean',
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

  let container: HTMLDivElement;
  let map: import('leaflet').Map | null = null;
  let leaflet: typeof import('leaflet') | null = null;
  let overlayLayer: import('leaflet').LayerGroup | null = null;
  let tileLayer: import('leaflet').TileLayer | null = null;

  const metersPerDegreeLatitude = 111320;
  const metersPerDegreeLongitude = (latitude: number) => Math.cos((latitude * Math.PI) / 180) * 111320;

  const colorScale = ({ value, min, max }: { value: number; min: number; max: number }) => {
    if (max <= min) {
      return '#00b6ff';
    }

    const ratio = Math.max(0, Math.min(1, (value - min) / (max - min)));
    if (ratio < 0.35) {
      return '#00b6ff';
    }
    if (ratio < 0.7) {
      return '#ffbc3a';
    }
    return '#ff1f5a';
  };

  const buildHexPoints = ({
    center,
    radiusMeters
  }: {
    center: { latitude: number; longitude: number };
    radiusMeters: number;
  }) => {
    const points = [];
    for (let index = 0; index < 6; index += 1) {
      const angle = ((60 * index) - 30) * (Math.PI / 180);
      const dx = Math.cos(angle) * radiusMeters;
      const dy = Math.sin(angle) * radiusMeters;
      points.push([
        center.latitude + (dy / metersPerDegreeLatitude),
        center.longitude + (dx / metersPerDegreeLongitude(center.latitude))
      ]);
    }
    return points as [number, number][];
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

  const render = () => {
    if (!map || !leaflet || !overlayLayer) {
      return;
    }

    overlayLayer.clearLayers();

    if (mode === 'raw') {
      for (const point of points) {
        const marker = leaflet.circleMarker([point.latitude, point.longitude], {
          radius: 5,
          color: '#00b6ff',
          weight: 1,
          fillColor: '#00b6ff',
          fillOpacity: 0.55
        });
        marker.bindPopup(`
          <strong>${point.occurredAt ?? 'Unknown time'}</strong><br />
          dose: ${point.doseRate ?? 'n/a'}<br />
          count: ${point.countRate ?? 'n/a'}<br />
          accuracy: ${point.accuracy ?? 'n/a'}<br />
          ${point.comment ? `comment: ${point.comment}` : ''}
        `);
        marker.on('click', () => {
          dispatch('selectpoint', point);
        });
        overlayLayer.addLayer(marker);
      }
      return;
    }

    const values = aggregates
      .map((cell) => cell.stats?.[aggregateStat])
      .filter((value): value is number => typeof value === 'number');
    const min = values.length ? Math.min(...values) : 0;
    const max = values.length ? Math.max(...values) : 1;

    for (const cell of aggregates) {
      const metricValue = cell.stats?.[aggregateStat];
      const fill = typeof metricValue === 'number'
        ? colorScale({ value: metricValue, min, max })
        : '#3a4658';

      if (shape === 'circle') {
        const layer = leaflet.circle([cell.center.latitude, cell.center.longitude], {
          radius: cell.radiusMeters,
          color: fill,
          fillColor: fill,
          fillOpacity: 0.35,
          weight: 1
        });
        layer.bindPopup(`count: ${cell.stats?.count ?? 0}<br />${aggregateStat}: ${metricValue ?? 'n/a'}`);
        overlayLayer.addLayer(layer);
        continue;
      }

      if (shape === 'square') {
        const latOffset = cell.radiusMeters / metersPerDegreeLatitude;
        const lonOffset = cell.radiusMeters / metersPerDegreeLongitude(cell.center.latitude);
        const layer = leaflet.rectangle(
          [
            [cell.center.latitude - latOffset, cell.center.longitude - lonOffset],
            [cell.center.latitude + latOffset, cell.center.longitude + lonOffset]
          ],
          {
            color: fill,
            fillColor: fill,
            fillOpacity: 0.28,
            weight: 1
          }
        );
        layer.bindPopup(`count: ${cell.stats?.count ?? 0}<br />${aggregateStat}: ${metricValue ?? 'n/a'}`);
        overlayLayer.addLayer(layer);
        continue;
      }

      const layer = leaflet.polygon(buildHexPoints({
        center: cell.center,
        radiusMeters: cell.radiusMeters
      }), {
        color: fill,
        fillColor: fill,
        fillOpacity: 0.28,
        weight: 1
      });
      layer.bindPopup(`count: ${cell.stats?.count ?? 0}<br />${aggregateStat}: ${metricValue ?? 'n/a'}`);
      overlayLayer.addLayer(layer);
    }
  };

  onMount(async () => {
    leaflet = await import('leaflet');
    map = leaflet.map(container, {
      zoomControl: true
    }).setView([41.9, -87.7], 9);

    tileLayer = leaflet.tileLayer(tileUrlTemplate, {
      attribution
    }).addTo(map);
    overlayLayer = leaflet.layerGroup().addTo(map);

    map.on('moveend', emitViewport);
    emitViewport();
    render();
  });

  onDestroy(() => {
    map?.remove();
  });

  $effect(() => {
    if (tileLayer && tileUrlTemplate) {
      tileLayer.setUrl(tileUrlTemplate);
    }
    render();
  });
</script>

<div bind:this={container} class="map-container"></div>
