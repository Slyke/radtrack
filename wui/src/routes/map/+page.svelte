<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import LeafletMap from '$lib/components/LeafletMap.svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let combinedDatasets = $state<any[]>([]);
  let points = $state<any[]>([]);
  let aggregates = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let loading = $state(false);
  let selectedPoint = $state<any>(null);
  let viewport = $state({
    minLat: 41.7,
    maxLat: 42.1,
    minLon: -88.0,
    maxLon: -87.5,
    zoom: 9
  });
  let filters = $state({
    datasetIds: [] as string[],
    combinedDatasetIds: [] as string[],
    metric: 'dose_rate',
    mode: 'aggregate',
    shape: 'hexagon',
    aggregateStat: 'mean',
    cellSizeMeters: 250,
    applyExcludeAreas: true
  });

  const loadLookups = async () => {
    try {
      const [datasetResponse, combinedResponse] = await Promise.all([
        apiFetch<any>({ path: '/api/datasets' }),
        apiFetch<any>({ path: '/api/combined-datasets' })
      ]);
      datasets = datasetResponse.datasets;
      combinedDatasets = combinedResponse.combinedDatasets;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load selectors';
    }
  };

  const loadMapData = async () => {
    loading = true;
    errorMessage = null;
    selectedPoint = null;

    const query = {
      datasetIds: filters.datasetIds,
      combinedDatasetIds: filters.combinedDatasetIds,
      metric: filters.metric,
      minLat: viewport.minLat,
      maxLat: viewport.maxLat,
      minLon: viewport.minLon,
      maxLon: viewport.maxLon,
      applyExcludeAreas: filters.applyExcludeAreas
    };

    try {
      if (filters.mode === 'raw') {
        const response = await apiFetch<any>({
          path: '/api/map/points',
          query
        });
        points = response.result.points;
        aggregates = [];
        return;
      }

      const response = await apiFetch<any>({
        path: '/api/map/aggregates',
        query: {
          ...query,
          shape: filters.shape,
          cellSizeMeters: filters.cellSizeMeters,
          zoom: viewport.zoom
        }
      });
      points = [];
      aggregates = response.result.cells;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load map data';
    } finally {
      loading = false;
    }
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
        reason: 'Hidden from map explorer'
      },
      csrf: $sessionStore.csrf
    });
    await loadMapData();
  };

  onMount(async () => {
    await loadLookups();
    await loadMapData();
  });

  $effect(() => {
    if (!datasets.length && !combinedDatasets.length) {
      return;
    }

    loadMapData();
  });
</script>

<div class="page-header">
  <div>
    <h1>Map Explorer</h1>
    <p class="muted">Viewport-driven raw point and aggregate queries with dataset, time, metric, and exclusion filters.</p>
  </div>
  {#if loading}
    <span class="chip warning">Loading</span>
  {/if}
</div>

<div class="map-layout">
  <aside class="grid">
    <section class="panel">
      <h2>Filters</h2>
      <div class="form-grid">
        <label>
          <div class="muted">Datasets</div>
          <select bind:value={filters.datasetIds} multiple size="8">
            {#each datasets as dataset}
              <option value={dataset.id}>{dataset.name}</option>
            {/each}
          </select>
        </label>
        <label>
          <div class="muted">Combined datasets</div>
          <select bind:value={filters.combinedDatasetIds} multiple size="4">
            {#each combinedDatasets as combinedDataset}
              <option value={combinedDataset.id}>{combinedDataset.name}</option>
            {/each}
          </select>
        </label>
        <label>
          <div class="muted">Metric</div>
          <select bind:value={filters.metric}>
            <option value="dose_rate">Dose rate</option>
            <option value="count_rate">Count rate</option>
            <option value="accuracy">Accuracy</option>
          </select>
        </label>
        <label>
          <div class="muted">Mode</div>
          <select bind:value={filters.mode}>
            <option value="aggregate">Aggregate</option>
            <option value="raw">Raw points</option>
          </select>
        </label>
        {#if filters.mode === 'aggregate'}
          <label>
            <div class="muted">Shape</div>
            <select bind:value={filters.shape}>
              <option value="hexagon">Hexagon</option>
              <option value="square">Square</option>
              <option value="circle">Circle</option>
            </select>
          </label>
        {/if}
        {#if filters.mode === 'aggregate'}
          <label>
            <div class="muted">Cell size meters</div>
            <input bind:value={filters.cellSizeMeters} min="10" type="number" />
          </label>
        {/if}
        {#if filters.mode === 'aggregate'}
          <label>
            <div class="muted">Aggregate stat</div>
            <select bind:value={filters.aggregateStat}>
              <option value="min">Min</option>
              <option value="max">Max</option>
              <option value="mean">Mean</option>
              <option value="median">Median</option>
              <option value="mode">Mode</option>
              <option value="count">Count</option>
            </select>
          </label>
        {/if}
        <label>
          <div class="muted">
            <input bind:checked={filters.applyExcludeAreas} type="checkbox" />
            Apply exclude areas
          </div>
        </label>
      </div>
    </section>

    <section class="panel">
      <h2>Viewport</h2>
      <div class="grid">
        <span class="chip start">lat {viewport.minLat.toFixed(4)} .. {viewport.maxLat.toFixed(4)}</span>
        <span class="chip start">lon {viewport.minLon.toFixed(4)} .. {viewport.maxLon.toFixed(4)}</span>
        <span class="chip mid">zoom {viewport.zoom}</span>
      </div>
    </section>

    {#if selectedPoint}
      <section class="panel">
        <h2>Selected Reading</h2>
        <div class="grid">
          <span class="chip start">{selectedPoint.occurredAt ?? 'Unknown time'}</span>
          <span class="chip mid">dose {selectedPoint.doseRate ?? 'n/a'}</span>
          <span class="chip mid">count {selectedPoint.countRate ?? 'n/a'}</span>
          <span class="chip warning">accuracy {selectedPoint.accuracy ?? 'n/a'}</span>
          {#if selectedPoint.comment}
            <p class="muted">{selectedPoint.comment}</p>
          {/if}
          {#if $sessionStore.user?.role !== 'view_only'}
            <button class="danger" onclick={hideSelectedPoint}>Hide point</button>
          {/if}
        </div>
      </section>
    {/if}

    <section class="panel legend">
      <h2>Legend</h2>
      <div class="chip-row">
        <span class="chip start">Low</span>
        <span class="chip warning">Mid</span>
        <span class="chip danger">High</span>
      </div>
    </section>
  </aside>

  <section class="grid">
    {#if errorMessage}
      <article class="panel">
        <p class="muted">{errorMessage}</p>
      </article>
    {/if}

    <LeafletMap
      aggregateStat={filters.aggregateStat}
      aggregates={aggregates}
      attribution={$sessionStore.ui?.attribution ?? ''}
      mode={filters.mode as 'raw' | 'aggregate'}
      on:selectpoint={(event) => {
        selectedPoint = event.detail;
      }}
      on:viewportchange={(event) => {
        viewport = event.detail;
      }}
      points={points}
      shape={filters.shape as 'hexagon' | 'square' | 'circle'}
      tileUrlTemplate={$sessionStore.ui?.tileUrlTemplate ?? ''}
    />
  </section>
</div>
