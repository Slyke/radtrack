<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let combinedDatasets = $state<any[]>([]);
  let exportResult = $state<string>('');
  let errorMessage = $state<string | null>(null);
  let form = $state({
    datasetIds: [] as string[],
    combinedDatasetIds: [] as string[],
    metric: 'dose_rate',
    includeRaw: true,
    includeAggregates: false,
    applyExcludeAreas: true,
    shape: 'hexagon',
    cellSizeMeters: 250
  });

  const loadLookups = async () => {
    const [datasetResponse, combinedResponse] = await Promise.all([
      apiFetch<any>({ path: '/api/datasets' }),
      apiFetch<any>({ path: '/api/combined-datasets' })
    ]);
    datasets = datasetResponse.datasets;
    combinedDatasets = combinedResponse.combinedDatasets;
  };

  const runExport = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: '/api/export',
        method: 'POST',
        body: form,
        csrf: $sessionStore.csrf
      });
      exportResult = JSON.stringify(response.export, null, 2);
      const blob = new Blob([exportResult], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `radiacode-export-${Date.now()}.json`;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Export failed';
    }
  };

  onMount(loadLookups);
</script>

<div class="page-header">
  <div>
    <h1>Export</h1>
    <p class="muted">JSON export for raw points, aggregates, or both, using the same access and exclusion rules as the map.</p>
  </div>
</div>

<section class="grid cols-2">
  <article class="panel">
    <h2>Export Request</h2>
    <div class="form-grid">
      <select bind:value={form.datasetIds} multiple size="8">
        {#each datasets as dataset}
          <option value={dataset.id}>{dataset.name}</option>
        {/each}
      </select>
      <select bind:value={form.combinedDatasetIds} multiple size="4">
        {#each combinedDatasets as combinedDataset}
          <option value={combinedDataset.id}>{combinedDataset.name}</option>
        {/each}
      </select>
      <select bind:value={form.metric}>
        <option value="dose_rate">Dose rate</option>
        <option value="count_rate">Count rate</option>
        <option value="accuracy">Accuracy</option>
      </select>
      <label class="chip-row">
        <input bind:checked={form.includeRaw} type="checkbox" />
        Raw points
      </label>
      <label class="chip-row">
        <input bind:checked={form.includeAggregates} type="checkbox" />
        Aggregates
      </label>
      <label class="chip-row">
        <input bind:checked={form.applyExcludeAreas} type="checkbox" />
        Apply exclude areas
      </label>
      <select bind:value={form.shape}>
        <option value="hexagon">Hexagon</option>
        <option value="square">Square</option>
        <option value="circle">Circle</option>
      </select>
      <input bind:value={form.cellSizeMeters} type="number" />
      <button class="primary" onclick={runExport}>Run export</button>
      {#if errorMessage}
        <p class="muted">{errorMessage}</p>
      {/if}
    </div>
  </article>

  <article class="panel">
    <h2>Preview</h2>
    <textarea readonly value={exportResult}></textarea>
  </article>
</section>
