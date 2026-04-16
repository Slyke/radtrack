<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let combinedDatasets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let form = $state({
    name: '',
    description: '',
    datasetIds: [] as string[]
  });

  const loadData = async () => {
    try {
      const [datasetResponse, combinedResponse] = await Promise.all([
        apiFetch<any>({ path: '/api/datasets' }),
        apiFetch<any>({ path: '/api/combined-datasets' })
      ]);
      datasets = datasetResponse.datasets;
      combinedDatasets = combinedResponse.combinedDatasets;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load combined datasets';
    }
  };

  const createCombinedDataset = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: '/api/combined-datasets',
      method: 'POST',
      body: form,
      csrf: $sessionStore.csrf
    });
    form = { name: '', description: '', datasetIds: [] };
    await loadData();
  };

  onMount(loadData);
</script>

<div class="page-header">
  <div>
    <h1>Combined Datasets</h1>
    <p class="muted">Virtual combined views over datasets you can already access.</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>Create Combined Dataset</h2>
    <div class="form-grid">
      <input bind:value={form.name} placeholder="Name" />
      <textarea bind:value={form.description} placeholder="Description"></textarea>
      <select bind:value={form.datasetIds} multiple size="10">
        {#each datasets as dataset}
          <option value={dataset.id}>{dataset.name}</option>
        {/each}
      </select>
      <button class="primary" onclick={createCombinedDataset}>Create combined view</button>
    </div>
  </article>

  <article class="panel">
    <h2>Your Combined Datasets</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          {#each combinedDatasets as combinedDataset}
            <tr>
              <td>{combinedDataset.name}</td>
              <td>{combinedDataset.description || 'No description'}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </article>
</section>
