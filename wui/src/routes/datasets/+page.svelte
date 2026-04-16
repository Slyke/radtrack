<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let createForm = $state({
    name: '',
    description: ''
  });

  const loadDatasets = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/datasets' });
      datasets = response.datasets;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load datasets';
    }
  };

  const createDataset = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: '/api/datasets',
        method: 'POST',
        body: createForm,
        csrf: $sessionStore.csrf
      });
      createForm = { name: '', description: '' };
      await loadDatasets();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to create dataset';
    }
  };

  onMount(loadDatasets);
</script>

<div class="page-header">
  <div>
    <h1>Datasets</h1>
    <p class="muted">Owned, shared, and imported datasets.</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>Create Empty Dataset</h2>
    <div class="form-grid">
      <input bind:value={createForm.name} placeholder="Dataset name" />
      <textarea bind:value={createForm.description} placeholder="Description"></textarea>
      <div class="actions">
        <button class="primary" onclick={createDataset}>Create</button>
      </div>
    </div>
  </article>

  <article class="panel">
    <h2>Accessible Datasets</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Access</th>
            <th>Tracks</th>
            <th>Readings</th>
          </tr>
        </thead>
        <tbody>
          {#each datasets as dataset}
            <tr>
              <td><a href={`/datasets/${dataset.id}`}>{dataset.name}</a></td>
              <td>{dataset.accessLevel}</td>
              <td>{dataset.trackCount}</td>
              <td>{dataset.readingCount}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </article>
</section>
