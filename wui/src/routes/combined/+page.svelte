<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let combinedDatasets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let form = $state({
    name: '',
    description: '',
    datasetIds: [] as string[]
  });

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
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
      errorMessage = error instanceof Error ? error.message : t('radtrack-combined_failed');
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
    <h1>{t('radtrack-combined_title')}</h1>
    <p class="muted">{t('radtrack-combined_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>{t('radtrack-combined_create-title')}</h2>
    <div class="form-grid">
      <input bind:value={form.name} placeholder={t('radtrack-common_name-label')} />
      <textarea bind:value={form.description} placeholder={t('radtrack-common_description-label')}></textarea>
      <select bind:value={form.datasetIds} multiple size="10">
        {#each datasets as dataset}
          <option value={dataset.id}>{dataset.name}</option>
        {/each}
      </select>
      <button class="primary" onclick={createCombinedDataset}>{t('radtrack-combined_create-button')}</button>
    </div>
  </article>

  <article class="panel">
    <h2>{t('radtrack-combined_yours-title')}</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radtrack-common_name-label')}</th>
            <th>{t('radtrack-common_description-label')}</th>
          </tr>
        </thead>
        <tbody>
          {#each combinedDatasets as combinedDataset}
            <tr>
              <td>{combinedDataset.name}</td>
              <td>{combinedDataset.description || t('radtrack-common_no-description')}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </article>
</section>
