<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import InfoTip from '$lib/components/InfoTip.svelte';
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
    <div class="title-with-info">
      <h1>{t('radtrack-combined_title')}</h1>
      <InfoTip text="Create virtual collections that let several datasets be queried together without copying their tracks." />
    </div>
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
    <div class="title-with-info">
      <h2>{t('radtrack-combined_create-title')}</h2>
      <InfoTip text="Define a named virtual dataset from two or more datasets you can access." />
    </div>
    <div class="form-grid">
      <label>
        <span class="muted field-title">
          {t('radtrack-common_name-label')}
          <InfoTip text="A recognizable name for this combined view." />
        </span>
        <input bind:value={form.name} placeholder={t('radtrack-common_name-label')} />
      </label>
      <label>
        <span class="muted field-title">
          {t('radtrack-common_description-label')}
          <InfoTip text="An optional note describing why these datasets are grouped." />
        </span>
        <textarea bind:value={form.description} placeholder={t('radtrack-common_description-label')}></textarea>
      </label>
      <label>
        <span class="muted field-title">
          {t('radtrack-datasets_title')}
          <InfoTip text="Select the source datasets to query as one combined collection. Use Ctrl or Command to select multiple entries." />
        </span>
        <select bind:value={form.datasetIds} multiple size="10">
          {#each datasets as dataset}
            <option value={dataset.id}>{dataset.name}</option>
          {/each}
        </select>
      </label>
      <div class="control-with-info">
        <button class="primary" onclick={createCombinedDataset}>{t('radtrack-combined_create-button')}</button>
        <InfoTip text="Save this virtual grouping. The original datasets and tracks remain unchanged." />
      </div>
    </div>
  </article>

  <article class="panel">
    <div class="title-with-info">
      <h2>{t('radtrack-combined_yours-title')}</h2>
      <InfoTip text="Combined datasets owned by your account and available for map or export selection." />
    </div>
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
