<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let datasets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let createForm = $state({
    name: '',
    description: ''
  });

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const loadDatasets = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/datasets' });
      datasets = response.datasets;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-datasets_failed_load');
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
      errorMessage = error instanceof Error ? error.message : t('radiacode-datasets_failed_create');
    }
  };

  const deleteDataset = async ({ datasetId, datasetName }: { datasetId: string; datasetName: string }) => {
    if (!browser || !$sessionStore.csrf) {
      return;
    }

    if (!window.confirm(t('radiacode-dataset_delete_confirm', { name: datasetName }))) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadDatasets();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-datasets_failed_delete');
    }
  };

  onMount(loadDatasets);
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-datasets_title')}</h1>
    <p class="muted">{t('radiacode-datasets_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>{t('radiacode-datasets_create-title')}</h2>
    <div class="form-grid">
      <input bind:value={createForm.name} placeholder={t('radiacode-common_dataset_name-label')} />
      <textarea bind:value={createForm.description} placeholder={t('radiacode-common_description-label')}></textarea>
      <div class="actions">
        <button class="primary" onclick={createDataset}>{t('radiacode-common_create-button')}</button>
      </div>
    </div>
  </article>

  <article class="panel">
    <h2>{t('radiacode-datasets_accessible-title')}</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_name-label')}</th>
            <th>{t('radiacode-common_access-label')}</th>
            <th>{t('radiacode-common_tracks-label')}</th>
            <th>{t('radiacode-common_readings-label')}</th>
            <th>{t('radiacode-common_actions-label')}</th>
          </tr>
        </thead>
        <tbody>
          {#each datasets as dataset}
            <tr>
              <td><a href={`/datasets/${dataset.id}`}>{dataset.name}</a></td>
              <td>{dataset.accessLevel}</td>
              <td>{dataset.trackCount}</td>
              <td>{dataset.readingCount}</td>
              <td>
                {#if dataset.accessLevel === 'edit'}
                  <button class="danger" onclick={() => deleteDataset({ datasetId: dataset.id, datasetName: dataset.name })}>
                    {t('radiacode-common_danger-delete-button')}
                  </button>
                {:else}
                  <span class="muted">{t('radiacode-common_none')}</span>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </article>
</section>
