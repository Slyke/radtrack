<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';

  let data = $state<any>(null);
  let errorMessage = $state<string | null>(null);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radiacode-common_none');

  const loadDashboard = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/dashboard' });
      data = response;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dashboard_failed');
    }
  };

  onMount(loadDashboard);
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-dashboard_title')}</h1>
    <p class="muted">{t('radiacode-dashboard_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{:else if !data}
  <section class="panel">
    <p class="muted">{t('radiacode-common_loading_dashboard')}</p>
  </section>
{:else}
  <section class="grid cols-3">
    <article class="metric-card">
      <div class="faint">{t('radiacode-datasets_title')}</div>
      <h2>{data.stats.datasetCount}</h2>
    </article>
    <article class="metric-card">
      <div class="faint">{t('radiacode-dashboard_recent_uploads-title')}</div>
      <h2>{data.stats.uploadCount}</h2>
    </article>
    <article class="metric-card">
      <div class="faint">{t('radiacode-dashboard_failed_imports-label')}</div>
      <h2>{data.stats.failedImportCount}</h2>
    </article>
  </section>

  <section class="grid cols-2">
    <article class="panel">
      <div class="page-header">
        <h2>{t('radiacode-dashboard_recent_datasets-title')}</h2>
        <a href="/datasets">{t('radiacode-common_open-button')}</a>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('radiacode-common_name-label')}</th>
              <th>{t('radiacode-common_access-label')}</th>
              <th>{t('radiacode-common_tracks-label')}</th>
            </tr>
          </thead>
          <tbody>
            {#each data.datasets as dataset}
              <tr>
                <td><a href={`/datasets/${dataset.id}`}>{dataset.name}</a></td>
                <td>{dataset.accessLevel}</td>
                <td>{dataset.trackCount}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>

    <article class="panel">
      <div class="page-header">
        <h2>{t('radiacode-dashboard_recent_uploads-title')}</h2>
        <a href="/import">{t('radiacode-common_import-button')}</a>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('radiacode-common_file-label')}</th>
              <th>{t('radiacode-common_status-label')}</th>
              <th>{t('radiacode-common_time-label')}</th>
            </tr>
          </thead>
          <tbody>
            {#each data.uploads as upload}
              <tr>
                <td>{upload.originalFilename}</td>
                <td>{upload.status}</td>
                <td>{formatTime(upload.uploadedAt)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
  </section>

  <section class="panel">
    <div class="page-header">
      <h2>{t('radiacode-audit_title')}</h2>
      <a href="/audit">{t('radiacode-common_open-button')}</a>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_time-label')}</th>
            <th>{t('radiacode-common_event-label')}</th>
            <th>{t('radiacode-common_entity-label')}</th>
          </tr>
        </thead>
        <tbody>
          {#each data.audit as event}
            <tr>
              <td>{formatTime(event.createdAt)}</td>
              <td>{event.eventType}</td>
              <td>{event.entityType}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
{/if}
