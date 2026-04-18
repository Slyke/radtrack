<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  interface NamedRecord {
    id: string;
    name: string;
  }

  interface ExportRawPayload {
    totalCount: number;
    capped: boolean;
    points: unknown[];
  }

  interface ExportAggregatePayload {
    shape: 'hexagon' | 'square' | 'circle';
    cellSizeMeters: number;
    cells: unknown[];
  }

  interface RadiacodeExportEnvelope {
    title: string;
    type: string;
    exportTime: string;
    build?: string;
    metric: string;
    filters: {
      datasetIds: string[];
      combinedDatasetIds: string[];
      dateFrom: string | null;
      dateTo: string | null;
      applyExcludeAreas: boolean;
    };
    raw: ExportRawPayload | null;
    aggregates: ExportAggregatePayload | null;
  }

  let datasets = $state<NamedRecord[]>([]);
  let combinedDatasets = $state<NamedRecord[]>([]);
  let exportEnvelope = $state<RadiacodeExportEnvelope | null>(null);
  let exportResult = $state('');
  let errorMessage = $state<string | null>(null);
  let form = $state({
    datasetIds: [] as string[],
    combinedDatasetIds: [] as string[],
    metric: 'dose_rate',
    includeRaw: true,
    includeAggregates: false,
    applyExcludeAreas: true,
    shape: 'hexagon' as 'hexagon' | 'square' | 'circle',
    cellSizeMeters: 250
  });

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radiacode-common_none');

  const formatShapeLabel = (shape: 'hexagon' | 'square' | 'circle') => {
    if (shape === 'circle') {
      return t('radiacode-common_circle-label');
    }

    if (shape === 'square') {
      return t('radiacode-common_square-label');
    }

    return t('radiacode-common_hexagon-label');
  };

  const buildExportFilename = ({ exportTime }: { exportTime: string }) => {
    if (/^\d{4}-\d{2}-\d{2}/.test(exportTime)) {
      return `radiacode-export-${exportTime.slice(0, 10)}.json`;
    }

    return `radiacode-export-${Date.now()}.json`;
  };

  const loadLookups = async () => {
    errorMessage = null;

    try {
      const [datasetResponse, combinedResponse] = await Promise.all([
        apiFetch<{ datasets: NamedRecord[] }>({ path: '/api/datasets' }),
        apiFetch<{ combinedDatasets: NamedRecord[] }>({ path: '/api/combined-datasets' })
      ]);
      datasets = datasetResponse.datasets;
      combinedDatasets = combinedResponse.combinedDatasets;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-export_failed');
    }
  };

  const runExport = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    errorMessage = null;

    try {
      const response = await apiFetch<{ export: RadiacodeExportEnvelope }>({
        path: '/api/export',
        method: 'POST',
        body: form,
        csrf: $sessionStore.csrf
      });
      exportEnvelope = response.export;
      const exportedJson = JSON.stringify(response.export, null, 2);
      exportResult = exportedJson;

      const blob = new Blob([exportedJson], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = buildExportFilename({ exportTime: response.export.exportTime });
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-export_failed');
    }
  };

  onMount(loadLookups);
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-export_title')}</h1>
    <p class="muted">{t('radiacode-export_description')}</p>
  </div>
</div>

<section class="grid cols-2 export-layout">
  <article class="panel export-panel">
    <h2>{t('radiacode-export_request-title')}</h2>

    <div class="form-grid">
      <div class="grid cols-2 export-source-grid">
        <label class="export-field">
          <span class="muted">{t('radiacode-datasets_title')}</span>
          <span class="faint export-help">{t('radiacode-export_datasets-help')}</span>
          <select bind:value={form.datasetIds} multiple size="8">
            {#each datasets as dataset}
              <option value={dataset.id}>{dataset.name}</option>
            {/each}
          </select>
        </label>

        <label class="export-field">
          <span class="muted">{t('radiacode-common_combined_datasets-label')}</span>
          <span class="faint export-help">{t('radiacode-export_combined_datasets-help')}</span>
          <select bind:value={form.combinedDatasetIds} multiple size="8">
            {#each combinedDatasets as combinedDataset}
              <option value={combinedDataset.id}>{combinedDataset.name}</option>
            {/each}
          </select>
        </label>
      </div>

      <label class="export-field">
        <span class="muted">{t('radiacode-common_metric-label')}</span>
        <span class="faint export-help">{t('radiacode-export_metric-help')}</span>
        <select bind:value={form.metric}>
          <option value="dose_rate">{t('radiacode-common_dose_rate-label')}</option>
          <option value="count_rate">{t('radiacode-common_count_rate-label')}</option>
          <option value="accuracy">{t('radiacode-common_accuracy-label')}</option>
        </select>
      </label>

      <div class="form-grid">
        <div class="muted">{t('radiacode-common_payload-label')}</div>
        <div class="grid cols-3 export-options-grid">
          <label class="export-checkbox-field">
            <input bind:checked={form.includeRaw} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radiacode-common_raw_points-label')}</span>
              <span class="faint export-help">{t('radiacode-export_raw_points-help')}</span>
            </span>
          </label>

          <label class="export-checkbox-field">
            <input bind:checked={form.includeAggregates} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radiacode-common_aggregates-label')}</span>
              <span class="faint export-help">{t('radiacode-export_aggregates-help')}</span>
            </span>
          </label>

          <label class="export-checkbox-field">
            <input bind:checked={form.applyExcludeAreas} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radiacode-common_apply_exclude_areas-label')}</span>
              <span class="faint export-help">{t('radiacode-export_apply_exclude_areas-help')}</span>
            </span>
          </label>
        </div>
      </div>

      {#if form.includeAggregates}
        <section class="export-aggregate-config">
          <div class="form-grid">
            <div class="muted">{t('radiacode-export_aggregate_layout-title')}</div>
            <span class="faint export-help">{t('radiacode-export_aggregate_layout-help')}</span>

            <div class="grid cols-2 export-aggregate-grid">
              <label class="export-field">
                <span class="muted">{t('radiacode-common_shape-label')}</span>
                <select bind:value={form.shape}>
                  <option value="hexagon">{t('radiacode-common_hexagon-label')}</option>
                  <option value="square">{t('radiacode-common_square-label')}</option>
                  <option value="circle">{t('radiacode-common_circle-label')}</option>
                </select>
              </label>

              <label class="export-field">
                <span class="muted">{t('radiacode-map_cell_size-label')}</span>
                <input bind:value={form.cellSizeMeters} min="10" type="number" />
              </label>
            </div>

            <span class="chip subtle export-aggregate-summary">
              {t('radiacode-export_aggregate_layout_summary', {
                shape: formatShapeLabel(form.shape),
                size: form.cellSizeMeters
              })}
            </span>
          </div>
        </section>
      {/if}

      <div class="actions export-actions">
        <button class="primary" onclick={runExport} type="button">{t('radiacode-export_run-button')}</button>
      </div>

      {#if errorMessage}
        <p class="muted">{errorMessage}</p>
      {/if}
    </div>
  </article>

  <article class="panel export-panel">
    <h2>{t('radiacode-common_preview-label')}</h2>

    {#if exportEnvelope}
      <div class="form-grid export-preview-meta">
        <div class="chip-row">
          <span class="chip subtle">{t('radiacode-common_type-label')}: {exportEnvelope.type}</span>
          <span class="chip subtle">
            {t('radiacode-common_build-label')}: {exportEnvelope.build ?? $sessionStore.build?.label ?? t('radiacode-common_none')}
          </span>
          <span class="chip subtle">{t('radiacode-common_time-label')}: {formatTime(exportEnvelope.exportTime)}</span>
        </div>

        <div class="chip-row">
          {#if exportEnvelope.raw}
            <span class="chip start">
              {t('radiacode-export_raw_summary', {
                visible: exportEnvelope.raw.points.length,
                total: exportEnvelope.raw.totalCount
              })}
            </span>
            {#if exportEnvelope.raw.capped}
              <span class="chip warning">{t('radiacode-export_raw_capped')}</span>
            {/if}
          {/if}

          {#if exportEnvelope.aggregates}
            <span class="chip mid">
              {t('radiacode-export_aggregate_summary', {
                count: exportEnvelope.aggregates.cells.length,
                shape: formatShapeLabel(exportEnvelope.aggregates.shape),
                size: exportEnvelope.aggregates.cellSizeMeters
              })}
            </span>
          {/if}
        </div>
      </div>
    {:else}
      <p class="muted">{t('radiacode-export_preview_empty')}</p>
    {/if}

    <textarea class="export-preview-text" readonly value={exportResult}></textarea>
  </article>
</section>

<style>
  .export-layout {
    align-items: start;
  }

  .export-panel {
    display: grid;
    gap: var(--space-3);
  }

  .export-field {
    display: grid;
    gap: var(--space-2);
  }

  .export-help {
    display: block;
  }

  .export-source-grid,
  .export-aggregate-grid {
    align-items: start;
  }

  .export-options-grid {
    align-items: stretch;
  }

  .export-checkbox-field {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: start;
    gap: var(--space-3);
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .export-checkbox-field input {
    width: auto;
    margin: 0;
  }

  .export-checkbox-copy {
    display: grid;
    gap: var(--space-1);
  }

  .export-aggregate-config {
    padding-top: var(--space-3);
    border-top: 1px solid var(--color-border);
  }

  .export-aggregate-summary {
    justify-self: start;
  }

  .export-actions {
    align-items: center;
  }

  .export-preview-meta {
    padding-bottom: var(--space-2);
    border-bottom: 1px solid var(--color-border);
  }

  .export-preview-text {
    min-height: 26rem;
  }

  @media (max-width: 1080px) {
    .export-options-grid {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 900px) {
    .export-layout,
    .export-source-grid,
    .export-aggregate-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
