<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { humanizePropKey, normalizePropKey } from '$lib/datalog-fields';
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

  const EXPORT_PREVIEW_CHAR_LIMIT = 250_000;

  interface RadTrackExportEnvelope {
    title: string;
    type: string;
    exportTime: string;
    build?: string;
    fullExport?: boolean;
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
  let exportEnvelope = $state<RadTrackExportEnvelope | null>(null);
  let exportResult = $state('');
  let previewTruncated = $state(false);
  let errorMessage = $state<string | null>(null);
  let form = $state({
    datasetIds: [] as string[],
    combinedDatasetIds: [] as string[],
    metric: 'doseRate',
    includeRaw: true,
    includeAggregates: false,
    applyExcludeAreas: true,
    fullExport: false,
    shape: 'hexagon' as 'hexagon' | 'square' | 'circle',
    cellSizeMeters: 250
  });
  const hasDatasets = $derived(datasets.length > 0);
  const hasCombinedDatasets = $derived(combinedDatasets.length > 0);
  const showBothSourceColumns = $derived(datasets.length > 0 && combinedDatasets.length > 0);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radtrack-common_none');

  const formatShapeLabel = (shape: 'hexagon' | 'square' | 'circle') => {
    if (shape === 'circle') {
      return t('radtrack-common_circle-label');
    }

    if (shape === 'square') {
      return t('radtrack-common_square-label');
    }

    return t('radtrack-common_hexagon-label');
  };

  const metricSuggestions = [
    'doseRate',
    'countRate',
    'accuracy',
    'latitude',
    'longitude',
    'altitudeMeters'
  ];

  const buildExportFilename = ({ exportTime }: { exportTime: string }) => {
    if (/^\d{4}-\d{2}-\d{2}/.test(exportTime)) {
      return `radtrack-export-${exportTime.slice(0, 10)}.json`;
    }

    return `radtrack-export-${Date.now()}.json`;
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
      errorMessage = error instanceof Error ? error.message : t('radtrack-export_failed');
    }
  };

  const buildPreviewText = ({ exportedJson }: { exportedJson: string }) => {
    if (exportedJson.length <= EXPORT_PREVIEW_CHAR_LIMIT) {
      return {
        text: exportedJson,
        truncated: false
      };
    }

    return {
      text: exportedJson.slice(0, EXPORT_PREVIEW_CHAR_LIMIT),
      truncated: true
    };
  };

  const triggerDownload = ({ exportedJson, fileName }: { exportedJson: string; fileName: string }) => {
    const blob = new Blob([exportedJson], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = fileName;
    document.body.append(anchor);
    anchor.click();

    window.setTimeout(() => {
      URL.revokeObjectURL(url);
      anchor.remove();
    }, 0);
  };

  const handleDatasetSelectAll = () => {
    form.datasetIds = datasets.map((dataset) => dataset.id);
  };

  const handleDatasetClearAll = () => {
    form.datasetIds = [];
  };

  const handleCombinedDatasetSelectAll = () => {
    form.combinedDatasetIds = combinedDatasets.map((combinedDataset) => combinedDataset.id);
  };

  const handleCombinedDatasetClearAll = () => {
    form.combinedDatasetIds = [];
  };

  const runExport = async () => {
    exportEnvelope = null;
    exportResult = '';
    previewTruncated = false;

    if (!$sessionStore.csrf) {
      return;
    }

    errorMessage = null;

    try {
      const response = await apiFetch<{ export: RadTrackExportEnvelope }>({
        path: '/api/export',
        method: 'POST',
        body: form,
        csrf: $sessionStore.csrf
      });
      const exportedJson = JSON.stringify(response.export, null, 2);
      triggerDownload({
        exportedJson,
        fileName: buildExportFilename({ exportTime: response.export.exportTime })
      });

      exportEnvelope = response.export;
      const preview = buildPreviewText({ exportedJson });
      exportResult = preview.text;
      previewTruncated = preview.truncated;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-export_failed');
    }
  };

  onMount(loadLookups);
</script>

<div class="page-header">
  <div>
    <h1>{t('radtrack-export_title')}</h1>
    <p class="muted">{t('radtrack-export_description')}</p>
  </div>
</div>

<section class="grid cols-2 export-layout">
  <article class="panel export-panel">
    <h2>{t('radtrack-export_request-title')}</h2>

    <div class="form-grid">
      <div class:export-source-grid-single={!showBothSourceColumns} class="grid export-source-grid">
        {#if hasDatasets}
          <section class="export-source-panel">
            <label class="export-source-copy" for="export-dataset-ids">
              <span class="muted">{t('radtrack-datasets_title')}</span>
              <span class="faint export-help">{t('radtrack-export_datasets-help')}</span>
            </label>

            <div class="actions export-source-actions">
              <button
                disabled={form.datasetIds.length === datasets.length}
                onclick={handleDatasetSelectAll}
                type="button"
              >
                {t('radtrack-map_tracks_all-button')}
              </button>
              <button disabled={!form.datasetIds.length} onclick={handleDatasetClearAll} type="button">
                {t('radtrack-map_tracks_none-button')}
              </button>
            </div>

            <select class="export-source-select" bind:value={form.datasetIds} id="export-dataset-ids" multiple size="12">
              {#each datasets as dataset}
                <option value={dataset.id}>{dataset.name}</option>
              {/each}
            </select>
          </section>
        {/if}

        {#if hasCombinedDatasets}
          <section class="export-source-panel">
            <label class="export-source-copy" for="export-combined-dataset-ids">
              <span class="muted">{t('radtrack-common_combined_datasets-label')}</span>
              <span class="faint export-help">{t('radtrack-export_combined_datasets-help')}</span>
            </label>

            <div class="actions export-source-actions">
              <button
                disabled={form.combinedDatasetIds.length === combinedDatasets.length}
                onclick={handleCombinedDatasetSelectAll}
                type="button"
              >
                {t('radtrack-map_tracks_all-button')}
              </button>
              <button
                disabled={!form.combinedDatasetIds.length}
                onclick={handleCombinedDatasetClearAll}
                type="button"
              >
                {t('radtrack-map_tracks_none-button')}
              </button>
            </div>

            <select
              class="export-source-select"
              bind:value={form.combinedDatasetIds}
              id="export-combined-dataset-ids"
              multiple
              size="12"
            >
              {#each combinedDatasets as combinedDataset}
                <option value={combinedDataset.id}>{combinedDataset.name}</option>
              {/each}
            </select>
          </section>
        {/if}
      </div>

      <div class="form-grid">
        <div class="muted">{t('radtrack-common_payload-label')}</div>
        <div class="grid cols-2 export-options-grid">
          <label class="export-checkbox-field">
            <input bind:checked={form.includeRaw} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radtrack-common_raw_points-label')}</span>
              <span class="faint export-help">{t('radtrack-export_raw_points-help')}</span>
            </span>
          </label>

          <label class="export-checkbox-field">
            <input bind:checked={form.includeAggregates} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radtrack-common_aggregates-label')}</span>
              <span class="faint export-help">{t('radtrack-export_aggregates-help')}</span>
            </span>
          </label>

          <label class="export-checkbox-field">
            <input bind:checked={form.applyExcludeAreas} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radtrack-common_apply_exclude_areas-label')}</span>
              <span class="faint export-help">{t('radtrack-export_apply_exclude_areas-help')}</span>
            </span>
          </label>

          <label class="export-checkbox-field">
            <input bind:checked={form.fullExport} type="checkbox" />
            <span class="export-checkbox-copy">
              <span>{t('radtrack-export_full_export-label')}</span>
              <span class="faint export-help">{t('radtrack-export_full_export-help')}</span>
            </span>
          </label>
        </div>
      </div>

      {#if form.includeAggregates}
        <section class="export-aggregate-config">
          <label class="export-field">
            <span class="muted">{t('radtrack-common_metric-label')}</span>
            <span class="faint export-help">{t('radtrack-export_metric-help')}</span>
            <input
              bind:value={form.metric}
              list="export-metric-options"
              onblur={() => {
                form.metric = normalizePropKey(form.metric) ?? form.metric.trim();
              }}
              placeholder="doseRate"
            />
            <datalist id="export-metric-options">
              {#each metricSuggestions as metric}
                <option value={metric}>{humanizePropKey(metric) ?? metric}</option>
              {/each}
            </datalist>
          </label>

          <div class="form-grid">
            <div class="muted">{t('radtrack-export_aggregate_layout-title')}</div>
            <span class="faint export-help">{t('radtrack-export_aggregate_layout-help')}</span>

            <div class="grid cols-2 export-aggregate-grid">
              <label class="export-field">
                <span class="muted">{t('radtrack-common_shape-label')}</span>
                <select bind:value={form.shape}>
                  <option value="hexagon">{t('radtrack-common_hexagon-label')}</option>
                  <option value="square">{t('radtrack-common_square-label')}</option>
                  <option value="circle">{t('radtrack-common_circle-label')}</option>
                </select>
              </label>

              <label class="export-field">
                <span class="muted">{t('radtrack-map_cell_size-label')}</span>
                <input bind:value={form.cellSizeMeters} min="10" type="number" />
              </label>
            </div>

            <span class="chip subtle export-aggregate-summary">
              {t('radtrack-export_aggregate_layout_summary', {
                shape: formatShapeLabel(form.shape),
                size: form.cellSizeMeters
              })}
            </span>
          </div>
        </section>
      {/if}

      <div class="actions export-actions">
        <button class="primary" onclick={runExport} type="button">{t('radtrack-export_run-button')}</button>
      </div>

      {#if errorMessage}
        <p class="muted">{errorMessage}</p>
      {/if}
    </div>
  </article>

  <article class="panel export-panel export-preview-panel">
    <h2>{t('radtrack-common_preview-label')}</h2>

    {#if exportEnvelope}
      <div class="form-grid export-preview-meta">
        <div class="chip-row">
          <span class="chip subtle">{t('radtrack-common_type-label')}: {exportEnvelope.type}</span>
          <span class="chip subtle">
            {t('radtrack-common_build-label')}: {exportEnvelope.build ?? $sessionStore.build?.label ?? t('radtrack-common_none')}
          </span>
          <span class="chip subtle">{t('radtrack-common_time-label')}: {formatTime(exportEnvelope.exportTime)}</span>
        </div>

        <div class="chip-row">
          {#if exportEnvelope.raw}
            <span class="chip start">
              {t('radtrack-export_raw_summary', {
                visible: exportEnvelope.raw.points.length,
                total: exportEnvelope.raw.totalCount
              })}
            </span>
            {#if exportEnvelope.raw.capped}
              <span class="chip warning">{t('radtrack-export_raw_capped')}</span>
            {/if}
          {/if}

          {#if exportEnvelope.aggregates}
            <span class="chip mid">
              {t('radtrack-export_aggregate_summary', {
                count: exportEnvelope.aggregates.cells.length,
                shape: formatShapeLabel(exportEnvelope.aggregates.shape),
                size: exportEnvelope.aggregates.cellSizeMeters
              })}
            </span>
          {/if}
        </div>
      </div>
    {:else}
      <p class="muted">{t('radtrack-export_preview_empty')}</p>
    {/if}

    {#if previewTruncated}
      <p class="faint">{t('radtrack-export_preview_truncated', { chars: EXPORT_PREVIEW_CHAR_LIMIT })}</p>
    {/if}

    <textarea bind:value={exportResult} class="export-preview-text" readonly></textarea>
  </article>
</section>

<style>
  .export-layout {
    width: 100%;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    align-items: stretch;
  }

  .export-panel {
    display: grid;
    gap: var(--space-3);
    min-width: 0;
  }

  .export-preview-panel {
    display: flex;
    flex-direction: column;
  }

  .export-field {
    display: grid;
    gap: var(--space-2);
  }

  .export-help {
    display: block;
  }

  .export-source-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    align-items: stretch;
  }

  .export-source-grid-single {
    grid-template-columns: minmax(0, 1fr);
  }

  .export-source-copy {
    display: grid;
    gap: var(--space-2);
  }

  .export-source-panel {
    display: grid;
    grid-template-rows: auto auto minmax(0, 1fr);
    gap: var(--space-2);
    min-width: 0;
  }

  .export-source-actions {
    align-items: center;
  }

  .export-source-select {
    width: 100%;
    min-width: 0;
    height: 18rem;
  }

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
    display: grid;
    gap: var(--space-4);
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
    flex: 1 1 auto;
    min-height: 18rem;
    width: 100%;
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
