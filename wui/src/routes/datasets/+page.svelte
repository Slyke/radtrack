<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import {
    isMapDatasetDefaultEnabled,
    loadMapDatasetDefaults,
    loadMapDatasetOrder,
    moveMapDatasetOrder,
    orderMapDatasets,
    persistMapDatasetDefaults,
    persistMapDatasetOrder,
    type MapDatasetDefaultRecord,
    type MapDatasetOrder
  } from '$lib/map-dataset-defaults';
  import {
    loadMapFieldOrder,
    moveMapFieldOrder,
    orderMapFields,
    persistMapFieldOrder,
    type MapFieldOrder
  } from '$lib/map-field-visibility';
  import type { MetricField } from '$lib/datalog-fields';
  import { sessionStore } from '$lib/stores/session';

  type DatasetSummary = {
    id: string;
    name: string;
    accessLevel: string;
    datalogCount: number;
    readingCount: number;
  };

  type FieldInventoryEntry = {
    propKey: string;
    source: 'core' | 'measurement' | 'synthetic';
    valueType: 'number' | 'time' | 'string';
    displayNames: string[];
    datasets: Array<{
      id: string;
      name: string;
    }>;
    datalogs: Array<{
      id: string;
      name: string | null;
      datasetId: string;
      datasetName: string;
      displayName: string;
      editable?: boolean;
      popupDefaultEnabled: boolean;
      metricListEnabled: boolean;
      supportedFields: MetricField[];
    }>;
  };

  let datasets = $state<DatasetSummary[]>([]);
  let fieldInventory = $state<FieldInventoryEntry[]>([]);
  let mapDatasetDefaults = $state<MapDatasetDefaultRecord>({});
  let mapDatasetOrder = $state<MapDatasetOrder>([]);
  let mapFieldOrder = $state<MapFieldOrder>([]);
  let updatingFieldKeys = $state<Record<string, boolean>>({});
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

  const formatFieldType = ({
    source,
    valueType
  }: {
    source: FieldInventoryEntry['source'];
    valueType: FieldInventoryEntry['valueType'];
  }) => {
    if (source === 'synthetic') {
      return t('radtrack-datasets_field_inventory_popup_only-label');
    }

    switch (valueType) {
      case 'time':
        return t('radtrack-common_time-label');
      case 'number':
      default:
        return t('radtrack-datasets_field_inventory_number-label');
    }
  };

  const syncMapSettings = () => {
    mapDatasetDefaults = loadMapDatasetDefaults({
      userId: $sessionStore.user?.id ?? null
    });
    mapDatasetOrder = loadMapDatasetOrder({
      userId: $sessionStore.user?.id ?? null
    });
    mapFieldOrder = loadMapFieldOrder({
      userId: $sessionStore.user?.id ?? null
    });
  };

  const persistDatasetDefaults = (defaults: MapDatasetDefaultRecord) => {
    mapDatasetDefaults = defaults;
    persistMapDatasetDefaults({
      userId: $sessionStore.user?.id ?? null,
      defaults
    });
  };

  const persistDatasetOrder = (order: MapDatasetOrder) => {
    mapDatasetOrder = order;
    persistMapDatasetOrder({
      userId: $sessionStore.user?.id ?? null,
      order
    });
  };

  const persistOrder = (order: MapFieldOrder) => {
    mapFieldOrder = order;
    persistMapFieldOrder({
      userId: $sessionStore.user?.id ?? null,
      order
    });
  };

  const setDatasetDefaultEnabled = ({
    datasetId,
    enabled
  }: {
    datasetId: string;
    enabled: boolean;
  }) => {
    persistDatasetDefaults({
      ...mapDatasetDefaults,
      [datasetId]: enabled
    });
  };

  const setAllDatasetDefaults = ({ enabled }: { enabled: boolean }) => {
    persistDatasetDefaults(Object.fromEntries(datasets.map((dataset) => [dataset.id, enabled])));
  };

  const orderedDatasets = $derived.by(() => orderMapDatasets({
    datasets,
    order: mapDatasetOrder
  }));

  const orderedFieldInventory = $derived.by(() => orderMapFields({
    fields: fieldInventory,
    order: mapFieldOrder
  }));

  const enabledFieldCount = $derived.by(() => fieldInventory
    .filter((field) => field.datalogs.some((datalog) => datalog.popupDefaultEnabled !== false))
    .length);

  const metricListFieldCount = $derived.by(() => fieldInventory
    .filter((field) => field.source !== 'synthetic' && field.valueType !== 'string')
    .filter((field) => field.datalogs.some((datalog) => datalog.metricListEnabled !== false))
    .length);

  const metricListEligibleFieldCount = $derived.by(() => fieldInventory
    .filter((field) => field.source !== 'synthetic' && field.valueType !== 'string')
    .length);

  const isFieldPopupDefaultEnabled = (field: FieldInventoryEntry) => (
    field.datalogs.some((datalog) => datalog.popupDefaultEnabled !== false)
  );

  const isFieldMetricListEnabled = (field: FieldInventoryEntry) => (
    field.datalogs.some((datalog) => datalog.metricListEnabled !== false)
  );

  const fieldHasDirectSupportedField = (field: FieldInventoryEntry) => field.datalogs
    .some((datalog) => datalog.editable !== false && datalog.supportedFields.some((supportedField) => supportedField.propKey === field.propKey));

  const setFieldFlag = async ({
    field,
    key,
    enabled
  }: {
    field: FieldInventoryEntry;
    key: 'popupDefaultEnabled' | 'metricListEnabled';
    enabled: boolean;
  }) => {
    const csrf = $sessionStore.csrf;
    if (!csrf) {
      return;
    }

    const updateKey = `${field.propKey}:${key}`;
    updatingFieldKeys = {
      ...updatingFieldKeys,
      [updateKey]: true
    };

    const patchTargets = field.datalogs
        .filter((datalog) => datalog.editable !== false)
        .map((datalog) => ({
          datalog,
          supportedFields: datalog.supportedFields.map((supportedField) => (
            supportedField.propKey === field.propKey
              ? {
                  ...supportedField,
                  [key]: enabled
                }
              : supportedField
          ))
        }))
        .filter((entry) => entry.supportedFields.some((supportedField) => supportedField.propKey === field.propKey));

    if (!patchTargets.length) {
      updatingFieldKeys = {
        ...updatingFieldKeys,
        [updateKey]: false
      };
      return;
    }

    try {
      await Promise.all(patchTargets.map((entry) => apiFetch({
          path: `/api/datalogs/${entry.datalog.id}`,
          method: 'PATCH',
          body: {
            supportedFields: entry.supportedFields
          },
          csrf
        })));
      await loadDatasets();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-datasets_failed_update_field');
    } finally {
      updatingFieldKeys = {
        ...updatingFieldKeys,
        [updateKey]: false
      };
    }
  };

  const setAllFieldFlag = async ({
    key,
    enabled
  }: {
    key: 'popupDefaultEnabled' | 'metricListEnabled';
    enabled: boolean;
  }) => {
    const fields = key === 'metricListEnabled'
      ? fieldInventory.filter((field) => field.source !== 'synthetic' && field.valueType !== 'string')
      : fieldInventory;

    for (const field of fields) {
      if (fieldHasDirectSupportedField(field)) {
        await setFieldFlag({ field, key, enabled });
      }
    }
  };

  const moveField = ({
    propKey,
    direction
  }: {
    propKey: string;
    direction: 'up' | 'down';
  }) => {
    persistOrder(moveMapFieldOrder({
      fields: orderedFieldInventory,
      order: mapFieldOrder,
      propKey,
      direction
    }));
  };

  const moveDataset = ({
    datasetId,
    direction
  }: {
    datasetId: string;
    direction: 'up' | 'down';
  }) => {
    persistDatasetOrder(moveMapDatasetOrder({
      datasets: orderedDatasets,
      order: mapDatasetOrder,
      datasetId,
      direction
    }));
  };

  const loadDatasets = async () => {
    try {
      const [datasetResponse, fieldResponse] = await Promise.all([
        apiFetch<any>({ path: '/api/datasets' }),
        apiFetch<any>({ path: '/api/datasets/field-inventory' })
      ]);
      datasets = datasetResponse.datasets;
      fieldInventory = fieldResponse.fields;
      errorMessage = null;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-datasets_failed_load');
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
      errorMessage = error instanceof Error ? error.message : t('radtrack-datasets_failed_create');
    }
  };

  const deleteDataset = async ({ datasetId, datasetName }: { datasetId: string; datasetName: string }) => {
    if (!browser || !$sessionStore.csrf) {
      return;
    }

    if (!window.confirm(t('radtrack-dataset_delete_confirm', { name: datasetName }))) {
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
      errorMessage = error instanceof Error ? error.message : t('radtrack-datasets_failed_delete');
    }
  };

  onMount(async () => {
    syncMapSettings();
    await loadDatasets();
  });
</script>

<div class="datasets-page">
  <div class="datasets-page-header">
    <div class="page-header">
      <div>
        <h1>{t('radtrack-datasets_title')}</h1>
        <p class="muted">{t('radtrack-datasets_description')}</p>
      </div>
    </div>

    {#if errorMessage}
      <section class="panel">
        <p class="muted">{errorMessage}</p>
      </section>
    {/if}
  </div>

  <section class="grid cols-2 datasets-layout">
    <div class="datasets-left-column">
      <article class="panel">
        <details class="datasets-accordion">
          <summary>
            <span>{t('radtrack-datasets_create-title')}</span>
            <span class="datasets-accordion-summary">
              <span aria-hidden="true" class="datasets-accordion-icon"></span>
            </span>
          </summary>

          <div class="form-grid">
            <input bind:value={createForm.name} placeholder={t('radtrack-common_dataset_name-label')} />
            <textarea bind:value={createForm.description} placeholder={t('radtrack-common_description-label')}></textarea>
            <div class="actions">
              <button class="primary" onclick={createDataset}>{t('radtrack-common_create-button')}</button>
            </div>
          </div>
        </details>
      </article>

      <article class="panel datasets-field-panel">
        <div class="datasets-field-header">
          <div>
            <h2>{t('radtrack-datasets_field_inventory-title')}</h2>
            <p class="muted">{t('radtrack-datasets_field_inventory-description')}</p>
          </div>
          <div class="actions">
            <button onclick={() => setAllFieldFlag({ key: 'popupDefaultEnabled', enabled: true })}>+ Popup</button>
            <button onclick={() => setAllFieldFlag({ key: 'popupDefaultEnabled', enabled: false })}>- Popup</button>
            <button onclick={() => setAllFieldFlag({ key: 'metricListEnabled', enabled: true })}>+ Metrics</button>
            <button onclick={() => setAllFieldFlag({ key: 'metricListEnabled', enabled: false })}>- Metrics</button>
          </div>
        </div>

        <div class="grid">
          <span class="chip subtle">{t('radtrack-datasets_field_inventory_enabled-label', {
            enabled: enabledFieldCount,
            total: fieldInventory.length
          })}</span>
          <span class="chip subtle">{t('radtrack-datasets_field_inventory_metric_list_enabled-label', {
            enabled: metricListFieldCount,
            total: metricListEligibleFieldCount
          })}</span>
        </div>

        {#if fieldInventory.length}
          <div class="field-inventory-list">
            {#each orderedFieldInventory as field, index}
              <div class="field-inventory-row">
                <div class="field-inventory-body">
                  <div class="field-inventory-topline">
                    <div class="field-inventory-toggle-stack">
                      <label class="checkbox-field field-inventory-toggle">
                        <input
                          checked={isFieldPopupDefaultEnabled(field)}
                          disabled={!fieldHasDirectSupportedField(field) || Boolean(updatingFieldKeys[`${field.propKey}:popupDefaultEnabled`])}
                          onchange={(event) => setFieldFlag({
                            field,
                            key: 'popupDefaultEnabled',
                            enabled: (event.currentTarget as HTMLInputElement).checked
                          })}
                          type="checkbox"
                        />
                        <span>{t('radtrack-datasets_field_inventory_visible-label')}</span>
                      </label>
                      {#if field.source !== 'synthetic' && field.valueType !== 'string'}
                        <label class="checkbox-field field-inventory-toggle">
                          <input
                            checked={isFieldMetricListEnabled(field)}
                            disabled={!fieldHasDirectSupportedField(field) || Boolean(updatingFieldKeys[`${field.propKey}:metricListEnabled`])}
                            onchange={(event) => setFieldFlag({
                              field,
                              key: 'metricListEnabled',
                              enabled: (event.currentTarget as HTMLInputElement).checked
                            })}
                            type="checkbox"
                          />
                          <span>{t('radtrack-track_metric_list_enabled-label')}</span>
                        </label>
                      {/if}
                    </div>
                    <code>{field.propKey}</code>
                    <span class="chip subtle">{formatFieldType({ source: field.source, valueType: field.valueType })}</span>
                    <span class="chip subtle">{field.datalogs.length} {t('radtrack-common_tracks-label')}</span>
                    <span class="chip subtle">{field.datasets.length} {t('radtrack-common_dataset-label')}</span>
                  </div>

                  <div class="field-inventory-line">
                    <span class="muted">{t('radtrack-common_label-label')}:</span>
                    <span>{field.displayNames.join(', ')}</span>
                  </div>

                  {#if field.datasets.length > 1}
                    <details class="field-inventory-source-accordion">
                      <summary>
                        <span class="field-inventory-line">
                          <span class="muted">{t('radtrack-common_datasets-label')}:</span>
                          <span>{field.datasets.length}</span>
                        </span>
                        <span class="datasets-accordion-summary">
                          <span aria-hidden="true" class="datasets-accordion-icon"></span>
                        </span>
                      </summary>

                      <div class="field-inventory-source-list">
                        {#each field.datasets as dataset}
                          <div class="field-inventory-source-item">
                            <span class="field-inventory-source-text">{dataset.name}</span>
                            <a class="button-link field-inventory-source-link" href={`/datasets/${dataset.id}`}>
                              {t('radtrack-common_open-button')}
                            </a>
                          </div>
                        {/each}
                      </div>
                    </details>
                  {:else if field.datasets.length === 1}
                    <div class="field-inventory-line">
                      <span class="muted">{t('radtrack-common_datasets-label')}:</span>
                      <a class="field-inventory-source-inline-link" href={`/datasets/${field.datasets[0].id}`}>
                        {field.datasets[0].name}
                      </a>
                    </div>
                  {:else}
                    <div class="field-inventory-line">
                      <span class="muted">{t('radtrack-common_datasets-label')}:</span>
                      <span>{t('radtrack-common_none')}</span>
                    </div>
                  {/if}

                  {#if field.datalogs.length > 1}
                    <details class="field-inventory-source-accordion">
                      <summary>
                        <span class="field-inventory-line">
                          <span class="muted">{t('radtrack-common_tracks-label')}:</span>
                          <span>{field.datalogs.length}</span>
                        </span>
                        <span class="datasets-accordion-summary">
                          <span aria-hidden="true" class="datasets-accordion-icon"></span>
                        </span>
                      </summary>

                      <div class="field-inventory-source-list">
                        {#each field.datalogs as datalog}
                          <div class="field-inventory-source-item">
                            <span class="field-inventory-source-text">{datalog.datasetName}: {datalog.name ?? datalog.id}</span>
                            <a class="button-link field-inventory-source-link" href={`/datalogs/${datalog.id}`}>
                              {t('radtrack-common_open-button')}
                            </a>
                          </div>
                        {/each}
                      </div>
                    </details>
                  {:else if field.datalogs.length === 1}
                    <div class="field-inventory-line">
                      <span class="muted">{t('radtrack-common_tracks-label')}:</span>
                      <a class="field-inventory-source-inline-link" href={`/datalogs/${field.datalogs[0].id}`}>
                        {field.datalogs[0].datasetName}: {field.datalogs[0].name ?? field.datalogs[0].id}
                      </a>
                    </div>
                  {:else}
                    <div class="field-inventory-line">
                      <span class="muted">{t('radtrack-common_tracks-label')}:</span>
                      <span>{t('radtrack-common_none')}</span>
                    </div>
                  {/if}
                </div>

                <div class="field-inventory-actions">
                  <button
                    aria-label={t('radtrack-common_move_up-button')}
                    disabled={index === 0}
                    onclick={() => moveField({ propKey: field.propKey, direction: 'up' })}
                    type="button"
                  >
                    ↑
                  </button>
                  <button
                    aria-label={t('radtrack-common_move_down-button')}
                    disabled={index === orderedFieldInventory.length - 1}
                    onclick={() => moveField({ propKey: field.propKey, direction: 'down' })}
                    type="button"
                  >
                    ↓
                  </button>
                </div>
              </div>
            {/each}
          </div>
        {:else}
          <p class="muted">{t('radtrack-datasets_field_inventory_empty')}</p>
        {/if}
      </article>
    </div>

    <article class="panel datasets-list-panel">
      <div class="datasets-field-header">
        <h2>{t('radtrack-datasets_accessible-title')}</h2>
        <div class="actions">
          <button onclick={() => setAllDatasetDefaults({ enabled: true })}>+ All</button>
          <button onclick={() => setAllDatasetDefaults({ enabled: false })}>- All</button>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('radtrack-common_name-label')}</th>
              <th>{t('radtrack-datasets_default_on_map-label')}</th>
              <th>{t('radtrack-common_access-label')}</th>
              <th>{t('radtrack-common_tracks-label')}</th>
              <th>{t('radtrack-common_readings-label')}</th>
              <th>{t('radtrack-common_actions-label')}</th>
            </tr>
          </thead>
          <tbody>
            {#each orderedDatasets as dataset, index}
              <tr>
                <td><a href={`/datasets/${dataset.id}`}>{dataset.name}</a></td>
                <td>
                  <label class="checkbox-field dataset-default-toggle">
                    <input
                      checked={isMapDatasetDefaultEnabled({
                        defaults: mapDatasetDefaults,
                        datasetId: dataset.id
                      })}
                      onchange={(event) => setDatasetDefaultEnabled({
                        datasetId: dataset.id,
                        enabled: (event.currentTarget as HTMLInputElement).checked
                      })}
                      type="checkbox"
                    />
                  </label>
                </td>
                <td>{dataset.accessLevel}</td>
                <td>{dataset.datalogCount}</td>
                <td>{dataset.readingCount}</td>
                <td>
                  <div class="dataset-table-actions">
                    <button
                      aria-label={t('radtrack-common_move_up-button')}
                      disabled={index === 0}
                      onclick={() => moveDataset({ datasetId: dataset.id, direction: 'up' })}
                      type="button"
                    >
                      ↑
                    </button>
                    <button
                      aria-label={t('radtrack-common_move_down-button')}
                      disabled={index === orderedDatasets.length - 1}
                      onclick={() => moveDataset({ datasetId: dataset.id, direction: 'down' })}
                      type="button"
                    >
                      ↓
                    </button>
                    {#if dataset.accessLevel === 'edit'}
                      <button class="danger" onclick={() => deleteDataset({ datasetId: dataset.id, datasetName: dataset.name })}>
                        {t('radtrack-common_danger-delete-button')}
                      </button>
                    {/if}
                  </div>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
  </section>
</div>

<style>
  .datasets-accordion {
    display: grid;
    gap: var(--space-4);
  }

  .datasets-accordion summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
    padding: 0.8rem 0.9rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .datasets-accordion summary::-webkit-details-marker {
    display: none;
  }

  .datasets-accordion[open] summary {
    margin-bottom: var(--space-3);
  }

  .datasets-accordion-summary {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
  }

  .datasets-accordion-icon::before {
    content: '>';
    display: inline-block;
    color: var(--color-text-muted);
    transition: transform var(--transition), color var(--transition);
  }

  .datasets-accordion[open] .datasets-accordion-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .grid.cols-2.datasets-layout {
    min-height: 0;
    align-items: stretch;
    width: 100%;
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .datasets-left-column {
    display: grid;
    gap: var(--space-4);
    align-content: start;
    min-height: 0;
    overflow-y: auto;
  }

  .datasets-field-panel {
    display: grid;
    gap: var(--space-4);
    min-height: 0;
  }

  .datasets-field-header {
    display: flex;
    justify-content: space-between;
    gap: var(--space-3);
    align-items: start;
  }

  .field-inventory-list {
    display: grid;
    gap: var(--space-3);
    max-height: 48rem;
    overflow-y: auto;
    padding-right: 0.25rem;
  }

  .field-inventory-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: var(--space-2);
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    background: color-mix(in srgb, var(--color-panel) 92%, var(--color-accent) 8%);
  }

  .field-inventory-toggle-stack {
    display: grid;
    gap: var(--space-2);
    width: 17rem;
  }

  .field-inventory-toggle {
    display: flex;
    align-items: center;
    gap: var(--space-3);
    white-space: nowrap;
  }

  .field-inventory-toggle input[type='checkbox'] {
    flex: 0 0 auto;
    width: auto;
  }

  .field-inventory-body {
    display: grid;
    gap: var(--space-2);
    min-width: 0;
    grid-column: 1;
  }

  .field-inventory-actions {
    display: grid;
    gap: var(--space-2);
    align-content: start;
    grid-column: 2;
    grid-row: 1 / span 2;
  }

  .field-inventory-topline {
    display: grid;
    grid-template-columns: 17rem auto auto auto auto;
    justify-content: start;
    gap: var(--space-2);
    align-items: center;
  }

  .field-inventory-line {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
    align-items: baseline;
    overflow-wrap: anywhere;
  }

  .field-inventory-source-accordion {
    display: grid;
    gap: var(--space-3);
  }

  .field-inventory-source-accordion summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
    padding: 0.65rem 0.8rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .field-inventory-source-accordion summary::-webkit-details-marker {
    display: none;
  }

  .field-inventory-source-accordion[open] summary {
    margin-bottom: 0;
  }

  .field-inventory-source-accordion[open] .datasets-accordion-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .field-inventory-source-list {
    display: grid;
    gap: var(--space-2);
    padding: 0.8rem;
    border: 1px solid color-mix(in srgb, var(--color-start) 30%, var(--color-border));
    border-radius: var(--radius-md);
    background: color-mix(in srgb, var(--color-start-soft) 56%, var(--color-panel));
  }

  .field-inventory-source-item {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
    gap: var(--space-3);
    padding: 0.65rem 0.8rem;
    border: 1px solid color-mix(in srgb, var(--color-mid) 24%, var(--color-border));
    border-radius: var(--radius-md);
    background: color-mix(in srgb, var(--color-mid-soft) 24%, var(--color-panel-strong));
    overflow-wrap: anywhere;
  }

  .field-inventory-source-text {
    min-width: 0;
    overflow-wrap: anywhere;
  }

  .field-inventory-source-link {
    min-height: 2.1rem;
    padding: 0.45rem 0.7rem;
    white-space: nowrap;
  }

  .field-inventory-source-inline-link {
    overflow-wrap: anywhere;
    text-decoration-thickness: 0.08em;
    text-underline-offset: 0.16em;
  }

  .datasets-list-panel .table-wrap {
    min-height: 0;
    height: 100%;
    overflow: auto;
  }

  .dataset-default-toggle {
    display: inline-flex;
    justify-content: center;
  }

  .dataset-table-actions {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: var(--space-2);
    width: 6rem;
    justify-items: stretch;
  }

  .dataset-table-actions > button {
    width: 100%;
    min-width: 0;
  }

  .dataset-table-actions > .danger {
    grid-column: 1 / -1;
  }

  @media (max-width: 1100px) {
    .datasets-field-header {
      flex-direction: column;
    }

    .field-inventory-row {
      grid-template-columns: 1fr;
    }

    .field-inventory-topline {
      grid-template-columns: minmax(0, 1fr);
      justify-content: stretch;
    }

    .field-inventory-toggle-stack {
      width: 17rem;
      max-width: 100%;
    }

    .field-inventory-body,
    .field-inventory-actions {
      grid-column: auto;
      grid-row: auto;
    }

    .field-inventory-actions {
      grid-auto-flow: column;
      justify-content: start;
    }
  }
</style>
