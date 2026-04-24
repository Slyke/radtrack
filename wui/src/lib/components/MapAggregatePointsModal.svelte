<svelte:options runes={true} />

<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';

  type CellValue = string | number | boolean | null;
  type SortDirection = 'asc' | 'desc';

  export type AggregatePointsModalColumn = {
    propKey: string;
    label: string;
    valueType: 'number' | 'time' | 'string';
  };

  export type AggregatePointsModalRow = {
    id: string;
    values: Record<string, CellValue>;
  };

  interface Props {
    columns?: AggregatePointsModalColumn[];
    rows?: AggregatePointsModalRow[];
    loading?: boolean;
    errorMessage?: string | null;
    pointCount?: number;
    subtitle?: string | null;
    maxHeightPx?: number | null;
  }

  let {
    columns = [],
    rows = [],
    loading = false,
    errorMessage = null,
    pointCount = 0,
    subtitle = null,
    maxHeightPx = null
  }: Props = $props();

  const dispatch = createEventDispatcher<{
    close: void;
  }>();

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const numberFormatter = () => new Intl.NumberFormat($localeStore.language, {
    useGrouping: false,
    maximumFractionDigits: 5
  });
  const countFormatter = () => new Intl.NumberFormat($localeStore.language);

  const defaultSortDirection = ({ column }: { column: AggregatePointsModalColumn | null }) => (
    column?.valueType === 'time' ? 'desc' : 'asc'
  );

  const getDefaultSortKey = ({
    availableColumns
  }: {
    availableColumns: AggregatePointsModalColumn[];
  }) => (
    availableColumns.find((column) => column.propKey === 'occurredAt')?.propKey
    ?? availableColumns.find((column) => column.propKey === 'receivedAt')?.propKey
    ?? availableColumns[0]?.propKey
    ?? null
  );

  const getColumnByPropKey = (propKey: string | null) => (
    propKey ? (columns.find((column) => column.propKey === propKey) ?? null) : null
  );

  const closeModal = () => {
    dispatch('close');
  };

  const handleWindowKeydown = (event: KeyboardEvent) => {
    if (event.key !== 'Escape') {
      return;
    }

    closeModal();
  };

  let sortKey = $state<string | null>(null);
  let sortDirection = $state<SortDirection>('desc');

  $effect(() => {
    const nextSortKey = sortKey && columns.some((column) => column.propKey === sortKey)
      ? sortKey
      : getDefaultSortKey({ availableColumns: columns });
    const nextColumn = getColumnByPropKey(nextSortKey);
    const shouldResetDirection = sortKey !== nextSortKey;

    if (shouldResetDirection) {
      sortKey = nextSortKey;
    }

    if (shouldResetDirection && nextColumn) {
      sortDirection = defaultSortDirection({ column: nextColumn });
    }
  });

  const resolveSortValue = ({
    column,
    value
  }: {
    column: AggregatePointsModalColumn;
    value: CellValue;
  }) => {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    if (column.valueType === 'time') {
      if (typeof value === 'number') {
        return value;
      }

      const parsed = Date.parse(String(value));
      return Number.isNaN(parsed)
        ? String(value).toLowerCase()
        : parsed;
    }

    if (typeof value === 'number') {
      return value;
    }

    if (typeof value === 'boolean') {
      return value ? 1 : 0;
    }

    return String(value).toLowerCase();
  };

  const compareRowValues = ({
    left,
    right,
    column,
    direction
  }: {
    left: AggregatePointsModalRow;
    right: AggregatePointsModalRow;
    column: AggregatePointsModalColumn;
    direction: SortDirection;
  }) => {
    const leftValue = resolveSortValue({ column, value: left.values[column.propKey] ?? null });
    const rightValue = resolveSortValue({ column, value: right.values[column.propKey] ?? null });

    if (leftValue === null && rightValue === null) {
      return left.id.localeCompare(right.id);
    }

    if (leftValue === null) {
      return 1;
    }

    if (rightValue === null) {
      return -1;
    }

    if (leftValue < rightValue) {
      return direction === 'asc' ? -1 : 1;
    }

    if (leftValue > rightValue) {
      return direction === 'asc' ? 1 : -1;
    }

    return left.id.localeCompare(right.id);
  };

  const sortedRows = $derived.by(() => {
    const activeColumn = getColumnByPropKey(sortKey);
    if (!activeColumn) {
      return rows;
    }

    return [...rows].sort((left, right) => compareRowValues({
      left,
      right,
      column: activeColumn,
      direction: sortDirection
    }));
  });

  const formatCellValue = ({
    column,
    value
  }: {
    column: AggregatePointsModalColumn;
    value: CellValue;
  }) => {
    if (value === null || value === undefined || value === '') {
      return t('radtrack-common_na-label');
    }

    if (column.valueType === 'time') {
      const normalizedValue = typeof value === 'number'
        ? new Date(value).toISOString()
        : String(value);

      return formatDateTime({
        value: normalizedValue,
        language: $localeStore.language
      }) ?? normalizedValue;
    }

    if (typeof value === 'number') {
      return numberFormatter().format(value);
    }

    return String(value);
  };

  const toggleSort = ({ column }: { column: AggregatePointsModalColumn }) => {
    if (sortKey === column.propKey) {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
      return;
    }

    sortKey = column.propKey;
    sortDirection = defaultSortDirection({ column });
  };

  const getSortLabel = ({ column }: { column: AggregatePointsModalColumn }) => {
    const nextDirection = sortKey === column.propKey
      ? (sortDirection === 'asc' ? 'desc' : 'asc')
      : defaultSortDirection({ column });

    return t(
      nextDirection === 'asc'
        ? 'radtrack-map_points_modal_sort_ascending-label'
        : 'radtrack-map_points_modal_sort_descending-label',
      { field: column.label }
    );
  };

  const getSortIndicator = ({ column }: { column: AggregatePointsModalColumn }) => {
    if (sortKey !== column.propKey) {
      return ' ';
    }

    return sortDirection === 'asc' ? '↑' : '↓';
  };
</script>

<svelte:window onkeydown={handleWindowKeydown} />

<div class="map-cell-points-modal-shell" role="presentation">
  <button
    aria-label={t('radtrack-common_close-button')}
    class="map-cell-points-modal-backdrop"
    onclick={closeModal}
    type="button"
  ></button>

  <section
    aria-labelledby="map-cell-points-modal-title"
    aria-modal="true"
    class="panel map-cell-points-modal"
    role="dialog"
    style:--map-popup-max-height={maxHeightPx ? `${maxHeightPx}px` : null}
    tabindex="-1"
  >
    <header class="map-cell-points-modal-header">
      <div class="map-cell-points-modal-heading">
        <div class="map-cell-points-modal-title-row">
          <h2 id="map-cell-points-modal-title">{t('radtrack-map_points_modal_title')}</h2>
          <span class="chip subtle">
            {t('radtrack-map_points_modal_count-label', {
              count: countFormatter().format(pointCount)
            })}
          </span>
          {#if loading}
            <span class="chip subtle">{t('radtrack-common_loading-label')}</span>
          {/if}
        </div>

        {#if subtitle}
          <p class="muted map-cell-points-modal-subtitle">{subtitle}</p>
        {/if}
      </div>

      <button onclick={closeModal} type="button">{t('radtrack-common_close-button')}</button>
    </header>

    {#if errorMessage}
      <p class="muted">{errorMessage}</p>
    {:else if !rows.length && loading}
      <p class="muted">{t('radtrack-map_points_modal_loading-label')}</p>
    {:else if !rows.length}
      <p class="muted">{t('radtrack-map_points_modal_empty')}</p>
    {:else}
      <div class="table-wrap map-cell-points-table-wrap">
        <table class="map-cell-points-table">
          <thead>
            <tr>
              {#each columns as column}
                <th>
                  <button
                    aria-label={getSortLabel({ column })}
                    class="map-cell-points-sort-button"
                    onclick={() => toggleSort({ column })}
                    type="button"
                  >
                    <span>{column.label}</span>
                    <span aria-hidden="true" class="map-cell-points-sort-indicator">
                      {getSortIndicator({ column })}
                    </span>
                  </button>
                </th>
              {/each}
            </tr>
          </thead>

          <tbody>
            {#each sortedRows as row}
              <tr>
                {#each columns as column}
                  {@const formattedValue = formatCellValue({
                    column,
                    value: row.values[column.propKey] ?? null
                  })}
                  <td>
                    <div class="map-cell-points-cell" title={formattedValue}>
                      {formattedValue}
                    </div>
                  </td>
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
</div>

<style>
  .map-cell-points-modal-shell {
    position: absolute;
    inset: 0;
    z-index: 700;
    display: grid;
    justify-items: center;
    align-items: start;
    padding: var(--space-3) var(--space-4) var(--space-4);
  }

  .map-cell-points-modal-backdrop {
    position: absolute;
    inset: 0;
    min-height: 0;
    padding: 0;
    border: none;
    border-radius: 0;
    background: color-mix(in srgb, var(--color-bg) 54%, transparent);
    backdrop-filter: blur(4px);
    box-shadow: none;
    transform: none;
  }

  .map-cell-points-modal-backdrop:hover {
    border: none;
    transform: none;
  }

  .map-cell-points-modal {
    position: relative;
    z-index: 1;
    width: min(75%, 72rem);
    max-width: calc(100% - (var(--space-4) * 2));
    max-height: min(var(--map-popup-max-height, 50vh), calc(100% - (var(--space-4) * 2)));
    display: grid;
    grid-template-rows: auto minmax(0, 1fr);
    gap: var(--space-3);
  }

  .map-cell-points-modal-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: var(--space-3);
  }

  .map-cell-points-modal-heading {
    min-width: 0;
    display: grid;
    gap: var(--space-2);
  }

  .map-cell-points-modal-title-row {
    display: flex;
    align-items: center;
    gap: var(--space-2);
    flex-wrap: wrap;
  }

  .map-cell-points-modal-title-row h2 {
    margin: 0;
  }

  .map-cell-points-modal-subtitle {
    margin: 0;
  }

  .map-cell-points-table-wrap {
    min-height: 0;
    height: 100%;
    overflow: auto;
  }

  .map-cell-points-table {
    width: max-content;
    min-width: 100%;
    table-layout: auto;
  }

  .map-cell-points-table thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: var(--color-panel);
  }

  .map-cell-points-sort-button {
    min-height: auto;
    width: 100%;
    padding: 0;
    border: none;
    border-radius: 0;
    border-bottom-width: 0;
    justify-content: flex-start;
    background: transparent;
    box-shadow: none;
    color: var(--color-text-muted);
    font-weight: 600;
    letter-spacing: 0.02em;
    text-align: left;
    transform: none;
  }

  .map-cell-points-sort-button:hover,
  .map-cell-points-sort-button:focus-visible,
  .map-cell-points-sort-button:active {
    border: none;
    background: transparent;
    box-shadow: none;
    color: var(--color-text);
    transform: none;
  }

  .map-cell-points-sort-indicator {
    min-width: 0.85rem;
    text-align: center;
  }

  .map-cell-points-cell {
    display: block;
    line-height: 1.35;
    white-space: nowrap;
  }

  @media (max-width: 720px) {
    .map-cell-points-modal-shell {
      padding-inline: var(--space-3);
    }

    .map-cell-points-modal {
      width: 75%;
    }

    .map-cell-points-modal-header {
      display: grid;
    }
  }
</style>
