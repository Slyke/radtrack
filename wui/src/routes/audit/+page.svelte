<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';

  interface AuditEntry {
    id: string;
    actorUserId: string | null;
    actorUsername: string | null;
    scopeUserId: string | null;
    eventType: string;
    entityType: string;
    entityId: string | null;
    payload: unknown;
    createdAt: string | null;
  }

  interface AuditPageResponse {
    entries: AuditEntry[];
    limitValue: string;
    totalEntries: number;
    hasMore: boolean;
    filters: {
      indexes: [number, number];
    };
  }

  interface JsonModalState {
    title: string;
    description: string;
    value: string;
  }

  const jsonPreviewLimit = 180;

  let entries = $state<AuditEntry[]>([]);
  let errorMessage = $state<string | null>(null);
  let limit = $state('50');
  let totalEntries = $state(0);
  let hasMore = $state(false);
  let selectedJson = $state<JsonModalState | null>(null);

  const limitOptions = ['10', '20', '50', '100', '250', 'all'];

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const isRecord = (value: unknown): value is Record<string, unknown> => (
    Boolean(value) && typeof value === 'object' && !Array.isArray(value)
  );

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radtrack-common_none');

  const formatEntity = (entry: Pick<AuditEntry, 'entityType' | 'entityId'>) => (
    `${entry.entityType}${entry.entityId ? `:${entry.entityId}` : ''}`
  );

  const formatJson = (value: unknown) => JSON.stringify(value ?? {}, null, 2);

  const formatJsonPreview = (value: unknown) => {
    const compact = JSON.stringify(value ?? {});
    if (!compact) {
      return '{}';
    }

    return compact.length > jsonPreviewLimit
      ? `${compact.slice(0, jsonPreviewLimit - 3)}...`
      : compact;
  };

  const formatLimitLabel = (value: string) => (
    value === 'all' ? t('radtrack-common_all-label') : value
  );

  const extractMetadata = (payload: unknown) => (
    isRecord(payload) && isRecord(payload.metadata) && Object.keys(payload.metadata).length
      ? payload.metadata
      : null
  );

  const buildAuditExportHref = () => `/api/audit?limit=${encodeURIComponent(limit)}&download=1`;

  const openJsonModal = ({
    title,
    description,
    value
  }: JsonModalState) => {
    selectedJson = {
      title,
      description,
      value
    };
  };

  const openPayloadModal = (entry: AuditEntry) => {
    openJsonModal({
      title: t('radtrack-audit_payload_modal-title'),
      description: `${entry.eventType} · ${formatEntity(entry)} · ${formatTime(entry.createdAt)}`,
      value: formatJson(entry.payload)
    });
  };

  const openMetadataModal = (entry: AuditEntry) => {
    openJsonModal({
      title: t('radtrack-audit_metadata_modal-title'),
      description: `${entry.eventType} · ${formatEntity(entry)} · ${formatTime(entry.createdAt)}`,
      value: formatJson(extractMetadata(entry.payload))
    });
  };

  const closeJsonModal = () => {
    selectedJson = null;
  };

  const downloadAudit = () => {
    const anchor = document.createElement('a');
    anchor.href = buildAuditExportHref();
    anchor.click();
  };

  const loadEntries = async () => {
    errorMessage = null;

    try {
      const response = await apiFetch<AuditPageResponse>({
        path: '/api/audit',
        query: {
          limit
        }
      });
      entries = response.entries;
      totalEntries = response.totalEntries;
      hasMore = response.hasMore;
      limit = response.limitValue;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-audit_failed');
    }
  };

  onMount(loadEntries);
</script>

<div class="page-header">
  <div>
    <h1>{t('radtrack-audit_title')}</h1>
    <p class="muted">{t('radtrack-audit_description')}</p>
    <div class="chip-row audit-meta-row">
      <span class="chip subtle">
        {t('radtrack-audit_showing_summary', {
          limit: limit === 'all' ? t('radtrack-common_all-label') : limit
        })}
      </span>
      <span class="chip subtle">{t('radtrack-audit_total_entries-label', { count: totalEntries })}</span>
    </div>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="panel">
  <div class="page-header audit-toolbar">
    <h2>{t('radtrack-common_recent_audit-label')}</h2>

    <div class="actions audit-actions">
      <label class="audit-limit-field">
        <span class="muted">{t('radtrack-audit_limit-label')}</span>
        <select bind:value={limit} onchange={loadEntries}>
          {#each limitOptions as option}
            <option value={option}>{formatLimitLabel(option)}</option>
          {/each}
        </select>
      </label>

      <button class="primary audit-export-button" onclick={downloadAudit} type="button">{t('radtrack-audit_export-button')}</button>
    </div>
  </div>

  <div class="table-wrap">
    <table class="audit-table">
      <colgroup>
        <col />
        <col />
        <col />
        <col />
        <col class="audit-json-column" />
        <col class="audit-json-column" />
      </colgroup>
      <thead>
        <tr>
          <th>{t('radtrack-common_time-label')}</th>
          <th>{t('radtrack-common_actor-label')}</th>
          <th>{t('radtrack-common_event-label')}</th>
          <th>{t('radtrack-common_entity-label')}</th>
          <th>{t('radtrack-common_metadata-label')}</th>
          <th>{t('radtrack-common_payload-label')}</th>
        </tr>
      </thead>
      <tbody>
        {#if !entries.length}
          <tr>
            <td colspan="6" class="muted audit-empty-cell">{t('radtrack-audit_empty')}</td>
          </tr>
        {:else}
          {#each entries as entry}
            {@const metadata = extractMetadata(entry.payload)}
            <tr>
              <td>{formatTime(entry.createdAt)}</td>
              <td>{entry.actorUsername ?? entry.actorUserId ?? t('radtrack-common_none')}</td>
              <td>{entry.eventType}</td>
              <td>{formatEntity(entry)}</td>
              <td class="audit-json-cell">
                {#if metadata}
                  <button
                    class="json-preview-button"
                    onclick={() => openMetadataModal(entry)}
                    title={t('radtrack-audit_metadata_open-button')}
                    type="button"
                  >
                    <code class="json-preview-code">{formatJsonPreview(metadata)}</code>
                    <span class="json-preview-action faint">{t('radtrack-audit_metadata_open-button')}</span>
                  </button>
                {:else}
                  <span class="faint">{t('radtrack-common_none')}</span>
                {/if}
              </td>
              <td class="audit-json-cell">
                <button
                  class="json-preview-button"
                  onclick={() => openPayloadModal(entry)}
                  title={t('radtrack-audit_payload_open-button')}
                  type="button"
                >
                  <code class="json-preview-code">{formatJsonPreview(entry.payload)}</code>
                  <span class="json-preview-action faint">{t('radtrack-audit_payload_open-button')}</span>
                </button>
              </td>
            </tr>
          {/each}
        {/if}
      </tbody>
    </table>
  </div>

  {#if hasMore}
    <p class="muted audit-footer-note">{t('radtrack-audit_has_more')}</p>
  {/if}
</section>

{#if selectedJson}
  <div class="audit-modal-shell" role="presentation">
    <button
      aria-label={t('radtrack-common_close-button')}
      class="audit-modal-backdrop"
      onclick={closeJsonModal}
      type="button"
    ></button>

    <div
      aria-labelledby="audit-json-modal-title"
      aria-modal="true"
      class="panel audit-modal"
      role="dialog"
      tabindex="-1"
    >
      <div class="page-header audit-modal-header">
        <div>
          <h2 id="audit-json-modal-title">{selectedJson.title}</h2>
          <p class="muted">{selectedJson.description}</p>
        </div>
        <button onclick={closeJsonModal} type="button">{t('radtrack-common_close-button')}</button>
      </div>

      <textarea class="audit-modal-text" readonly rows="20" value={selectedJson.value}></textarea>
    </div>
  </div>
{/if}

<style>
  .audit-toolbar {
    align-items: end;
    margin-bottom: var(--space-3);
  }

  .audit-actions {
    align-items: end;
    gap: var(--space-3);
  }

  .audit-limit-field {
    display: grid;
    gap: var(--space-2);
  }

  .audit-limit-field select {
    min-width: 9.5rem;
  }

  .audit-export-button {
    align-self: end;
  }

  .audit-meta-row {
    margin-top: var(--space-3);
  }

  .audit-table {
    table-layout: fixed;
  }

  .audit-json-column {
    width: 24rem;
  }

  .audit-json-cell {
    min-width: 0;
    overflow: hidden;
  }

  .audit-empty-cell {
    padding: var(--space-5);
    text-align: center;
  }

  .json-preview-button {
    width: 100%;
    max-width: 100%;
    min-width: 0;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0;
    padding: 0.7rem 0.85rem;
    overflow: hidden;
    text-align: left;
  }

  .json-preview-code {
    display: block;
    width: 100%;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .json-preview-action {
    display: none;
    margin-top: var(--space-2);
  }

  .json-preview-button:hover .json-preview-action,
  .json-preview-button:focus-visible .json-preview-action {
    display: block;
  }

  .audit-footer-note {
    margin-top: var(--space-4);
  }

  .audit-modal-shell {
    position: fixed;
    inset: 0;
    z-index: 40;
    display: grid;
    place-items: center;
    padding: var(--space-4);
  }

  .audit-modal-backdrop {
    position: absolute;
    inset: 0;
    display: block;
    min-height: 0;
    padding: 0;
    border: none;
    border-radius: 0;
    background: color-mix(in srgb, var(--color-bg) 58%, transparent);
    backdrop-filter: blur(4px);
    box-shadow: none;
    transform: none;
  }

  .audit-modal-backdrop:hover {
    border: none;
    transform: none;
  }

  .audit-modal {
    position: relative;
    z-index: 1;
    width: min(72rem, 100%);
    max-height: min(90vh, 56rem);
    display: grid;
    gap: var(--space-3);
  }

  .audit-modal-header {
    align-items: flex-start;
  }

  .audit-modal-text {
    width: 100%;
    min-height: 18rem;
    resize: vertical;
  }

  @media (max-width: 960px) {
    .audit-toolbar,
    .audit-actions {
      align-items: stretch;
    }
  }
</style>
