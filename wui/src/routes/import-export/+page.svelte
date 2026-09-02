<svelte:options runes={true} />

<script lang="ts">
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });
</script>

<div class="page-header">
  <div>
    <div class="title-with-info">
      <h1>{t('radtrack-import_export_title')}</h1>
      <InfoTip text="Choose a supported workflow to bring track data into RadTrack or export data for another tool." />
    </div>
    <p class="muted">{t('radtrack-import_export_description')}</p>
  </div>
</div>

<section class="grid cols-2 import-export-layout">
  <article class="panel import-export-panel">
    <div class="grid import-export-copy">
      <div class="chip-row">
        <span class="chip subtle">{t('radtrack-import_export_json-chip')}</span>
      </div>
      <div>
        <div class="title-with-info">
          <h2>{t('radtrack-import_export_json-title')}</h2>
          <InfoTip text="Use RadTrack JSON for portable backups, interchange, and filtered raw or aggregate exports." />
        </div>
        <p class="muted">{t('radtrack-import_export_json-description')}</p>
      </div>
    </div>

    <div class="actions import-export-actions">
      <div class="control-with-info">
        <a class="button-link primary" href="/export/json">{t('radtrack-import_export_export_json-button')}</a>
        <InfoTip text="Open the JSON export builder to select sources, payload content, and aggregation settings." />
      </div>
      <div class="control-with-info">
        <a class="button-link mid" href="/import/json">{t('radtrack-import_export_import_json-button')}</a>
        <InfoTip text="Upload RadTrack JSON files or backup archives and create datasets from them." />
      </div>
    </div>
  </article>

  <article class="panel import-export-panel">
    <div class="grid import-export-copy">
      <div class="chip-row">
        <span class="chip subtle">{t('radtrack-import_export_radiacode-chip')}</span>
      </div>
      <div>
        <div class="title-with-info">
          <h2>{t('radtrack-import_export_radiacode-title')}</h2>
          <InfoTip text="Import native Radiacode tracks, archives, and supported bulk exports." />
        </div>
        <p class="muted">{t('radtrack-import_export_radiacode-description')}</p>
      </div>
    </div>

    <div class="actions import-export-actions">
      <div class="control-with-info">
        <a class="button-link primary" href="/import/radiacode">{t('radtrack-import_export_import_radiacode-button')}</a>
        <InfoTip text="Open the Radiacode upload workflow." />
      </div>
    </div>
  </article>
</section>

<style>
  .import-export-layout {
    align-items: stretch;
  }

  .import-export-panel {
    display: grid;
    gap: var(--space-4);
    align-content: space-between;
    min-height: 100%;
  }

  .import-export-copy {
    gap: var(--space-3);
  }

  .import-export-copy p {
    margin: var(--space-2) 0 0;
  }

  .import-export-actions {
    align-items: center;
  }

  @media (max-width: 900px) {
    .import-export-layout {
      grid-template-columns: 1fr;
    }
  }
</style>
