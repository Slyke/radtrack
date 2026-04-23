<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let cellCacheRefreshTtlOnRead = $state(false);
  let cacheTtlSeconds = $state<number | null>(null);
  let errorMessage = $state<string | null>(null);
  let saveMessage = $state<string | null>(null);
  let saving = $state(false);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const loadSettings = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/user-settings' });
      cellCacheRefreshTtlOnRead = response.settings?.cellCacheRefreshTtlOnRead === true;
      cacheTtlSeconds = Number.isFinite(Number(response.aggregation?.cacheTtlSeconds))
        ? Number(response.aggregation.cacheTtlSeconds)
        : null;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-settings_failed_load');
    }
  };

  const saveSettings = async () => {
    if (!$sessionStore.csrf || saving) {
      return;
    }

    saving = true;
    errorMessage = null;
    saveMessage = null;
    try {
      const response = await apiFetch<any>({
        path: '/api/user-settings',
        method: 'PUT',
        body: {
          settings: {
            cellCacheRefreshTtlOnRead
          }
        },
        csrf: $sessionStore.csrf
      });
      cellCacheRefreshTtlOnRead = response.settings?.cellCacheRefreshTtlOnRead === true;
      saveMessage = t('radtrack-settings_saved');
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-settings_failed_save');
    } finally {
      saving = false;
    }
  };

  onMount(loadSettings);
</script>

<div class="page-header">
  <div>
    <h1>{t('radtrack-settings_title')}</h1>
    <p class="muted">{t('radtrack-settings_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

{#if saveMessage}
  <section class="panel">
    <p class="muted">{saveMessage}</p>
  </section>
{/if}

<section class="panel settings-panel">
  <h2>{t('radtrack-settings_cache-title')}</h2>
  <label class="checkbox-field">
    <input
      bind:checked={cellCacheRefreshTtlOnRead}
      onchange={() => {
        saveMessage = null;
      }}
      type="checkbox"
    />
    <span>{t('radtrack-settings_cell_cache_refresh_ttl_on_read-label')}</span>
  </label>
  <p class="muted">
    {#if cacheTtlSeconds === null}
      {t('radtrack-settings_cell_cache_refresh_ttl_on_read-description')}
    {:else}
      {t('radtrack-settings_cell_cache_refresh_ttl_on_read_seconds-description', {
        seconds: cacheTtlSeconds
      })}
    {/if}
  </p>
  <button class="primary" disabled={saving} onclick={saveSettings}>
    {t('radtrack-common_save-button')}
  </button>
</section>

<style>
  .settings-panel {
    display: grid;
    gap: var(--space-4);
    max-width: 48rem;
  }

  .checkbox-field {
    display: inline-flex;
    align-items: center;
    gap: var(--space-3);
    font-weight: 700;
  }

  .checkbox-field input {
    width: auto;
  }
</style>
