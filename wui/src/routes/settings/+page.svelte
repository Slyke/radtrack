<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore, updateSessionUserSettings } from '$lib/stores/session';
  import type { UserSettings } from '$lib/types';

  type UserSettingsResponse = {
    settings: UserSettings;
    aggregation?: {
      cacheTtlSeconds?: number | null;
    };
  };

  let cellCacheRefreshTtlOnRead = $state(false);
  let showInfoIcons = $state(true);
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
      const response = await apiFetch<UserSettingsResponse>({ path: '/api/user-settings' });
      cellCacheRefreshTtlOnRead = response.settings?.cellCacheRefreshTtlOnRead === true;
      showInfoIcons = response.settings?.showInfoIcons !== false;
      updateSessionUserSettings({ userSettings: response.settings });
      const loadedCacheTtlSeconds = response.aggregation?.cacheTtlSeconds;
      cacheTtlSeconds = Number.isFinite(Number(loadedCacheTtlSeconds))
        ? Number(loadedCacheTtlSeconds)
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
      const response = await apiFetch<UserSettingsResponse>({
        path: '/api/user-settings',
        method: 'PUT',
        body: {
          settings: {
            cellCacheRefreshTtlOnRead,
            showInfoIcons
          }
        },
        csrf: $sessionStore.csrf
      });
      cellCacheRefreshTtlOnRead = response.settings?.cellCacheRefreshTtlOnRead === true;
      showInfoIcons = response.settings?.showInfoIcons !== false;
      updateSessionUserSettings({ userSettings: response.settings });
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
    <div class="title-with-info">
      <h1>{t('radtrack-settings_title')}</h1>
      <InfoTip text="Manage preferences that apply only to your signed-in RadTrack account." />
    </div>
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
  <div class="title-with-info">
    <h2>{t('radtrack-settings_appearance-title')}</h2>
    <InfoTip text="Control optional guidance displayed throughout the interface." />
  </div>
  <label class="checkbox-field">
    <input
      bind:checked={showInfoIcons}
      onchange={() => {
        saveMessage = null;
      }}
      type="checkbox"
    />
    <span class="field-title">
      {t('radtrack-settings_show_info_icons-label')}
      <InfoTip text={t('radtrack-settings_show_info_icons-description')} />
    </span>
  </label>

  <div class="title-with-info settings-section-title">
    <h2>{t('radtrack-settings_cache-title')}</h2>
    <InfoTip text="Choose how your aggregate map requests interact with cached cells." />
  </div>
  <label class="checkbox-field">
    <input
      bind:checked={cellCacheRefreshTtlOnRead}
      onchange={() => {
        saveMessage = null;
      }}
      type="checkbox"
    />
    <span class="field-title">
      {t('radtrack-settings_cell_cache_refresh_ttl_on_read-label')}
      <InfoTip text="When enabled, every cache hit extends that cell’s expiration time for your aggregate map queries." />
    </span>
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
  <div class="control-with-info">
    <button class="primary" disabled={saving} onclick={saveSettings}>
      {t('radtrack-common_save-button')}
    </button>
    <InfoTip text="Save both guidance and map-cache preferences to your user account." />
  </div>
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
