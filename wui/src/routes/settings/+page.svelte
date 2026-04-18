<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let bootstrapInfo = $state<any>(null);
  let settings = $state<any[]>([]);
  let updatesJson = $state('{\n  "map.defaultMetric": "dose_rate"\n}');
  let errorMessage = $state<string | null>(null);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const loadSettings = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/settings' });
      bootstrapInfo = response.bootstrap;
      settings = response.settings;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-settings_failed_load');
    }
  };

  const saveSettings = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: '/api/settings',
        method: 'PUT',
        body: {
          updates: JSON.parse(updatesJson)
        },
        csrf: $sessionStore.csrf
      });
      await loadSettings();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-settings_failed_save');
    }
  };

  onMount(loadSettings);
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-settings_title')}</h1>
    <p class="muted">{t('radiacode-settings_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>{t('radiacode-settings_bootstrap-title')}</h2>
    <pre>{JSON.stringify(bootstrapInfo, null, 2)}</pre>
  </article>

  <article class="panel">
    <h2>{t('radiacode-settings_update-title')}</h2>
    <div class="form-grid">
      <textarea bind:value={updatesJson}></textarea>
      <button class="primary" onclick={saveSettings}>{t('radiacode-common_save-button')}</button>
    </div>
  </article>
</section>

<section class="panel">
  <h2>{t('radiacode-settings_current_runtime-title')}</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>{t('radiacode-common_name-label')}</th>
          <th>{t('radiacode-common_value-label')}</th>
          <th>{t('radiacode-common_source-label')}</th>
        </tr>
      </thead>
      <tbody>
        {#each settings as setting}
          <tr>
            <td>{setting.key}</td>
            <td><code>{JSON.stringify(setting.value)}</code></td>
            <td>{setting.source}</td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
