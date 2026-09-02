<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let bootstrapInfo = $state<any>(null);
  let settings = $state<any[]>([]);
  let updatesJson = $state('{\n  "map.defaultMetric": "doseRate"\n}');
  let errorMessage = $state<string | null>(null);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const loadSettings = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/admin/settings' });
      bootstrapInfo = response.bootstrap;
      settings = response.settings;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-admin_settings_failed_load');
    }
  };

  const saveSettings = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: '/api/admin/settings',
        method: 'PUT',
        body: {
          updates: JSON.parse(updatesJson)
        },
        csrf: $sessionStore.csrf
      });
      await loadSettings();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-admin_settings_failed_save');
    }
  };

  onMount(loadSettings);
</script>

<div class="page-header">
  <div>
    <div class="title-with-info">
      <h1>{t('radtrack-admin_settings_title')}</h1>
      <InfoTip text="Inspect bootstrap configuration and override mutable runtime settings for the whole RadTrack deployment." />
    </div>
    <p class="muted">{t('radtrack-admin_settings_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <div class="title-with-info">
      <h2>{t('radtrack-admin_settings_bootstrap-title')}</h2>
      <InfoTip text="Read-only startup configuration loaded from the deployment’s config and secrets sources." />
    </div>
    <pre>{JSON.stringify(bootstrapInfo, null, 2)}</pre>
  </article>

  <article class="panel">
    <div class="title-with-info">
      <h2>{t('radtrack-admin_settings_update-title')}</h2>
      <InfoTip text="Submit a JSON object of runtime setting keys and values. These changes affect all users." />
    </div>
    <div class="form-grid">
      <label>
        <span class="muted field-title">
          {t('radtrack-common_payload-label')}
          <InfoTip text="Enter valid JSON containing only the runtime keys you want to replace." />
        </span>
        <textarea bind:value={updatesJson}></textarea>
      </label>
      <div class="control-with-info">
        <button class="primary" onclick={saveSettings}>{t('radtrack-common_save-button')}</button>
        <InfoTip text="Validate and persist these system-wide runtime overrides." />
      </div>
    </div>
  </article>
</section>

<section class="panel">
  <div class="title-with-info">
    <h2>{t('radtrack-admin_settings_current_runtime-title')}</h2>
    <InfoTip text="The effective mutable settings, including their current values and whether they came from bootstrap defaults or a user override." />
  </div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>{t('radtrack-common_name-label')}</th>
          <th>{t('radtrack-common_value-label')}</th>
          <th>{t('radtrack-common_source-label')}</th>
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
