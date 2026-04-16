<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let bootstrapInfo = $state<any>(null);
  let settings = $state<any[]>([]);
  let updatesJson = $state('{\n  "map.defaultMetric": "dose_rate"\n}');
  let errorMessage = $state<string | null>(null);

  const loadSettings = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/settings' });
      bootstrapInfo = response.bootstrap;
      settings = response.settings;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load settings';
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
      errorMessage = error instanceof Error ? error.message : 'Failed to save settings';
    }
  };

  onMount(loadSettings);
</script>

<div class="page-header">
  <div>
    <h1>Settings</h1>
    <p class="muted">Bootstrap auth flags stay file-driven. Mutable runtime settings are DB-backed.</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

<section class="grid cols-2">
  <article class="panel">
    <h2>Bootstrap</h2>
    <pre>{JSON.stringify(bootstrapInfo, null, 2)}</pre>
  </article>

  <article class="panel">
    <h2>Update Settings</h2>
    <div class="form-grid">
      <textarea bind:value={updatesJson}></textarea>
      <button class="primary" onclick={saveSettings}>Save</button>
    </div>
  </article>
</section>

<section class="panel">
  <h2>Current Runtime Settings</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Key</th>
          <th>Value</th>
          <th>Source</th>
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
