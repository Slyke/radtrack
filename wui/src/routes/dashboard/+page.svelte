<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';

  let data = $state<any>(null);
  let errorMessage = $state<string | null>(null);

  const loadDashboard = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/dashboard' });
      data = response;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load dashboard';
    }
  };

  onMount(loadDashboard);
</script>

<div class="page-header">
  <div>
    <h1>Dashboard</h1>
    <p class="muted">Uploads, datasets, and recent audit activity.</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{:else if !data}
  <section class="panel">
    <p class="muted">Loading dashboard…</p>
  </section>
{:else}
  <section class="grid cols-3">
    <article class="metric-card">
      <div class="faint">Datasets</div>
      <h2>{data.stats.datasetCount}</h2>
    </article>
    <article class="metric-card">
      <div class="faint">Uploads</div>
      <h2>{data.stats.uploadCount}</h2>
    </article>
    <article class="metric-card">
      <div class="faint">Failed Imports</div>
      <h2>{data.stats.failedImportCount}</h2>
    </article>
  </section>

  <section class="grid cols-2">
    <article class="panel">
      <div class="page-header">
        <h2>Recent Datasets</h2>
        <a href="/datasets">Open</a>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Access</th>
              <th>Tracks</th>
            </tr>
          </thead>
          <tbody>
            {#each data.datasets as dataset}
              <tr>
                <td><a href={`/datasets/${dataset.id}`}>{dataset.name}</a></td>
                <td>{dataset.accessLevel}</td>
                <td>{dataset.trackCount}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>

    <article class="panel">
      <div class="page-header">
        <h2>Recent Uploads</h2>
        <a href="/import">Import</a>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>File</th>
              <th>Status</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            {#each data.uploads as upload}
              <tr>
                <td>{upload.originalFilename}</td>
                <td>{upload.status}</td>
                <td>{new Date(upload.uploadedAt).toLocaleString()}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
  </section>

  <section class="panel">
    <div class="page-header">
      <h2>Audit</h2>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Event</th>
            <th>Entity</th>
          </tr>
        </thead>
        <tbody>
          {#each data.audit as event}
            <tr>
              <td>{new Date(event.createdAt).toLocaleString()}</td>
              <td>{event.eventType}</td>
              <td>{event.entityType}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
{/if}
