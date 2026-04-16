<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { page } from '$app/stores';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let dataset = $state<any>(null);
  let shareTargets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let shareForm = $state({
    targetUserId: '',
    accessLevel: 'view'
  });
  let polygonForm = $state({
    label: '',
    pointsJson: '[\n  { "latitude": 41.93, "longitude": -87.85 },\n  { "latitude": 41.94, "longitude": -87.84 },\n  { "latitude": 41.92, "longitude": -87.82 }\n]'
  });
  let circleForm = $state({
    label: '',
    latitude: '41.93',
    longitude: '-87.85',
    radiusMeters: '250'
  });

  const datasetId = $derived($page.params.id);

  const loadDetail = async () => {
    try {
      const [datasetResponse, shareTargetsResponse] = await Promise.all([
        apiFetch<any>({ path: `/api/datasets/${datasetId}` }),
        apiFetch<any>({ path: '/api/share-targets' })
      ]);
      dataset = datasetResponse.dataset;
      shareTargets = shareTargetsResponse.users;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load dataset detail';
    }
  };

  const saveShare = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/datasets/${datasetId}/shares`,
      method: 'POST',
      body: shareForm,
      csrf: $sessionStore.csrf
    });
    await loadDetail();
  };

  const removeShare = async (shareId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/datasets/${datasetId}/shares/${shareId}`,
      method: 'DELETE',
      body: {},
      csrf: $sessionStore.csrf
    });
    await loadDetail();
  };

  const addPolygon = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/datasets/${datasetId}/exclude-areas`,
      method: 'POST',
      body: {
        shapeType: 'polygon',
        label: polygonForm.label,
        geometry: {
          points: JSON.parse(polygonForm.pointsJson)
        }
      },
      csrf: $sessionStore.csrf
    });
    await loadDetail();
  };

  const addCircle = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/datasets/${datasetId}/exclude-areas`,
      method: 'POST',
      body: {
        shapeType: 'circle',
        label: circleForm.label,
        geometry: {
          center: {
            latitude: Number(circleForm.latitude),
            longitude: Number(circleForm.longitude)
          },
          radiusMeters: Number(circleForm.radiusMeters)
        }
      },
      csrf: $sessionStore.csrf
    });
    await loadDetail();
  };

  const removeExcludeArea = async (excludeAreaId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/datasets/${datasetId}/exclude-areas/${excludeAreaId}`,
      method: 'DELETE',
      body: {},
      csrf: $sessionStore.csrf
    });
    await loadDetail();
  };

  onMount(loadDetail);
</script>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{:else if !dataset}
  <section class="panel">
    <p class="muted">Loading dataset…</p>
  </section>
{:else}
  <div class="page-header">
    <div>
      <h1>{dataset.name}</h1>
      <p class="muted">{dataset.description || 'No description'}</p>
    </div>
    <div class="chip-row">
      <span class="chip start">{dataset.accessLevel}</span>
      <a class="button-link" href="/map">Open map</a>
    </div>
  </div>

  <section class="grid cols-2">
    <article class="panel">
      <h2>Tracks</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Device</th>
              <th>Rows</th>
              <th>Warnings</th>
            </tr>
          </thead>
          <tbody>
            {#each dataset.tracks as track}
              <tr>
                <td>{track.trackName}</td>
                <td>{track.deviceIdentifierRaw || 'n/a'}</td>
                <td>{track.validRowCount}/{track.rowCount}</td>
                <td>{track.warningCount}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>

    <article class="panel">
      <h2>Sharing</h2>
      <div class="form-grid">
        <select bind:value={shareForm.targetUserId}>
          <option value="">Select user</option>
          {#each shareTargets as target}
            {#if target.id !== $sessionStore.user?.id}
              <option value={target.id}>{target.username} ({target.role})</option>
            {/if}
          {/each}
        </select>
        <select bind:value={shareForm.accessLevel}>
          <option value="view">View</option>
          <option value="edit">Edit</option>
        </select>
        <div class="actions">
          <button class="primary" onclick={saveShare}>Save share</button>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>User</th>
              <th>Access</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each dataset.shares as share}
              <tr>
                <td>{share.username}</td>
                <td>{share.accessLevel}</td>
                <td><button class="danger" onclick={() => removeShare(share.id)}>Remove</button></td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
  </section>

  <section class="grid cols-2">
    <article class="panel">
      <h2>Add Polygon Exclude Area</h2>
      <div class="form-grid">
        <input bind:value={polygonForm.label} placeholder="Label" />
        <textarea bind:value={polygonForm.pointsJson}></textarea>
        <button class="warning" onclick={addPolygon}>Add polygon</button>
      </div>
    </article>

    <article class="panel">
      <h2>Add Circle Exclude Area</h2>
      <div class="form-grid">
        <input bind:value={circleForm.label} placeholder="Label" />
        <input bind:value={circleForm.latitude} placeholder="Latitude" />
        <input bind:value={circleForm.longitude} placeholder="Longitude" />
        <input bind:value={circleForm.radiusMeters} placeholder="Radius meters" />
        <button class="warning" onclick={addCircle}>Add circle</button>
      </div>
    </article>
  </section>

  <section class="panel">
    <h2>Exclude Areas</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Label</th>
            <th>Shape</th>
            <th>Export default</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#each dataset.excludeAreas as area}
            <tr>
              <td>{area.label || 'Unnamed'}</td>
              <td>{area.shapeType}</td>
              <td>{area.applyByDefaultOnExport ? 'yes' : 'no'}</td>
              <td><button class="danger" onclick={() => removeExcludeArea(area.id)}>Delete</button></td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
{/if}
