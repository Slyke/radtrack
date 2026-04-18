<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import ExcludeAreaEditorMap from '$lib/components/ExcludeAreaEditorMap.svelte';
  import { apiFetch } from '$lib/api/client';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  type Coordinate = {
    latitude: number;
    longitude: number;
  };

  let dataset = $state<any>(null);
  let shareTargets = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let shareForm = $state({
    targetUserId: '',
    accessLevel: 'view'
  });
  let liveTrackForm = $state({
    name: ''
  });
  let excludeAreaForm = $state({
    label: '',
    applyByDefaultOnExport: false,
    effectType: 'hard_remove',
    compressMinPoints: '2',
    compressMaxPoints: '20'
  });
  let circleForm = $state({
    latitude: '',
    longitude: '',
    radiusMeters: '250'
  });

  const datasetId = $derived($page.params.id);
  const draftCircleCenter = $derived.by<Coordinate | null>(() => {
    const latitude = Number(circleForm.latitude);
    const longitude = Number(circleForm.longitude);

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
      return null;
    }

    return {
      latitude,
      longitude
    };
  });
  const draftCircleRadiusMeters = $derived.by(() => {
    const radius = Number(circleForm.radiusMeters);
    return Number.isFinite(radius) && radius > 0 ? radius : 250;
  });
  const draftCompressMinPoints = $derived.by(() => {
    const value = Number(excludeAreaForm.compressMinPoints);
    return Number.isInteger(value) && value >= 0 ? value : 2;
  });
  const draftCompressMaxPoints = $derived.by(() => {
    const value = Number(excludeAreaForm.compressMaxPoints);
    return Number.isInteger(value) && value >= draftCompressMinPoints ? value : 20;
  });

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const trackSourceLabel = (sourceType: string) => t('radiacode-track_source_type-label', {
    type: t(`radiacode-track_source_type-${sourceType}`)
  });

  const loadDetail = async () => {
    try {
      const [datasetResponse, shareTargetsResponse] = await Promise.all([
        apiFetch<any>({ path: `/api/datasets/${datasetId}` }),
        apiFetch<any>({ path: '/api/share-targets' })
      ]);
      dataset = datasetResponse.dataset;
      shareTargets = shareTargetsResponse.users;
      errorMessage = null;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_detail_failed');
    }
  };

  const createLiveTrack = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}/live-tracks`,
        method: 'POST',
        body: liveTrackForm,
        csrf: $sessionStore.csrf
      });
      liveTrackForm = {
        name: ''
      };
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_live_track_failed');
    }
  };

  const saveShare = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}/shares`,
        method: 'POST',
        body: shareForm,
        csrf: $sessionStore.csrf
      });
      shareForm = {
        targetUserId: '',
        accessLevel: 'view'
      };
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_share_failed');
    }
  };

  const removeShare = async (shareId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}/shares/${shareId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_share_failed');
    }
  };

  const resetExcludeAreaDraft = () => {
    excludeAreaForm = {
      label: '',
      applyByDefaultOnExport: false,
      effectType: 'hard_remove',
      compressMinPoints: '2',
      compressMaxPoints: '20'
    };
    circleForm = {
      latitude: '',
      longitude: '',
      radiusMeters: '250'
    };
  };

  const saveCircle = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    if (!draftCircleCenter) {
      errorMessage = t('radiacode-dataset_exclude_circle_center_required');
      return;
    }

    if (!(draftCircleRadiusMeters > 0)) {
      errorMessage = t('radiacode-dataset_exclude_circle_radius_required');
      return;
    }

    if (excludeAreaForm.effectType === 'compress' && draftCompressMaxPoints < draftCompressMinPoints) {
      errorMessage = t('radiacode-dataset_exclude_compress_invalid_range');
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}/exclude-areas`,
        method: 'POST',
        body: {
          shapeType: 'circle',
          label: excludeAreaForm.label,
          applyByDefaultOnExport: excludeAreaForm.applyByDefaultOnExport,
          effectType: excludeAreaForm.effectType,
          compressMinPoints: excludeAreaForm.effectType === 'compress' ? draftCompressMinPoints : null,
          compressMaxPoints: excludeAreaForm.effectType === 'compress' ? draftCompressMaxPoints : null,
          geometry: {
            center: draftCircleCenter,
            radiusMeters: draftCircleRadiusMeters
          }
        },
        csrf: $sessionStore.csrf
      });
      resetExcludeAreaDraft();
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_exclude_area_failed');
    }
  };

  const handleExcludeMapClick = (event: CustomEvent<Coordinate>) => {
    circleForm = {
      ...circleForm,
      latitude: event.detail.latitude.toFixed(6),
      longitude: event.detail.longitude.toFixed(6)
    };
  };

  const removeExcludeArea = async (excludeAreaId: string, label: string | null) => {
    if (!browser || !$sessionStore.csrf) {
      return;
    }

    if (!window.confirm(t('radiacode-dataset_exclude_delete_confirm', {
      name: label || t('radiacode-common_unnamed-label')
    }))) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}/exclude-areas/${excludeAreaId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_exclude_area_failed');
    }
  };

  const deleteTrack = async ({ trackId, trackName }: { trackId: string; trackName: string | null }) => {
    if (!browser || !$sessionStore.csrf) {
      return;
    }

    if (!window.confirm(t('radiacode-track_delete_confirm', {
      name: trackName || t('radiacode-layout_track_page-label')
    }))) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadDetail();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_delete');
    }
  };

  const deleteDataset = async () => {
    if (!browser || !$sessionStore.csrf || !dataset) {
      return;
    }

    if (!window.confirm(t('radiacode-dataset_delete_confirm', { name: dataset.name }))) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datasets/${datasetId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await goto('/datasets');
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-dataset_detail_failed_delete');
    }
  };

  onMount(loadDetail);
</script>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{:else if !dataset}
  <section class="panel">
    <p class="muted">{t('radiacode-common_loading_dataset')}</p>
  </section>
{:else}
  <div class="page-header">
    <div>
      <h1>{dataset.name}</h1>
      <p class="muted">{dataset.description || t('radiacode-common_no-description')}</p>
    </div>
    <div class="chip-row">
      <span class="chip start">{dataset.accessLevel}</span>
      <a class="button-link" href="/map">{t('radiacode-dataset_detail_open_map-button')}</a>
      {#if dataset.accessLevel === 'edit'}
        <button class="danger" onclick={deleteDataset}>{t('radiacode-common_danger-delete-button')}</button>
      {/if}
    </div>
  </div>

  <section class="panel">
    <div class="page-header">
      <h2>{t('radiacode-common_tracks-label')}</h2>
      {#if dataset.accessLevel === 'edit'}
        <a class="button-link" href="#live-track-form">{t('radiacode-dataset_live_track_create-button')}</a>
      {/if}
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_name-label')}</th>
            <th>{t('radiacode-common_source-label')}</th>
            <th>{t('radiacode-common_device-label')}</th>
            <th>{t('radiacode-common_rows-label')}</th>
            <th>{t('radiacode-common_warnings-label')}</th>
            {#if dataset.accessLevel === 'edit'}
              <th>{t('radiacode-common_actions-label')}</th>
            {/if}
          </tr>
        </thead>
        <tbody>
          {#if !dataset.tracks.length}
            <tr>
              <td colspan={dataset.accessLevel === 'edit' ? 6 : 5} class="muted">{t('radiacode-dataset_tracks_empty')}</td>
            </tr>
          {:else}
            {#each dataset.tracks as track}
              <tr>
                <td>
                  <div class="form-grid">
                    <a href={`/tracks/${track.id}`}>{track.trackName}</a>
                    {#if track.sourceType === 'live' && track.ingestTrackId}
                      <code>{track.ingestTrackId}</code>
                    {/if}
                  </div>
                </td>
                <td>{trackSourceLabel(track.sourceType)}</td>
                <td>{track.deviceIdentifierRaw || t('radiacode-common_na-label')}</td>
                <td>{track.validRowCount}/{track.rowCount}</td>
                <td>{track.warningCount}</td>
                {#if dataset.accessLevel === 'edit'}
                  <td>
                    <div class="actions">
                      <a class="button-link" href={`/tracks/${track.id}`}>{t('radiacode-common_edit-label')}</a>
                      <button
                        class="danger"
                        onclick={() => deleteTrack({ trackId: track.id, trackName: track.trackName })}
                      >
                        {t('radiacode-common_danger-delete-button')}
                      </button>
                    </div>
                  </td>
                {/if}
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>

    {#if dataset.accessLevel === 'edit'}
      <div class="form-grid" id="live-track-form">
        <h3>{t('radiacode-dataset_live_track_create-title')}</h3>
        <p class="muted">{t('radiacode-dataset_live_track_description')}</p>
        <input bind:value={liveTrackForm.name} placeholder={t('radiacode-dataset_live_track_name-placeholder')} />
        <div class="actions">
          <button class="primary" onclick={createLiveTrack}>{t('radiacode-dataset_live_track_create-button')}</button>
        </div>
      </div>
    {/if}
  </section>

  <section class="panel">
    <details class="sharing-accordion">
      <summary>
        <span>{t('radiacode-dataset_sharing-title')}</span>
        <span class="exclude-editor-summary">
          <span class="chip subtle">{dataset.shares.length}</span>
          <span aria-hidden="true" class="exclude-editor-icon"></span>
        </span>
      </summary>

      <div class="form-grid">
        <p class="muted">{t('radiacode-dataset_sharing_dataset_help')}</p>

        {#if dataset.accessLevel === 'edit'}
          <div class="form-grid sharing-controls">
            <select bind:value={shareForm.targetUserId}>
              <option value="">{t('radiacode-common_select_user-option')}</option>
              {#each shareTargets as target}
                {#if target.id !== $sessionStore.user?.id}
                  <option value={target.id}>{target.username} ({target.role})</option>
                {/if}
              {/each}
            </select>
            <select bind:value={shareForm.accessLevel}>
              <option value="view">{t('radiacode-common_view-label')}</option>
              <option value="edit">{t('radiacode-common_edit-label')}</option>
            </select>
            <div class="actions">
              <button class="primary" onclick={saveShare}>{t('radiacode-common_share-button')}</button>
            </div>
          </div>
        {/if}

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{t('radiacode-common_user-label')}</th>
                <th>{t('radiacode-common_access-label')}</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {#if !dataset.shares.length}
                <tr>
                  <td colspan="3" class="muted">{t('radiacode-dataset_sharing_empty')}</td>
                </tr>
              {:else}
                {#each dataset.shares as share}
                  <tr>
                    <td>{share.username}</td>
                    <td>{share.accessLevel}</td>
                    <td>
                      {#if dataset.accessLevel === 'edit'}
                        <button class="danger" onclick={() => removeShare(share.id)}>{t('radiacode-common_remove-button')}</button>
                      {/if}
                    </td>
                  </tr>
                {/each}
              {/if}
            </tbody>
          </table>
        </div>

        <p class="muted">{t('radiacode-dataset_sharing_track_help')}</p>
      </div>
    </details>
  </section>

  {#if dataset.accessLevel === 'edit'}
    <section class="panel">
      <details class="exclude-editor-accordion">
        <summary>
          <span>{t('radiacode-dataset_exclude_editor-title')}</span>
          <span class="exclude-editor-summary">
            <span class="chip subtle">{t('radiacode-common_circle-label')}</span>
            <span aria-hidden="true" class="exclude-editor-icon"></span>
          </span>
        </summary>

        <div class="exclude-editor-layout">
          <div class="exclude-editor-map-wrap">
            <ExcludeAreaEditorMap
              areas={dataset.excludeAreas}
              attribution={$sessionStore.ui?.attribution ?? ''}
              draftCircleCenter={draftCircleCenter}
              draftCircleRadiusMeters={draftCircleRadiusMeters}
              fallbackCenter={dataset.mapFocusPoint}
              on:mapclick={handleExcludeMapClick}
              tileUrlTemplate={$sessionStore.ui?.tileUrlTemplate ?? ''}
            />
          </div>

          <div class="form-grid">
            <p class="muted">{t('radiacode-dataset_exclude_editor-description')}</p>
            <span class="chip subtle">{t('radiacode-common_circle-label')}</span>

            <label>
              <div class="muted">{t('radiacode-common_label-label')}</div>
              <input bind:value={excludeAreaForm.label} placeholder={t('radiacode-common_label-placeholder')} />
            </label>

            <label>
              <div class="muted">{t('radiacode-common_type-label')}</div>
              <select bind:value={excludeAreaForm.effectType}>
                <option value="hard_remove">{t('radiacode-dataset_exclude_effect_hard_remove')}</option>
                <option value="compress">{t('radiacode-dataset_exclude_effect_compress')}</option>
              </select>
            </label>

            <label class="checkbox-field">
              <input bind:checked={excludeAreaForm.applyByDefaultOnExport} type="checkbox" />
              <span>{t('radiacode-common_apply_export_default-label')}</span>
            </label>

            <div class="form-grid exclude-mode-panel">
              <p class="muted">{t('radiacode-dataset_exclude_circle_help')}</p>
              <input bind:value={circleForm.latitude} placeholder={t('radiacode-common_latitude-label')} />
              <input bind:value={circleForm.longitude} placeholder={t('radiacode-common_longitude-label')} />
              <input bind:value={circleForm.radiusMeters} placeholder={t('radiacode-common_radius_meters-placeholder')} />

              {#if excludeAreaForm.effectType === 'compress'}
                <div class="grid cols-2">
                  <input bind:value={excludeAreaForm.compressMinPoints} placeholder={t('radiacode-dataset_exclude_compress_min-placeholder')} />
                  <input bind:value={excludeAreaForm.compressMaxPoints} placeholder={t('radiacode-dataset_exclude_compress_max-placeholder')} />
                </div>
                <p class="muted">{t('radiacode-dataset_exclude_compress_help')}</p>
              {/if}

              <div class="actions">
                <button onclick={() => {
                  circleForm = {
                    ...circleForm,
                    latitude: '',
                    longitude: ''
                  };
                }}>
                  {t('radiacode-common_clear-button')}
                </button>
              </div>
              <button class="warning" onclick={saveCircle} disabled={!draftCircleCenter}>
                {t('radiacode-common_add_circle-button')}
              </button>
            </div>

            <button onclick={resetExcludeAreaDraft}>{t('radiacode-dataset_exclude_reset-button')}</button>
          </div>
        </div>
      </details>
    </section>
  {/if}

  <section class="panel">
    <h2>{t('radiacode-dataset_exclude_areas-title')}</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_label-label')}</th>
            <th>{t('radiacode-common_shape-label')}</th>
            <th>{t('radiacode-common_type-label')}</th>
            <th>{t('radiacode-common_export_default-label')}</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {#if !dataset.excludeAreas.length}
            <tr>
              <td colspan="5" class="muted">{t('radiacode-dataset_exclude_empty')}</td>
            </tr>
          {:else}
            {#each dataset.excludeAreas as area}
              <tr>
                <td>{area.label || t('radiacode-common_unnamed-label')}</td>
                <td>{t(`radiacode-common_${area.shapeType}-label`)}</td>
                <td>
                  {area.effectType === 'compress'
                    ? t('radiacode-dataset_exclude_effect_compress_with_range', {
                      min: area.compressMinPoints ?? 2,
                      max: area.compressMaxPoints ?? 20
                    })
                    : t('radiacode-dataset_exclude_effect_hard_remove')}
                </td>
                <td>{area.applyByDefaultOnExport ? t('radiacode-common_yes-label') : t('radiacode-common_no-label')}</td>
                <td>
                  {#if dataset.accessLevel === 'edit'}
                    <button class="danger" onclick={() => removeExcludeArea(area.id, area.label)}>
                      {t('radiacode-common_danger-delete-button')}
                    </button>
                  {/if}
                </td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
{/if}

<style>
  .sharing-accordion,
  .exclude-editor-accordion {
    display: grid;
    gap: var(--space-4);
  }

  .sharing-accordion summary,
  .exclude-editor-accordion summary {
    list-style: none;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    cursor: pointer;
    padding: 0.8rem 0.9rem;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .sharing-accordion summary::-webkit-details-marker,
  .exclude-editor-accordion summary::-webkit-details-marker {
    display: none;
  }

  .sharing-accordion[open] summary,
  .exclude-editor-accordion[open] summary {
    margin-bottom: var(--space-3);
  }

  .sharing-controls {
    grid-template-columns: minmax(0, 1.5fr) minmax(10rem, 0.8fr) auto;
    align-items: end;
  }

  .exclude-editor-summary {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
  }

  .exclude-editor-icon::before {
    content: '>';
    display: inline-block;
    color: var(--color-text-muted);
    transition: transform var(--transition), color var(--transition);
  }

  .exclude-editor-accordion[open] .exclude-editor-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .exclude-editor-layout {
    display: grid;
    grid-template-columns: minmax(0, 1.65fr) minmax(18rem, 1fr);
    gap: var(--space-4);
  }

  .exclude-editor-map-wrap {
    min-height: 24rem;
  }

  .exclude-mode-panel {
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .checkbox-field {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: center;
    gap: var(--space-3);
  }

  .checkbox-field input {
    width: auto;
    margin: 0;
  }

  @media (max-width: 1024px) {
    .sharing-controls,
    .exclude-editor-layout {
      grid-template-columns: 1fr;
    }

    .exclude-editor-map-wrap {
      min-height: 20rem;
    }
  }
</style>
