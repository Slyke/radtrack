<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import { apiFetch, resolveApiPath } from '$lib/api/client';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let track = $state<any>(null);
  let errorMessage = $state<string | null>(null);
  let generatedKey = $state<string | null>(null);
  let shareTargets = $state<any[]>([]);
  let keyForm = $state({
    label: '',
    notes: ''
  });
  let shareForm = $state({
    targetUserId: '',
    accessLevel: 'view'
  });
  let readingOffset = $state(0);
  let editingReadingId = $state<string | null>(null);
  let savingReading = $state(false);
  let restoringOriginal = $state(false);
  let readingForm = $state({
    occurredAt: '',
    latitude: '',
    longitude: '',
    altitudeMeters: '',
    accuracy: '',
    usv: '',
    countRate: '',
    comment: ''
  });

  const readingPageLimit = 100;

  const trackId = $derived($page.params.trackId);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radiacode-common_none');

  const formatNumber = (value: number | null | undefined, digits = 6) => {
    if (value === null || value === undefined) {
      return t('radiacode-common_na-label');
    }

    return new Intl.NumberFormat($localeStore.language, {
      maximumFractionDigits: digits
    }).format(value);
  };

  const trackSourceLabel = (sourceType: string) => t('radiacode-track_source_type-label', {
    type: t(`radiacode-track_source_type-${sourceType}`)
  });

  const endpointUrl = $derived(
    !track?.ingest?.endpointPath
      ? ''
      : (
        (() => {
          const path = resolveApiPath({ path: track.ingest.endpointPath });
          if (!browser) {
            return path;
          }

          return path.startsWith('http://') || path.startsWith('https://')
            ? path
            : new URL(path, window.location.origin).toString();
        })()
      )
  );

  const curlExample = $derived(
    !track?.ingest
      ? ''
      : `curl -X POST '${endpointUrl}' \\
  -H 'Content-Type: application/json' \\
  -H '${track.ingest.headerName}: ${generatedKey ?? '<generated-key>'}' \\
  --data '${JSON.stringify({
    occurredAt: '2026-04-17T19:15:00.000Z',
    latitude: 49.2827,
    longitude: -123.1207,
    accuracy: 4.2,
    altitudeMeters: 18.5,
    usv: 0.11,
    countRate: 84,
    deviceId: 'rc-001',
    deviceName: 'Pocket Gamma',
    deviceType: 'Radiacode 102',
    deviceCalibration: 'factory-2026-01',
    temperatureC: 21.3,
    humidityPct: 42,
    pressureHpa: 1014.6,
    batteryPct: 88,
    firmwareVersion: '1.2.3',
    sourceReadingId: 'reading-000123',
    comment: 'Delayed upload after reconnect',
    custom: '{"trip":"harbour"}',
    extra: {
      gpsFix: '3d',
      satelliteCount: 14
    }
  })}'`
  );

  const payloadExample = JSON.stringify({
    occurredAt: '2026-04-17T19:15:00.000Z',
    latitude: 49.2827,
    longitude: -123.1207,
    accuracy: 4.2,
    altitudeMeters: 18.5,
    usv: 0.11,
    countRate: 84,
    deviceId: 'rc-001',
    deviceName: 'Pocket Gamma',
    deviceType: 'Radiacode 102',
    deviceCalibration: 'factory-2026-01',
    temperatureC: 21.3,
    humidityPct: 42,
    pressureHpa: 1014.6,
    batteryPct: 88,
    firmwareVersion: '1.2.3',
    sourceReadingId: 'reading-000123',
    comment: 'Delayed upload after reconnect',
    custom: '{"trip":"harbour"}',
    extra: {
      gpsFix: '3d',
      satelliteCount: 14
    }
  }, null, 2);

  const readingPageStart = $derived((track?.readingsPage?.offset ?? 0) + 1);
  const readingPageEnd = $derived(Math.min(
    (track?.readingsPage?.offset ?? 0) + (track?.readingsPage?.readings?.length ?? 0),
    track?.readingsPage?.totalCount ?? 0
  ));

  const resetReadingForm = () => {
    editingReadingId = null;
    readingForm = {
      occurredAt: '',
      latitude: '',
      longitude: '',
      altitudeMeters: '',
      accuracy: '',
      usv: '',
      countRate: '',
      comment: ''
    };
  };

  const startEditingReading = (reading: any) => {
    editingReadingId = reading.id;
    readingForm = {
      occurredAt: reading.occurredAt ?? '',
      latitude: reading.latitude === null || reading.latitude === undefined ? '' : String(reading.latitude),
      longitude: reading.longitude === null || reading.longitude === undefined ? '' : String(reading.longitude),
      altitudeMeters: reading.altitudeMeters === null || reading.altitudeMeters === undefined ? '' : String(reading.altitudeMeters),
      accuracy: reading.accuracy === null || reading.accuracy === undefined ? '' : String(reading.accuracy),
      usv: reading.usv === null || reading.usv === undefined ? '' : String(reading.usv),
      countRate: reading.countRate === null || reading.countRate === undefined ? '' : String(reading.countRate),
      comment: reading.comment ?? ''
    };
  };

  const loadTrack = async ({ offset = readingOffset } = {}) => {
    try {
      const [trackResponse, shareTargetsResponse] = await Promise.all([
        apiFetch<any>({
          path: `/api/tracks/${trackId}`,
          query: {
            limit: readingPageLimit,
            offset
          }
        }),
        apiFetch<any>({ path: '/api/share-targets' })
      ]);
      track = trackResponse.track;
      shareTargets = shareTargetsResponse.users;
      readingOffset = offset;
      errorMessage = null;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed');
    }
  };

  const createKey = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: `/api/tracks/${trackId}/ingest-keys`,
        method: 'POST',
        body: keyForm,
        csrf: $sessionStore.csrf
      });
      generatedKey = response.result.plaintextKey;
      keyForm = {
        label: '',
        notes: ''
      };
      await loadTrack();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_create_key');
    }
  };

  const revokeKey = async (ingestKeyId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}/ingest-keys/${ingestKeyId}/revoke`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      await loadTrack();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_revoke_key');
    }
  };

  const rotateKey = async (ingestKeyId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: `/api/tracks/${trackId}/ingest-keys/${ingestKeyId}/rotate`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      generatedKey = response.result.replacement.plaintextKey;
      await loadTrack();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_rotate_key');
    }
  };

  const deleteTrack = async () => {
    if (!browser || !$sessionStore.csrf || track?.accessLevel !== 'edit') {
      return;
    }

    if (!window.confirm(t('radiacode-track_delete_confirm', { name: track.trackName }))) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await goto(`/datasets/${track.dataset.id}`);
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_delete');
    }
  };

  const saveReading = async (readingId: string) => {
    if (!$sessionStore.csrf || savingReading) {
      return;
    }

    savingReading = true;

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}/readings/${readingId}`,
        method: 'PATCH',
        body: readingForm,
        csrf: $sessionStore.csrf
      });
      resetReadingForm();
      await loadTrack({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed');
    } finally {
      savingReading = false;
    }
  };

  const restoreOriginal = async () => {
    if (!browser || !$sessionStore.csrf || !track?.canRestoreOriginal || restoringOriginal) {
      return;
    }

    if (!window.confirm(t('radiacode-track_restore_original_confirm', { name: track.trackName }))) {
      return;
    }

    restoringOriginal = true;

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}/restore-original`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      resetReadingForm();
      await loadTrack({ offset: 0 });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_restore_original');
    } finally {
      restoringOriginal = false;
    }
  };

  const saveShare = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}/shares`,
        method: 'POST',
        body: shareForm,
        csrf: $sessionStore.csrf
      });
      shareForm = {
        targetUserId: '',
        accessLevel: 'view'
      };
      await loadTrack({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_share');
    }
  };

  const removeShare = async (shareId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    try {
      await apiFetch({
        path: `/api/tracks/${trackId}/shares/${shareId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadTrack({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-track_failed_share');
    }
  };

  onMount(loadTrack);
</script>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{:else if !track}
  <section class="panel">
    <p class="muted">{t('radiacode-common_loading-label')}…</p>
  </section>
{:else}
  <div class="page-header">
    <div>
      <h1>{track.trackName}</h1>
      <p class="muted">{t('radiacode-track_description')}</p>
    </div>
    <div class="chip-row">
      <span class="chip start">{trackSourceLabel(track.sourceType)}</span>
      <span class="chip mid">{t('radiacode-track_access_level-label', { level: track.accessLevel })}</span>
      <a class="button-link" href={`/datasets/${track.dataset.id}`}>{track.dataset.name}</a>
      {#if track.accessLevel === 'edit'}
        {#if track.canRestoreOriginal}
          <button class="warning" disabled={restoringOriginal} onclick={restoreOriginal}>
            {t('radiacode-track_restore_original-button')}
          </button>
        {/if}
        <button class="danger" onclick={deleteTrack}>{t('radiacode-common_danger-delete-button')}</button>
      {/if}
    </div>
  </div>

  {#if generatedKey}
    <section class="panel">
      <h2>{t('radiacode-track_created_key-title')}</h2>
      <p class="muted">{t('radiacode-track_plaintext_warning')}</p>
      <textarea readonly value={generatedKey}></textarea>
    </section>
  {/if}

  <section class="grid cols-2">
    <article class="panel">
      <h2>{t('radiacode-track_title')}</h2>
      <div class="form-grid">
        <label>
          <div class="muted">{t('radiacode-common_source-label')}</div>
          <input readonly value={trackSourceLabel(track.sourceType)} />
        </label>
        {#if track.ingest}
          <label>
            <div class="muted">{t('radiacode-track_live_id-title')}</div>
            <input readonly value={track.ingestTrackId} />
          </label>
        {/if}
        <label>
          <div class="muted">{t('radiacode-common_created-label')}</div>
          <input readonly value={formatTime(track.createdAt)} />
        </label>
      </div>
    </article>

    <article class="panel">
      {#if !track.ingest}
        <h2>{t('radiacode-track_header-title')}</h2>
        <p class="muted">{t('radiacode-track_not_live')}</p>
      {:else}
        <h2>{t('radiacode-track_endpoint-title')}</h2>
        <div class="form-grid">
          <label>
            <div class="muted">{t('radiacode-common_endpoint-label')}</div>
            <input readonly value={endpointUrl} />
          </label>
          <label>
            <div class="muted">{t('radiacode-common_header_name-label')}</div>
            <input readonly value={track.ingest.headerName} />
          </label>
        </div>
      {/if}
    </article>
  </section>

  {#if track.accessLevel === 'edit'}
    <section class="panel">
      <details class="sharing-accordion">
        <summary>
          <span>{t('radiacode-common_sharing-label')}</span>
          <span class="sharing-summary">
            <span class="chip subtle">{track.shares.length}</span>
            <span aria-hidden="true" class="sharing-icon"></span>
          </span>
        </summary>

        <div class="form-grid">
          <p class="muted">{t('radiacode-track_sharing_help')}</p>

          <div class="sharing-controls">
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

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t('radiacode-common_user-label')}</th>
                  <th>{t('radiacode-common_access-label')}</th>
                  <th>{t('radiacode-common_actions-label')}</th>
                </tr>
              </thead>
              <tbody>
                {#if !track.shares.length}
                  <tr>
                    <td colspan="3" class="muted">{t('radiacode-track_sharing_empty')}</td>
                  </tr>
                {:else}
                  {#each track.shares as share}
                    <tr>
                      <td>{share.username}</td>
                      <td>{share.accessLevel}</td>
                      <td>
                        <button class="danger" onclick={() => removeShare(share.id)}>{t('radiacode-common_remove-button')}</button>
                      </td>
                    </tr>
                  {/each}
                {/if}
              </tbody>
            </table>
          </div>
        </div>
      </details>
    </section>
  {/if}

  {#if track.ingest}
    <section class="grid cols-2">
      <article class="panel">
        <h2>{t('radiacode-track_ingest_keys-title')}</h2>

        {#if track.canManageIngest}
          <div class="form-grid">
            <input bind:value={keyForm.label} placeholder={t('radiacode-track_key_label-placeholder')} />
            <textarea bind:value={keyForm.notes} placeholder={t('radiacode-track_key_notes-placeholder')}></textarea>
            <div class="actions">
              <button class="primary" onclick={createKey}>{t('radiacode-track_create_key-button')}</button>
            </div>
          </div>
        {/if}

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{t('radiacode-common_label-label')}</th>
                <th>{t('radiacode-common_prefix-label')}</th>
                <th>{t('radiacode-common_state-label')}</th>
                <th>{t('radiacode-track_last_used-label')}</th>
                <th>{t('radiacode-common_actions-label')}</th>
              </tr>
            </thead>
            <tbody>
              {#if !track.keys.length}
                <tr>
                  <td colspan="5" class="muted">{t('radiacode-track_keys_empty')}</td>
                </tr>
              {:else}
                {#each track.keys as key}
                  <tr>
                    <td>
                      <div class="form-grid">
                        <span>{key.label}</span>
                        {#if key.notes}
                          <span class="muted">{key.notes}</span>
                        {/if}
                      </div>
                    </td>
                    <td><code>{key.keyPrefix}</code></td>
                    <td>
                      {#if key.active}
                        <span class="chip mid">{t('radiacode-common_active-label')}</span>
                      {:else}
                        <span class="chip danger">{t('radiacode-common_revoked-label')}</span>
                      {/if}
                    </td>
                    <td>{formatTime(key.lastUsedAt)}</td>
                    <td>
                      {#if track.canManageIngest && key.active}
                        <div class="actions">
                          <button class="warning" onclick={() => rotateKey(key.id)}>{t('radiacode-track_rotate-button')}</button>
                          <button class="danger" onclick={() => revokeKey(key.id)}>{t('radiacode-track_revoke-button')}</button>
                        </div>
                      {/if}
                    </td>
                  </tr>
                {/each}
              {/if}
            </tbody>
          </table>
        </div>
      </article>

      <article class="panel">
        <h2>{t('radiacode-track_curl-title')}</h2>
        <textarea readonly value={curlExample}></textarea>
        <p class="muted">{t('radiacode-track_sample_payload_description')}</p>

        <h3>{t('radiacode-track_payload-title')}</h3>
        <textarea readonly value={payloadExample}></textarea>
      </article>
    </section>
  {/if}

  <section class="panel">
    <div class="page-header">
      <div>
        <h2>{t('radiacode-track_recent_points-title')}</h2>
        <p class="muted">
          {t('radiacode-track_points_range-label', {
            start: track.readingsPage.totalCount ? readingPageStart : 0,
            end: readingPageEnd,
            total: track.readingsPage.totalCount
          })}
        </p>
      </div>
      <div class="actions">
        <button onclick={() => { void loadTrack({ offset: Math.max(0, readingOffset - readingPageLimit) }); }} disabled={readingOffset <= 0}>
          {t('radiacode-common_previous-label')}
        </button>
        <button
          onclick={() => { void loadTrack({ offset: readingOffset + readingPageLimit }); }}
          disabled={(readingOffset + (track.readingsPage.readings?.length ?? 0)) >= track.readingsPage.totalCount}
        >
          {t('radiacode-common_next-label')}
        </button>
      </div>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_row-label')}</th>
            <th>{t('radiacode-common_occurred_at-label')}</th>
            <th>{t('radiacode-common_received_at-label')}</th>
            <th>{t('radiacode-common_latitude-label')}</th>
            <th>{t('radiacode-common_longitude-label')}</th>
            <th>{t('radiacode-common_altitude-label')}</th>
            <th>{t('radiacode-common_accuracy-label')}</th>
            <th>{t('radiacode-common_dose_rate-label')}</th>
            <th>{t('radiacode-common_count_rate-label')}</th>
            <th>{t('radiacode-common_comment-label')}</th>
            <th>{t('radiacode-common_state-label')}</th>
            {#if track.accessLevel === 'edit'}
              <th>{t('radiacode-common_actions-label')}</th>
            {/if}
          </tr>
        </thead>
        <tbody>
          {#if !track.readingsPage.readings.length}
            <tr>
              <td colspan={track.accessLevel === 'edit' ? 12 : 11} class="muted">{t('radiacode-track_no_recent_points')}</td>
            </tr>
          {:else}
            {#each track.readingsPage.readings as reading}
              <tr>
                <td>{reading.rowNumber}</td>
                {#if editingReadingId === reading.id}
                  <td><input bind:value={readingForm.occurredAt} /></td>
                  <td>{formatTime(reading.receivedAt)}</td>
                  <td><input bind:value={readingForm.latitude} /></td>
                  <td><input bind:value={readingForm.longitude} /></td>
                  <td><input bind:value={readingForm.altitudeMeters} /></td>
                  <td><input bind:value={readingForm.accuracy} /></td>
                  <td><input bind:value={readingForm.usv} /></td>
                  <td><input bind:value={readingForm.countRate} /></td>
                  <td><input bind:value={readingForm.comment} /></td>
                  <td>
                    {#if reading.isModified}
                      <span class="chip warning">{t('radiacode-track_modified-label')}</span>
                    {:else}
                      <span class="chip subtle">{t('radiacode-common_original-label')}</span>
                    {/if}
                  </td>
                  {#if track.accessLevel === 'edit'}
                    <td>
                      <div class="actions">
                        <button class="primary" disabled={savingReading} onclick={() => saveReading(reading.id)}>
                          {t('radiacode-common_save-button')}
                        </button>
                        <button onclick={resetReadingForm}>{t('radiacode-common_cancel-button')}</button>
                      </div>
                    </td>
                  {/if}
                {:else}
                  <td>{formatTime(reading.occurredAt)}</td>
                  <td>{formatTime(reading.receivedAt)}</td>
                  <td>{formatNumber(reading.latitude)}</td>
                  <td>{formatNumber(reading.longitude)}</td>
                  <td>{formatNumber(reading.altitudeMeters, 2)}</td>
                  <td>{formatNumber(reading.accuracy, 2)}</td>
                  <td>{formatNumber(reading.usv, 4)}</td>
                  <td>{formatNumber(reading.countRate, 2)}</td>
                  <td>{reading.comment ?? t('radiacode-common_none')}</td>
                  <td>
                    {#if reading.isModified}
                      <span class="chip warning">{t('radiacode-track_modified-label')}</span>
                    {:else}
                      <span class="chip subtle">{t('radiacode-common_original-label')}</span>
                    {/if}
                  </td>
                  {#if track.accessLevel === 'edit'}
                    <td>
                      <button onclick={() => startEditingReading(reading)}>{t('radiacode-common_edit-label')}</button>
                    </td>
                  {/if}
                {/if}
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
{/if}

<style>
  .sharing-accordion {
    display: grid;
    gap: var(--space-4);
  }

  .sharing-accordion summary {
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

  .sharing-accordion summary::-webkit-details-marker {
    display: none;
  }

  .sharing-accordion[open] summary {
    margin-bottom: var(--space-3);
  }

  .sharing-summary {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
  }

  .sharing-icon::before {
    content: '>';
    display: inline-block;
    color: var(--color-text-muted);
    transition: transform var(--transition), color var(--transition);
  }

  .sharing-accordion[open] .sharing-icon::before {
    transform: rotate(90deg);
    color: var(--color-text);
  }

  .sharing-controls {
    display: grid;
    grid-template-columns: minmax(0, 1.5fr) minmax(10rem, 0.8fr) auto;
    gap: var(--space-3);
    align-items: end;
  }

  @media (max-width: 1024px) {
    .sharing-controls {
      grid-template-columns: 1fr;
    }
  }
</style>
