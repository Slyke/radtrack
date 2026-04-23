<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import { ApiError, apiFetch, resolveApiPath } from '$lib/api/client';
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';
  import {
    humanizePropKey,
    normalizePropKey,
    normalizeSupportedFields,
    type MetricField
  } from '$lib/datalog-fields';
  import { sessionStore } from '$lib/stores/session';

  type SupportedFieldDraft = {
    propKey: string;
    displayName: string;
    valueType: 'number' | 'time' | 'string';
    popupDefaultEnabled: boolean;
  };

  let datalog = $state<any>(null);
  let errorMessage = $state<string | null>(null);
  let nameFormError = $state<string | null>(null);
  let generatedKey = $state<string | null>(null);
  let shareTargets = $state<any[]>([]);
  let datalogNameInput = $state<HTMLInputElement | null>(null);
  let savingMetadata = $state(false);
  let savingReading = $state(false);
  let restoringOriginal = $state(false);
  let deletingDatalog = $state(false);
  let metadataForm = $state({
    name: ''
  });
  let supportedFieldDrafts = $state<SupportedFieldDraft[]>([]);
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
  let readingForm = $state({
    occurredAt: '',
    latitude: '',
    longitude: '',
    altitudeMeters: '',
    accuracy: '',
    comment: '',
    measurements: {} as Record<string, string>
  });

  const readingPageLimit = 100;
  const datalogId = $derived($page.params.datalogId);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const formatTime = (value: string | null | undefined) => formatDateTime({
    value,
    language: $localeStore.language
  }) ?? t('radtrack-common_none');

  const formatNumber = (value: number | null | undefined, digits = 6) => {
    if (value === null || value === undefined) {
      return t('radtrack-common_na-label');
    }

    return new Intl.NumberFormat($localeStore.language, {
      maximumFractionDigits: digits
    }).format(value);
  };

  const supportedFields = $derived.by<MetricField[]>(() => normalizeSupportedFields(datalog?.supportedFields ?? []));

  const measurementFields = $derived.by<MetricField[]>(() => {
    const normalized = supportedFields;
    const byKey = new Map(normalized.map((field) => [field.propKey, field]));

    for (const reading of datalog?.readingsPage?.readings ?? []) {
      for (const propKey of Object.keys(reading.measurements ?? {})) {
        if (byKey.has(propKey)) {
          continue;
        }

        byKey.set(propKey, {
          propKey,
          displayName: humanizePropKey(propKey) || propKey,
          source: 'measurement',
          valueType: 'number',
          popupDefaultEnabled: true
        });
      }
    }

    return [...byKey.values()];
  });
  const numericMeasurementFields = $derived.by<MetricField[]>(() => measurementFields
    .filter((field) => field.source === 'measurement' && field.valueType === 'number'));

  const endpointUrl = $derived(
    !datalog?.ingest?.endpointPath
      ? ''
      : (
        (() => {
          const path = resolveApiPath({ path: datalog.ingest.endpointPath });
          if (!browser) {
            return path;
          }

          return path.startsWith('http://') || path.startsWith('https://')
            ? path
            : new URL(path, window.location.origin).toString();
        })()
      )
  );

  const exampleMeasurements = $derived.by(() => {
    const fields = numericMeasurementFields;
    if (!fields.length) {
      return {
        doseRate: 0.11,
        countRate: 84
      };
    }

    return Object.fromEntries(
      fields.slice(0, 4).map((field, index) => [field.propKey, Number(((index + 1) * 1.25).toFixed(2))])
    );
  });

  const payloadExample = $derived.by(() => JSON.stringify({
    occurredAt: '2026-04-17T19:15:00.000Z',
    latitude: 49.2827,
    longitude: -123.1207,
    altitudeMeters: 18.5,
    accuracy: 4.2,
    measurements: exampleMeasurements,
    deviceId: 'device-001',
    deviceName: 'Pocket Sensor',
    deviceType: 'Generic Sensor',
    deviceCalibration: 'factory-2026-01',
    firmwareVersion: '1.2.3',
    sourceReadingId: 'reading-000123',
    comment: 'Delayed upload after reconnect',
    extra: {
      gpsFix: '3d',
      satelliteCount: 14
    }
  }, null, 2));

  const curlExample = $derived(
    !datalog?.ingest
      ? ''
      : `curl -X POST '${endpointUrl}' \\
  -H 'Content-Type: application/json' \\
  -H '${datalog.ingest.headerName}: ${generatedKey ?? '<generated-key>'}' \\
  --data '${payloadExample}'`
  );

  const readingPageStart = $derived((datalog?.readingsPage?.offset ?? 0) + 1);
  const readingPageEnd = $derived(Math.min(
    (datalog?.readingsPage?.offset ?? 0) + (datalog?.readingsPage?.readings?.length ?? 0),
    datalog?.readingsPage?.totalCount ?? 0
  ));

  const buildReadingMeasurementForm = (reading: any | null) => {
    const values: Record<string, string> = {};
    const keys = new Set<string>(numericMeasurementFields.map((field) => field.propKey));

    for (const [propKey, value] of Object.entries(reading?.measurements ?? {})) {
      keys.add(propKey);
      values[propKey] = value === null || value === undefined ? '' : String(value);
    }

    for (const propKey of keys) {
      if (!(propKey in values)) {
        values[propKey] = '';
      }
    }

    return values;
  };

  const syncMetadataForm = () => {
    metadataForm = {
      name: datalog?.datalogName ?? ''
    };
    supportedFieldDrafts = (datalog?.supportedFields ?? []).map((field: any) => ({
      propKey: field.propKey ?? '',
      displayName: field.displayName ?? '',
      valueType: field.valueType === 'time' ? 'time' : (field.valueType === 'string' ? 'string' : 'number'),
      popupDefaultEnabled: field.popupDefaultEnabled !== false
    }));
  };

  const resetReadingForm = () => {
    editingReadingId = null;
    readingForm = {
      occurredAt: '',
      latitude: '',
      longitude: '',
      altitudeMeters: '',
      accuracy: '',
      comment: '',
      measurements: buildReadingMeasurementForm(null)
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
      comment: reading.comment ?? '',
      measurements: buildReadingMeasurementForm(reading)
    };
  };

  const focusDatalogName = () => {
    if (!browser) {
      return;
    }

    datalogNameInput?.scrollIntoView({ block: 'center' });
    datalogNameInput?.focus();
  };

  const loadDatalog = async ({ offset = readingOffset } = {}) => {
    try {
      const [datalogResponse, shareTargetsResponse] = await Promise.all([
        apiFetch<any>({
          path: `/api/datalogs/${datalogId}`,
          query: {
            limit: readingPageLimit,
            offset
          }
        }),
        apiFetch<any>({ path: '/api/share-targets' })
      ]);
      datalog = datalogResponse.datalog;
      shareTargets = shareTargetsResponse.users;
      readingOffset = offset;
      syncMetadataForm();
      if (editingReadingId) {
        const activeReading = datalogResponse.datalog.readingsPage?.readings?.find((reading: any) => reading.id === editingReadingId) ?? null;
        if (activeReading) {
          startEditingReading(activeReading);
        } else {
          resetReadingForm();
        }
      } else {
        resetReadingForm();
      }
      nameFormError = null;
      errorMessage = null;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed');
    }
  };

  const normalizeSupportedFieldPayload = () => normalizeSupportedFields(
    supportedFieldDrafts.map((field) => ({
      propKey: field.propKey,
      displayName: field.displayName,
      valueType: field.valueType,
      popupDefaultEnabled: field.popupDefaultEnabled
    }))
  ).map((field) => ({
    propKey: field.propKey,
    displayName: field.displayName,
    valueType: field.valueType,
    popupDefaultEnabled: field.popupDefaultEnabled
  }));

  const saveMetadata = async () => {
    if (!$sessionStore.csrf || savingMetadata || datalog?.accessLevel !== 'edit') {
      return;
    }

    const trimmedName = metadataForm.name.trim();
    if (!trimmedName) {
      nameFormError = t('radtrack-track_name_required');
      focusDatalogName();
      return;
    }

    const supportedFields = normalizeSupportedFieldPayload();
    savingMetadata = true;

    try {
      nameFormError = null;
      await apiFetch({
        path: `/api/datalogs/${datalogId}`,
        method: 'PATCH',
        body: {
          name: trimmedName,
          supportedFields
        },
        csrf: $sessionStore.csrf
      });
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      if (error instanceof ApiError && error.status === 400) {
        nameFormError = error.message;
        focusDatalogName();
        return;
      }

      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_update');
    } finally {
      savingMetadata = false;
    }
  };

  const addSupportedFieldDraft = () => {
    supportedFieldDrafts = [
      ...supportedFieldDrafts,
      {
        propKey: '',
        displayName: '',
        valueType: 'number',
        popupDefaultEnabled: true
      }
    ];
  };

  const removeSupportedFieldDraft = (index: number) => {
    supportedFieldDrafts = supportedFieldDrafts.filter((_, fieldIndex) => fieldIndex !== index);
  };

  const normalizeSupportedFieldDraftKey = (index: number) => {
    const current = supportedFieldDrafts[index];
    if (!current) {
      return;
    }

    const propKey = normalizePropKey(current.propKey) ?? '';
    const normalizedField = normalizeSupportedFields([{
      propKey,
      displayName: current.displayName,
      valueType: current.valueType,
      popupDefaultEnabled: current.popupDefaultEnabled
    }])[0];
    const displayName = current.displayName.trim()
      || normalizedField?.displayName
      || (propKey ? (humanizePropKey(propKey) || propKey) : '');
    supportedFieldDrafts = supportedFieldDrafts.map((field, fieldIndex) => (
      fieldIndex === index
        ? {
            propKey,
            displayName,
            valueType: field.valueType === 'time' ? 'time' : (field.valueType === 'string' ? 'string' : 'number'),
            popupDefaultEnabled: field.popupDefaultEnabled !== false
          }
        : field
    ));
  };

  const saveReading = async () => {
    if (!$sessionStore.csrf || !editingReadingId || savingReading || datalog?.accessLevel !== 'edit') {
      return;
    }

    savingReading = true;

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}/readings/${editingReadingId}`,
        method: 'PATCH',
        body: {
          occurredAt: readingForm.occurredAt || null,
          latitude: readingForm.latitude,
          longitude: readingForm.longitude,
          altitudeMeters: readingForm.altitudeMeters || null,
          accuracy: readingForm.accuracy || null,
          comment: readingForm.comment,
          measurements: readingForm.measurements
        },
        csrf: $sessionStore.csrf
      });
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_update');
    } finally {
      savingReading = false;
    }
  };

  const createIngestKey = async () => {
    if (!$sessionStore.csrf || datalog?.accessLevel !== 'edit') {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: `/api/datalogs/${datalogId}/ingest-keys`,
        method: 'POST',
        body: keyForm,
        csrf: $sessionStore.csrf
      });
      generatedKey = response.result.plaintextKey;
      keyForm = {
        label: '',
        notes: ''
      };
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_create_key');
    }
  };

  const revokeIngestKey = async (ingestKeyId: string) => {
    if (!$sessionStore.csrf || datalog?.accessLevel !== 'edit') {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}/ingest-keys/${ingestKeyId}/revoke`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_revoke_key');
    }
  };

  const rotateIngestKey = async (ingestKeyId: string) => {
    if (!$sessionStore.csrf || datalog?.accessLevel !== 'edit') {
      return;
    }

    try {
      const response = await apiFetch<any>({
        path: `/api/datalogs/${datalogId}/ingest-keys/${ingestKeyId}/rotate`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      generatedKey = response.result.replacement.plaintextKey;
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_rotate_key');
    }
  };

  const saveShare = async () => {
    if (!$sessionStore.csrf || datalog?.accessLevel !== 'edit') {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}/shares`,
        method: 'POST',
        body: shareForm,
        csrf: $sessionStore.csrf
      });
      shareForm = {
        targetUserId: '',
        accessLevel: 'view'
      };
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_share');
    }
  };

  const removeShare = async (shareId: string) => {
    if (!$sessionStore.csrf || datalog?.accessLevel !== 'edit') {
      return;
    }

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}/shares/${shareId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_share');
    }
  };

  const restoreOriginal = async () => {
    if (!browser || !$sessionStore.csrf || restoringOriginal || !datalog?.canRestoreOriginal) {
      return;
    }

    if (!window.confirm(t('radtrack-track_restore_original_confirm', {
      name: datalog.datalogName ?? t('radtrack-layout_track_page-label')
    }))) {
      return;
    }

    restoringOriginal = true;

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}/restore-original`,
        method: 'POST',
        body: {},
        csrf: $sessionStore.csrf
      });
      await loadDatalog({ offset: readingOffset });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_restore_original');
    } finally {
      restoringOriginal = false;
    }
  };

  const deleteDatalog = async () => {
    if (!browser || !$sessionStore.csrf || deletingDatalog || datalog?.accessLevel !== 'edit') {
      return;
    }

    if (!window.confirm(t('radtrack-track_delete_confirm', {
      name: datalog.datalogName ?? t('radtrack-layout_track_page-label')
    }))) {
      return;
    }

    deletingDatalog = true;

    try {
      await apiFetch({
        path: `/api/datalogs/${datalogId}`,
        method: 'DELETE',
        csrf: $sessionStore.csrf
      });
      await goto(`/datasets/${datalog.dataset.id}`);
    } catch (error) {
      deletingDatalog = false;
      errorMessage = error instanceof Error ? error.message : t('radtrack-track_failed_delete');
    }
  };

  onMount(async () => {
    await loadDatalog();
  });
</script>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

{#if !datalog}
  <section class="panel">
    <p class="muted">{t('radtrack-common_loading-label')}</p>
  </section>
{:else}
  <div class="detail-page-header">
    <div class="page-header">
      <h1>{datalog.datalogName ?? t('radtrack-track_title')}</h1>
      <div class="actions">
        <a class="button-link" href={`/datasets/${datalog.dataset.id}`}>{t('radtrack-common_back-button')}</a>
      </div>
    </div>
    <div class="page-header detail-page-header-meta-row">
      <p class="muted detail-page-header-description">{t('radtrack-track_description')}</p>
      <div class="actions">
        <a class="button-link" href={`/datasets/${datalog.dataset.id}`}>{t('radtrack-common_open_dataset-button')}</a>
        {#if datalog.accessLevel === 'edit'}
          <button class="danger" disabled={deletingDatalog} onclick={deleteDatalog}>{t('radtrack-common_danger-delete-button')}</button>
        {/if}
      </div>
    </div>
  </div>

  <section class="grid cols-2 datalog-layout">
    <article class="panel datalog-panel">
      <div class="chip-row">
        <span class="chip start">{t('radtrack-track_access_level-label', { level: datalog.accessLevel })}</span>
        <span class="chip mid">{t('radtrack-track_source_type-label', { type: t(`radtrack-track_source_type-${datalog.sourceType}`) })}</span>
        <span class="chip subtle">{t('radtrack-common_readings-label')}: {datalog.validRowCount}/{datalog.rowCount}</span>
      </div>

      <div class="grid">
        <span class="chip subtle">{t('radtrack-common_dataset-label')}: {datalog.dataset.name}</span>
        <span class="chip subtle">{t('radtrack-common_device-label')}: {datalog.deviceIdentifierRaw ?? t('radtrack-common_na-label')}</span>
        <span class="chip subtle">{t('radtrack-common_created-label')}: {formatTime(datalog.createdAt)}</span>
        <span class="chip subtle">{t('radtrack-common_warnings-label')}: {datalog.warningCount}</span>
      </div>

      {#if datalog.accessLevel === 'edit'}
        <div class="form-grid">
          <label>
            <div class="muted">{t('radtrack-track_name-label')}</div>
            <input bind:this={datalogNameInput} bind:value={metadataForm.name} />
          </label>

          <div class="form-grid field-block">
            <div class="field-block-header">
              <div>
                <div class="muted">{t('radtrack-common_metric-label')}</div>
                <p class="faint">Fields defined on this datalog. Number and time fields can be plotted on the map. Text fields are popup-only.</p>
              </div>
              <button type="button" onclick={addSupportedFieldDraft}>{t('radtrack-common_create-button')}</button>
            </div>

            {#if supportedFieldDrafts.length}
              <div class="supported-field-list">
                {#each supportedFieldDrafts as field, index}
                  <div class="supported-field-row">
                    <label>
                      <div class="muted">propKey</div>
                      <input
                        bind:value={field.propKey}
                        onblur={() => normalizeSupportedFieldDraftKey(index)}
                        placeholder="doseRate"
                      />
                    </label>
                    <label>
                      <div class="muted">{t('radtrack-common_label-label')}</div>
                      <input bind:value={field.displayName} placeholder="Dose Rate" />
                    </label>
                    <label>
                      <div class="muted">{t('radtrack-common_type-label')}</div>
                      <select bind:value={field.valueType}>
                        <option value="number">Number</option>
                        <option value="time">Time</option>
                        <option value="string">Text</option>
                      </select>
                    </label>
                    <label class="checkbox-field supported-field-toggle">
                      <input bind:checked={field.popupDefaultEnabled} type="checkbox" />
                      <span>{t('radtrack-track_popup_default-label')}</span>
                    </label>
                    <div class="supported-field-actions">
                      <button class="danger" type="button" onclick={() => removeSupportedFieldDraft(index)}>{t('radtrack-common_remove-button')}</button>
                    </div>
                  </div>
                {/each}
              </div>
            {:else}
              <p class="muted">No fields defined yet. Live ingest can infer numeric fields from payloads.</p>
            {/if}
          </div>

          {#if nameFormError}
            <p class="field-error">{nameFormError}</p>
          {/if}

          <div class="actions">
            <button class="primary" disabled={savingMetadata} onclick={saveMetadata}>{t('radtrack-track_update_name-button')}</button>
            {#if datalog.canRestoreOriginal}
              <button disabled={restoringOriginal} onclick={restoreOriginal}>{t('radtrack-track_restore_original-button')}</button>
            {/if}
          </div>
        </div>
      {:else}
        <div class="form-grid">
          <div>
            <div class="muted">{t('radtrack-track_name-label')}</div>
            <div>{datalog.datalogName ?? t('radtrack-common_none')}</div>
          </div>
          <div>
            <div class="muted">{t('radtrack-common_metric-label')}</div>
            <div>{supportedFields.length ? supportedFields.map((field) => `${field.propKey} (${field.displayName}, ${field.valueType})`).join(', ') : t('radtrack-common_none')}</div>
          </div>
        </div>
      {/if}

      {#if datalog.supportedFieldWarnings?.length}
        <div class="form-grid field-block">
          <div class="muted">{t('radtrack-common_warnings-label')}</div>
          {#each datalog.supportedFieldWarnings as warning}
            <p class="muted"><code>{warning.propKey}</code> {warning.reason}</p>
          {/each}
        </div>
      {/if}
    </article>

    <article class="panel datalog-panel">
      {#if datalog.ingest}
        <div class="form-grid">
          <div class="grid">
            <span class="chip subtle">{t('radtrack-track_live_id-title')}: <code>{datalog.ingestDatalogId}</code></span>
            <span class="chip subtle">{t('radtrack-track_header-title')}: <code>{datalog.ingest.headerName}</code></span>
          </div>

          <div>
            <div class="muted">{t('radtrack-track_endpoint-title')}</div>
            <code class="endpoint-value">{endpointUrl}</code>
          </div>

          {#if datalog.accessLevel === 'edit'}
            <div class="grid cols-3">
              <label>
                <div class="muted">{t('radtrack-track_key_label-placeholder')}</div>
                <input bind:value={keyForm.label} placeholder={t('radtrack-track_key_label-placeholder')} />
              </label>
              <label>
                <div class="muted">{t('radtrack-track_key_notes-placeholder')}</div>
                <input bind:value={keyForm.notes} placeholder={t('radtrack-track_key_notes-placeholder')} />
              </label>
              <div class="supported-field-actions">
                <button class="primary" onclick={createIngestKey}>{t('radtrack-track_create_key-button')}</button>
              </div>
            </div>
          {/if}

          {#if generatedKey}
            <div class="form-grid field-block">
              <div class="muted">{t('radtrack-track_created_key-title')}</div>
              <code>{generatedKey}</code>
              <p class="faint">{t('radtrack-track_plaintext_warning')}</p>
            </div>
          {/if}

          <div class="form-grid field-block">
            <div class="muted">{t('radtrack-track_curl-title')}</div>
            <pre>{curlExample}</pre>
          </div>

          <div class="form-grid field-block">
            <div class="muted">{t('radtrack-track_payload-title')}</div>
            <pre>{payloadExample}</pre>
            <p class="faint">{t('radtrack-track_sample_payload_description')}</p>
          </div>

          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t('radtrack-common_label-label')}</th>
                  <th>{t('radtrack-common_prefix-label')}</th>
                  <th>{t('radtrack-track_last_used-label')}</th>
                  <th>{t('radtrack-common_state-label')}</th>
                  {#if datalog.accessLevel === 'edit'}
                    <th>{t('radtrack-common_actions-label')}</th>
                  {/if}
                </tr>
              </thead>
              <tbody>
                {#if datalog.keys?.length}
                  {#each datalog.keys as key}
                    <tr>
                      <td>{key.label}</td>
                      <td><code>{key.keyPrefix}</code></td>
                      <td>{formatTime(key.lastUsedAt)}</td>
                      <td>{key.active ? t('radtrack-common_active-label') : t('radtrack-common_revoked-label')}</td>
                      {#if datalog.accessLevel === 'edit'}
                        <td>
                          <div class="actions">
                            <button disabled={!key.active} onclick={() => rotateIngestKey(key.id)}>{t('radtrack-track_rotate-button')}</button>
                            <button class="danger" disabled={!key.active} onclick={() => revokeIngestKey(key.id)}>{t('radtrack-track_revoke-button')}</button>
                          </div>
                        </td>
                      {/if}
                    </tr>
                  {/each}
                {:else}
                  <tr>
                    <td colspan={datalog.accessLevel === 'edit' ? 5 : 4} class="muted">{t('radtrack-track_keys_empty')}</td>
                  </tr>
                {/if}
              </tbody>
            </table>
          </div>
        </div>
      {:else}
        <p class="muted">{t('radtrack-track_not_live')}</p>
      {/if}
    </article>
  </section>

  {#if datalog.accessLevel === 'edit'}
    <section class="panel datalog-panel">
      <h2>{t('radtrack-common_sharing-label')}</h2>
      <p class="muted">{t('radtrack-track_sharing_help')}</p>

      <div class="grid cols-3">
        <label>
          <div class="muted">{t('radtrack-common_user-label')}</div>
          <select bind:value={shareForm.targetUserId}>
            <option value="">{t('radtrack-common_select_user-option')}</option>
            {#each shareTargets as target}
              <option value={target.id}>{target.username}</option>
            {/each}
          </select>
        </label>

        <label>
          <div class="muted">{t('radtrack-common_access-label')}</div>
          <select bind:value={shareForm.accessLevel}>
            <option value="view">{t('radtrack-common_view-label')}</option>
            <option value="edit">{t('radtrack-common_edit-label')}</option>
          </select>
        </label>

        <div class="supported-field-actions">
          <button class="primary" onclick={saveShare}>{t('radtrack-common_share-button')}</button>
        </div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('radtrack-common_user-label')}</th>
              <th>{t('radtrack-common_access-label')}</th>
              <th>{t('radtrack-common_actions-label')}</th>
            </tr>
          </thead>
          <tbody>
            {#if datalog.shares?.length}
              {#each datalog.shares as share}
                <tr>
                  <td>{share.username}</td>
                  <td>{share.accessLevel}</td>
                  <td><button class="danger" onclick={() => removeShare(share.id)}>{t('radtrack-common_remove-button')}</button></td>
                </tr>
              {/each}
            {:else}
              <tr>
                <td colspan="3" class="muted">{t('radtrack-track_sharing_empty')}</td>
              </tr>
            {/if}
          </tbody>
        </table>
      </div>
    </section>
  {/if}

  <section class="panel datalog-panel">
    <div class="page-header compact-header">
      <div>
        <h2>{t('radtrack-track_recent_points-title')}</h2>
        <p class="muted">{t('radtrack-track_points_range-label', {
          start: readingPageStart,
          end: readingPageEnd,
          total: datalog.readingsPage?.totalCount ?? 0
        })}</p>
      </div>

      <div class="actions">
        <button
          disabled={(datalog.readingsPage?.offset ?? 0) <= 0}
          onclick={() => loadDatalog({ offset: Math.max(0, (datalog.readingsPage?.offset ?? 0) - readingPageLimit) })}
        >
          {t('radtrack-common_previous-label')}
        </button>
        <button
          disabled={(datalog.readingsPage?.offset ?? 0) + readingPageLimit >= (datalog.readingsPage?.totalCount ?? 0)}
          onclick={() => loadDatalog({ offset: (datalog.readingsPage?.offset ?? 0) + readingPageLimit })}
        >
          {t('radtrack-common_next-label')}
        </button>
      </div>
    </div>

    {#if datalog.accessLevel === 'edit' && editingReadingId}
      <div class="form-grid field-block">
        <div class="grid cols-3">
          <label>
            <div class="muted">{t('radtrack-common_occurred_at-label')}</div>
            <input bind:value={readingForm.occurredAt} placeholder="2026-04-17T19:15:00.000Z" />
          </label>
          <label>
            <div class="muted">{t('radtrack-common_latitude-label')}</div>
            <input bind:value={readingForm.latitude} step="any" type="number" />
          </label>
          <label>
            <div class="muted">{t('radtrack-common_longitude-label')}</div>
            <input bind:value={readingForm.longitude} step="any" type="number" />
          </label>
          <label>
            <div class="muted">{t('radtrack-common_altitude-label')}</div>
            <input bind:value={readingForm.altitudeMeters} step="any" type="number" />
          </label>
          <label>
            <div class="muted">{t('radtrack-common_accuracy-label')}</div>
            <input bind:value={readingForm.accuracy} step="any" type="number" />
          </label>
          <label class="span-3">
            <div class="muted">{t('radtrack-common_comment-label')}</div>
            <input bind:value={readingForm.comment} />
          </label>
        </div>

        {#if numericMeasurementFields.length}
          <div class="grid cols-3">
            {#each numericMeasurementFields as field}
              <label>
                <div class="muted">{field.displayName}</div>
                <input bind:value={readingForm.measurements[field.propKey]} step="any" type="number" />
              </label>
            {/each}
          </div>
        {/if}

        <div class="actions">
          <button class="primary" disabled={savingReading} onclick={saveReading}>{t('radtrack-common_save-button')}</button>
          <button onclick={resetReadingForm}>{t('radtrack-common_cancel-button')}</button>
        </div>
      </div>
    {/if}

    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radtrack-common_row-label')}</th>
            <th>{t('radtrack-common_occurred_at-label')}</th>
            <th>{t('radtrack-common_latitude-label')}</th>
            <th>{t('radtrack-common_longitude-label')}</th>
            <th>{t('radtrack-common_altitude-label')}</th>
            <th>{t('radtrack-common_accuracy-label')}</th>
            {#each numericMeasurementFields as field}
              <th>{field.displayName}</th>
            {/each}
            <th>{t('radtrack-common_comment-label')}</th>
            {#if datalog.accessLevel === 'edit'}
              <th>{t('radtrack-common_actions-label')}</th>
            {/if}
          </tr>
        </thead>
        <tbody>
          {#if datalog.readingsPage?.readings?.length}
            {#each datalog.readingsPage.readings as reading}
              <tr class:active-row={editingReadingId === reading.id}>
                <td>{reading.rowNumber}</td>
                <td>{formatTime(reading.occurredAt ?? reading.receivedAt)}</td>
                <td>{formatNumber(reading.latitude)}</td>
                <td>{formatNumber(reading.longitude)}</td>
                <td>{formatNumber(reading.altitudeMeters)}</td>
                <td>{formatNumber(reading.accuracy)}</td>
                {#each numericMeasurementFields as field}
                  <td>{formatNumber(reading.measurements?.[field.propKey] ?? null)}</td>
                {/each}
                <td>{reading.comment || t('radtrack-common_none')}</td>
                {#if datalog.accessLevel === 'edit'}
                  <td>
                    <button onclick={() => startEditingReading(reading)}>{t('radtrack-common_edit-label')}</button>
                  </td>
                {/if}
              </tr>
            {/each}
          {:else}
            <tr>
              <td colspan={8 + numericMeasurementFields.length + (datalog.accessLevel === 'edit' ? 1 : 0)} class="muted">
                {t('radtrack-track_no_recent_points')}
              </td>
            </tr>
          {/if}
        </tbody>
      </table>
    </div>
  </section>
{/if}

<style>
  .datalog-layout {
    align-items: start;
  }

  .datalog-panel {
    display: grid;
    gap: var(--space-4);
  }

  .compact-header {
    margin-bottom: 0;
  }

  .field-block {
    padding-top: var(--space-3);
    border-top: 1px solid var(--color-border);
  }

  .field-block-header {
    display: flex;
    justify-content: space-between;
    gap: var(--space-3);
    align-items: start;
    flex-wrap: wrap;
  }

  .supported-field-list {
    display: grid;
    gap: var(--space-3);
  }

  .supported-field-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) minmax(9rem, auto) auto auto;
    gap: var(--space-3);
    align-items: end;
  }

  .supported-field-actions {
    display: flex;
    align-items: end;
    gap: var(--space-2);
  }

  .supported-field-toggle {
    display: inline-flex;
    align-items: center;
    gap: var(--space-2);
    min-height: 100%;
    white-space: nowrap;
    align-self: center;
  }

  .endpoint-value {
    display: inline-block;
    max-width: 100%;
    overflow-wrap: anywhere;
  }

  .span-3 {
    grid-column: span 3;
  }

  .active-row {
    background: color-mix(in srgb, var(--color-panel-strong) 80%, transparent);
  }

  pre {
    margin: 0;
    overflow-x: auto;
    white-space: pre-wrap;
    word-break: break-word;
  }

  @media (max-width: 900px) {
    .datalog-layout {
      grid-template-columns: minmax(0, 1fr);
    }

    .supported-field-row,
    .span-3 {
      grid-column: auto;
    }

    .supported-field-row {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
