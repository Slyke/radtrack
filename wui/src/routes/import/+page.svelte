<svelte:options runes={true} />

<script lang="ts">
  import { formatDateTime, localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';
  import { uploadImport } from '$lib/api/client';

  interface ImportWarningEntry {
    rowNumber?: number | null;
    type: string;
    reason: string;
  }

  interface ImportWarningBreakdownEntry {
    type: string;
    count: number;
    reason: string;
  }

  interface ImportChildParseSummary {
    trackId?: string;
    warningCount?: number;
    warningBreakdown?: ImportWarningBreakdownEntry[];
    warnings?: ImportWarningEntry[];
    dedupeMethod?: string | null;
    duplicateOfRawFileId?: string | null;
    duplicateOfTrackId?: string | null;
    skippedParsing?: boolean;
  }

  interface ImportChildSummary {
    originalFilename: string;
    parseStatus: string;
    duplicateOfRawFileId?: string | null;
    summary?: ImportChildParseSummary | null;
  }

  interface ImportFileSummary {
    originalFilename: string;
    importKind: string;
    sourceArchiveFilename?: string | null;
    error?: string | null;
    children?: ImportChildSummary[];
  }

  interface ImportDatasetSummary {
    datasetId: string;
    datasetName: string;
    fileCount: number;
    parsedFileCount: number;
    duplicateFileCount: number;
    failedFileCount: number;
    warningCount: number;
    errorCount: number;
    skippedRowCount: number;
    files: ImportFileSummary[];
  }

  interface ImportDatasetResult {
    datasetId: string;
    datasetName: string;
    status: string;
    summary: ImportDatasetSummary;
  }

  interface ImportBatchSummary {
    datasetId: string | null;
    datasetName: string | null;
    datasetIds?: string[];
    datasetCount?: number;
    fileCount: number;
    parsedFileCount: number;
    failedFileCount: number;
    duplicateFileCount: number;
    warningCount: number;
    errorCount: number;
    skippedRowCount: number;
    splitBulkArchivesIntoDatasets?: boolean;
    advancedTrackDeduplication?: boolean;
    files: ImportFileSummary[];
    datasets?: ImportDatasetResult[];
  }

  interface ImportResult {
    batchId: string;
    datasetId: string | null;
    datasetIds?: string[];
    status: string;
    summary: ImportBatchSummary;
  }

  interface ImportWarningReport {
    sourceFileName: string;
    childFileName: string;
    parseStatus: string;
    trackId: string | null;
    dedupeMethod: string | null;
    duplicateOfRawFileId: string | null;
    duplicateOfTrackId: string | null;
    warningCount: number;
    warningBreakdown: ImportWarningBreakdownEntry[];
    rowWarnings: ImportWarningEntry[];
    persisted: boolean;
  }

  let fileInput: HTMLInputElement | null = null;
  let selectedFiles = $state<File[]>([]);
  let datasetName = $state('');
  let description = $state('');
  let splitBulkArchivesIntoDatasets = $state(true);
  let advancedTrackDeduplication = $state(true);
  let uploadProgress = $state(0);
  let busy = $state(false);
  let dragActive = $state(false);
  let dragDepth = $state(0);
  let result = $state<ImportResult | null>(null);
  let errorMessage = $state<string | null>(null);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const isFileDrag = (event: DragEvent) => Array.from(event.dataTransfer?.types ?? []).includes('Files');
  const isSplitArchiveFileName = (fileName: string) => {
    const normalizedFileName = fileName.trim().toLowerCase();
    return normalizedFileName.endsWith('.zrctrk') || normalizedFileName.endsWith('.zip');
  };
  const duplicateFileReason = 'File matched an earlier raw artifact and was skipped.';
  const datasetNameDerivedFromBulkFiles = $derived(
    splitBulkArchivesIntoDatasets
    && selectedFiles.length > 0
    && selectedFiles.every((file) => isSplitArchiveFileName(file.name))
  );

  const fileSummary = (files: File[]) => files.map((file) => file.name).join(', ');

  const syncSelectedFiles = (files: File[]) => {
    selectedFiles = files;
    const nextDatasetNameDerivedFromBulkFiles = (
      splitBulkArchivesIntoDatasets
      && files.length > 0
      && files.every((file) => isSplitArchiveFileName(file.name))
    );

    if (!datasetName.trim() && files.length && !nextDatasetNameDerivedFromBulkFiles) {
      datasetName = files[0].name;
    }

    if (fileInput) {
      fileInput.value = '';
    }
  };

  const resetImportForm = () => {
    datasetName = '';
    description = '';
    uploadProgress = 0;
    dragActive = false;
    dragDepth = 0;
    syncSelectedFiles([]);
  };

  const handleFiles = (files: FileList | null) => {
    syncSelectedFiles(files ? Array.from(files) : []);
  };

  const removeSelectedFile = (index: number) => {
    syncSelectedFiles(selectedFiles.filter((_, fileIndex) => fileIndex !== index));
  };

  const autofillDescription = () => {
    if (!description.trim() && selectedFiles.length) {
      description = fileSummary(selectedFiles);
    }
  };

  const normalizeDatasetResults = (importResult: ImportResult | null | undefined): ImportDatasetResult[] => {
    if (!importResult) {
      return [];
    }

    if (importResult.summary.datasets?.length) {
      return importResult.summary.datasets;
    }

    if (!importResult.datasetId || !importResult.summary.datasetName) {
      return [];
    }

    return [{
      datasetId: importResult.datasetId,
      datasetName: importResult.summary.datasetName,
      status: importResult.status,
      summary: {
        datasetId: importResult.datasetId,
        datasetName: importResult.summary.datasetName,
        fileCount: importResult.summary.fileCount,
        parsedFileCount: importResult.summary.parsedFileCount,
        duplicateFileCount: importResult.summary.duplicateFileCount,
        failedFileCount: importResult.summary.failedFileCount,
        warningCount: importResult.summary.warningCount,
        errorCount: importResult.summary.errorCount,
        skippedRowCount: importResult.summary.skippedRowCount,
        files: importResult.summary.files
      }
    }];
  };

  const getFileWarningCount = (file: ImportFileSummary) => (
    (file.children ?? []).reduce((total, child) => total + getWarningCount(child), 0)
  );

  const getFileStatus = (file: ImportFileSummary) => {
    if (file.error) {
      return 'failed';
    }

    const children = file.children ?? [];
    if (!children.length) {
      return 'pending';
    }

    const hasFailedChild = children.some((child) => child.parseStatus === 'failed');
    const hasParsedChild = children.some((child) => child.parseStatus === 'parsed');
    const hasWarningChild = children.some((child) => (
      child.parseStatus === 'duplicate_skipped'
      || (child.summary?.warningCount ?? 0) > 0
    ));

    if (hasFailedChild) {
      return hasParsedChild ? 'completed_with_errors' : 'failed';
    }

    if (hasWarningChild) {
      return 'completed_with_warnings';
    }

    return 'completed';
  };

  const getWarningBreakdown = (child: ImportChildSummary): ImportWarningBreakdownEntry[] => {
    if (child.summary?.warningBreakdown?.length) {
      return child.summary.warningBreakdown;
    }

    if (child.parseStatus === 'duplicate_skipped') {
      return [{
        type: 'duplicate_file',
        count: 1,
        reason: duplicateFileReason
      }];
    }

    return [];
  };

  const getRowWarnings = (child: ImportChildSummary) => child.summary?.warnings ?? [];

  const getWarningCount = (child: ImportChildSummary) => Math.max(
    child.summary?.warningCount ?? 0,
    getWarningBreakdown(child).reduce((total, item) => total + item.count, 0),
    getRowWarnings(child).length
  );

  const collectWarningReports = (summary: { files?: ImportFileSummary[] } | null | undefined): ImportWarningReport[] => (
    (summary?.files ?? [])
      .flatMap((file) => (file.children ?? [])
        .map((child) => {
          const warningBreakdown = getWarningBreakdown(child);
          const rowWarnings = getRowWarnings(child);
          const warningCount = getWarningCount(child);

          return {
            sourceFileName: file.sourceArchiveFilename ?? file.originalFilename,
            childFileName: child.originalFilename,
            parseStatus: child.parseStatus,
            trackId: child.summary?.trackId ?? null,
            dedupeMethod: child.summary?.dedupeMethod ?? null,
            duplicateOfRawFileId: child.duplicateOfRawFileId ?? child.summary?.duplicateOfRawFileId ?? null,
            duplicateOfTrackId: child.summary?.duplicateOfTrackId ?? null,
            warningCount,
            warningBreakdown,
            rowWarnings,
            persisted: true
          };
        })
        .filter((child) => child.warningCount > 0))
  );

  const getWarningFileNameBase = ({
    importResult,
    datasetResult
  }: {
    importResult: ImportResult;
    datasetResult: ImportDatasetResult;
  }) => (
    `radtrack-import-warnings-${importResult.batchId}-${datasetResult.datasetId}`
  );

  const buildWarningExportEnvelope = ({
    importResult,
    datasetResult,
    warningReports
  }: {
    importResult: ImportResult;
    datasetResult: ImportDatasetResult;
    warningReports: ImportWarningReport[];
  }) => ({
    title: 'RadTrack',
    type: 'import warnings',
    exportTime: formatDateTime({
      value: new Date().toISOString(),
      language: $localeStore.language
    }) ?? new Date().toISOString(),
    build: $sessionStore.build?.label ?? null,
    filters: {
      batchId: importResult.batchId,
      batchStatus: importResult.status,
      datasetId: datasetResult.datasetId,
      datasetName: datasetResult.datasetName,
      status: datasetResult.status,
      totalFiles: datasetResult.summary.fileCount,
      duplicateFiles: datasetResult.summary.duplicateFileCount,
      splitBulkArchivesIntoDatasets: importResult.summary.splitBulkArchivesIntoDatasets ?? null,
      advancedTrackDeduplication: importResult.summary.advancedTrackDeduplication ?? null,
      totalWarningSources: warningReports.length,
      totalWarningEvents: warningReports.reduce((total, report) => total + report.warningCount, 0),
      persisted: true
    },
    integrity: {
      verified: false,
      algorithm: null,
      hashInput: null,
      totalEntriesVerified: warningReports.length,
      latestHash: null,
      reason: 'Derived from the saved import batch summary.'
    },
    data: warningReports
  });

  const buildWarningExportText = ({
    envelope
  }: {
    envelope: ReturnType<typeof buildWarningExportEnvelope>;
  }) => [
    `${envelope.title} ${envelope.type}`,
    `Export time: ${envelope.exportTime}`,
    `Build: ${envelope.build ?? t('radtrack-common_none')}`,
    `Batch: ${envelope.filters.batchId}`,
    `Dataset: ${envelope.filters.datasetId}`,
    `Dataset name: ${envelope.filters.datasetName}`,
    `Status: ${envelope.filters.status}`,
    `Warning sources: ${envelope.filters.totalWarningSources}`,
    `Warning events: ${envelope.filters.totalWarningEvents}`,
    `Persisted: ${envelope.filters.persisted ? 'yes' : 'no'}`,
    '',
    ...envelope.data.flatMap((report) => {
      const sections = [
        `File: ${report.childFileName}`,
        report.sourceFileName !== report.childFileName ? `Source file: ${report.sourceFileName}` : null,
        `Parse status: ${report.parseStatus}`,
        report.trackId ? `Track: ${report.trackId}` : null,
        report.duplicateOfTrackId ? `Duplicate of track: ${report.duplicateOfTrackId}` : null,
        report.duplicateOfRawFileId ? `Duplicate of raw file: ${report.duplicateOfRawFileId}` : null,
        report.dedupeMethod ? `Dedupe method: ${report.dedupeMethod}` : null,
        `Warning count: ${report.warningCount}`,
        ...report.warningBreakdown.map((item) => `- ${item.type} (${item.count}): ${item.reason}`),
        ...report.rowWarnings.map((warning) => `- row ${warning.rowNumber ?? '?'} ${warning.type}: ${warning.reason}`),
        ''
      ];

      return sections.filter((value): value is string => Boolean(value));
    })
  ].join('\n');

  const downloadWarningBlob = ({
    content,
    fileName,
    type
  }: {
    content: string;
    fileName: string;
    type: string;
  }) => {
    const url = URL.createObjectURL(new Blob([content], { type }));
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = fileName;
    anchor.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
  };

  const downloadWarningsText = ({
    importResult,
    datasetResult
  }: {
    importResult: ImportResult;
    datasetResult: ImportDatasetResult;
  }) => {
    const warningReports = collectWarningReports(datasetResult.summary);
    if (!warningReports.length) {
      return;
    }

    const envelope = buildWarningExportEnvelope({
      importResult,
      datasetResult,
      warningReports
    });
    downloadWarningBlob({
      content: buildWarningExportText({ envelope }),
      fileName: `${getWarningFileNameBase({ importResult, datasetResult })}.txt`,
      type: 'text/plain;charset=utf-8'
    });
  };

  const downloadWarningsJson = ({
    importResult,
    datasetResult
  }: {
    importResult: ImportResult;
    datasetResult: ImportDatasetResult;
  }) => {
    const warningReports = collectWarningReports(datasetResult.summary);
    if (!warningReports.length) {
      return;
    }

    const envelope = buildWarningExportEnvelope({
      importResult,
      datasetResult,
      warningReports
    });
    downloadWarningBlob({
      content: JSON.stringify(envelope, null, 2),
      fileName: `${getWarningFileNameBase({ importResult, datasetResult })}.json`,
      type: 'application/json;charset=utf-8'
    });
  };

  const handleDragEnter = (event: DragEvent) => {
    if (!isFileDrag(event)) {
      return;
    }

    event.preventDefault();
    dragDepth += 1;
    dragActive = true;
  };

  const handleDragLeave = (event: DragEvent) => {
    if (!isFileDrag(event)) {
      return;
    }

    event.preventDefault();
    dragDepth = Math.max(0, dragDepth - 1);
    dragActive = dragDepth > 0;
  };

  const handleDragOver = (event: DragEvent) => {
    if (!isFileDrag(event)) {
      return;
    }

    const dataTransfer = event.dataTransfer;
    if (!dataTransfer) {
      return;
    }

    event.preventDefault();
    dataTransfer.dropEffect = 'copy';
    dragActive = true;
  };

  const handleDrop = (event: DragEvent) => {
    if (!isFileDrag(event)) {
      return;
    }

    event.preventDefault();
    dragDepth = 0;
    dragActive = false;
    handleFiles(event.dataTransfer?.files ?? null);
  };

  const submit = async () => {
    if (!$sessionStore.csrf || !selectedFiles.length) {
      return;
    }

    busy = true;
    errorMessage = null;
    result = null;
    uploadProgress = 0;
    try {
      result = await uploadImport({
        files: selectedFiles,
        datasetName,
        description,
        splitBulkArchivesIntoDatasets,
        advancedTrackDeduplication,
        csrf: $sessionStore.csrf,
        onProgress: (percent) => {
          uploadProgress = percent;
        }
      }) as ImportResult;
      resetImportForm();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-import_failed');
    } finally {
      busy = false;
    }
  };
</script>

<div class="page-header">
  <div>
    <h1>{t('radtrack-import_title')}</h1>
    <p class="muted">{t('radtrack-import_description')}</p>
  </div>
</div>

<section class="panel">
  <div class="form-grid">
    <label>
      <div class="muted">{t('radtrack-common_dataset_name-label')}</div>
      <input
        bind:value={datasetName}
        disabled={datasetNameDerivedFromBulkFiles}
        placeholder={t('radtrack-import_optional_dataset_name-placeholder')}
      />
      {#if datasetNameDerivedFromBulkFiles}
        <div class="faint">{t('radtrack-import_dataset_name_ignored-help')}</div>
      {/if}
    </label>
    <label>
      <div class="muted">{t('radtrack-common_description-label')}</div>
      <textarea bind:value={description} placeholder={t('radtrack-import_optional_description-placeholder')}></textarea>
    </label>
    {#if !description.trim() && selectedFiles.length}
      <div class="actions">
        <button class="mid" onclick={autofillDescription} type="button">{t('radtrack-import_autofill_description-button')}</button>
      </div>
    {/if}
    <div
      class:active={dragActive}
      class="dropzone"
      role="button"
      tabindex="0"
      ondragenter={handleDragEnter}
      ondragleave={handleDragLeave}
      ondragover={handleDragOver}
      onkeydown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          fileInput?.click();
        }
      }}
      ondrop={handleDrop}
    >
      <p>{t('radtrack-import_drag_drop-label')}</p>
      <input
        accept=".rctrk,.zrctrk,.zip,.json,application/json"
        bind:this={fileInput}
        multiple
        onchange={(event) => handleFiles((event.currentTarget as HTMLInputElement).files)}
        type="file"
      />
    </div>
    <div class="chip-row">
      {#each selectedFiles as file, index}
        <span class="chip start file-chip">
          <span>{file.name}</span>
          <button class="chip-action" onclick={() => removeSelectedFile(index)} type="button">{t('radtrack-common_remove-button')}</button>
        </span>
      {/each}
    </div>
    {#if busy}
      <div class="progress"><span style={`width:${uploadProgress}%`}></span></div>
    {/if}
    <div class="actions import-submit-actions">
      <button class="primary" disabled={!selectedFiles.length || busy} onclick={submit}>{t('radtrack-common_upload-button')}</button>
      <div class="import-submit-options">
        <label class="import-option-field">
          <input bind:checked={splitBulkArchivesIntoDatasets} type="checkbox" />
          <span class="import-option-copy">
            <span>{t('radtrack-import_split_bulk_datasets-label')}</span>
            <span class="faint">{t('radtrack-import_split_bulk_datasets-help')}</span>
          </span>
        </label>
        <label class="import-option-field">
          <input bind:checked={advancedTrackDeduplication} type="checkbox" />
          <span class="import-option-copy">
            <span>{t('radtrack-import_advanced_deduplication-label')}</span>
            <span class="faint">{t('radtrack-import_advanced_deduplication-help')}</span>
          </span>
        </label>
      </div>
    </div>
    {#if errorMessage}
      <p class="muted">{errorMessage}</p>
    {/if}
  </div>
</section>

{#if result}
  {@const datasetResults = normalizeDatasetResults(result)}
  {@const totalWarningEvents = datasetResults.reduce((total, datasetResult) => (
    total + collectWarningReports(datasetResult.summary).reduce((datasetTotal, report) => datasetTotal + report.warningCount, 0)
  ), 0)}
  <section class="panel import-summary-panel">
    <div class="page-header">
      <h2>{t('radtrack-import_summary-title')}</h2>
      {#if datasetResults.length === 1}
        <a href={`/datasets/${datasetResults[0].datasetId}`}>{t('radtrack-import_open_dataset-button')}</a>
      {/if}
    </div>
    <div class="chip-row">
      <span class="chip mid">{result.status}</span>
      <span class="chip start">{t('radtrack-import_dataset_count-label', { count: datasetResults.length || result.summary.datasetCount || 0 })}</span>
      {#if totalWarningEvents}
        <span class="chip warning">{t('radtrack-common_warnings-label')}: {totalWarningEvents}</span>
      {/if}
    </div>

    {#if datasetResults.length}
      <div class="grid import-dataset-results">
        {#each datasetResults as datasetResult}
          {@const warningReports = collectWarningReports(datasetResult.summary)}
          {@const datasetWarningEvents = warningReports.reduce((total, report) => total + report.warningCount, 0)}
          <section class="import-dataset-card">
            <div class="page-header">
              <div class="grid import-dataset-heading">
                <h3>{datasetResult.datasetName}</h3>
                <p class="muted">{t('radtrack-common_dataset-label')} {datasetResult.datasetId}</p>
              </div>
              <a href={`/datasets/${datasetResult.datasetId}`}>{t('radtrack-import_open_dataset-button')}</a>
            </div>

            <div class="chip-row">
              <span class="chip mid">{datasetResult.status}</span>
              {#if datasetWarningEvents}
                <span class="chip warning">{t('radtrack-common_warnings-label')}: {datasetWarningEvents}</span>
              {/if}
            </div>

            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>{t('radtrack-common_file-label')}</th>
                    <th>{t('radtrack-common_kind-label')}</th>
                    <th>{t('radtrack-common_status-label')}</th>
                    <th>{t('radtrack-common_children-label')}</th>
                    <th>{t('radtrack-common_warnings-label')}</th>
                  </tr>
                </thead>
                <tbody>
                  {#each datasetResult.summary.files as file}
                    <tr>
                      <td>
                        <div class="import-warning-file">
                          <span>{file.originalFilename}</span>
                          {#if file.sourceArchiveFilename && file.sourceArchiveFilename !== file.originalFilename}
                            <span class="faint">{file.sourceArchiveFilename}</span>
                          {/if}
                        </div>
                      </td>
                      <td>{file.importKind}</td>
                      <td>{getFileStatus(file)}</td>
                      <td>{file.children?.length ?? 0}</td>
                      <td>{getFileWarningCount(file)}</td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>

            {#if warningReports.length}
              <div class="grid import-warning-section">
                <div class="page-header">
                  <div>
                    <h4>{t('radtrack-import_warning_summary-title')}</h4>
                    <p class="muted">{t('radtrack-import_warning_summary-description', { count: warningReports.length })}</p>
                    <p class="faint">{t('radtrack-import_warning_saved-note')}</p>
                  </div>
                  <div class="actions">
                    <button
                      class="mid"
                      onclick={() => downloadWarningsText({ importResult: result!, datasetResult })}
                      type="button"
                    >
                      {t('radtrack-import_warning_download_text-button')}
                    </button>
                    <button
                      onclick={() => downloadWarningsJson({ importResult: result!, datasetResult })}
                      type="button"
                    >
                      {t('radtrack-import_warning_download_json-button')}
                    </button>
                  </div>
                </div>

                <div class="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>{t('radtrack-common_file-label')}</th>
                        <th>{t('radtrack-common_status-label')}</th>
                        <th>{t('radtrack-common_warnings-label')}</th>
                        <th>{t('radtrack-common_details-label')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {#each warningReports as report}
                        <tr>
                          <td>
                            <div class="import-warning-file">
                              <span>{report.childFileName}</span>
                              {#if report.sourceFileName !== report.childFileName}
                                <span class="faint">{report.sourceFileName}</span>
                              {/if}
                            </div>
                          </td>
                          <td>{report.parseStatus}</td>
                          <td>{report.warningCount}</td>
                          <td class="import-warning-details">
                            {#if report.warningBreakdown.length}
                              <div class="grid warning-group">
                                <strong>{t('radtrack-import_warning_breakdown-title')}</strong>
                                <ul class="import-warning-list">
                                  {#each report.warningBreakdown as item}
                                    <li>{item.type} ({item.count}): {item.reason}</li>
                                  {/each}
                                </ul>
                              </div>
                            {/if}

                            {#if report.rowWarnings.length}
                              <div class="grid warning-group">
                                <strong>{t('radtrack-import_warning_rows-title')}</strong>
                                <ul class="import-warning-list">
                                  {#each report.rowWarnings as warning}
                                    <li>row {warning.rowNumber ?? '?'} {warning.type}: {warning.reason}</li>
                                  {/each}
                                </ul>
                              </div>
                            {/if}
                          </td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              </div>
            {/if}
          </section>
        {/each}
      </div>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>{t('radtrack-common_file-label')}</th>
              <th>{t('radtrack-common_kind-label')}</th>
              <th>{t('radtrack-common_status-label')}</th>
              <th>{t('radtrack-common_children-label')}</th>
              <th>{t('radtrack-common_warnings-label')}</th>
            </tr>
          </thead>
          <tbody>
            {#each result.summary.files as file}
              <tr>
                <td>{file.originalFilename}</td>
                <td>{file.importKind}</td>
                <td>{getFileStatus(file)}</td>
                <td>{file.children?.length ?? 0}</td>
                <td>{getFileWarningCount(file)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
{/if}

<style>
  .import-option-field {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: center;
    gap: var(--space-3);
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
    flex: 1 1 20rem;
    max-width: 42rem;
  }

  .import-option-field input {
    width: auto;
    margin: 0;
  }

  .import-option-copy {
    display: grid;
    gap: var(--space-1);
  }

  .import-submit-actions {
    align-items: center;
    justify-content: space-between;
    gap: var(--space-3);
    flex-wrap: wrap;
  }

  .import-submit-options {
    display: flex;
    flex: 1;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: var(--space-3);
  }

  .import-summary-panel {
    display: grid;
    gap: var(--space-3);
  }

  .import-dataset-results {
    gap: var(--space-3);
  }

  .import-dataset-card {
    display: grid;
    gap: var(--space-3);
    padding: var(--space-3);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    background: var(--color-panel-strong);
  }

  .import-dataset-heading {
    gap: 0;
  }

  .import-dataset-heading h3,
  .import-dataset-heading p {
    margin: 0;
  }

  .import-warning-section {
    gap: var(--space-3);
  }

  .import-warning-file,
  .warning-group {
    gap: var(--space-1);
  }

  .import-warning-details {
    min-width: 22rem;
  }

  .import-warning-list {
    margin: 0;
    padding-left: 1.2rem;
  }
</style>
