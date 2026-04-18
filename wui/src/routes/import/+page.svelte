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
    duplicateOfRawFileId?: string | null;
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
    children?: ImportChildSummary[];
  }

  interface ImportBatchSummary {
    datasetId: string;
    datasetName: string;
    fileCount: number;
    duplicateFileCount: number;
    warningCount: number;
    files: ImportFileSummary[];
  }

  interface ImportResult {
    batchId: string;
    datasetId: string;
    status: string;
    summary: ImportBatchSummary;
  }

  interface ImportWarningReport {
    sourceFileName: string;
    childFileName: string;
    parseStatus: string;
    trackId: string | null;
    duplicateOfRawFileId: string | null;
    warningCount: number;
    warningBreakdown: ImportWarningBreakdownEntry[];
    rowWarnings: ImportWarningEntry[];
    persisted: boolean;
  }

  let fileInput: HTMLInputElement | null = null;
  let selectedFiles = $state<File[]>([]);
  let datasetName = $state('');
  let description = $state('');
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
  const duplicateFileReason = 'File matched an earlier raw artifact and was skipped.';

  const fileSummary = (files: File[]) => files.map((file) => file.name).join(', ');

  const syncSelectedFiles = (files: File[]) => {
    selectedFiles = files;

    if (!datasetName.trim() && files.length) {
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

  const collectWarningReports = (summary: ImportBatchSummary | null | undefined): ImportWarningReport[] => (
    (summary?.files ?? [])
      .flatMap((file) => (file.children ?? [])
        .map((child) => {
          const warningBreakdown = getWarningBreakdown(child);
          const rowWarnings = getRowWarnings(child);
          const warningCount = getWarningCount(child);

          return {
            sourceFileName: file.originalFilename,
            childFileName: child.originalFilename,
            parseStatus: child.parseStatus,
            trackId: child.summary?.trackId ?? null,
            duplicateOfRawFileId: child.duplicateOfRawFileId ?? child.summary?.duplicateOfRawFileId ?? null,
            warningCount,
            warningBreakdown,
            rowWarnings,
            persisted: true
          };
        })
        .filter((child) => child.warningCount > 0))
  );

  const getWarningFileNameBase = (importResult: ImportResult) => (
    `radiacode-import-warnings-${importResult.batchId}`
  );

  const buildWarningExportEnvelope = ({
    importResult,
    warningReports
  }: {
    importResult: ImportResult;
    warningReports: ImportWarningReport[];
  }) => ({
    title: 'Radiacode',
    type: 'import warnings',
    exportTime: formatDateTime({
      value: new Date().toISOString(),
      language: $localeStore.language
    }) ?? new Date().toISOString(),
    build: $sessionStore.build?.label ?? null,
    filters: {
      batchId: importResult.batchId,
      datasetId: importResult.datasetId,
      datasetName: importResult.summary.datasetName,
      status: importResult.status,
      totalFiles: importResult.summary.fileCount,
      duplicateFiles: importResult.summary.duplicateFileCount,
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
    `Build: ${envelope.build ?? t('radiacode-common_none')}`,
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
        report.duplicateOfRawFileId ? `Duplicate of raw file: ${report.duplicateOfRawFileId}` : null,
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

  const downloadWarningsText = (importResult: ImportResult) => {
    const warningReports = collectWarningReports(importResult.summary);
    if (!warningReports.length) {
      return;
    }

    const envelope = buildWarningExportEnvelope({ importResult, warningReports });
    downloadWarningBlob({
      content: buildWarningExportText({ envelope }),
      fileName: `${getWarningFileNameBase(importResult)}.txt`,
      type: 'text/plain;charset=utf-8'
    });
  };

  const downloadWarningsJson = (importResult: ImportResult) => {
    const warningReports = collectWarningReports(importResult.summary);
    if (!warningReports.length) {
      return;
    }

    const envelope = buildWarningExportEnvelope({ importResult, warningReports });
    downloadWarningBlob({
      content: JSON.stringify(envelope, null, 2),
      fileName: `${getWarningFileNameBase(importResult)}.json`,
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
        csrf: $sessionStore.csrf,
        onProgress: (percent) => {
          uploadProgress = percent;
        }
      }) as ImportResult;
      resetImportForm();
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radiacode-import_failed');
    } finally {
      busy = false;
    }
  };
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-import_title')}</h1>
    <p class="muted">{t('radiacode-import_description')}</p>
  </div>
</div>

<section class="panel">
  <div class="form-grid">
    <label>
      <div class="muted">{t('radiacode-common_dataset_name-label')}</div>
      <input bind:value={datasetName} placeholder={t('radiacode-import_optional_dataset_name-placeholder')} />
    </label>
    <label>
      <div class="muted">{t('radiacode-common_description-label')}</div>
      <textarea bind:value={description} placeholder={t('radiacode-import_optional_description-placeholder')}></textarea>
    </label>
    {#if !description.trim() && selectedFiles.length}
      <div class="actions">
        <button class="mid" onclick={autofillDescription} type="button">{t('radiacode-import_autofill_description-button')}</button>
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
      <p>{t('radiacode-import_drag_drop-label')}</p>
      <input bind:this={fileInput} multiple onchange={(event) => handleFiles((event.currentTarget as HTMLInputElement).files)} type="file" />
    </div>
    <div class="chip-row">
      {#each selectedFiles as file, index}
        <span class="chip start file-chip">
          <span>{file.name}</span>
          <button class="chip-action" onclick={() => removeSelectedFile(index)} type="button">{t('radiacode-common_remove-button')}</button>
        </span>
      {/each}
    </div>
    {#if busy}
      <div class="progress"><span style={`width:${uploadProgress}%`}></span></div>
    {/if}
    <div class="actions">
      <button class="primary" disabled={!selectedFiles.length || busy} onclick={submit}>{t('radiacode-common_upload-button')}</button>
    </div>
    {#if errorMessage}
      <p class="muted">{errorMessage}</p>
    {/if}
  </div>
</section>

{#if result}
  {@const warningReports = collectWarningReports(result.summary)}
  {@const totalWarningEvents = warningReports.reduce((total, report) => total + report.warningCount, 0)}
  <section class="panel import-summary-panel">
    <div class="page-header">
      <h2>{t('radiacode-import_summary-title')}</h2>
      <a href={`/datasets/${result.datasetId}`}>{t('radiacode-import_open_dataset-button')}</a>
    </div>
    <div class="chip-row">
      <span class="chip mid">{result.status}</span>
      <span class="chip start">{t('radiacode-common_dataset-label')} {result.datasetId}</span>
      {#if totalWarningEvents}
        <span class="chip warning">{t('radiacode-common_warnings-label')}: {totalWarningEvents}</span>
      {/if}
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>{t('radiacode-common_file-label')}</th>
            <th>{t('radiacode-common_kind-label')}</th>
            <th>{t('radiacode-common_children-label')}</th>
            <th>{t('radiacode-common_warnings-label')}</th>
          </tr>
        </thead>
        <tbody>
          {#each result.summary.files as file}
            <tr>
              <td>{file.originalFilename}</td>
              <td>{file.importKind}</td>
              <td>{file.children?.length ?? 0}</td>
              <td>{(file.children ?? []).reduce((total, child) => total + getWarningCount(child), 0)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>

    {#if warningReports.length}
      <div class="grid import-warning-section">
        <div class="page-header">
          <div>
            <h3>{t('radiacode-import_warning_summary-title')}</h3>
            <p class="muted">{t('radiacode-import_warning_summary-description', { count: warningReports.length })}</p>
            <p class="faint">{t('radiacode-import_warning_saved-note')}</p>
          </div>
          <div class="actions">
            <button class="mid" onclick={() => downloadWarningsText(result!)} type="button">{t('radiacode-import_warning_download_text-button')}</button>
            <button onclick={() => downloadWarningsJson(result!)} type="button">{t('radiacode-import_warning_download_json-button')}</button>
          </div>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>{t('radiacode-common_file-label')}</th>
                <th>{t('radiacode-common_status-label')}</th>
                <th>{t('radiacode-common_warnings-label')}</th>
                <th>{t('radiacode-common_details-label')}</th>
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
                        <strong>{t('radiacode-import_warning_breakdown-title')}</strong>
                        <ul class="import-warning-list">
                          {#each report.warningBreakdown as item}
                            <li>{item.type} ({item.count}): {item.reason}</li>
                          {/each}
                        </ul>
                      </div>
                    {/if}

                    {#if report.rowWarnings.length}
                      <div class="grid warning-group">
                        <strong>{t('radiacode-import_warning_rows-title')}</strong>
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
{/if}

<style>
  .import-summary-panel {
    display: grid;
    gap: var(--space-3);
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
