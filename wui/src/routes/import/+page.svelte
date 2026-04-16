<svelte:options runes={true} />

<script lang="ts">
  import { sessionStore } from '$lib/stores/session';
  import { uploadImport } from '$lib/api/client';

  let selectedFiles = $state<File[]>([]);
  let datasetName = $state('');
  let description = $state('');
  let uploadProgress = $state(0);
  let busy = $state(false);
  let dragActive = $state(false);
  let result = $state<any>(null);
  let errorMessage = $state<string | null>(null);

  const handleFiles = (files: FileList | null) => {
    selectedFiles = files ? Array.from(files) : [];
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
      });
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Upload failed';
    } finally {
      busy = false;
    }
  };
</script>

<div class="page-header">
  <div>
    <h1>Import Tracks</h1>
    <p class="muted">Accepts `.rctrk`, bulk `.rctrk`, `.zip`, and `.zrctrk`. Raw artifacts are archived in Postgres before parsing.</p>
  </div>
</div>

<section class="panel">
  <div class="form-grid">
    <label>
      <div class="muted">Dataset name</div>
      <input bind:value={datasetName} placeholder="Optional dataset name" />
    </label>
    <label>
      <div class="muted">Description</div>
      <textarea bind:value={description} placeholder="Optional dataset description"></textarea>
    </label>
    <div
      class:active={dragActive}
      class="dropzone"
      role="button"
      tabindex="0"
      ondragenter={() => (dragActive = true)}
      ondragleave={() => (dragActive = false)}
      ondragover={(event) => event.preventDefault()}
      onkeydown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          const input = event.currentTarget?.querySelector('input[type=\"file\"]') as HTMLInputElement | null;
          input?.click();
        }
      }}
      ondrop={(event) => {
        event.preventDefault();
        dragActive = false;
        handleFiles(event.dataTransfer?.files ?? null);
      }}
    >
      <p>Drag files here or pick them manually.</p>
      <input multiple onchange={(event) => handleFiles((event.currentTarget as HTMLInputElement).files)} type="file" />
    </div>
    <div class="chip-row">
      {#each selectedFiles as file}
        <span class="chip start">{file.name}</span>
      {/each}
    </div>
    {#if busy}
      <div class="progress"><span style={`width:${uploadProgress}%`}></span></div>
    {/if}
    <div class="actions">
      <button class="primary" disabled={!selectedFiles.length || busy} onclick={submit}>Upload</button>
    </div>
    {#if errorMessage}
      <p class="muted">{errorMessage}</p>
    {/if}
  </div>
</section>

{#if result}
  <section class="panel">
    <div class="page-header">
      <h2>Import Summary</h2>
      <a href={`/datasets/${result.datasetId}`}>Open dataset</a>
    </div>
    <div class="chip-row">
      <span class="chip mid">{result.status}</span>
      <span class="chip start">dataset {result.datasetId}</span>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>File</th>
            <th>Kind</th>
            <th>Children</th>
          </tr>
        </thead>
        <tbody>
          {#each result.summary.files as file}
            <tr>
              <td>{file.originalFilename}</td>
              <td>{file.importKind}</td>
              <td>{file.children?.length ?? 0}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <pre>{JSON.stringify(result.summary, null, 2)}</pre>
  </section>
{/if}
