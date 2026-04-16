<script lang="ts">
  export let error: {
    message?: string;
    errorKey?: string;
    errorCode?: string;
    correlationId?: string | null;
  };
</script>

<section class="panel">
  <h1>Request Failed</h1>
  <p class="muted">{error?.message ?? 'Unexpected frontend error'}</p>
  <div class="chip-row">
    {#if error?.errorKey}
      <span class="chip warning">{error.errorKey}</span>
    {/if}
    {#if error?.errorCode}
      <span class="chip danger">{error.errorCode}</span>
    {/if}
    {#if error?.correlationId}
      <span class="chip start">{error.correlationId}</span>
    {/if}
  </div>
</section>
