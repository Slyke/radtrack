<script lang="ts">
  import { localeStore, translateMessage } from '$lib/i18n';

  export let error: {
    message?: string;
    errorKey?: string;
    errorCode?: string;
    correlationId?: string | null;
  };

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });
</script>

<section class="panel">
  <h1>{t('radiacode-common_error-request_failed')}</h1>
  <p class="muted">{error?.message ?? t('radiacode-common_error-unexpected_frontend')}</p>
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
