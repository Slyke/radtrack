<script lang="ts">
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });
</script>

<section class="panel">
  <div class="title-with-info">
    <h1>{t('radtrack-common_authentication_error-title')}</h1>
    <InfoTip text="Authentication could not be completed. Return to sign-in to retry or choose another configured method." />
  </div>
  <p class="muted">{t('radtrack-auth_error-description')}</p>
  <div class="actions">
    <div class="control-with-info">
      <a class="button-link" href="/login">{t('radtrack-common_back_to_login-button')}</a>
      <InfoTip text="Return to the sign-in page without changing any account data." />
    </div>
  </div>
</section>
