<script lang="ts">
  import { localeStore, translateMessage } from '$lib/i18n';

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });
</script>

<section class="panel">
  <h1>{t('radtrack-common_authentication_error-title')}</h1>
  <p class="muted">{t('radtrack-auth_error-description')}</p>
  <div class="actions">
    <a class="button-link" href="/login">{t('radtrack-common_back_to_login-button')}</a>
  </div>
</section>
