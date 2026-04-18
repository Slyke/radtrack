<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  $effect(() => {
    if (!$sessionStore.loaded) {
      return;
    }

    goto($sessionStore.authenticated ? '/dashboard' : '/login');
  });
</script>

<section class="panel">
  <h1>{t('radiacode-app_title')}</h1>
  <p class="muted">{t('radiacode-root_redirecting-description')}</p>
</section>
