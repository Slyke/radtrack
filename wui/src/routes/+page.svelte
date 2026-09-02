<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation';
  import InfoTip from '$lib/components/InfoTip.svelte';
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
  <div class="title-with-info">
    <h1>{t('radtrack-app_title')}</h1>
    <InfoTip text="RadTrack imports, organizes, maps, combines, and exports radiation track data." />
  </div>
  <p class="muted">{t('radtrack-root_redirecting-description')}</p>
</section>
