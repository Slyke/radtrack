<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation';
  import { sessionStore } from '$lib/stores/session';

  $effect(() => {
    if (!$sessionStore.loaded) {
      return;
    }

    goto($sessionStore.authenticated ? '/dashboard' : '/login');
  });
</script>

<section class="panel">
  <h1>Radiacode</h1>
  <p class="muted">Redirecting…</p>
</section>
