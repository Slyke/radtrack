<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation';
  import { bootstrapSession, sessionStore } from '$lib/stores/session';
  import { localLogin, startOidcLogin } from '$lib/api/client';

  let username = $state('');
  let password = $state('');
  let errorMessage = $state<string | null>(null);
  let busy = $state(false);

  const submit = async () => {
    busy = true;
    errorMessage = null;
    try {
      await localLogin({ username, password });
      await bootstrapSession();
      await goto('/dashboard');
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Login failed';
    } finally {
      busy = false;
    }
  };
</script>

<div class="page-header">
  <div>
    <h1>Login</h1>
    <p class="muted">Local login is app-owned. OIDC and trusted-header auth link back to local DB users.</p>
  </div>
  <code>{$sessionStore.build?.label ?? 'v0.0.1-unknown'}</code>
</div>

<section class="panel">
  <div class="form-grid">
    <label>
      <div class="muted">Username</div>
      <input bind:value={username} autocomplete="username" />
    </label>
    <label>
      <div class="muted">Password</div>
      <input bind:value={password} autocomplete="current-password" type="password" />
    </label>
    <div class="actions">
      {#if $sessionStore.authModes.local}
        <button class="primary" disabled={busy} onclick={submit}>Sign in locally</button>
      {/if}
      {#if $sessionStore.authModes.oidc}
        <button class="mid" onclick={() => startOidcLogin({ redirectTo: '/dashboard' })}>Continue with OIDC</button>
      {/if}
    </div>
    {#if $sessionStore.authModes.header}
      <p class="muted">Trusted reverse-proxy auth is enabled. If upstream auth already succeeded, refreshing this page should establish access automatically.</p>
    {/if}
    {#if errorMessage}
      <p class="muted">{errorMessage}</p>
    {/if}
  </div>
</section>
