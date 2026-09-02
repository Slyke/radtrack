<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation';
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { bootstrapSession, sessionStore } from '$lib/stores/session';
  import { localLogin, startOidcLogin } from '$lib/api/client';

  let username = $state('');
  let password = $state('');
  let errorMessage = $state<string | null>(null);
  let busy = $state(false);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const submit = async () => {
    busy = true;
    errorMessage = null;
    try {
      await localLogin({ username, password });
      await bootstrapSession();
      await goto('/dashboard');
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-login_failed');
    } finally {
      busy = false;
    }
  };
</script>

<div class="login-shell">
  <section class="panel login-card">
    <div class="page-header">
      <div>
        <div class="title-with-info">
          <h1>{t('radtrack-login_title')}</h1>
          <InfoTip text="Authenticate with one of the sign-in methods enabled by this RadTrack deployment." />
        </div>
        <p class="muted">{t('radtrack-login_description')}</p>
      </div>
      <code>{$sessionStore.build?.label ?? 'v0.0.1-unknown'}</code>
    </div>

    <div class="form-grid">
      {#if $sessionStore.authModes.local}
        <form class="form-grid" onsubmit={(event) => {
          event.preventDefault();
          submit();
        }}>
          <label>
            <div class="muted field-title">
              {t('radtrack-common_username-label')}
              <InfoTip text="Enter the username for a local RadTrack account." />
            </div>
            <input bind:value={username} autocomplete="username" />
          </label>
          <label>
            <div class="muted field-title">
              {t('radtrack-common_password-label')}
              <InfoTip text="Enter the password for the local account. It is sent only to the configured RadTrack API." />
            </div>
            <input bind:value={password} autocomplete="current-password" type="password" />
          </label>
          <div class="actions">
            <div class="control-with-info">
              <button class="primary" disabled={busy} type="submit">{t('radtrack-common_sign_in_locally-button')}</button>
              <InfoTip text="Start a local username-and-password session." />
            </div>
          </div>
        </form>
      {/if}

      {#if $sessionStore.authModes.oidc}
        <div class="actions">
          <div class="control-with-info">
            <button class="mid" onclick={() => startOidcLogin({ redirectTo: '/dashboard' })}>{t('radtrack-login_continue_oidc-button')}</button>
            <InfoTip text="Continue through the deployment’s external OpenID Connect identity provider." />
          </div>
        </div>
      {/if}

      {#if $sessionStore.authModes.header}
        <p class="muted">{t('radtrack-login_header_auth-description')}</p>
      {/if}

      {#if errorMessage}
        <p class="muted">{errorMessage}</p>
      {/if}
    </div>
  </section>
</div>
