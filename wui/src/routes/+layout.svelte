<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import '$lib/styles/app.css';
  import { bootstrapSession, sessionStore } from '$lib/stores/session';
  import { changePassword, logout } from '$lib/api/client';

  const { children } = $props();

  const navigation = [
    { href: '/dashboard', label: 'Dashboard' },
    { href: '/import', label: 'Import' },
    { href: '/datasets', label: 'Datasets' },
    { href: '/map', label: 'Map' },
    { href: '/combined', label: 'Combined' },
    { href: '/export', label: 'Export' },
    { href: '/users', label: 'Users', roles: ['moderator', 'admin'] },
    { href: '/settings', label: 'Settings', roles: ['admin'] }
  ];

  let bootstrapError = $state<string | null>(null);
  let passwordForm = $state({
    currentPassword: '',
    newPassword: ''
  });
  let passwordMessage = $state<string | null>(null);
  let passwordBusy = $state(false);

  const showShell = $derived(
    $sessionStore.authenticated
    && $page.url.pathname !== '/login'
    && $page.url.pathname !== '/auth-error'
  );

  const visibleNavigation = $derived(
    navigation.filter((entry) => !entry.roles || entry.roles.includes($sessionStore.user?.role ?? 'view_only'))
  );

  const applyUi = () => {
    if (!browser || !$sessionStore.ui) {
      return;
    }

    document.documentElement.dataset.theme = $sessionStore.ui.theme || 'dark';
    document.documentElement.dataset.font = $sessionStore.ui.font || 'ui-mono';
  };

  const handleLogout = async () => {
    await logout({ csrf: $sessionStore.csrf });
    await bootstrapSession();
    await goto('/login');
  };

  const submitPasswordChange = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    passwordBusy = true;
    passwordMessage = null;
    try {
      await changePassword({
        csrf: $sessionStore.csrf,
        currentPassword: passwordForm.currentPassword,
        newPassword: passwordForm.newPassword
      });
      passwordForm = {
        currentPassword: '',
        newPassword: ''
      };
      passwordMessage = 'Password updated. Sign in again if prompted.';
      await bootstrapSession();
    } catch (error) {
      passwordMessage = error instanceof Error ? error.message : 'Password update failed';
    } finally {
      passwordBusy = false;
    }
  };

  onMount(async () => {
    try {
      await bootstrapSession();
      applyUi();
    } catch (error) {
      bootstrapError = error instanceof Error ? error.message : 'Failed to bootstrap session.';
    }
  });

  $effect(() => {
    applyUi();

    if (!$sessionStore.loaded) {
      return;
    }

    if (!$sessionStore.authenticated && $page.url.pathname !== '/login' && $page.url.pathname !== '/auth-error') {
      goto('/login');
      return;
    }

    if ($sessionStore.authenticated && $page.url.pathname === '/login') {
      goto('/dashboard');
    }
  });
</script>

<svelte:head>
  <title>Radiacode</title>
</svelte:head>

{#if bootstrapError}
  <main class="content narrow">
    <section class="panel">
      <h1>Bootstrap Error</h1>
      <p class="muted">{bootstrapError}</p>
    </section>
  </main>
{:else if showShell}
  <div class="shell">
    <aside class="sidebar">
      <div>
        <h2>Radiacode</h2>
        <p class="muted">Tracks, maps, sharing, export.</p>
      </div>

      <div class="chip-row">
        <span class="chip start">{$sessionStore.user?.username}</span>
        <span class="chip mid">{$sessionStore.user?.role}</span>
      </div>

      <nav class="nav-list">
        {#each visibleNavigation as item}
          <a
            class:active={$page.url.pathname === item.href}
            class="nav-link"
            href={item.href}
          >
            {item.label}
          </a>
        {/each}
      </nav>

      {#if $sessionStore.user?.mustChangePassword}
        <section class="panel">
          <h3>Password Change Required</h3>
          <p class="muted">Local-password users and bootstrap admins must rotate passwords on first login.</p>
          <div class="form-grid">
            <input bind:value={passwordForm.currentPassword} placeholder="Current password" type="password" />
            <input bind:value={passwordForm.newPassword} placeholder="New password" type="password" />
            <button class="warning" disabled={passwordBusy} onclick={submitPasswordChange}>Update password</button>
            {#if passwordMessage}
              <p class="muted">{passwordMessage}</p>
            {/if}
          </div>
        </section>
      {/if}

      <div class="sidebar-footer grid">
        <button onclick={handleLogout}>Logout</button>
        <div>
          <div class="faint">Build</div>
          <code>{$sessionStore.build?.label ?? 'v0.0.1-unknown'}</code>
        </div>
      </div>
    </aside>

    <main class="content wide">
      {@render children()}
    </main>
  </div>
{:else}
  <main class="content narrow">
    {@render children()}
  </main>
{/if}
