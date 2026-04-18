<svelte:options runes={true} />

<script lang="ts">
  import { browser } from '$app/environment';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { onMount } from 'svelte';
  import '$lib/styles/app.css';
  import { bootstrapSession, sessionStore } from '$lib/stores/session';
  import { changePassword, logout } from '$lib/api/client';
  import { localeStore, setLocale, translateMessage } from '$lib/i18n';

  const { children } = $props();
  const sidebarStorageKey = 'radiacode.sidebar.open';

  const navigation = [
    { href: '/dashboard', labelKey: 'radiacode-layout_nav-dashboard-label' },
    { href: '/import', labelKey: 'radiacode-layout_nav-import-label' },
    { href: '/datasets', labelKey: 'radiacode-layout_nav-datasets-label' },
    { href: '/map', labelKey: 'radiacode-layout_nav-map-label' },
    { href: '/combined', labelKey: 'radiacode-layout_nav-combined-label' },
    { href: '/export', labelKey: 'radiacode-layout_nav-export-label' },
    { href: '/audit', labelKey: 'radiacode-layout_nav-audit-label' },
    { href: '/users', labelKey: 'radiacode-layout_nav-users-label', roles: ['moderator', 'admin'] },
    { href: '/settings', labelKey: 'radiacode-layout_nav-settings-label', roles: ['admin'] }
  ];

  let bootstrapError = $state<string | null>(null);
  let sidebarOpen = $state(true);
  let passwordForm = $state({
    newPassword: '',
    confirmPassword: ''
  });
  let passwordMessage = $state<string | null>(null);
  let passwordBusy = $state(false);

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const isAuthRoute = $derived(
    $page.url.pathname === '/login'
    || $page.url.pathname === '/auth-error'
  );

  const isLoginRoute = $derived($page.url.pathname === '/login');
  const isMapRoute = $derived($page.url.pathname === '/map');

  const requiresPasswordReset = $derived(
    $sessionStore.authenticated
    && Boolean($sessionStore.user?.mustChangePassword)
  );

  const showShell = $derived(
    $sessionStore.authenticated
    && !requiresPasswordReset
    && !isAuthRoute
  );

  const visibleNavigation = $derived(
    navigation.filter((entry) => !entry.roles || entry.roles.includes($sessionStore.user?.role ?? 'view_only'))
  );

  const loadSidebarState = () => {
    if (!browser) {
      return;
    }

    const storedValue = window.localStorage.getItem(sidebarStorageKey);
    sidebarOpen = storedValue === null ? true : storedValue !== 'false';
  };

  const persistSidebarState = () => {
    if (!browser) {
      return;
    }

    window.localStorage.setItem(sidebarStorageKey, String(sidebarOpen));
  };

  const isActive = (href: string) => (
    $page.url.pathname === href
    || (
      href !== '/dashboard'
      && $page.url.pathname.startsWith(`${href}/`)
    )
  );

  const applyUi = () => {
    if (!browser || !$sessionStore.ui) {
      return;
    }

    document.documentElement.dataset.theme = $sessionStore.ui.theme || 'dark';
    document.documentElement.dataset.font = $sessionStore.ui.font || 'ui-mono';
    document.documentElement.lang = $sessionStore.ui.language || 'en-US';
    setLocale({ language: $sessionStore.ui.language });
  };

  const handleLogout = async () => {
    await logout({ csrf: $sessionStore.csrf });
    await bootstrapSession();
    await goto('/login');
  };

  const toggleSidebar = () => {
    sidebarOpen = !sidebarOpen;
    persistSidebarState();
  };

  const submitPasswordChange = async (event?: Event) => {
    event?.preventDefault();

    if (!$sessionStore.csrf) {
      return;
    }

    if (!passwordForm.newPassword) {
      passwordMessage = t('radiacode-layout_password_update_failed');
      return;
    }

    if (passwordForm.newPassword !== passwordForm.confirmPassword) {
      passwordMessage = t('radiacode-layout_password_mismatch');
      return;
    }

    passwordBusy = true;
    passwordMessage = null;
    try {
      await changePassword({
        csrf: $sessionStore.csrf,
        newPassword: passwordForm.newPassword
      });
      passwordForm = {
        newPassword: '',
        confirmPassword: ''
      };
      passwordMessage = t('radiacode-layout_password_updated');
      await bootstrapSession();
    } catch (error) {
      passwordMessage = error instanceof Error ? error.message : t('radiacode-layout_password_update_failed');
    } finally {
      passwordBusy = false;
    }
  };

  onMount(async () => {
    try {
      loadSidebarState();
      await bootstrapSession();
      applyUi();
    } catch (error) {
      bootstrapError = error instanceof Error ? error.message : t('radiacode-layout_bootstrap_error-description');
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
  <title>{t('radiacode-app_title')}</title>
</svelte:head>

{#if bootstrapError}
  <main class="content narrow">
    <section class="panel">
      <h1>{t('radiacode-common_error-bootstrap')}</h1>
      <p class="muted">{bootstrapError}</p>
    </section>
  </main>
{:else if showShell}
  <div class:sidebar-collapsed={!sidebarOpen} class="shell">
    <aside class="sidebar-shell">
      <div class="sidebar">
        <div class="sidebar-header">
          <div>
            <h2>{t('radiacode-app_title')}</h2>
            <p class="muted">{t('radiacode-layout_shell-description')}</p>
          </div>
          <button
            aria-label={t('radiacode-layout_collapse_nav-button')}
            aria-expanded={sidebarOpen}
            class="sidebar-toggle"
            onclick={toggleSidebar}
            type="button"
          >
            &lt;
          </button>
        </div>

        <div class="chip-row">
          <span class="chip start">{t('radiacode-common_user-label')}: {$sessionStore.user?.username}</span>
          <span class="chip mid">{t('radiacode-common_group-label')}: {$sessionStore.user?.role}</span>
        </div>

        <nav class="nav-list">
          {#each visibleNavigation as item}
            <a
              class:active={isActive(item.href)}
              class="nav-link"
              href={item.href}
            >
              {t(item.labelKey)}
            </a>
          {/each}
        </nav>

        <div class="sidebar-footer grid">
          <button class="danger" onclick={handleLogout}>{t('radiacode-common_logout-button')}</button>
          <div>
            <div class="faint">{t('radiacode-common_build-label')}</div>
            <code>{$sessionStore.build?.label ?? 'v0.0.1-unknown'}</code>
          </div>
        </div>
      </div>
    </aside>

    {#if !sidebarOpen}
      <button
        aria-label={t('radiacode-layout_expand_nav-button')}
        aria-expanded={sidebarOpen}
        class="sidebar-toggle sidebar-toggle-floating"
        onclick={toggleSidebar}
        type="button"
      >
        &gt;
      </button>
    {/if}

    <main class:map-content={isMapRoute} class="content wide">
      {@render children()}
    </main>
  </div>
{:else if requiresPasswordReset}
  <main class="content password-reset-view">
    <section class="panel password-reset-panel">
      <form class="form-grid" onsubmit={submitPasswordChange}>
        <div>
          <div class="faint">{t('radiacode-app_title')}</div>
          <h1>{t('radiacode-layout_password_change_required-title')}</h1>
          <p class="muted">{t('radiacode-layout_must_change_password-description')}</p>
        </div>

        <div class="chip-row">
          <span class="chip start">{t('radiacode-common_user-label')}: {$sessionStore.user?.username}</span>
          <span class="chip mid">{t('radiacode-common_group-label')}: {$sessionStore.user?.role}</span>
        </div>

        <input bind:value={passwordForm.newPassword} placeholder={t('radiacode-common_new_password-placeholder')} type="password" />
        <input bind:value={passwordForm.confirmPassword} placeholder={t('radiacode-common_confirm_new_password-placeholder')} type="password" />

        <div class="actions">
          <button class="warning" disabled={passwordBusy} type="submit">{t('radiacode-common_update_password-button')}</button>
          <button class="danger" disabled={passwordBusy} onclick={handleLogout} type="button">{t('radiacode-common_logout-button')}</button>
        </div>

        {#if passwordMessage}
          <p class="muted">{passwordMessage}</p>
        {/if}
      </form>
    </section>
  </main>
{:else}
  <main class:login-screen={isLoginRoute} class="content narrow">
    {@render children()}
  </main>
{/if}
