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
  const sidebarStorageKey = 'radtrack.sidebar.open';

  const navigation = [
    { href: '/dashboard', labelKey: 'radtrack-layout_nav-dashboard-label' },
    {
      href: '/import-export',
      labelKey: 'radtrack-layout_nav-import_export-label',
      activePrefixes: ['/import-export', '/import/', '/export/']
    },
    { href: '/datasets', labelKey: 'radtrack-layout_nav-datasets-label' },
    { href: '/map', labelKey: 'radtrack-layout_nav-map-label' },
    { href: '/combined', labelKey: 'radtrack-layout_nav-combined-label' },
    { href: '/audit', labelKey: 'radtrack-layout_nav-audit-label' },
    { href: '/users', labelKey: 'radtrack-layout_nav-users-label', roles: ['moderator', 'admin'] },
    { href: '/settings', labelKey: 'radtrack-layout_nav-settings-label' },
    { href: '/admin-settings', labelKey: 'radtrack-layout_nav-admin_settings-label', roles: ['admin'] }
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
  const isDatasetsRoute = $derived(
    $page.url.pathname === '/datasets'
    || $page.url.pathname.startsWith('/datasets/')
  );
  const isImportExportRoute = $derived(
    $page.url.pathname === '/import-export'
    || $page.url.pathname.startsWith('/import/')
    || $page.url.pathname.startsWith('/export/')
  );
  const isDatalogRoute = $derived($page.url.pathname.startsWith('/datalogs/'));

  const requiresPasswordReset = $derived(
    $sessionStore.authenticated
    && Boolean($sessionStore.user?.mustChangePassword)
  );

  const showShell = $derived(
    $sessionStore.loaded
    && $sessionStore.authenticated
    && !requiresPasswordReset
    && !isAuthRoute
  );

  const sessionBootstrapping = $derived(
    !$sessionStore.loaded
    && !isAuthRoute
  );

  const redirectingToLogin = $derived(
    $sessionStore.loaded
    && !$sessionStore.authenticated
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

  const isActive = (entry: { href: string; activePrefixes?: string[] }) => (
    entry.activePrefixes
      ? entry.activePrefixes.some((prefix) => (
          $page.url.pathname === prefix
          || $page.url.pathname.startsWith(prefix)
        ))
      : (
          $page.url.pathname === entry.href
          || (
            entry.href !== '/dashboard'
            && $page.url.pathname.startsWith(`${entry.href}/`)
          )
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
      passwordMessage = t('radtrack-layout_password_update_failed');
      return;
    }

    if (passwordForm.newPassword !== passwordForm.confirmPassword) {
      passwordMessage = t('radtrack-layout_password_mismatch');
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
      passwordMessage = t('radtrack-layout_password_updated');
      await bootstrapSession();
    } catch (error) {
      passwordMessage = error instanceof Error ? error.message : t('radtrack-layout_password_update_failed');
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
      bootstrapError = error instanceof Error ? error.message : t('radtrack-layout_bootstrap_error-description');
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
  <title>{t('radtrack-app_title')}</title>
</svelte:head>

{#if bootstrapError}
  <main class="content narrow">
    <section class="panel">
      <h1>{t('radtrack-common_error-bootstrap')}</h1>
      <p class="muted">{bootstrapError}</p>
    </section>
  </main>
{:else if sessionBootstrapping}
  <main class="content wide">
    <section class="panel">
      <p class="muted">{t('radtrack-common_loading-label')}</p>
    </section>
  </main>
{:else if redirectingToLogin}
  <main class="content wide">
    <section class="panel">
      <p class="muted">{t('radtrack-common_redirecting')}</p>
    </section>
  </main>
{:else if showShell}
  <div class:sidebar-collapsed={!sidebarOpen} class="shell">
    <aside class="sidebar-shell">
      <div class="sidebar">
        <div class="sidebar-header">
          <div>
            <h2>{t('radtrack-app_title')}</h2>
            <p class="muted">{t('radtrack-layout_shell-description')}</p>
          </div>
          <button
            aria-label={t('radtrack-layout_collapse_nav-button')}
            aria-expanded={sidebarOpen}
            class="sidebar-toggle"
            onclick={toggleSidebar}
            type="button"
          >
            &lt;
          </button>
        </div>

        <div class="chip-row">
          <span class="chip start">{t('radtrack-common_user-label')}: {$sessionStore.user?.username}</span>
          <span class="chip mid">{t('radtrack-common_group-label')}: {$sessionStore.user?.role}</span>
        </div>

        <nav class="nav-list">
          {#each visibleNavigation as item}
            <a
              class:active={isActive(item)}
              class="nav-link"
              href={item.href}
            >
              {t(item.labelKey)}
            </a>
          {/each}
        </nav>

        <div class="sidebar-footer grid">
          <button class="danger" onclick={handleLogout}>{t('radtrack-common_logout-button')}</button>
          <div>
            <div class="faint">{t('radtrack-common_build-label')}</div>
            <code>{$sessionStore.build?.label ?? 'v0.0.1-unknown'}</code>
          </div>
        </div>
      </div>
    </aside>

    {#if !sidebarOpen}
      <button
        aria-label={t('radtrack-layout_expand_nav-button')}
        aria-expanded={sidebarOpen}
        class="sidebar-toggle sidebar-toggle-floating"
        onclick={toggleSidebar}
        type="button"
      >
        &gt;
      </button>
    {/if}

    <main
      class:datalog-content={isDatalogRoute}
      class:datasets-content={isDatasetsRoute}
      class:import-export-content={isImportExportRoute}
      class:map-content={isMapRoute}
      class="content wide"
    >
      {@render children()}
    </main>
  </div>
{:else if requiresPasswordReset}
  <main class="content password-reset-view">
    <section class="panel password-reset-panel">
      <form class="form-grid" onsubmit={submitPasswordChange}>
        <div>
          <div class="faint">{t('radtrack-app_title')}</div>
          <h1>{t('radtrack-layout_password_change_required-title')}</h1>
          <p class="muted">{t('radtrack-layout_must_change_password-description')}</p>
        </div>

        <div class="chip-row">
          <span class="chip start">{t('radtrack-common_user-label')}: {$sessionStore.user?.username}</span>
          <span class="chip mid">{t('radtrack-common_group-label')}: {$sessionStore.user?.role}</span>
        </div>

        <input bind:value={passwordForm.newPassword} placeholder={t('radtrack-common_new_password-placeholder')} type="password" />
        <input bind:value={passwordForm.confirmPassword} placeholder={t('radtrack-common_confirm_new_password-placeholder')} type="password" />

        <div class="actions">
          <button class="warning" disabled={passwordBusy} type="submit">{t('radtrack-common_update_password-button')}</button>
          <button class="danger" disabled={passwordBusy} onclick={handleLogout} type="button">{t('radtrack-common_logout-button')}</button>
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
