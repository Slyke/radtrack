<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import InfoTip from '$lib/components/InfoTip.svelte';
  import { localeStore, translateMessage } from '$lib/i18n';
  import { sessionStore } from '$lib/stores/session';

  let users = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let resetPasswordResult = $state<string | null>(null);
  let createForm = $state({
    username: '',
    role: 'standard',
    password: ''
  });

  const t = (key: string, values: Record<string, unknown> = {}) => translateMessage({
    key,
    values,
    messages: $localeStore.messages
  });

  const loadUsers = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/users' });
      users = response.users;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : t('radtrack-users_failed');
    }
  };

  const createUser = async () => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: '/api/users',
      method: 'POST',
      body: createForm,
      csrf: $sessionStore.csrf
    });
    createForm = { username: '', role: 'standard', password: '' };
    await loadUsers();
  };

  const toggleDisabled = async (user: any) => {
    if (!$sessionStore.csrf) {
      return;
    }

    await apiFetch({
      path: `/api/users/${user.id}`,
      method: 'PATCH',
      body: {
        isDisabled: !user.isDisabled
      },
      csrf: $sessionStore.csrf
    });
    await loadUsers();
  };

  const resetPassword = async (userId: string) => {
    if (!$sessionStore.csrf) {
      return;
    }

    const response = await apiFetch<any>({
      path: `/api/users/${userId}/reset-password`,
      method: 'POST',
      body: {},
      csrf: $sessionStore.csrf
    });
    resetPasswordResult = response.result.password;
  };

  const canToggleDisabled = (user: any) => user.isDisabled || !user.disableProtectionReason;

  const getDisableProtectionMessage = (user: any) => {
    if (user.disableProtectionReason === 'bootstrap_admin') {
      return t('radtrack-users_disable_blocked_bootstrap_admin');
    }

    if (user.disableProtectionReason === 'external_auth') {
      return t('radtrack-users_disable_blocked_external_auth');
    }

    return undefined;
  };

  onMount(loadUsers);
</script>

<div class="page-header">
  <div>
    <div class="title-with-info">
      <h1>{t('radtrack-users_title')}</h1>
      <InfoTip text="Review RadTrack accounts and, when authorized, create accounts or manage their access state." />
    </div>
    <p class="muted">{t('radtrack-users_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

{#if $sessionStore.user?.role === 'admin'}
  <section class="panel">
    <div class="title-with-info">
      <h2>{t('radtrack-users_create-title')}</h2>
      <InfoTip text="Create a local password-authenticated user and assign its initial application role." />
    </div>
    <div class="form-grid">
      <label>
        <span class="muted field-title">
          {t('radtrack-common_username-label')}
          <InfoTip text="The unique local name the person will use to sign in." />
        </span>
        <input bind:value={createForm.username} placeholder={t('radtrack-common_username-label')} />
      </label>
      <label>
        <span class="muted field-title">
          {t('radtrack-common_role-label')}
          <InfoTip text="Controls the account’s baseline permissions, from read-only access through full administration." />
        </span>
        <select bind:value={createForm.role}>
          <option value="view_only">view_only</option>
          <option value="standard">standard</option>
          <option value="moderator">moderator</option>
          <option value="admin">admin</option>
        </select>
      </label>
      <label>
        <span class="muted field-title">
          {t('radtrack-common_password-label')}
          <InfoTip text="Optional initial password. If omitted, RadTrack generates one for you to deliver securely." />
        </span>
        <input bind:value={createForm.password} placeholder={t('radtrack-users_optional_password-placeholder')} type="password" />
      </label>
      <div class="control-with-info">
        <button class="primary" onclick={createUser}>{t('radtrack-users_create-title')}</button>
        <InfoTip text="Create the account with the username, role, and password shown above." />
      </div>
    </div>
  </section>
{/if}

{#if resetPasswordResult}
  <section class="panel">
    <div class="title-with-info">
      <h2>{t('radtrack-users_generated_password-title')}</h2>
      <InfoTip text="This temporary credential is shown so it can be delivered securely to the user; they will be required to replace it." />
    </div>
    <code>{resetPasswordResult}</code>
  </section>
{/if}

<section class="panel">
  <div class="title-with-info">
    <h2>{t('radtrack-layout_nav-users-label')}</h2>
    <InfoTip text="All known users, their roles, account flags, linked identities, and available account actions." />
  </div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>{t('radtrack-common_username-label')}</th>
          <th>{t('radtrack-common_role-label')}</th>
          <th>{t('radtrack-common_flags-label')}</th>
          <th>{t('radtrack-common_identities-label')}</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#each users as user}
          <tr>
            <td>{user.username}</td>
            <td>{user.role}</td>
            <td>
              <div class="chip-row">
                {#if user.mustChangePassword}
                  <span class="chip warning">must change password</span>
                {/if}
                {#if user.isDisabled}
                  <span class="chip danger">disabled</span>
                {/if}
                {#if user.disableProtectionReason}
                  <span class="chip subtle" title={getDisableProtectionMessage(user)}>{t('radtrack-users_protected-chip')}</span>
                {/if}
              </div>
            </td>
            <td>
              <div class="chip-row">
                {#each user.identities as identity}
                  <span class="chip start">{identity.providerType}:{identity.subjectOrPrincipal}</span>
                {/each}
              </div>
            </td>
            <td>
              <div class="actions">
                <div class="control-with-info">
                  <button
                    class="warning"
                    disabled={!canToggleDisabled(user)}
                    onclick={() => toggleDisabled(user)}
                    title={canToggleDisabled(user) ? undefined : getDisableProtectionMessage(user)}
                  >
                    {user.isDisabled ? t('radtrack-users_enable-button') : t('radtrack-users_disable-button')}
                  </button>
                  <InfoTip text={user.isDisabled ? 'Allow this local account to authenticate again.' : 'Prevent this local account from authenticating until it is re-enabled.'} />
                </div>
                <div class="control-with-info">
                  <button class="danger" onclick={() => resetPassword(user.id)}>{t('radtrack-users_reset_password-button')}</button>
                  <InfoTip text="Replace this user’s local password with a generated temporary password and require a change at next sign-in." />
                </div>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
