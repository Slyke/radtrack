<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
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
      errorMessage = error instanceof Error ? error.message : t('radiacode-users_failed');
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

  onMount(loadUsers);
</script>

<div class="page-header">
  <div>
    <h1>{t('radiacode-users_title')}</h1>
    <p class="muted">{t('radiacode-users_description')}</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

{#if $sessionStore.user?.role === 'admin'}
  <section class="panel">
    <h2>{t('radiacode-users_create-title')}</h2>
    <div class="form-grid">
      <input bind:value={createForm.username} placeholder={t('radiacode-common_username-label')} />
      <select bind:value={createForm.role}>
        <option value="view_only">view_only</option>
        <option value="standard">standard</option>
        <option value="moderator">moderator</option>
        <option value="admin">admin</option>
      </select>
      <input bind:value={createForm.password} placeholder={t('radiacode-users_optional_password-placeholder')} type="password" />
      <button class="primary" onclick={createUser}>{t('radiacode-users_create-title')}</button>
    </div>
  </section>
{/if}

{#if resetPasswordResult}
  <section class="panel">
    <h2>{t('radiacode-users_generated_password-title')}</h2>
    <code>{resetPasswordResult}</code>
  </section>
{/if}

<section class="panel">
  <h2>{t('radiacode-layout_nav-users-label')}</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>{t('radiacode-common_username-label')}</th>
          <th>{t('radiacode-common_role-label')}</th>
          <th>{t('radiacode-common_flags-label')}</th>
          <th>{t('radiacode-common_identities-label')}</th>
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
                <button class="warning" onclick={() => toggleDisabled(user)}>{user.isDisabled ? t('radiacode-users_enable-button') : t('radiacode-users_disable-button')}</button>
                <button class="danger" onclick={() => resetPassword(user.id)}>{t('radiacode-users_reset_password-button')}</button>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
