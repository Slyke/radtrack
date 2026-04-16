<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte';
  import { apiFetch } from '$lib/api/client';
  import { sessionStore } from '$lib/stores/session';

  let users = $state<any[]>([]);
  let errorMessage = $state<string | null>(null);
  let resetPasswordResult = $state<string | null>(null);
  let createForm = $state({
    username: '',
    role: 'standard',
    password: ''
  });

  const loadUsers = async () => {
    try {
      const response = await apiFetch<any>({ path: '/api/users' });
      users = response.users;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : 'Failed to load users';
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
    <h1>User Management</h1>
    <p class="muted">Moderators can disable accounts and reset passwords. Admins can also create users and manage identities.</p>
  </div>
</div>

{#if errorMessage}
  <section class="panel">
    <p class="muted">{errorMessage}</p>
  </section>
{/if}

{#if $sessionStore.user?.role === 'admin'}
  <section class="panel">
    <h2>Create User</h2>
    <div class="form-grid">
      <input bind:value={createForm.username} placeholder="Username" />
      <select bind:value={createForm.role}>
        <option value="view_only">view_only</option>
        <option value="standard">standard</option>
        <option value="moderator">moderator</option>
        <option value="admin">admin</option>
      </select>
      <input bind:value={createForm.password} placeholder="Optional password" type="password" />
      <button class="primary" onclick={createUser}>Create user</button>
    </div>
  </section>
{/if}

{#if resetPasswordResult}
  <section class="panel">
    <h2>Generated Password</h2>
    <code>{resetPasswordResult}</code>
  </section>
{/if}

<section class="panel">
  <h2>Users</h2>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Username</th>
          <th>Role</th>
          <th>Flags</th>
          <th>Identities</th>
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
                <button class="warning" onclick={() => toggleDisabled(user)}>{user.isDisabled ? 'Enable' : 'Disable'}</button>
                <button class="danger" onclick={() => resetPassword(user.id)}>Reset password</button>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</section>
