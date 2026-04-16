import { writable } from 'svelte/store';
import type { SessionPayload } from '$lib/types';
import { ApiError, getMe } from '$lib/api/client';

const initialState: SessionPayload & {
  loaded: boolean;
} = {
  loaded: false,
  authenticated: false,
  user: null,
  authMethod: null,
  csrf: null,
  authModes: {
    local: true,
    oidc: false,
    header: false
  },
  ui: null,
  build: null
};

export const sessionStore = writable(initialState);

export const bootstrapSession = async () => {
  try {
    const response = await getMe();
    sessionStore.set({
      loaded: true,
      authenticated: Boolean(response.authenticated),
      user: response.user ?? null,
      authMethod: response.authMethod ?? null,
      csrf: response.csrf ?? null,
      authModes: response.authModes,
      ui: response.ui ?? null,
      build: response.build ?? null
    });
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      const body = error.body as { authModes?: SessionPayload['authModes']; build?: SessionPayload['build'] } | null;
      sessionStore.set({
        loaded: true,
        authenticated: false,
        user: null,
        authMethod: null,
        csrf: null,
        authModes: body?.authModes ?? initialState.authModes,
        ui: null,
        build: body?.build ?? null
      });
      return;
    }

    throw error;
  }
};
