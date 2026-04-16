import type { SessionPayload } from '$lib/types';

const API_BASE = (import.meta.env.PUBLIC_API_URL || '').replace(/\/$/, '');

export class ApiError extends Error {
  status: number;
  body: unknown;

  constructor({ status, body, message }: { status: number; body: unknown; message?: string }) {
    super(message ?? `HTTP ${status}`);
    this.status = status;
    this.body = body;
  }
}

const resolvePath = ({ path }: { path: string }) => `${API_BASE}${path}`;

const parseBody = async ({ response }: { response: Response }) => {
  const contentType = response.headers.get('content-type') ?? '';
  if (contentType.includes('application/json')) {
    return response.json().catch(() => null);
  }

  return response.text().catch(() => null);
};

const toQuery = ({ params }: { params: Record<string, unknown> }) => {
  const search = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') {
      continue;
    }

    if (Array.isArray(value)) {
      if (!value.length) {
        continue;
      }
      search.set(key, value.join(','));
      continue;
    }

    search.set(key, String(value));
  }

  const query = search.toString();
  return query ? `?${query}` : '';
};

export const apiFetch = async <T>({
  path,
  method = 'GET',
  body,
  csrf,
  query
}: {
  path: string;
  method?: string;
  body?: unknown;
  csrf?: SessionPayload['csrf'];
  query?: Record<string, unknown>;
}) => {
  const headers = new Headers({
    Accept: 'application/json'
  });

  if (body !== undefined) {
    headers.set('Content-Type', 'application/json');
  }

  if (csrf && method !== 'GET' && method !== 'HEAD') {
    headers.set(csrf.headerName, csrf.token);
  }

  const response = await fetch(resolvePath({ path: `${path}${query ? toQuery({ params: query }) : ''}` }), {
    method,
    headers,
    credentials: 'include',
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const parsedBody = await parseBody({ response });

  if (!response.ok) {
    throw new ApiError({
      status: response.status,
      body: parsedBody
    });
  }

  return parsedBody as T;
};

export const getMe = async () => apiFetch<{
  ok: boolean;
  authenticated?: boolean;
  user?: SessionPayload['user'];
  authMethod?: string;
  csrf?: SessionPayload['csrf'];
  authModes: SessionPayload['authModes'];
  ui?: SessionPayload['ui'];
  build?: SessionPayload['build'];
}>({ path: '/api/me' });

export const localLogin = async ({ username, password }: { username: string; password: string }) => (
  apiFetch<{ ok: boolean; user: SessionPayload['user'] }>({
    path: '/auth/local/login',
    method: 'POST',
    body: { username, password }
  })
);

export const changePassword = async ({
  csrf,
  currentPassword,
  newPassword
}: {
  csrf: SessionPayload['csrf'];
  currentPassword: string;
  newPassword: string;
}) => apiFetch({
  path: '/auth/change-password',
  method: 'POST',
  body: { currentPassword, newPassword },
  csrf
});

export const logout = async ({ csrf }: { csrf: SessionPayload['csrf'] }) => apiFetch({
  path: '/auth/logout',
  method: 'POST',
  body: {},
  csrf: csrf ?? undefined
});

export const startOidcLogin = ({ redirectTo = '/dashboard' }: { redirectTo?: string } = {}) => {
  window.location.href = resolvePath({ path: `/auth/oidc/login${toQuery({ params: { redirectTo } })}` });
};

export const uploadImport = ({
  files,
  datasetName,
  description,
  csrf,
  onProgress
}: {
  files: File[];
  datasetName: string;
  description: string;
  csrf: SessionPayload['csrf'];
  onProgress?: (percent: number) => void;
}) => new Promise<unknown>((resolve, reject) => {
  const request = new XMLHttpRequest();
  request.open('POST', resolvePath({ path: '/api/imports' }));
  request.withCredentials = true;
  request.setRequestHeader('Accept', 'application/json');
  if (csrf) {
    request.setRequestHeader(csrf.headerName, csrf.token);
  }

  request.upload.onprogress = (event) => {
    if (!event.lengthComputable || !onProgress) {
      return;
    }

    onProgress(Math.round((event.loaded / event.total) * 100));
  };

  request.onerror = () => reject(new Error('Upload failed'));
  request.onload = () => {
    try {
      const body = request.responseText ? JSON.parse(request.responseText) : null;
      if (request.status < 200 || request.status >= 300) {
        reject(new ApiError({ status: request.status, body }));
        return;
      }

      resolve(body);
    } catch (error) {
      reject(error);
    }
  };

  const formData = new FormData();
  if (datasetName) {
    formData.append('datasetName', datasetName);
  }
  if (description) {
    formData.append('description', description);
  }
  for (const file of files) {
    formData.append('files', file, file.name);
  }
  request.send(formData);
});
