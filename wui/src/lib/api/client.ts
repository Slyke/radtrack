import { env } from '$env/dynamic/public';
import type { SessionPayload } from '$lib/types';

const API_BASE = (env.PUBLIC_API_URL || '').replace(/\/$/, '');

export class ApiError extends Error {
  status: number;
  body: unknown;

  constructor({ status, body, message }: { status: number; body: unknown; message?: string }) {
    super(message ?? `HTTP ${status}`);
    this.status = status;
    this.body = body;
  }
}

export const resolveApiPath = ({ path }: { path: string }) => `${API_BASE}${path}`;

const parseBody = async ({ response }: { response: Response }) => {
  const contentType = response.headers.get('content-type') ?? '';
  if (contentType.includes('application/json')) {
    return response.json().catch(() => null);
  }

  return response.text().catch(() => null);
};

const resolveErrorMessage = ({ body, status }: { body: unknown; status: number }) => {
  if (!body || typeof body !== 'object') {
    return `HTTP ${status}`;
  }

  const payload = body as {
    reason?: unknown;
    errorCode?: unknown;
    errorKey?: unknown;
  };

  if (typeof payload.reason === 'string' && payload.reason.trim()) {
    if (typeof payload.errorCode === 'string' && payload.errorCode.trim()) {
      return `${payload.reason} (${payload.errorCode})`;
    }

    if (typeof payload.errorKey === 'string' && payload.errorKey.trim()) {
      return `${payload.reason} (${payload.errorKey})`;
    }

    return payload.reason;
  }

  return `HTTP ${status}`;
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

  const response = await fetch(resolveApiPath({ path: `${path}${query ? toQuery({ params: query }) : ''}` }), {
    method,
    headers,
    credentials: 'include',
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const parsedBody = await parseBody({ response });

  if (!response.ok) {
    throw new ApiError({
      status: response.status,
      body: parsedBody,
      message: resolveErrorMessage({ body: parsedBody, status: response.status })
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
  currentPassword?: string;
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
  window.location.href = resolveApiPath({ path: `/auth/oidc/login${toQuery({ params: { redirectTo } })}` });
};

export const uploadImport = ({
  files,
  datasetName,
  description,
  splitBulkArchivesIntoDatasets,
  advancedTrackDeduplication,
  csrf,
  onProgress,
  onStageChange
}: {
  files: File[];
  datasetName: string;
  description: string;
  splitBulkArchivesIntoDatasets: boolean;
  advancedTrackDeduplication: boolean;
  csrf: SessionPayload['csrf'];
  onProgress?: (percent: number) => void;
  onStageChange?: (stage: 'uploading' | 'processing') => void;
}) => new Promise<unknown>((resolve, reject) => {
  const request = new XMLHttpRequest();
  request.open('POST', resolveApiPath({ path: '/api/imports' }));
  request.withCredentials = true;
  request.setRequestHeader('Accept', 'application/json');
  if (csrf) {
    request.setRequestHeader(csrf.headerName, csrf.token);
  }

  onStageChange?.('uploading');

  request.upload.onprogress = (event) => {
    if (!event.lengthComputable || !onProgress) {
      return;
    }

    onProgress(Math.round((event.loaded / event.total) * 100));
  };
  request.upload.onload = () => {
    onProgress?.(100);
    onStageChange?.('processing');
  };

  request.onerror = () => reject(new Error('Upload failed'));
  request.onabort = () => reject(new Error('Upload cancelled'));
  request.onload = () => {
    try {
      const body = request.responseText ? JSON.parse(request.responseText) : null;
      if (request.status < 200 || request.status >= 300) {
        reject(new ApiError({
          status: request.status,
          body,
          message: resolveErrorMessage({ body, status: request.status })
        }));
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
  formData.append('splitBulkArchivesIntoDatasets', String(splitBulkArchivesIntoDatasets));
  formData.append('advancedTrackDeduplication', String(advancedTrackDeduplication));
  for (const file of files) {
    formData.append('files', file, file.name);
  }
  request.send(formData);
});
