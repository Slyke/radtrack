import { createAppError } from '../lib/errors.js';

export const parseCookies = ({ headerValue }) => {
  if (!headerValue) {
    return {};
  }

  return Object.fromEntries(
    String(headerValue)
      .split(';')
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((entry) => {
        const equalsIndex = entry.indexOf('=');
        if (equalsIndex === -1) {
          return [entry, ''];
        }

        return [
          decodeURIComponent(entry.slice(0, equalsIndex)),
          decodeURIComponent(entry.slice(equalsIndex + 1))
        ];
      })
  );
};

export const serializeCookie = ({
  name,
  value,
  path = '/',
  httpOnly = true,
  sameSite = 'Lax',
  secure = false,
  maxAgeSeconds = null
}) => {
  const segments = [`${encodeURIComponent(name)}=${encodeURIComponent(value)}`, `Path=${path}`, `SameSite=${sameSite}`];

  if (httpOnly) {
    segments.push('HttpOnly');
  }

  if (secure) {
    segments.push('Secure');
  }

  if (maxAgeSeconds !== null) {
    segments.push(`Max-Age=${maxAgeSeconds}`);
  }

  return segments.join('; ');
};

export const getSourceIp = ({ req }) => {
  const forwarded = req.headers['x-forwarded-for'];
  if (typeof forwarded === 'string' && forwarded.trim()) {
    return forwarded.split(',')[0].trim();
  }

  return req.socket.remoteAddress ?? '';
};

export const requireJsonBodyObject = ({ body, correlationId }) => {
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    throw createAppError({
      caller: 'http::requireJsonBodyObject',
      reason: 'Request body must be an object.',
      errorKey: 'REQUEST_INVALID',
      correlationId,
      status: 400
    });
  }

  return body;
};

export const ensureSameOrigin = ({ req, runtimeConfig, correlationId }) => {
  const origin = req.headers.origin;
  if (!origin) {
    return;
  }

  const allowedOrigin = runtimeConfig.app.publicBaseUrl;
  if (origin !== allowedOrigin) {
    throw createAppError({
      caller: 'http::ensureSameOrigin',
      reason: 'Origin is not allowed for this request.',
      errorKey: 'AUTH_CSRF_INVALID',
      correlationId,
      context: { origin, allowedOrigin },
      status: 403
    });
  }
};
