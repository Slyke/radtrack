import { createHash, createHmac, randomBytes } from 'node:crypto';
import { BlockList, isIP } from 'node:net';
import { createAppError } from '../lib/errors.js';
import { createBootstrapPassword, createOpaqueId, sha256Hex } from '../utils/ids.js';
import { hashPassword, normalizeUsername, safeCompare, verifyPassword } from '../utils/security.js';
import { parseCookies, serializeCookie } from '../utils/http.js';
import { isRole } from './permissions.js';

const sessionCookieName = 'radtrack_session';
const csrfCookieName = 'radtrack_csrf';
const sessionLifetimeSeconds = 31 * 24 * 60 * 60;

const base64Url = (buffer) => buffer.toString('base64url');

const createPkceVerifier = () => base64Url(randomBytes(32));

const createPkceChallenge = ({ verifier }) => createHash('sha256').update(verifier).digest('base64url');

const formBody = ({ values }) => new URLSearchParams(
  Object.entries(values).filter(([, value]) => value !== undefined && value !== null)
);

const maybeJson = async ({ response }) => {
  const contentType = response.headers.get('content-type') ?? '';
  if (contentType.includes('application/json')) {
    return response.json();
  }

  return response.text();
};

export const createAuthService = ({ db, runtimeConfig, logger, audit }) => {
  const trustedProxyList = new BlockList();
  for (const cidr of runtimeConfig.auth.header.trustedCidrs) {
    const [address, prefix] = cidr.split('/');
    const version = isIP(address);
    if (!version || !prefix) {
      continue;
    }

    trustedProxyList.addSubnet(address, Number.parseInt(prefix, 10), version === 6 ? 'ipv6' : 'ipv4');
  }

  let cachedOidcDiscovery = null;

  const cookieOptions = {
    path: runtimeConfig.app.basePath || '/',
    secure: runtimeConfig.app.publicBaseUrl.startsWith('https://')
  };

  const safeUser = ({ row }) => ({
    id: row.id,
    username: row.username,
    role: row.role,
    mustChangePassword: row.must_change_password,
    isDisabled: row.is_disabled,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  });

  const computeHeaderCsrf = ({ userId }) => createHmac('sha256', runtimeConfig.session.secret).update(`header:${userId}`).digest('hex');

  const serializeSessionCookies = ({ sessionId, csrfToken }) => ([
    serializeCookie({
      name: sessionCookieName,
      value: sessionId,
      path: cookieOptions.path,
      httpOnly: true,
      sameSite: 'Lax',
      secure: cookieOptions.secure,
      maxAgeSeconds: sessionLifetimeSeconds
    }),
    serializeCookie({
      name: csrfCookieName,
      value: csrfToken,
      path: cookieOptions.path,
      httpOnly: false,
      sameSite: 'Lax',
      secure: cookieOptions.secure,
      maxAgeSeconds: sessionLifetimeSeconds
    })
  ]);

  const clearSessionCookies = () => ([
    serializeCookie({
      name: sessionCookieName,
      value: '',
      path: cookieOptions.path,
      httpOnly: true,
      sameSite: 'Lax',
      secure: cookieOptions.secure,
      maxAgeSeconds: 0
    }),
    serializeCookie({
      name: csrfCookieName,
      value: '',
      path: cookieOptions.path,
      httpOnly: false,
      sameSite: 'Lax',
      secure: cookieOptions.secure,
      maxAgeSeconds: 0
    })
  ]);

  const getUserByUsername = async ({ username }) => {
    const result = await db.query('SELECT * FROM users WHERE username = $1', [normalizeUsername({ username })]);
    return result.rows[0] ?? null;
  };

  const getUserById = async ({ userId }) => {
    const result = await db.query('SELECT * FROM users WHERE id = $1', [userId]);
    return result.rows[0] ?? null;
  };

  const getIdentity = async ({ providerType, subject }) => {
    const result = await db.query(
      `SELECT ai.*, u.username, u.role, u.password_hash, u.must_change_password, u.is_disabled, u.created_at, u.updated_at
       FROM auth_identities ai
       JOIN users u ON u.id = ai.user_id
       WHERE ai.provider_type = $1 AND ai.provider_subject_or_principal = $2`,
      [providerType, subject]
    );
    return result.rows[0] ?? null;
  };

  const createSession = async ({ userId, authMethod }) => {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + (sessionLifetimeSeconds * 1000)).toISOString();
    const sessionId = createOpaqueId({ bytes: 24 });
    const csrfToken = createOpaqueId({ bytes: 24 });
    const timestamp = now.toISOString();

    await db.query(
      `INSERT INTO sessions (id, user_id, auth_method, csrf_token, expires_at, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $6)`,
      [sessionId, userId, authMethod, csrfToken, expiresAt, timestamp]
    );

    return {
      sessionId,
      csrfToken,
      expiresAt
    };
  };

  const deleteSession = async ({ sessionId }) => {
    await db.query('DELETE FROM sessions WHERE id = $1', [sessionId]);
  };

  const deleteSessionsForUser = async ({ userId }) => {
    await db.query('DELETE FROM sessions WHERE user_id = $1', [userId]);
  };

  const getSessionUser = async ({ sessionId }) => {
    if (!sessionId) {
      return null;
    }

    const result = await db.query(
      `SELECT s.id AS session_id, s.auth_method, s.csrf_token, s.expires_at,
              u.id, u.username, u.role, u.password_hash, u.must_change_password, u.is_disabled, u.created_at, u.updated_at
       FROM sessions s
       JOIN users u ON u.id = s.user_id
       WHERE s.id = $1 AND s.expires_at > NOW()`,
      [sessionId]
    );

    return result.rows[0] ?? null;
  };

  const getBootstrapResetMarker = async () => {
    const result = await db.query(
      `SELECT value_json FROM app_settings WHERE key_name = 'auth.bootstrapResetToken'`
    );
    return result.rows[0]?.value_json ?? null;
  };

  const setBootstrapResetMarker = async ({ token }) => {
    await db.query(
      `INSERT INTO app_settings (key_name, value_json, source, updated_at)
       VALUES ($1, $2::jsonb, $3, $4)
       ON CONFLICT (key_name) DO UPDATE
       SET value_json = EXCLUDED.value_json,
           source = EXCLUDED.source,
           updated_at = EXCLUDED.updated_at`,
      ['auth.bootstrapResetToken', JSON.stringify(token), 'bootstrap_runtime', new Date().toISOString()]
    );
  };

  const logGeneratedBootstrapPassword = ({ username, password, correlationId }) => {
    console.error(JSON.stringify({
      timestamp: new Date().toISOString(),
      level: 'warn',
      caller: 'auth::bootstrapAdmin',
      correlationId,
      message: 'Generated bootstrap admin password.',
      username,
      generatedPassword: password
    }));
  };

  const bootstrapAdmin = async ({ correlationId = null }) => {
    const existingAdminResult = await db.query(
      `SELECT * FROM users WHERE role = 'admin' ORDER BY created_at ASC LIMIT 1`
    );
    const existingAdmin = existingAdminResult.rows[0] ?? null;
    const bootstrapUsername = normalizeUsername({
      username: runtimeConfig.bootstrapAdmin.username || 'admin'
    }) || 'admin';
    const resetMarker = await getBootstrapResetMarker();
    const resetToken = sha256Hex({
      value: JSON.stringify({
        username: bootstrapUsername,
        password: runtimeConfig.bootstrapAdmin.password || '__generated__',
        resetAdminPassword: runtimeConfig.resetAdminPassword
      })
    });

    if (existingAdmin && !runtimeConfig.resetAdminPassword) {
      return;
    }

    if (
      existingAdmin
      && runtimeConfig.resetAdminPassword
      && resetMarker === resetToken
    ) {
      return;
    }

    const password = runtimeConfig.bootstrapAdmin.password || createBootstrapPassword();
    const passwordHash = hashPassword({ password });
    const now = new Date().toISOString();

    if (!existingAdmin) {
      try {
        await db.query(
          `INSERT INTO users (id, username, role, password_hash, must_change_password, is_disabled, created_at, updated_at)
           VALUES ($1, $2, 'admin', $3, TRUE, FALSE, $4, $4)`,
          [createOpaqueId(), bootstrapUsername, passwordHash, now]
        );
      } catch (cause) {
        throw createAppError({
          caller: 'auth::bootstrapAdmin',
          reason: 'Failed creating bootstrap administrator.',
          errorKey: 'USER_BOOTSTRAP_FAILED',
          correlationId,
          cause
        });
      }
    } else {
      await db.query(
        `UPDATE users
         SET username = $2,
             role = 'admin',
             password_hash = $3,
             must_change_password = TRUE,
             is_disabled = FALSE,
             updated_at = $4
         WHERE id = $1`,
        [existingAdmin.id, bootstrapUsername, passwordHash, now]
      );
      await deleteSessionsForUser({ userId: existingAdmin.id });
    }

    if (!runtimeConfig.bootstrapAdmin.password) {
      logGeneratedBootstrapPassword({ username: bootstrapUsername, password, correlationId });
    }

    await setBootstrapResetMarker({ token: runtimeConfig.resetAdminPassword ? resetToken : null });
  };

  const discoverOidc = async ({ correlationId = null }) => {
    if (cachedOidcDiscovery) {
      return cachedOidcDiscovery;
    }

    const discoveryUrl = new URL('/.well-known/openid-configuration', runtimeConfig.auth.oidc.issuer).toString();
    const response = await fetch(discoveryUrl);
    if (!response.ok) {
      throw createAppError({
        caller: 'auth::discoverOidc',
        reason: 'OIDC discovery failed.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId,
        context: { status: response.status, discoveryUrl }
      });
    }

    const discovery = await response.json();
    cachedOidcDiscovery = discovery;
    return discovery;
  };

  const ensureUserCanLogin = ({ row, correlationId }) => {
    if (!row) {
      throw createAppError({
        caller: 'auth::ensureUserCanLogin',
        reason: 'Invalid credentials.',
        errorKey: 'AUTH_INVALID_CREDENTIALS',
        correlationId,
        status: 401
      });
    }

    if (row.is_disabled) {
      throw createAppError({
        caller: 'auth::ensureUserCanLogin',
        reason: 'Account is disabled.',
        errorKey: 'AUTH_DISABLED_ACCOUNT',
        correlationId,
        status: 403
      });
    }
  };

  const resolveLinkedUser = async ({ providerType, subject, fallbackUsername, correlationId, metadata }) => {
    const existingIdentity = await getIdentity({ providerType, subject });
    if (existingIdentity) {
      ensureUserCanLogin({ row: existingIdentity, correlationId });
      return existingIdentity;
    }

    const existingUser = fallbackUsername ? await getUserByUsername({ username: fallbackUsername }) : null;
    if (!existingUser) {
      throw createAppError({
        caller: 'auth::resolveLinkedUser',
        reason: 'No linked local user exists for this identity.',
        errorKey: providerType === 'header' ? 'AUTH_HEADER_INVALID' : 'AUTH_OIDC_FAILED',
        correlationId,
        status: 403
      });
    }

    ensureUserCanLogin({ row: existingUser, correlationId });

    await db.query(
      `INSERT INTO auth_identities (id, user_id, provider_type, provider_key, provider_subject_or_principal, metadata_json, created_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
       ON CONFLICT (provider_type, provider_subject_or_principal) DO NOTHING`,
      [
        createOpaqueId(),
        existingUser.id,
        providerType,
        providerType,
        subject,
        JSON.stringify(metadata ?? {}),
        new Date().toISOString()
      ]
    );

    return {
      ...existingUser,
      metadata_json: metadata ?? {}
    };
  };

  const getAuthenticatedRequest = async ({ req, correlationId = null }) => {
    const cookies = parseCookies({ headerValue: req.headers.cookie });
    const sessionId = cookies[sessionCookieName] ?? null;
    const session = await getSessionUser({ sessionId });
    if (session) {
      ensureUserCanLogin({ row: session, correlationId });
      return {
        user: safeUser({ row: session }),
        authMethod: session.auth_method,
        csrfToken: session.csrf_token,
        sessionId: session.session_id
      };
    }

    if (!runtimeConfig.auth.headerEnabled) {
      return null;
    }

    const sourceIp = req.sourceIp ?? req.ip ?? '';
    if (!sourceIp || !trustedProxyList.check(sourceIp)) {
      return null;
    }

    const usernameHeader = req.headers[runtimeConfig.auth.header.usernameHeader];
    const emailHeader = req.headers[runtimeConfig.auth.header.emailHeader];
    const username = typeof usernameHeader === 'string' ? normalizeUsername({ username: usernameHeader }) : '';
    const email = typeof emailHeader === 'string' ? emailHeader : null;

    if (!username) {
      return null;
    }

    const resolved = await resolveLinkedUser({
      providerType: 'header',
      subject: username,
      fallbackUsername: username,
      correlationId,
      metadata: {
        principal: username,
        email
      }
    });

    return {
      user: safeUser({ row: resolved }),
      authMethod: 'header',
      csrfToken: computeHeaderCsrf({ userId: resolved.id }),
      sessionId: null
    };
  };

  const localLogin = async ({ username, password, correlationId = null }) => {
    if (!runtimeConfig.auth.localEnabled) {
      throw createAppError({
        caller: 'auth::localLogin',
        reason: 'Local login is disabled.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const user = await getUserByUsername({ username });
    ensureUserCanLogin({ row: user, correlationId });

    if (!user.password_hash || !verifyPassword({ password, storedHash: user.password_hash })) {
      throw createAppError({
        caller: 'auth::localLogin',
        reason: 'Invalid credentials.',
        errorKey: 'AUTH_INVALID_CREDENTIALS',
        correlationId,
        status: 401
      });
    }

    const session = await createSession({ userId: user.id, authMethod: 'local' });
    await audit.record({
      actorUserId: user.id,
      eventType: 'auth.local_login',
      entityType: 'user',
      entityId: user.id,
      payload: { username: user.username }
    });

    return {
      user: safeUser({ row: user }),
      session,
      setCookieHeaders: serializeSessionCookies({ sessionId: session.sessionId, csrfToken: session.csrfToken })
    };
  };

  const changePassword = async ({
    userId,
    currentPassword,
    newPassword,
    requireCurrentPassword = true,
    correlationId = null
  }) => {
    const user = await getUserById({ userId });
    ensureUserCanLogin({ row: user, correlationId });

    if (typeof newPassword !== 'string' || !newPassword.length) {
      throw createAppError({
        caller: 'auth::changePassword',
        reason: 'New password is required.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    if (requireCurrentPassword && user.password_hash) {
      if (typeof currentPassword !== 'string' || !verifyPassword({ password: currentPassword, storedHash: user.password_hash })) {
        throw createAppError({
          caller: 'auth::changePassword',
          reason: 'Current password is incorrect.',
          errorKey: 'AUTH_INVALID_CREDENTIALS',
          correlationId,
          status: 401
        });
      }
    }

    await db.query(
      `UPDATE users
       SET password_hash = $2,
           must_change_password = FALSE,
           updated_at = $3
       WHERE id = $1`,
      [userId, hashPassword({ password: newPassword }), new Date().toISOString()]
    );
    await deleteSessionsForUser({ userId });
    await audit.record({
      actorUserId: userId,
      eventType: 'auth.password_changed',
      entityType: 'user',
      entityId: userId,
      payload: {}
    });
  };

  const startOidcLogin = async ({ redirectTo = null, correlationId = null }) => {
    if (!runtimeConfig.auth.oidcEnabled) {
      throw createAppError({
        caller: 'auth::startOidcLogin',
        reason: 'OIDC is disabled.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId,
        status: 403
      });
    }

    const discovery = await discoverOidc({ correlationId });
    const state = createOpaqueId({ bytes: 20 });
    const verifier = createPkceVerifier();
    const challenge = createPkceChallenge({ verifier });

    await db.query(
      `INSERT INTO auth_requests (state, code_verifier, redirect_to, created_at)
       VALUES ($1, $2, $3, $4)`,
      [state, verifier, redirectTo, new Date().toISOString()]
    );

    const url = new URL(discovery.authorization_endpoint);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('client_id', runtimeConfig.auth.oidc.clientId);
    url.searchParams.set('redirect_uri', runtimeConfig.auth.oidc.callbackUrl || `${runtimeConfig.app.publicBaseUrl}/auth/callback`);
    url.searchParams.set('scope', runtimeConfig.auth.oidc.scopes.join(' '));
    url.searchParams.set('state', state);
    url.searchParams.set('code_challenge', challenge);
    url.searchParams.set('code_challenge_method', 'S256');

    return url.toString();
  };

  const completeOidcCallback = async ({ code, state, correlationId = null }) => {
    const authRequestResult = await db.query('SELECT * FROM auth_requests WHERE state = $1', [state]);
    const authRequest = authRequestResult.rows[0] ?? null;
    if (!authRequest) {
      throw createAppError({
        caller: 'auth::completeOidcCallback',
        reason: 'OIDC state is invalid or expired.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId,
        status: 400
      });
    }

    await db.query('DELETE FROM auth_requests WHERE state = $1', [state]);
    const discovery = await discoverOidc({ correlationId });

    const tokenResponse = await fetch(discovery.token_endpoint, {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded'
      },
      body: formBody({
        values: {
          grant_type: 'authorization_code',
          code,
          client_id: runtimeConfig.auth.oidc.clientId,
          client_secret: runtimeConfig.auth.oidc.clientSecret,
          redirect_uri: runtimeConfig.auth.oidc.callbackUrl || `${runtimeConfig.app.publicBaseUrl}/auth/callback`,
          code_verifier: authRequest.code_verifier
        }
      })
    });

    if (!tokenResponse.ok) {
      throw createAppError({
        caller: 'auth::completeOidcCallback',
        reason: 'OIDC token exchange failed.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId,
        context: {
          status: tokenResponse.status,
          body: await maybeJson({ response: tokenResponse })
        }
      });
    }

    const tokenBody = await tokenResponse.json();
    const accessToken = tokenBody.access_token;
    if (!accessToken) {
      throw createAppError({
        caller: 'auth::completeOidcCallback',
        reason: 'OIDC provider did not return an access token.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId
      });
    }

    const userinfoResponse = await fetch(discovery.userinfo_endpoint, {
      headers: {
        authorization: `Bearer ${accessToken}`
      }
    });
    if (!userinfoResponse.ok) {
      throw createAppError({
        caller: 'auth::completeOidcCallback',
        reason: 'OIDC userinfo lookup failed.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId,
        context: {
          status: userinfoResponse.status
        }
      });
    }

    const claims = await userinfoResponse.json();
    const subject = String(claims.sub ?? '');
    const preferredUsername = normalizeUsername({
      username: claims.preferred_username ?? claims.email ?? subject
    });

    if (!subject) {
      throw createAppError({
        caller: 'auth::completeOidcCallback',
        reason: 'OIDC subject is missing.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId
      });
    }

    const user = await resolveLinkedUser({
      providerType: 'oidc',
      subject,
      fallbackUsername: preferredUsername,
      correlationId,
      metadata: {
        issuer: runtimeConfig.auth.oidc.issuer,
        email: claims.email ?? null,
        preferredUsername: claims.preferred_username ?? null
      }
    });

    const session = await createSession({ userId: user.id, authMethod: 'oidc' });
    await audit.record({
      actorUserId: user.id,
      eventType: 'auth.oidc_login',
      entityType: 'user',
      entityId: user.id,
      payload: {
        subject,
        username: user.username
      }
    });

    return {
      redirectTo: authRequest.redirect_to || '/dashboard',
      user: safeUser({ row: user }),
      session,
      setCookieHeaders: serializeSessionCookies({ sessionId: session.sessionId, csrfToken: session.csrfToken })
    };
  };

  const logout = async ({ req }) => {
    const cookies = parseCookies({ headerValue: req.headers.cookie });
    const sessionId = cookies[sessionCookieName] ?? null;
    if (sessionId) {
      await deleteSession({ sessionId });
    }

    return {
      clearCookieHeaders: clearSessionCookies()
    };
  };

  const requireMutationCsrf = ({ req, auth, correlationId = null }) => {
    const csrfHeader = req.headers['x-csrf-token'];
    if (typeof csrfHeader !== 'string' || !csrfHeader) {
      throw createAppError({
        caller: 'auth::requireMutationCsrf',
        reason: 'Missing CSRF token.',
        errorKey: 'AUTH_CSRF_INVALID',
        correlationId,
        status: 403
      });
    }

    const valid = safeCompare({ left: csrfHeader, right: auth.csrfToken });
    if (!valid) {
      throw createAppError({
        caller: 'auth::requireMutationCsrf',
        reason: 'Invalid CSRF token.',
        errorKey: 'AUTH_CSRF_INVALID',
        correlationId,
        status: 403
      });
    }
  };

  const listUsers = async () => {
    const [usersResult, identitiesResult] = await Promise.all([
      db.query('SELECT * FROM users ORDER BY username'),
      db.query('SELECT * FROM auth_identities ORDER BY provider_type, provider_subject_or_principal')
    ]);

    const identitiesByUser = new Map();
    for (const row of identitiesResult.rows) {
      const current = identitiesByUser.get(row.user_id) ?? [];
      current.push({
        id: row.id,
        providerType: row.provider_type,
        providerKey: row.provider_key,
        subjectOrPrincipal: row.provider_subject_or_principal,
        metadata: row.metadata_json,
        createdAt: row.created_at
      });
      identitiesByUser.set(row.user_id, current);
    }

    return usersResult.rows.map((row) => ({
      ...safeUser({ row }),
      identities: identitiesByUser.get(row.id) ?? []
    }));
  };

  const createUser = async ({ actorUserId, username, role, password = null, correlationId = null }) => {
    const normalized = normalizeUsername({ username });
    if (!normalized || !isRole({ role })) {
      throw createAppError({
        caller: 'auth::createUser',
        reason: 'Invalid username or role.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const createdAt = new Date().toISOString();
    const userId = createOpaqueId();
    await db.query(
      `INSERT INTO users (id, username, role, password_hash, must_change_password, is_disabled, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, FALSE, $6, $6)`,
      [
        userId,
        normalized,
        role,
        password ? hashPassword({ password }) : null,
        Boolean(password),
        createdAt
      ]
    );

    await audit.record({
      actorUserId,
      eventType: 'user.created',
      entityType: 'user',
      entityId: userId,
      payload: {
        username: normalized,
        role,
        hasPassword: Boolean(password)
      }
    });

    return getUserById({ userId });
  };

  const updateUser = async ({ actorUserId, userId, role, isDisabled, mustChangePassword, correlationId = null }) => {
    const existing = await getUserById({ userId });
    if (!existing) {
      throw createAppError({
        caller: 'auth::updateUser',
        reason: 'User was not found.',
        errorKey: 'USER_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    await db.query(
      `UPDATE users
       SET role = COALESCE($2, role),
           is_disabled = COALESCE($3, is_disabled),
           must_change_password = COALESCE($4, must_change_password),
           updated_at = $5
       WHERE id = $1`,
      [
        userId,
        role ?? null,
        isDisabled ?? null,
        mustChangePassword ?? null,
        new Date().toISOString()
      ]
    );

    if (isDisabled === true) {
      await deleteSessionsForUser({ userId });
    }

    await audit.record({
      actorUserId,
      eventType: 'user.updated',
      entityType: 'user',
      entityId: userId,
      payload: {
        role,
        isDisabled,
        mustChangePassword
      }
    });

    return getUserById({ userId });
  };

  const resetPassword = async ({ actorUserId, userId, correlationId = null }) => {
    const existing = await getUserById({ userId });
    if (!existing) {
      throw createAppError({
        caller: 'auth::resetPassword',
        reason: 'User was not found.',
        errorKey: 'USER_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    const password = createBootstrapPassword();
    await db.query(
      `UPDATE users
       SET password_hash = $2,
           must_change_password = TRUE,
           updated_at = $3
       WHERE id = $1`,
      [userId, hashPassword({ password }), new Date().toISOString()]
    );
    await deleteSessionsForUser({ userId });

    await audit.record({
      actorUserId,
      eventType: 'user.password_reset',
      entityType: 'user',
      entityId: userId,
      payload: {}
    });

    return {
      password
    };
  };

  const attachIdentity = async ({
    actorUserId,
    userId,
    providerType,
    providerKey,
    subjectOrPrincipal,
    metadata = {},
    correlationId = null
  }) => {
    if (!['oidc', 'header', 'local'].includes(providerType)) {
      throw createAppError({
        caller: 'auth::attachIdentity',
        reason: 'Invalid identity provider type.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    await db.query(
      `INSERT INTO auth_identities (id, user_id, provider_type, provider_key, provider_subject_or_principal, metadata_json, created_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
       ON CONFLICT (provider_type, provider_subject_or_principal) DO UPDATE
       SET user_id = EXCLUDED.user_id,
           provider_key = EXCLUDED.provider_key,
           metadata_json = EXCLUDED.metadata_json`,
      [
        createOpaqueId(),
        userId,
        providerType,
        providerKey,
        subjectOrPrincipal,
        JSON.stringify(metadata),
        new Date().toISOString()
      ]
    );

    await audit.record({
      actorUserId,
      eventType: 'user.identity_attached',
      entityType: 'user',
      entityId: userId,
      payload: {
        providerType,
        providerKey,
        subjectOrPrincipal
      }
    });
  };

  return {
    sessionCookieName,
    csrfCookieName,
    bootstrapAdmin,
    getAuthenticatedRequest,
    requireMutationCsrf,
    localLogin,
    changePassword,
    startOidcLogin,
    completeOidcCallback,
    logout,
    listUsers,
    createUser,
    updateUser,
    resetPassword,
    attachIdentity,
    getUserById,
    getUserByUsername
  };
};
