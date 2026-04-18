import { readFile } from 'node:fs/promises';
import JSON5 from 'json5';
import { createAppError } from './errors.js';

const readJson5 = async ({ filePath, correlationId }) => {
  try {
    const content = await readFile(filePath, 'utf8');
    return JSON5.parse(content);
  } catch (cause) {
    throw createAppError({
      caller: 'config::readJson5',
      reason: `Failed loading ${filePath}`,
      errorKey: 'CONFIG_LOAD_FAILED',
      correlationId,
      context: { filePath },
      cause
    });
  }
};

const requiredString = ({ value, field, correlationId }) => {
  if (typeof value === 'string' && value.trim()) {
    return value.trim();
  }

  throw createAppError({
    caller: 'config::validate',
    reason: `Missing required config field: ${field}`,
    errorKey: 'CONFIG_VALIDATION_FAILED',
    correlationId,
    context: { field }
  });
};

const asBoolean = ({ value, fallback = false }) => {
  if (value === undefined || value === null) {
    return fallback;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
};

const asInteger = ({ value, fallback }) => {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  const parsed = Number.parseInt(String(value), 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

export const loadRuntimeConfig = async ({ correlationId = null } = {}) => {
  const configPath = process.env.RADIACODE_CONFIG_PATH ?? '';
  const secretsPath = process.env.RADIACODE_SECRETS_PATH ?? '';

  if (!configPath || !secretsPath) {
    throw createAppError({
      caller: 'config::loadRuntimeConfig',
      reason: 'RADIACODE_CONFIG_PATH and RADIACODE_SECRETS_PATH are required.',
      errorKey: 'CONFIG_LOAD_FAILED',
      correlationId,
      context: { configPath, secretsPath }
    });
  }

  const [rawConfig, rawSecrets] = await Promise.all([
    readJson5({ filePath: configPath, correlationId }),
    readJson5({ filePath: secretsPath, correlationId })
  ]);

  const runtimeConfig = {
    configPath,
    secretsPath,
    port: asInteger({ value: process.env.PORT, fallback: 8192 }),
    trustProxy: asBoolean({ value: process.env.TRUST_PROXY, fallback: false }),
    monolith: asBoolean({ value: process.env.MONOLITH, fallback: false }),
    monolithMode: (process.env.MONOLITH_MODE ?? 'auto').toLowerCase(),
    monolithDevProxy: process.env.MONOLITH_DEV_PROXY === undefined
      ? true
      : asBoolean({ value: process.env.MONOLITH_DEV_PROXY, fallback: true }),
    publicApiUrl: process.env.PUBLIC_API_URL ?? '',
    app: {
      publicBaseUrl: requiredString({ value: rawConfig?.app?.publicBaseUrl, field: 'app.publicBaseUrl', correlationId }),
      wuiBaseUrl: requiredString({ value: rawConfig?.app?.wuiBaseUrl, field: 'app.wuiBaseUrl', correlationId }),
      basePath: typeof rawConfig?.app?.basePath === 'string' ? rawConfig.app.basePath : ''
    },
    database: {
      postgres: {
        host: requiredString({ value: rawConfig?.database?.postgres?.host, field: 'database.postgres.host', correlationId }),
        port: asInteger({ value: rawConfig?.database?.postgres?.port, fallback: 5432 }),
        database: requiredString({ value: rawConfig?.database?.postgres?.database, field: 'database.postgres.database', correlationId }),
        user: requiredString({ value: rawConfig?.database?.postgres?.user, field: 'database.postgres.user', correlationId }),
        password: requiredString({ value: rawSecrets?.postgresPassword, field: 'postgresPassword', correlationId })
      }
    },
    redis: {
      url: requiredString({ value: rawConfig?.redis?.url, field: 'redis.url', correlationId })
    },
    auth: {
      localEnabled: asBoolean({ value: rawConfig?.auth?.localEnabled, fallback: true }),
      oidcEnabled: asBoolean({ value: rawConfig?.auth?.oidcEnabled, fallback: false }),
      headerEnabled: asBoolean({ value: rawConfig?.auth?.headerEnabled, fallback: false }),
      oidc: {
        issuer: typeof rawConfig?.auth?.oidc?.issuer === 'string' ? rawConfig.auth.oidc.issuer.trim() : '',
        clientId: typeof rawConfig?.auth?.oidc?.clientId === 'string' ? rawConfig.auth.oidc.clientId.trim() : '',
        clientSecret: typeof rawSecrets?.oidcClientSecret === 'string' ? rawSecrets.oidcClientSecret.trim() : '',
        callbackUrl: typeof rawConfig?.auth?.oidc?.callbackUrl === 'string' ? rawConfig.auth.oidc.callbackUrl.trim() : '',
        scopes: Array.isArray(rawConfig?.auth?.oidc?.scopes) && rawConfig.auth.oidc.scopes.length
          ? rawConfig.auth.oidc.scopes.map((entry) => String(entry))
          : ['openid', 'profile', 'email']
      },
      header: {
        trustedCidrs: Array.isArray(rawConfig?.auth?.header?.trustedCidrs)
          ? rawConfig.auth.header.trustedCidrs.map((entry) => String(entry))
          : [],
        usernameHeader: typeof rawConfig?.auth?.header?.usernameHeader === 'string'
          ? rawConfig.auth.header.usernameHeader.toLowerCase()
          : 'x-auth-request-user',
        emailHeader: typeof rawConfig?.auth?.header?.emailHeader === 'string'
          ? rawConfig.auth.header.emailHeader.toLowerCase()
          : 'x-auth-request-email'
      }
    },
    ui: {
      defaultTheme: rawConfig?.ui?.defaultTheme === 'light' ? 'light' : 'dark',
      defaultFont: typeof rawConfig?.ui?.defaultFont === 'string' ? rawConfig.ui.defaultFont : 'ui-mono',
      defaultLanguage: typeof rawConfig?.ui?.defaultLanguage === 'string' && rawConfig.ui.defaultLanguage.trim()
        ? rawConfig.ui.defaultLanguage.trim()
        : 'en-US'
    },
    map: {
      tileUrlTemplate: requiredString({ value: rawConfig?.map?.tileUrlTemplate, field: 'map.tileUrlTemplate', correlationId }),
      attribution: typeof rawConfig?.map?.attribution === 'string' ? rawConfig.map.attribution : '',
      defaultMetric: typeof rawConfig?.map?.defaultMetric === 'string' ? rawConfig.map.defaultMetric : 'dose_rate',
      defaultAggregateShape: typeof rawConfig?.map?.defaultAggregateShape === 'string' ? rawConfig.map.defaultAggregateShape : 'hexagon',
      defaultCellSizeMeters: asInteger({ value: rawConfig?.map?.defaultCellSizeMeters, fallback: 250 }),
      rawPointCap: asInteger({ value: rawConfig?.map?.rawPointCap, fallback: 5000 })
    },
    aggregation: {
      modeBucketDecimals: asInteger({ value: rawConfig?.aggregation?.modeBucketDecimals, fallback: 2 }),
      cacheTtlSeconds: asInteger({ value: rawConfig?.aggregation?.cacheTtlSeconds, fallback: 3600 })
    },
    session: {
      secret: requiredString({ value: rawSecrets?.sessionSecret, field: 'sessionSecret', correlationId })
    },
    bootstrapAdmin: {
      username: typeof rawSecrets?.bootstrapAdmin?.username === 'string' ? rawSecrets.bootstrapAdmin.username.trim() : '',
      password: typeof rawSecrets?.bootstrapAdmin?.password === 'string' ? rawSecrets.bootstrapAdmin.password : ''
    },
    resetAdminPassword: asBoolean({ value: rawSecrets?.resetAdminPassword, fallback: false })
  };

  if (
    runtimeConfig.auth.oidcEnabled
    && (
      !runtimeConfig.auth.oidc.issuer
      || !runtimeConfig.auth.oidc.clientId
      || !runtimeConfig.auth.oidc.clientSecret
    )
  ) {
    throw createAppError({
      caller: 'config::loadRuntimeConfig',
      reason: 'OIDC is enabled but issuer, clientId, or clientSecret is missing.',
      errorKey: 'CONFIG_VALIDATION_FAILED',
      correlationId
    });
  }

  if (
    runtimeConfig.monolithMode !== 'auto'
    && runtimeConfig.monolithMode !== 'serve'
    && runtimeConfig.monolithMode !== 'proxy'
  ) {
    throw createAppError({
      caller: 'config::loadRuntimeConfig',
      reason: 'MONOLITH_MODE must be auto, serve, or proxy.',
      errorKey: 'CONFIG_VALIDATION_FAILED',
      correlationId,
      context: { monolithMode: runtimeConfig.monolithMode }
    });
  }

  return runtimeConfig;
};
