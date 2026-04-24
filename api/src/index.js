import { existsSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';
import { getBuildInfo } from './lib/build-info.js';
import { loadRuntimeConfig } from './lib/config.js';
import { createDatabase } from './lib/db.js';
import { AppError, toErrorBody } from './lib/errors.js';
import { createLogger, logError } from './lib/logger.js';
import { createCache } from './lib/cache.js';
import { createAuditService } from './services/audit.js';
import { createAuthService } from './services/auth.js';
import { createSettingsService } from './services/settings.js';
import { createImportService } from './services/imports.js';
import { createDatasetService } from './services/datasets.js';
import { createDatalogService } from './services/datalogs.js';
import { createQueryService } from './services/query.js';
import { createExportService } from './services/export.js';
import { canModerateUsers, canManageSystem, ensureRole } from './services/permissions.js';
import { createCorrelationId } from './utils/ids.js';
import { ensureSameOrigin, getSourceIp, requireJsonBodyObject } from './utils/http.js';

const asyncHandler = (run) => async (req, res, next) => {
  try {
    await run(req, res, next);
  } catch (error) {
    next(error);
  }
};

const sendJson = ({ res, status = 200, body }) => {
  res.status(status).json(body);
};

const toAttachmentFileName = ({ value, fallback = 'download.bin' }) => {
  const normalized = String(value ?? '').replace(/["\r\n]/g, '_').trim();
  return normalized || fallback;
};

const invalidateDatasetsSafely = async ({
  logger,
  queryService,
  datasetIds,
  correlationId,
  caller,
  reason = 'Failed invalidating dataset cache.',
  context = null
}) => {
  if (!Array.isArray(datasetIds) || !datasetIds.length) {
    return;
  }

  try {
    await queryService.invalidateDatasets({ datasetIds });
  } catch (cause) {
    logger.warn({
      caller,
      message: reason,
      correlationId,
      errorKey: 'CACHE_FAILED',
      context: {
        datasetIds,
        ...(context ?? {})
      },
      rootCause: cause
    });
  }
};

const toHealthBody = ({ service, buildInfo, extra = {} }) => ({
  ok: true,
  message: 'ok',
  service,
  version: buildInfo.version,
  build: buildInfo.label,
  ...extra
});

const applyCors = ({ app, runtimeConfig }) => {
  const allowedOrigins = new Set([
    runtimeConfig.app.wuiBaseUrl,
    runtimeConfig.app.publicBaseUrl
  ]);

  app.use((req, res, next) => {
    const origin = req.headers.origin;
    if (origin && allowedOrigins.has(origin)) {
      res.header('Access-Control-Allow-Origin', origin);
      res.header('Access-Control-Allow-Credentials', 'true');
      res.header('Access-Control-Allow-Headers', 'Content-Type, X-CSRF-Token, X-Correlation-Id');
      res.header('Access-Control-Allow-Methods', 'GET,POST,PATCH,PUT,DELETE,OPTIONS');
    }

    if (req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }

    next();
  });
};

const requireAuth = ({ req, correlationId }) => {
  if (!req.auth) {
    throw new AppError({
      caller: 'api::requireAuth',
      reason: 'Authentication is required.',
      errorKey: 'AUTH_REQUIRED',
      correlationId,
      status: 401
    });
  }

  return req.auth;
};

const requireMutationAuth = ({ req, runtimeConfig, authService, correlationId }) => {
  ensureSameOrigin({ req, runtimeConfig, correlationId });
  const auth = requireAuth({ req, correlationId });
  authService.requireMutationCsrf({ req, auth, correlationId });
  return auth;
};

const loadMonolithHandler = async ({ runtimeConfig, logger, correlationId }) => {
  const handlerPath = path.resolve(process.cwd(), 'wui/build/handler.js');
  const hasBuild = existsSync(handlerPath);
  const mode = runtimeConfig.monolithMode;
  const shouldServeBuild = mode === 'serve' || (mode === 'auto' && hasBuild);
  const shouldProxy = mode === 'proxy' || (mode === 'auto' && !hasBuild && runtimeConfig.monolithDevProxy);

  if (shouldServeBuild && hasBuild) {
    const imported = await import(pathToFileURL(handlerPath).href);
    const handler = imported.handler ?? imported.default?.handler ?? imported.default;
    if (typeof handler !== 'function') {
      throw new Error(`Invalid WUI handler export in ${handlerPath}`);
    }

    logger.info({
      caller: 'startup::monolith',
      message: 'Serving built WUI in monolith mode.',
      correlationId,
      context: { handlerPath }
    });
    return handler;
  }

  if (shouldProxy) {
    logger.warn({
      caller: 'startup::monolith',
      message: 'Proxying WUI in monolith mode.',
      correlationId,
      context: {
        target: runtimeConfig.app.wuiBaseUrl,
        mode
      }
    });
    return createProxyMiddleware({
      target: runtimeConfig.app.wuiBaseUrl,
      changeOrigin: true,
      ws: true
    });
  }

  throw new Error(`Monolith frontend is unavailable for mode=${mode}`);
};

const main = async () => {
  const startupCorrelationId = createCorrelationId();
  const runtimeConfig = await loadRuntimeConfig({ correlationId: startupCorrelationId });
  const logger = createLogger({ config: runtimeConfig.logging });
  const buildInfo = getBuildInfo();
  const db = await createDatabase({ runtimeConfig, logger, correlationId: startupCorrelationId });
  const cache = await createCache({ runtimeConfig, logger, correlationId: startupCorrelationId });
  const audit = createAuditService({ db });
  const settingsService = createSettingsService({ db, runtimeConfig });
  const datasetService = createDatasetService({ db, audit });
  const authService = createAuthService({ db, runtimeConfig, logger, audit });
  const importService = createImportService({ db, audit, logger });
  const datalogService = createDatalogService({ db, audit, datasetService });
  const queryService = createQueryService({ db, cache, logger, runtimeConfig, settingsService, datasetService });
  const exportService = createExportService({ db, queryService, settingsService });

  await settingsService.seedRuntimeSettings({ correlationId: startupCorrelationId });
  await authService.bootstrapAdmin({ correlationId: startupCorrelationId });

  logger.info({
    caller: 'startup::main',
    message: 'RadTrack API starting.',
    correlationId: startupCorrelationId,
    context: {
      build: buildInfo.label,
      port: runtimeConfig.port,
      authModes: {
        local: runtimeConfig.auth.localEnabled,
        oidc: runtimeConfig.auth.oidcEnabled,
        header: runtimeConfig.auth.headerEnabled
      },
      monolith: runtimeConfig.monolith
    }
  });

  await audit.record({
    actorUserId: null,
    scopeUserId: null,
    eventType: 'app.started',
    entityType: 'service',
    entityId: 'radtrack-api',
    payload: {
      build: buildInfo.label,
      version: buildInfo.version,
      commitHash: buildInfo.commitHash,
      port: runtimeConfig.port,
      authModes: {
        local: runtimeConfig.auth.localEnabled,
        oidc: runtimeConfig.auth.oidcEnabled,
        header: runtimeConfig.auth.headerEnabled
      },
      monolith: runtimeConfig.monolith,
      ...(logger.kubernetes
        ? {
            metadata: {
              kubernetes: logger.kubernetes
            }
          }
        : {})
    }
  });

  const app = express();
  if (runtimeConfig.trustProxy) {
    app.set('trust proxy', true);
  }
  app.disable('x-powered-by');
  app.use(express.json({ limit: '10mb' }));
  applyCors({ app, runtimeConfig });

  app.use((req, res, next) => {
    req.correlationId = typeof req.headers['x-correlation-id'] === 'string'
      ? req.headers['x-correlation-id']
      : createCorrelationId();
    req.sourceIp = getSourceIp({ req });
    res.setHeader('x-correlation-id', req.correlationId);
    next();
  });

  app.get('/health', asyncHandler(async (_req, res) => {
    sendJson({
      res,
      body: toHealthBody({
        service: 'radtrack-api',
        buildInfo,
        extra: {
          services: {
            postgres: 'ready',
            redis: 'ready'
          }
        }
      })
    });
  }));

  app.get('/healthz', asyncHandler(async (_req, res) => {
    sendJson({
      res,
      body: toHealthBody({
        service: 'radtrack-api',
        buildInfo
      })
    });
  }));

  app.use(asyncHandler(async (req, _res, next) => {
    req.auth = await authService.getAuthenticatedRequest({
      req,
      correlationId: req.correlationId
    });
    next();
  }));

  app.get('/api/build', asyncHandler(async (_req, res) => {
    sendJson({ res, body: { ok: true, build: buildInfo } });
  }));

  app.get('/api/me', asyncHandler(async (req, res) => {
    if (!req.auth) {
      sendJson({
        res,
        status: 401,
        body: {
          ok: false,
          authenticated: false,
          authModes: {
            local: runtimeConfig.auth.localEnabled,
            oidc: runtimeConfig.auth.oidcEnabled,
            header: runtimeConfig.auth.headerEnabled
          },
          build: buildInfo
        }
      });
      return;
    }

    const ui = await settingsService.getUiConfig();
    sendJson({
      res,
      body: {
        ok: true,
        authenticated: true,
        user: req.auth.user,
        authMethod: req.auth.authMethod,
        csrf: {
          headerName: 'X-CSRF-Token',
          token: req.auth.csrfToken
        },
        authModes: {
          local: runtimeConfig.auth.localEnabled,
          oidc: runtimeConfig.auth.oidcEnabled,
          header: runtimeConfig.auth.headerEnabled
        },
        ui,
        build: buildInfo
      }
    });
  }));

  app.post('/api/ingest/datalogs/:ingestDatalogId/points', asyncHandler(async (req, res) => {
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    const apiKey = req.headers[datalogService.ingestKeyHeaderName];
    const result = await datalogService.ingestPoint({
      ingestDatalogId: req.params.ingestDatalogId,
      apiKey: typeof apiKey === 'string' ? apiKey : Array.isArray(apiKey) ? apiKey[0] : null,
      input: body,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [result.datasetId],
      correlationId: req.correlationId,
      caller: 'api::ingest::invalidateDatasets',
      context: {
        ingestDatalogId: req.params.ingestDatalogId,
        datalogId: result.datalogId
      }
    });
    sendJson({
      res,
      status: result.duplicate ? 200 : 201,
      body: {
        ok: true,
        duplicate: result.duplicate,
        datalogId: result.datalogId,
        datasetId: result.datasetId,
        reading: result.reading
      }
    });
  }));

  app.post('/auth/local/login', asyncHandler(async (req, res) => {
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    const result = await authService.localLogin({
      username: body.username,
      password: body.password,
      correlationId: req.correlationId
    });

    for (const cookie of result.setCookieHeaders) {
      res.append('set-cookie', cookie);
    }

    sendJson({
      res,
      body: {
        ok: true,
        user: result.user
      }
    });
  }));

  app.post('/auth/change-password', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await authService.changePassword({
      userId: auth.user.id,
      currentPassword: body.currentPassword,
      newPassword: body.newPassword,
      requireCurrentPassword: !auth.user.mustChangePassword,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.get('/auth/oidc/login', asyncHandler(async (req, res) => {
    const redirectTo = typeof req.query.redirectTo === 'string' ? req.query.redirectTo : null;
    const redirectUrl = await authService.startOidcLogin({
      redirectTo,
      correlationId: req.correlationId
    });
    res.redirect(302, redirectUrl);
  }));

  app.get('/auth/callback', asyncHandler(async (req, res) => {
    const { code, state } = req.query;
    if (typeof code !== 'string' || typeof state !== 'string') {
      throw new AppError({
        caller: 'auth::callback',
        reason: 'OIDC callback is missing code or state.',
        errorKey: 'AUTH_OIDC_FAILED',
        correlationId: req.correlationId,
        status: 400
      });
    }

    const result = await authService.completeOidcCallback({
      code,
      state,
      correlationId: req.correlationId
    });
    for (const cookie of result.setCookieHeaders) {
      res.append('set-cookie', cookie);
    }

    const redirectUrl = new URL(result.redirectTo, runtimeConfig.app.wuiBaseUrl).toString();
    res.redirect(302, redirectUrl);
  }));

  app.post('/auth/logout', asyncHandler(async (req, res) => {
    if (req.auth?.sessionId) {
      authService.requireMutationCsrf({
        req,
        auth: req.auth,
        correlationId: req.correlationId
      });
    }

    const result = await authService.logout({ req });
    for (const cookie of result.clearCookieHeaders) {
      res.append('set-cookie', cookie);
    }

    sendJson({ res, body: { ok: true } });
  }));

  app.get('/api/dashboard', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const [datasets, uploads, audits] = await Promise.all([
      datasetService.listDatasets({ user: auth.user }),
      importService.listUploadBatches({ user: auth.user }),
      audit.listRecent({ user: auth.user, limit: 12 })
    ]);

    sendJson({
      res,
      body: {
        ok: true,
        stats: {
          datasetCount: datasets.length,
          uploadCount: uploads.length,
          failedImportCount: uploads.filter((entry) => ['failed', 'completed_with_errors'].includes(entry.status)).length
        },
        datasets: datasets.slice(0, 10),
        uploads: uploads.slice(0, 10),
        audit: audits
      }
    });
  }));

  app.get('/api/audit', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const download = String(Array.isArray(req.query.download) ? req.query.download[0] : req.query.download ?? '') === '1';

    if (download) {
      const auditLog = await audit.exportEntries({
        user: auth.user,
        limitValue: req.query.limit,
        buildLabel: buildInfo.label
      });
      const payload = JSON.stringify(auditLog);
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader(
        'Content-Disposition',
        `attachment; filename="radtrack-audit-${auditLog.filters.limit}.json"`
      );
      res.setHeader('Content-Length', String(Buffer.byteLength(payload, 'utf8')));
      res.status(200).send(payload);
      return;
    }

    const page = await audit.listPage({
      user: auth.user,
      limitValue: req.query.limit
    });
    sendJson({
      res,
      body: {
        ok: true,
        ...page
      }
    });
  }));

  app.post('/api/imports', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const result = await importService.importRequest({
      req,
      user: auth.user,
      correlationId: req.correlationId
    });
    const importedDatasetIds = Array.isArray(result.datasetIds)
      ? result.datasetIds
      : result.datasetId ? [result.datasetId] : [];
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: importedDatasetIds,
      correlationId: req.correlationId,
      caller: 'api::imports::invalidateDatasets',
      reason: 'Failed invalidating dataset cache after import.',
      context: {
        batchId: result.batchId,
        datasetIds: importedDatasetIds
      }
    });
    sendJson({ res, status: 201, body: { ok: true, ...result } });
  }));

  app.get('/api/imports', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const failedOnly = req.query.failedOnly === 'true';
    sendJson({
      res,
      body: {
        ok: true,
        batches: await importService.listUploadBatches({ user: auth.user, failedOnly })
      }
    });
  }));

  app.get('/api/imports/:batchId', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        batch: await importService.getUploadBatch({
          batchId: req.params.batchId,
          user: auth.user,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/datasets', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({ res, body: { ok: true, datasets: await datasetService.listDatasets({ user: auth.user }) } });
  }));

  app.get('/api/datasets/field-inventory', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        fields: await datasetService.listFieldInventory({ user: auth.user })
      }
    });
  }));

  app.post('/api/datasets', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      status: 201,
      body: {
        ok: true,
        dataset: await datasetService.createDataset({
          user: auth.user,
          name: body.name,
          description: body.description ?? null,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/datasets/:datasetId', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        dataset: await datasetService.getDatasetDetail({
          datasetId: req.params.datasetId,
          user: auth.user,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/datasets/:datasetId/live-datalogs', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      status: 201,
      body: {
        ok: true,
        datalog: await datalogService.createLiveDatalog({
          datasetId: req.params.datasetId,
          user: auth.user,
          name: body.name,
          supportedFields: body.supportedFields,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.patch('/api/datasets/:datasetId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        dataset: await datasetService.updateDataset({
          datasetId: req.params.datasetId,
          user: auth.user,
          name: body.name ?? null,
          description: body.description ?? null,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.delete('/api/datasets/:datasetId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    await datasetService.deleteDataset({
      datasetId: req.params.datasetId,
      user: auth.user,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [req.params.datasetId],
      correlationId: req.correlationId,
      caller: 'api::datasets::delete::invalidateDatasets',
      reason: 'Failed invalidating dataset cache after delete.',
      context: {
        datasetId: req.params.datasetId
      }
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.post('/api/datasets/:datasetId/shares', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await datasetService.upsertShare({
      datasetId: req.params.datasetId,
      user: auth.user,
      targetUserId: body.targetUserId,
      accessLevel: body.accessLevel,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.delete('/api/datasets/:datasetId/shares/:shareId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    await datasetService.removeShare({
      datasetId: req.params.datasetId,
      shareId: req.params.shareId,
      user: auth.user,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.post('/api/datasets/:datasetId/exclude-areas', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await datasetService.createExcludeArea({
      datasetId: req.params.datasetId,
      user: auth.user,
      shapeType: body.shapeType,
      geometry: body.geometry,
      label: body.label ?? null,
      applyByDefaultOnExport: Boolean(body.applyByDefaultOnExport),
      effectType: body.effectType ?? 'hard_remove',
      compressMinPoints: body.compressMinPoints ?? null,
      compressMaxPoints: body.compressMaxPoints ?? null,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [req.params.datasetId],
      correlationId: req.correlationId,
      caller: 'api::excludeAreas::create::invalidateDatasets',
      context: {
        datasetId: req.params.datasetId
      }
    });
    sendJson({ res, status: 201, body: { ok: true } });
  }));

  app.delete('/api/datasets/:datasetId/exclude-areas/:excludeAreaId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    await datasetService.deleteExcludeArea({
      datasetId: req.params.datasetId,
      excludeAreaId: req.params.excludeAreaId,
      user: auth.user,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [req.params.datasetId],
      correlationId: req.correlationId,
      caller: 'api::excludeAreas::delete::invalidateDatasets',
      context: {
        datasetId: req.params.datasetId,
        excludeAreaId: req.params.excludeAreaId
      }
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.get('/api/combined-datasets', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const combinedDatasets = await datasetService.listCombinedDatasets({ user: auth.user });
    sendJson({ res, body: { ok: true, combinedDatasets } });
  }));

  app.post('/api/combined-datasets', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      status: 201,
      body: {
        ok: true,
        combinedDataset: await datasetService.createCombinedDataset({
          user: auth.user,
          name: body.name,
          description: body.description ?? null,
          datasetIds: body.datasetIds,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/combined-datasets/:combinedDatasetId', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        combinedDataset: await datasetService.getCombinedDataset({
          combinedDatasetId: req.params.combinedDatasetId,
          user: auth.user,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.patch('/api/combined-datasets/:combinedDatasetId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        combinedDataset: await datasetService.updateCombinedDataset({
          combinedDatasetId: req.params.combinedDatasetId,
          user: auth.user,
          name: body.name ?? null,
          description: body.description ?? null,
          datasetIds: body.datasetIds,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.delete('/api/combined-datasets/:combinedDatasetId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    await datasetService.deleteCombinedDataset({
      combinedDatasetId: req.params.combinedDatasetId,
      user: auth.user,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.get('/api/datalogs/:datalogId', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        datalog: await datalogService.getDatalogDetail({
          datalogId: req.params.datalogId,
          user: auth.user,
          limit: req.query.limit,
          offset: req.query.offset,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/datalogs/:datalogId/original-file', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const result = await datalogService.getDatalogOriginalFile({
      datalogId: req.params.datalogId,
      user: auth.user,
      correlationId: req.correlationId
    });
    const fileName = toAttachmentFileName({
      value: result.fileName,
      fallback: `datalog-${req.params.datalogId}.bin`
    });

    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Content-Type', 'application/octet-stream');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.setHeader('Content-Length', String(result.buffer.length));
    res.status(200).send(result.buffer);
  }));

  app.patch('/api/datalogs/:datalogId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    const result = await datalogService.updateDatalog({
      datalogId: req.params.datalogId,
      user: auth.user,
      name: body.name,
      supportedFields: body.supportedFields,
      correlationId: req.correlationId
    });
    if (result.updated) {
      await invalidateDatasetsSafely({
        logger,
        queryService,
        datasetIds: [result.datasetId],
      correlationId: req.correlationId,
      caller: 'api::datalogs::patch::invalidateDatasets',
      context: {
        datasetId: result.datasetId,
        datalogId: req.params.datalogId
      }
    });
    }
    sendJson({ res, body: { ok: true, updated: result.updated } });
  }));

  app.delete('/api/datalogs/:datalogId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const result = await datalogService.deleteDatalog({
      datalogId: req.params.datalogId,
      user: auth.user,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [result.datasetId],
      correlationId: req.correlationId,
      caller: 'api::datalogs::delete::invalidateDatasets',
      context: {
        datasetId: result.datasetId,
        datalogId: req.params.datalogId
      }
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.patch('/api/datalogs/:datalogId/readings/:readingId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    const result = await datalogService.updateDatalogReading({
      datalogId: req.params.datalogId,
      readingId: req.params.readingId,
      user: auth.user,
      occurredAt: body.occurredAt ?? null,
      latitude: body.latitude,
      longitude: body.longitude,
      accuracy: body.accuracy ?? null,
      altitudeMeters: body.altitudeMeters ?? null,
      comment: body.comment ?? null,
      measurements: body.measurements ?? null,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [result.datasetId],
      correlationId: req.correlationId,
      caller: 'api::datalogs::readings::patch::invalidateDatasets',
      context: {
        datasetId: result.datasetId,
        datalogId: req.params.datalogId,
        readingId: req.params.readingId
      }
    });
    sendJson({ res, body: { ok: true, reading: result.reading } });
  }));

  app.post('/api/datalogs/:datalogId/restore-original', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const result = await datalogService.restoreDatalogOriginal({
      datalogId: req.params.datalogId,
      user: auth.user,
      correlationId: req.correlationId
    });
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds: [result.datasetId],
      correlationId: req.correlationId,
      caller: 'api::datalogs::restoreOriginal::invalidateDatasets',
      context: {
        datasetId: result.datasetId,
        datalogId: req.params.datalogId
      }
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.post('/api/datalogs/:datalogId/shares', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await datalogService.upsertDatalogShare({
      datalogId: req.params.datalogId,
      user: auth.user,
      targetUserId: body.targetUserId,
      accessLevel: body.accessLevel,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.delete('/api/datalogs/:datalogId/shares/:shareId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    await datalogService.removeDatalogShare({
      datalogId: req.params.datalogId,
      shareId: req.params.shareId,
      user: auth.user,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.post('/api/datalogs/:datalogId/ingest-keys', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      status: 201,
      body: {
        ok: true,
        result: await datalogService.createDatalogIngestKey({
          datalogId: req.params.datalogId,
          user: auth.user,
          label: body.label,
          notes: body.notes ?? null,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/datalogs/:datalogId/ingest-keys/:ingestKeyId/revoke', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    sendJson({
      res,
      body: {
        ok: true,
        key: await datalogService.revokeDatalogIngestKey({
          datalogId: req.params.datalogId,
          keyId: req.params.ingestKeyId,
          user: auth.user,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/datalogs/:datalogId/ingest-keys/:ingestKeyId/rotate', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    sendJson({
      res,
      body: {
        ok: true,
        result: await datalogService.rotateDatalogIngestKey({
          datalogId: req.params.datalogId,
          keyId: req.params.ingestKeyId,
          user: auth.user,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/readings/hide', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await datasetService.hideReadings({
      user: auth.user,
      readingIds: body.readingIds,
      reason: body.reason ?? null,
      correlationId: req.correlationId
    });
    const datasetIds = Array.isArray(body.datasetIds) ? body.datasetIds : [];
    await invalidateDatasetsSafely({
      logger,
      queryService,
      datasetIds,
      correlationId: req.correlationId,
      caller: 'api::readings::hide::invalidateDatasets',
      context: {
        readingCount: Array.isArray(body.readingIds) ? body.readingIds.length : 0
      }
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.get('/api/map/points', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        result: await queryService.getRawPoints({
          user: auth.user,
          input: req.query,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/map/aggregates', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        result: await queryService.getAggregates({
          user: auth.user,
          input: req.query,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/export', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      body: {
        ok: true,
        export: await exportService.exportJson({
          user: auth.user,
          input: body,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.get('/api/users', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    ensureRole({
      allowed: ['moderator', 'admin'],
      user: auth.user,
      correlationId: req.correlationId,
      reason: 'Only moderators and admins can view users.'
    });
    sendJson({ res, body: { ok: true, users: await authService.listUsers() } });
  }));

  app.get('/api/share-targets', asyncHandler(async (req, res) => {
    requireAuth({ req, correlationId: req.correlationId });
    const result = await db.query(
      `SELECT id, username, role
       FROM users
       WHERE is_disabled = FALSE
       ORDER BY username`
    );
    sendJson({
      res,
      body: {
        ok: true,
        users: result.rows.map((row) => ({
          id: row.id,
          username: row.username,
          role: row.role
        }))
      }
    });
  }));

  app.post('/api/users', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    if (!canManageSystem({ user: auth.user })) {
      throw new AppError({
        caller: 'api::users::create',
        reason: 'Only admins can create users.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    sendJson({
      res,
      status: 201,
      body: {
        ok: true,
        user: await authService.createUser({
          actorUserId: auth.user.id,
          username: body.username,
          role: body.role,
          password: body.password ?? null,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.patch('/api/users/:userId', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    if (!canModerateUsers({ user: auth.user })) {
      throw new AppError({
        caller: 'api::users::patch',
        reason: 'Only moderators and admins can update users.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    if (auth.user.role !== 'admin' && body.role) {
      throw new AppError({
        caller: 'api::users::patch',
        reason: 'Only admins can change roles.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    sendJson({
      res,
      body: {
        ok: true,
        user: await authService.updateUser({
          actorUserId: auth.user.id,
          userId: req.params.userId,
          role: auth.user.role === 'admin' ? (body.role ?? null) : null,
          isDisabled: body.isDisabled ?? null,
          mustChangePassword: body.mustChangePassword ?? null,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/users/:userId/reset-password', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    if (!canModerateUsers({ user: auth.user })) {
      throw new AppError({
        caller: 'api::users::resetPassword',
        reason: 'Only moderators and admins can reset passwords.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    sendJson({
      res,
      body: {
        ok: true,
        result: await authService.resetPassword({
          actorUserId: auth.user.id,
          userId: req.params.userId,
          correlationId: req.correlationId
        })
      }
    });
  }));

  app.post('/api/users/:userId/identities', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    if (!canManageSystem({ user: auth.user })) {
      throw new AppError({
        caller: 'api::users::attachIdentity',
        reason: 'Only admins can attach identities.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await authService.attachIdentity({
      actorUserId: auth.user.id,
      userId: req.params.userId,
      providerType: body.providerType,
      providerKey: body.providerKey ?? body.providerType,
      subjectOrPrincipal: body.subjectOrPrincipal,
      metadata: body.metadata ?? {},
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  app.get('/api/user-settings', asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    const uiConfig = await settingsService.getUiConfig();
    sendJson({
      res,
      body: {
        ok: true,
        settings: await settingsService.getUserSettings({ userId: auth.user.id }),
        aggregation: {
          cacheTtlSeconds: uiConfig.cacheTtlSeconds
        }
      }
    });
  }));

  app.put('/api/user-settings', asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    const updatedSettings = await settingsService.updateUserSettings({
      userId: auth.user.id,
      updates: body.settings ?? body.updates ?? body,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true, settings: updatedSettings } });
  }));

  app.get(['/api/settings', '/api/admin/settings'], asyncHandler(async (req, res) => {
    const auth = requireAuth({ req, correlationId: req.correlationId });
    if (!canManageSystem({ user: auth.user })) {
      throw new AppError({
        caller: 'api::settings::get',
        reason: 'Only admins can view runtime settings.',
        errorKey: 'SETTINGS_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    sendJson({
      res,
      body: {
        ok: true,
        bootstrap: {
          auth: {
            localEnabled: runtimeConfig.auth.localEnabled,
            oidcEnabled: runtimeConfig.auth.oidcEnabled,
            headerEnabled: runtimeConfig.auth.headerEnabled
          },
          map: runtimeConfig.map,
          aggregation: runtimeConfig.aggregation
        },
        settings: await settingsService.listSettings()
      }
    });
  }));

  app.put(['/api/settings', '/api/admin/settings'], asyncHandler(async (req, res) => {
    const auth = requireMutationAuth({
      req,
      runtimeConfig,
      authService,
      correlationId: req.correlationId
    });
    if (!canManageSystem({ user: auth.user })) {
      throw new AppError({
        caller: 'api::settings::put',
        reason: 'Only admins can update runtime settings.',
        errorKey: 'SETTINGS_FORBIDDEN',
        correlationId: req.correlationId,
        status: 403
      });
    }

    const body = requireJsonBodyObject({ body: req.body, correlationId: req.correlationId });
    await settingsService.updateSettings({
      updates: body.updates,
      actorUserId: auth.user.id,
      correlationId: req.correlationId
    });
    sendJson({ res, body: { ok: true } });
  }));

  if (runtimeConfig.monolith) {
    app.use(await loadMonolithHandler({ runtimeConfig, logger, correlationId: startupCorrelationId }));
  }

  app.use((error, req, res, _next) => {
    const appError = error instanceof AppError
      ? error
      : logError({
          logger,
          caller: 'api::error',
          reason: 'Unhandled request failure.',
          errorKey: 'ERR_UNKNOWN',
          correlationId: req.correlationId ?? null,
          cause: error
        });

    logger.error({
      caller: appError.caller ?? 'api::error',
      message: appError.reason,
      correlationId: req.correlationId ?? appError.correlationId ?? null,
      errorKey: appError.errorKey,
      errorCode: appError.errorCode,
      context: appError.context,
      rootCause: error
    });

    sendJson({
      res,
      status: appError.status ?? 500,
      body: toErrorBody({ error: appError })
    });
  });

  setInterval(() => {
    db.cleanupSessions().catch((error) => {
      logger.warn({
        caller: 'maintenance::cleanupSessions',
        message: 'Session cleanup failed.',
        correlationId: null,
        errorKey: 'ERR_UNKNOWN',
        context: { message: error.message }
      });
    });
  }, 60_000).unref();

  const server = app.listen(runtimeConfig.port, () => {
    logger.info({
      caller: 'startup::listen',
      message: `RadTrack API listening on ${runtimeConfig.port}`,
      correlationId: startupCorrelationId,
      context: {
        build: buildInfo.label
      }
    });
  });

  const close = async () => {
    server.close();
    await Promise.all([db.close(), cache.close()]);
  };

  process.on('SIGTERM', () => {
    close().finally(() => process.exit(0));
  });
  process.on('SIGINT', () => {
    close().finally(() => process.exit(0));
  });
};

main().catch((error) => {
  const logger = createLogger();
  const startupError = error instanceof AppError
    ? error
    : logError({
        logger,
        caller: 'startup::fatal',
        reason: 'RadTrack API failed during startup.',
        errorKey: 'SYSTEM_STARTUP_FAILED',
        correlationId: null,
        cause: error
      });

  logger.error({
    caller: startupError.caller,
    message: startupError.reason,
    correlationId: startupError.correlationId,
    errorKey: startupError.errorKey,
    errorCode: startupError.errorCode,
    context: startupError.context,
    rootCause: error
  });
  process.exit(1);
});
