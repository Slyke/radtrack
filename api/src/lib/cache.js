import { createClient } from 'redis';
import { createAppError } from './errors.js';
import { logFeature } from './feature-logging.js';

export const createCache = async ({ runtimeConfig, logger, correlationId }) => {
  const client = createClient({ url: runtimeConfig.redis.url });

  client.on('error', (error) => {
    logger.warn({
      caller: 'cache::redis',
      message: 'Redis client error.',
      correlationId,
      errorKey: 'CACHE_FAILED',
      context: { message: error.message }
    });
  });

  try {
    await client.connect();
    logFeature({
      caller: 'cache::createCache',
      feature: 'cache',
      level: 'info',
      logger,
      message: 'Redis cache connected.',
      correlationId,
      runtimeConfig,
      context: {
        url: runtimeConfig.redis.url.replace(/\/\/([^:@/]+):([^@/]+)@/, '//<redacted>:<redacted>@')
      }
    });
  } catch (cause) {
    throw createAppError({
      caller: 'cache::createCache',
      reason: 'Failed connecting to Redis.',
      errorKey: 'CACHE_FAILED',
      correlationId,
      cause
    });
  }

  const readJson = async ({
    key,
    ttlSeconds = null,
    includeMeta = false,
    refreshTtlOnRead = false
  }) => {
    const value = await client.get(key);
    let ttlRefreshed = false;
    if (value && refreshTtlOnRead && Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
      ttlRefreshed = Boolean(await client.expire(key, ttlSeconds));
    }
    const ttlValue = includeMeta ? await client.ttl(key) : null;
    logFeature({
      caller: 'cache::readJson',
      feature: 'cache',
      level: 'debug',
      logger,
      message: value ? 'Redis cache read hit.' : 'Redis cache read miss.',
      runtimeConfig,
      context: {
        key,
        includeMeta,
        refreshTtlOnRead,
        ttlRefreshed,
        ttlSecondsRemaining: typeof ttlValue === 'number' && ttlValue >= 0 ? ttlValue : null
      }
    });
    if (!value) {
      return null;
    }

    const parsed = JSON.parse(value);
    if (!includeMeta) {
      return parsed;
    }

    return {
      value: parsed,
      ttlSecondsRemaining: typeof ttlValue === 'number' && ttlValue >= 0 ? ttlValue : null
    };
  };

  const writeJson = async ({ key, value, ttlSeconds }) => {
    await client.set(key, JSON.stringify(value), { EX: ttlSeconds });
    logFeature({
      caller: 'cache::writeJson',
      feature: 'cache',
      level: 'debug',
      logger,
      message: 'Redis cache value written.',
      runtimeConfig,
      context: {
        key,
        ttlSeconds
      }
    });
  };

  return {
    client,
    readJson,
    writeJson,
    deleteKey: async ({ key }) => {
      if (!key) {
        return;
      }

      const deletedCount = await client.del(key);
      logFeature({
        caller: 'cache::deleteKey',
        feature: 'cache',
        level: 'debug',
        logger,
        message: 'Redis cache key delete completed.',
        runtimeConfig,
        context: {
          key,
          deletedCount
        }
      });
    },
    getTtlSeconds: async ({ key }) => {
      const ttlSeconds = await client.ttl(key);
      const ttlSecondsRemaining = ttlSeconds >= 0 ? ttlSeconds : null;
      logFeature({
        caller: 'cache::getTtlSeconds',
        feature: 'cache',
        level: 'debug',
        logger,
        message: 'Redis cache TTL checked.',
        runtimeConfig,
        context: {
          key,
          ttlSecondsRemaining
        }
      });
      return ttlSecondsRemaining;
    },
    deletePattern: async ({ pattern }) => {
      let deletedCount = 0;
      let scannedCount = 0;
      let batchCount = 0;
      for await (const batchKeys of client.scanIterator({ MATCH: pattern })) {
        const keys = Array.isArray(batchKeys) ? batchKeys : [batchKeys];
        scannedCount += keys.length;
        batchCount += 1;
        if (keys.length) {
          deletedCount += await client.del(keys);
        }
      }

      logFeature({
        caller: 'cache::deletePattern',
        feature: 'cache',
        level: 'info',
        logger,
        message: 'Redis cache pattern delete completed.',
        runtimeConfig,
        context: {
          pattern,
          scannedCount,
          deletedCount,
          batchCount
        }
      });
    },
    close: async () => {
      await client.quit();
    }
  };
};
