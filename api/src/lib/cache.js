import { createClient } from 'redis';
import { createAppError } from './errors.js';

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
  } catch (cause) {
    throw createAppError({
      caller: 'cache::createCache',
      reason: 'Failed connecting to Redis.',
      errorKey: 'CACHE_FAILED',
      correlationId,
      cause
    });
  }

  const readJson = async ({ key, ttlSeconds: _ttlSeconds, includeMeta = false }) => {
    const [value, ttlValue] = await Promise.all([
      client.get(key),
      includeMeta ? client.ttl(key) : Promise.resolve(null)
    ]);
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
  };

  return {
    client,
    readJson,
    writeJson,
    getTtlSeconds: async ({ key }) => {
      const ttlSeconds = await client.ttl(key);
      return ttlSeconds >= 0 ? ttlSeconds : null;
    },
    deletePattern: async ({ pattern }) => {
      const keys = [];
      for await (const key of client.scanIterator({ MATCH: pattern })) {
        keys.push(key);
      }

      if (keys.length) {
        await client.del(keys);
      }
    },
    close: async () => {
      await client.quit();
    }
  };
};
