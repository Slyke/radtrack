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

  const readJson = async ({ key, ttlSeconds }) => {
    const value = await client.get(key);
    if (!value) {
      return null;
    }

    await client.expire(key, ttlSeconds);
    return JSON.parse(value);
  };

  const writeJson = async ({ key, value, ttlSeconds }) => {
    await client.set(key, JSON.stringify(value), { EX: ttlSeconds });
  };

  return {
    client,
    readJson,
    writeJson,
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
