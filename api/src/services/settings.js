import { createAppError } from '../lib/errors.js';

const defaultSettingsFromConfig = ({ runtimeConfig }) => ({
  'ui.defaultTheme': runtimeConfig.ui.defaultTheme,
  'ui.defaultFont': runtimeConfig.ui.defaultFont,
  'ui.defaultLanguage': runtimeConfig.ui.defaultLanguage,
  'map.tileUrlTemplate': runtimeConfig.map.tileUrlTemplate,
  'map.attribution': runtimeConfig.map.attribution,
  'map.defaultMetric': runtimeConfig.map.defaultMetric,
  'map.defaultAggregateShape': runtimeConfig.map.defaultAggregateShape,
  'map.defaultCellSizeMeters': runtimeConfig.map.defaultCellSizeMeters,
  'map.rawPointCap': runtimeConfig.map.rawPointCap,
  'aggregation.modeBucketDecimals': runtimeConfig.aggregation.modeBucketDecimals,
  'aggregation.cacheTtlSeconds': runtimeConfig.aggregation.cacheTtlSeconds
});

const fromRows = ({ rows }) => Object.fromEntries(rows.map((row) => [row.key_name, row.value_json]));

export const createSettingsService = ({ db, runtimeConfig }) => {
  const seedRuntimeSettings = async ({ correlationId = null } = {}) => {
    const defaults = defaultSettingsFromConfig({ runtimeConfig });
    const now = new Date().toISOString();

    await db.withTransaction(async (client) => {
      for (const [key, value] of Object.entries(defaults)) {
        await client.query(
          `INSERT INTO app_settings (key_name, value_json, source, updated_at)
           VALUES ($1, $2::jsonb, $3, $4)
           ON CONFLICT (key_name) DO NOTHING`,
          [key, JSON.stringify(value), 'bootstrap_seed', now]
        );
      }
    });
  };

  const listSettings = async () => {
    const result = await db.query('SELECT key_name, value_json, source, updated_at FROM app_settings ORDER BY key_name');
    return result.rows.map((row) => ({
      key: row.key_name,
      value: row.value_json,
      source: row.source,
      updatedAt: row.updated_at
    }));
  };

  const getSettingsMap = async () => {
    const result = await db.query('SELECT key_name, value_json FROM app_settings');
    return fromRows({ rows: result.rows });
  };

  const getUiConfig = async () => {
    const settings = await getSettingsMap();
    return {
      theme: settings['ui.defaultTheme'] ?? runtimeConfig.ui.defaultTheme,
      font: settings['ui.defaultFont'] ?? runtimeConfig.ui.defaultFont,
      language: settings['ui.defaultLanguage'] ?? runtimeConfig.ui.defaultLanguage,
      tileUrlTemplate: settings['map.tileUrlTemplate'] ?? runtimeConfig.map.tileUrlTemplate,
      attribution: settings['map.attribution'] ?? runtimeConfig.map.attribution,
      defaultMetric: settings['map.defaultMetric'] ?? runtimeConfig.map.defaultMetric,
      defaultAggregateShape: settings['map.defaultAggregateShape'] ?? runtimeConfig.map.defaultAggregateShape,
      defaultCellSizeMeters: settings['map.defaultCellSizeMeters'] ?? runtimeConfig.map.defaultCellSizeMeters,
      rawPointCap: settings['map.rawPointCap'] ?? runtimeConfig.map.rawPointCap,
      modeBucketDecimals: settings['aggregation.modeBucketDecimals'] ?? runtimeConfig.aggregation.modeBucketDecimals,
      cacheTtlSeconds: settings['aggregation.cacheTtlSeconds'] ?? runtimeConfig.aggregation.cacheTtlSeconds
    };
  };

  const updateSettings = async ({ updates, actorUserId, correlationId = null }) => {
    if (!updates || typeof updates !== 'object') {
      throw createAppError({
        caller: 'settings::updateSettings',
        reason: 'Settings update payload must be an object.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const now = new Date().toISOString();
    await db.withTransaction(async (client) => {
      for (const [key, value] of Object.entries(updates)) {
        await client.query(
          `INSERT INTO app_settings (key_name, value_json, source, updated_at)
           VALUES ($1, $2::jsonb, $3, $4)
           ON CONFLICT (key_name) DO UPDATE
           SET value_json = EXCLUDED.value_json,
               source = EXCLUDED.source,
               updated_at = EXCLUDED.updated_at`,
          [key, JSON.stringify(value), `user:${actorUserId}`, now]
        );
      }
    });
  };

  return {
    seedRuntimeSettings,
    listSettings,
    getSettingsMap,
    getUiConfig,
    updateSettings
  };
};
