const levels = ['debug', 'info', 'warn', 'error'];

const getFeatureConfig = ({ runtimeConfig, feature }) => runtimeConfig?.logging?.features?.[feature] ?? null;

export const isFeatureLogEnabled = ({
  feature,
  level = 'debug',
  runtimeConfig
}) => {
  const config = getFeatureConfig({ runtimeConfig, feature });
  if (!config?.enabled) {
    return false;
  }

  const configuredLevelIndex = levels.indexOf(config.level ?? 'info');
  const eventLevelIndex = levels.indexOf(level);
  return eventLevelIndex >= (configuredLevelIndex < 0 ? levels.indexOf('info') : configuredLevelIndex);
};

export const logFeature = ({
  caller,
  context = null,
  correlationId = null,
  feature,
  level = 'debug',
  logger,
  message,
  runtimeConfig
}) => {
  if (!isFeatureLogEnabled({ feature, level, runtimeConfig })) {
    return;
  }

  const log = logger?.[level] ?? logger?.info;
  if (!log) {
    return;
  }

  log({
    caller,
    message,
    correlationId,
    context: {
      feature,
      ...context
    }
  });
};
