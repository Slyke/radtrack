import { appendFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { AppError } from './errors.js';

const levels = ['debug', 'info', 'warn', 'error'];
const textTemplateDefault = '[{$timestamp}] {$level} {$caller} {$correlationId} {$errorCode} {$message}{$context}{$rootCause}{$kubernetes}';
const sensitiveKeyPattern = /(password|secret|token|cookie|authorization|apikey|clientsecret|sessionsecret)/i;
const errorMetadataKeys = [
  'code',
  'detail',
  'hint',
  'position',
  'internalPosition',
  'internalQuery',
  'where',
  'schema',
  'table',
  'column',
  'dataType',
  'constraint',
  'file',
  'line',
  'routine',
  'severity',
  'severity_local'
];

const parseBoolean = ({ value, fallback = false }) => {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
};

const parseInteger = ({ value, fallback }) => {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  const parsed = Number.parseInt(String(value), 10);
  return Number.isNaN(parsed) ? fallback : parsed;
};

const parseLevels = ({ value }) => {
  if (!value) {
    return [];
  }

  return String(value)
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter((entry) => entry && levels.includes(entry));
};

const redact = ({ value }) => {
  if (Array.isArray(value)) {
    return value.map((entry) => redact({ value: entry }));
  }

  if (!value || typeof value !== 'object') {
    return value;
  }

  return Object.fromEntries(
    Object.entries(value).map(([key, nestedValue]) => [
      key,
      sensitiveKeyPattern.test(key)
        ? '<redacted>'
        : redact({ value: nestedValue })
    ])
  );
};

const stringifySafe = ({ value }) => {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
};

const serializeError = ({ error }) => {
  const serialized = {
    name: error.name,
    message: error.message,
    stack: error.stack
  };

  for (const key of errorMetadataKeys) {
    if (error[key] !== undefined) {
      serialized[key] = error[key];
    }
  }

  for (const [key, value] of Object.entries(error)) {
    if (serialized[key] !== undefined) {
      continue;
    }

    serialized[key] = value;
  }

  return redact({ value: serialized });
};

const resolveKubernetesMetadata = () => {
  const enabled = parseBoolean({
    value: process.env.LOG_K8S_METADATA_ENABLED ?? process.env.MQTTCTL_LOG_K8S_METADATA_ENABLED,
    fallback: false
  });

  if (!enabled) {
    return undefined;
  }

  const metadata = Object.fromEntries(
    Object.entries({
      podName: process.env.K8S_POD_NAME ?? process.env.POD_NAME ?? process.env.HOSTNAME,
      deployment: process.env.K8S_DEPLOYMENT ?? process.env.DEPLOYMENT_NAME,
      namespace: process.env.K8S_NAMESPACE ?? process.env.POD_NAMESPACE,
      podIp: process.env.K8S_POD_IP ?? process.env.POD_IP,
      podIps: process.env.K8S_POD_IPS,
      nodeName: process.env.K8S_NODE_NAME ?? process.env.NODE_NAME
    }).filter(([, value]) => value !== undefined && value !== null && value !== '')
  );

  return Object.keys(metadata).length > 0 ? metadata : undefined;
};

const interpolate = ({ template, values }) => template.replace(/\{\$([a-zA-Z0-9_]+)\}/g, (_, key) => values[key] ?? '');

const ensureDirectory = ({ filePath }) => {
  const dir = path.dirname(filePath);
  if (!dir) {
    return;
  }

  mkdirSync(dir, { recursive: true });
};

export const createLogger = () => {
  const consoleEnabled = parseBoolean({ value: process.env.LOG_CONSOLE_ENABLED, fallback: true });
  const consoleFormat = String(process.env.LOG_CONSOLE_FORMAT ?? 'text').toLowerCase() === 'json' ? 'json' : 'text';
  const consoleLevels = parseLevels({ value: process.env.LOG_CONSOLE_LEVELS });
  const fileEnabled = parseBoolean({ value: process.env.LOG_FILE_ENABLED, fallback: false });
  const fileFormat = String(process.env.LOG_FILE_FORMAT ?? 'json').toLowerCase() === 'text' ? 'text' : 'json';
  const filePath = process.env.LOG_FILE_PATH ?? '';
  const fileLevels = parseLevels({ value: process.env.LOG_FILE_LEVELS });
  const textTemplate = process.env.LOG_TEXT_FORMAT || textTemplateDefault;
  const kubernetes = resolveKubernetesMetadata();
  const httpTimeoutMs = parseInteger({ value: process.env.LOG_HTTP_TIMEOUT_MS, fallback: 2500 });

  const emit = ({ level, caller, message, correlationId = null, errorKey = null, errorCode = null, context = null, rootCause = null }) => {
    const event = {
      timestamp: new Date().toISOString(),
      level,
      caller,
      message,
      correlationId,
      errorKey,
      errorCode,
      context: context ? redact({ value: context }) : undefined,
      rootCause: rootCause instanceof Error
        ? serializeError({ error: rootCause })
        : (rootCause ? redact({ value: rootCause }) : undefined),
      kubernetes
    };

    const consoleAllowed = consoleEnabled && (!consoleLevels.length || consoleLevels.includes(level));
    const fileAllowed = fileEnabled && filePath && (!fileLevels.length || fileLevels.includes(level));

    if (consoleAllowed) {
      if (consoleFormat === 'json') {
        const method = level === 'error' ? console.error : (level === 'warn' ? console.warn : console.log);
        method(JSON.stringify(event));
      } else {
        const rendered = interpolate({
          template: textTemplate,
          values: {
            timestamp: event.timestamp,
            level: level.toUpperCase(),
            caller,
            correlationId: correlationId ?? '',
            errorCode: errorCode ?? '',
            message: typeof message === 'string' ? message : stringifySafe({ value: message }),
            context: event.context ? ` context=${stringifySafe({ value: event.context })}` : '',
            rootCause: event.rootCause ? ` rootCause=${stringifySafe({ value: event.rootCause })}` : '',
            kubernetes: kubernetes ? ` kubernetes=${stringifySafe({ value: kubernetes })}` : ''
          }
        }).trim();

        const method = level === 'error' ? console.error : (level === 'warn' ? console.warn : console.log);
        method(rendered);
      }
    }

    if (fileAllowed) {
      ensureDirectory({ filePath });
      const content = fileFormat === 'json'
        ? `${JSON.stringify(event)}\n`
        : `${interpolate({
            template: textTemplate,
            values: {
              timestamp: event.timestamp,
              level: level.toUpperCase(),
              caller,
              correlationId: correlationId ?? '',
              errorCode: errorCode ?? '',
              message: typeof message === 'string' ? message : stringifySafe({ value: message }),
              context: event.context ? ` context=${stringifySafe({ value: event.context })}` : '',
              rootCause: event.rootCause ? ` rootCause=${stringifySafe({ value: event.rootCause })}` : '',
              kubernetes: kubernetes ? ` kubernetes=${stringifySafe({ value: kubernetes })}` : ''
            }
          })}\n`;
      appendFileSync(filePath, content);
    }

    return event;
  };

  return {
    debug: (args) => emit({ level: 'debug', ...args }),
    info: (args) => emit({ level: 'info', ...args }),
    warn: (args) => emit({ level: 'warn', ...args }),
    error: (args) => emit({ level: 'error', ...args }),
    httpTimeoutMs,
    kubernetes
  };
};

export const logError = ({ logger, caller, reason, errorKey, correlationId = null, context = null, cause = null, status = 500 }) => {
  const error = cause instanceof AppError
    ? cause
    : new AppError({ caller, reason, errorKey, correlationId, context, cause, status });

  logger.error({
    caller,
    message: reason,
    correlationId,
    errorKey: error.errorKey,
    errorCode: error.errorCode,
    context,
    rootCause: cause ?? error
  });

  return error;
};
