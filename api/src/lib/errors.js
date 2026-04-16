import { readFileSync } from 'node:fs';
import path from 'node:path';

const defaultCatalogPath = path.resolve(process.cwd(), 'api/src/errors.json');

const loadCatalog = () => {
  try {
    return JSON.parse(readFileSync(defaultCatalogPath, 'utf8'));
  } catch {
    return { ERR_UNKNOWN: 'FFFFFFFFFFFFFFFF' };
  }
};

const errorCatalog = loadCatalog();

export class AppError extends Error {
  constructor({
    caller,
    reason,
    errorKey = 'ERR_UNKNOWN',
    correlationId = null,
    context = null,
    cause = null,
    status = 500
  }) {
    super(reason, cause ? { cause } : undefined);
    this.name = 'AppError';
    this.caller = caller;
    this.reason = reason;
    this.errorKey = errorKey;
    this.errorCode = errorCatalog[errorKey] ?? errorCatalog.ERR_UNKNOWN ?? 'FFFFFFFFFFFFFFFF';
    this.correlationId = correlationId;
    this.context = context;
    this.status = status;
    this.cause = cause;
  }
}

export const createAppError = ({
  caller,
  reason,
  errorKey = 'ERR_UNKNOWN',
  correlationId = null,
  context = null,
  cause = null,
  status = 500
}) => new AppError({
  caller,
  reason,
  errorKey,
  correlationId,
  context,
  cause,
  status
});

export const toErrorBody = ({ error }) => {
  if (error instanceof AppError) {
    return {
      ok: false,
      reason: error.reason,
      errorKey: error.errorKey,
      errorCode: error.errorCode,
      correlationId: error.correlationId,
      details: error.context
    };
  }

  return {
    ok: false,
    reason: 'Unexpected error',
    errorKey: 'ERR_UNKNOWN',
    errorCode: errorCatalog.ERR_UNKNOWN ?? 'FFFFFFFFFFFFFFFF',
    correlationId: null,
    details: null
  };
};

export const getErrorCatalog = () => errorCatalog;
