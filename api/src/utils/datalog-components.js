import { normalizePropKey } from './datalog-fields.js';

export const componentsExtraJsonKey = '__radtrackComponents';

export const knownComponentFields = [
  {
    propKey: 'deviceName',
    camelKey: 'deviceName',
    snakeKey: 'device_name',
    maxLength: 255
  },
  {
    propKey: 'deviceType',
    camelKey: 'deviceType',
    snakeKey: 'device_type',
    maxLength: 255
  },
  {
    propKey: 'deviceCalibration',
    camelKey: 'deviceCalibration',
    snakeKey: 'device_calibration',
    maxLength: 1000
  },
  {
    propKey: 'firmwareVersion',
    camelKey: 'firmwareVersion',
    snakeKey: 'firmware_version',
    maxLength: 255
  },
  {
    propKey: 'sourceReadingId',
    camelKey: 'sourceReadingId',
    snakeKey: 'source_reading_id',
    maxLength: 255
  },
  {
    propKey: 'comment',
    camelKey: 'comment',
    snakeKey: 'comment',
    maxLength: 1000
  },
  {
    propKey: 'custom',
    camelKey: 'custom',
    snakeKey: 'custom_text',
    maxLength: 4000
  }
];

const knownComponentFieldMap = new Map(knownComponentFields.map((field) => [field.propKey, field]));

const asPlainObject = (value) => (
  value && typeof value === 'object' && !Array.isArray(value)
    ? value
    : null
);

const coerceNonEmptyString = ({ value }) => {
  if (value === undefined || value === null || typeof value === 'object') {
    return null;
  }

  const normalizedValue = String(value).trim();
  return normalizedValue || null;
};

export const getKnownComponentField = ({ propKey }) => {
  const field = knownComponentFieldMap.get(String(propKey ?? ''));
  return field ? { ...field } : null;
};

export const buildStoredComponents = ({ source, extraJson }) => {
  const components = {};

  for (const field of knownComponentFields) {
    const value = coerceNonEmptyString({
      value: source?.[field.camelKey] ?? source?.[field.snakeKey]
    });
    if (value === null) {
      continue;
    }

    components[field.propKey] = value;
  }

  const storedComponents = asPlainObject(extraJson?.[componentsExtraJsonKey]);
  if (!storedComponents) {
    return components;
  }

  for (const [rawKey, rawValue] of Object.entries(storedComponents)) {
    const propKey = normalizePropKey({ value: rawKey });
    const value = coerceNonEmptyString({ value: rawValue });
    if (!propKey || !value || propKey === 'deviceId') {
      continue;
    }

    components[propKey] = value;
  }

  return components;
};

export const stripStoredComponentsFromExtra = ({ extraJson }) => {
  const extra = asPlainObject(extraJson);
  if (!extra) {
    return {};
  }

  const sanitizedExtra = { ...extra };
  delete sanitizedExtra[componentsExtraJsonKey];
  return sanitizedExtra;
};

export const resolveStoredComponentValue = ({ source, extraJson, propKey }) => {
  const normalizedPropKey = normalizePropKey({ value: propKey });
  if (!normalizedPropKey) {
    return null;
  }

  const knownField = knownComponentFieldMap.get(normalizedPropKey);
  if (knownField) {
    return coerceNonEmptyString({
      value: source?.[knownField.camelKey] ?? source?.[knownField.snakeKey]
    });
  }

  const storedComponents = asPlainObject(extraJson?.[componentsExtraJsonKey]);
  if (!storedComponents) {
    return null;
  }

  if (normalizedPropKey in storedComponents) {
    return coerceNonEmptyString({ value: storedComponents[normalizedPropKey] });
  }

  for (const [rawKey, rawValue] of Object.entries(storedComponents)) {
    if (normalizePropKey({ value: rawKey }) === normalizedPropKey) {
      return coerceNonEmptyString({ value: rawValue });
    }
  }

  return null;
};
