const coreMetricFields = [
  {
    propKey: 'occurredAt',
    displayName: 'Time',
    source: 'core',
    coreColumn: 'occurred_at',
    valueType: 'time',
    popupDefaultEnabled: true
  },
  {
    propKey: 'latitude',
    displayName: 'Latitude',
    source: 'core',
    coreColumn: 'latitude',
    valueType: 'number',
    popupDefaultEnabled: true
  },
  {
    propKey: 'longitude',
    displayName: 'Longitude',
    source: 'core',
    coreColumn: 'longitude',
    valueType: 'number',
    popupDefaultEnabled: true
  },
  {
    propKey: 'altitudeMeters',
    displayName: 'Altitude',
    source: 'core',
    coreColumn: 'altitude_meters',
    valueType: 'number',
    popupDefaultEnabled: true
  },
  {
    propKey: 'accuracy',
    displayName: 'Accuracy',
    source: 'core',
    coreColumn: 'accuracy',
    valueType: 'number',
    popupDefaultEnabled: true
  }
];

const aggregateTimeSuffix = '_aggregated';
const aggregateDataCountPropKey = 'radtrackDataCount';

const syntheticPopupFields = [
  {
    propKey: aggregateDataCountPropKey,
    displayName: 'Data Count',
    source: 'synthetic',
    valueType: 'number',
    popupDefaultEnabled: true
  },
  {
    propKey: 'radtrackCacheKey',
    displayName: 'Cache Key',
    source: 'synthetic',
    valueType: 'string',
    popupDefaultEnabled: false
  },
  {
    propKey: 'radtrackCacheSource',
    displayName: 'Cache Source',
    source: 'synthetic',
    valueType: 'string',
    popupDefaultEnabled: false
  },
  {
    propKey: 'radtrackCacheTtlSeconds',
    displayName: 'Cache TTL',
    source: 'synthetic',
    valueType: 'string',
    popupDefaultEnabled: false
  }
];

const coreMetricFieldMap = new Map(coreMetricFields.map((field) => [field.propKey, field]));
const syntheticPopupFieldMap = new Map(syntheticPopupFields.map((field) => [field.propKey, field]));

const splitAggregateTimeSuffix = ({ value }) => {
  const rawValue = String(value ?? '').trim();
  const hasAggregateSuffix = rawValue.toLowerCase().endsWith(aggregateTimeSuffix);

  return {
    hasAggregateSuffix,
    baseValue: hasAggregateSuffix
      ? rawValue.slice(0, -aggregateTimeSuffix.length)
      : rawValue
  };
};

const splitWords = ({ value }) => String(value ?? '')
  .trim()
  .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
  .replace(/[^a-zA-Z0-9]+/g, ' ')
  .split(/\s+/)
  .map((entry) => entry.trim())
  .filter(Boolean);

export const normalizePropKey = ({ value }) => {
  const { hasAggregateSuffix, baseValue } = splitAggregateTimeSuffix({ value });
  const words = splitWords({ value: baseValue }).map((entry) => entry.toLowerCase());
  if (!words.length) {
    return null;
  }

  const normalizedBase = words
    .map((word, index) => (
      index === 0
        ? word
        : `${word.charAt(0).toUpperCase()}${word.slice(1)}`
    ))
    .join('');

  return hasAggregateSuffix
    ? `${normalizedBase}${aggregateTimeSuffix}`
    : normalizedBase;
};

export const humanizePropKey = ({ value }) => {
  const aggregateBasePropKey = getAggregateTimeBasePropKey({ value });
  if (aggregateBasePropKey) {
    return `${humanizePropKey({ value: aggregateBasePropKey })} (Aggregate)`;
  }

  if (normalizePropKey({ value }) === aggregateDataCountPropKey) {
    return 'Data Count';
  }

  if (normalizePropKey({ value }) === 'occurredAt') {
    return 'Time';
  }

  const words = splitWords({ value });
  if (!words.length) {
    return null;
  }

  return words
    .map((word) => `${word.charAt(0).toUpperCase()}${word.slice(1)}`)
    .join(' ');
};

export const getAggregateTimePropKey = ({ value }) => {
  const normalizedPropKey = normalizePropKey({ value });
  if (!normalizedPropKey) {
    return null;
  }

  return `${normalizedPropKey}${aggregateTimeSuffix}`;
};

export const getAggregateTimeBasePropKey = ({ value }) => {
  const normalizedPropKey = normalizePropKey({ value });
  if (!normalizedPropKey || !normalizedPropKey.endsWith(aggregateTimeSuffix)) {
    return null;
  }

  return normalizedPropKey.slice(0, -aggregateTimeSuffix.length);
};

export const isAggregateTimePropKey = ({ value }) => Boolean(getAggregateTimeBasePropKey({ value }));

export const normalizeSupportedFields = ({ value }) => {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set();
  const normalized = [];

  for (const entry of value) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      continue;
    }

    const propKey = normalizePropKey({ value: entry.propKey ?? entry.key ?? entry.name });
    if (!propKey || seen.has(propKey)) {
      continue;
    }

    const coreField = coreMetricFieldMap.get(propKey);
    const syntheticField = syntheticPopupFieldMap.get(propKey);
    const defaultField = syntheticField ?? coreField ?? null;
    const displayName = String(
      entry.displayName
      ?? entry.label
      ?? defaultField?.displayName
      ?? humanizePropKey({ value: propKey })
      ?? propKey
    ).trim();
    if (!displayName) {
      continue;
    }

    normalized.push({
      propKey,
      displayName,
      source: defaultField?.source ?? 'measurement',
      valueType: defaultField?.valueType
        ?? (entry.valueType === 'time' || entry.valueType === 'timestamp'
          ? 'time'
          : (entry.valueType === 'string' ? 'string' : 'number')),
      popupDefaultEnabled: entry.popupDefaultEnabled !== false
    });
    seen.add(propKey);
  }

  return normalized;
};

export const inferSupportedFieldsFromMeasurementSets = ({ measurementSets }) => {
  const inferred = [];
  const seen = new Set();

  for (const measurementSet of measurementSets) {
    if (!measurementSet || typeof measurementSet !== 'object' || Array.isArray(measurementSet)) {
      continue;
    }

    for (const [rawPropKey, rawValue] of Object.entries(measurementSet)) {
      const propKey = normalizePropKey({ value: rawPropKey });
      const numericValue = Number(rawValue);
      if (!propKey || seen.has(propKey) || !Number.isFinite(numericValue)) {
        continue;
      }

      inferred.push({
        propKey,
        displayName: humanizePropKey({ value: rawPropKey }) ?? humanizePropKey({ value: propKey }) ?? propKey,
        source: 'measurement',
        valueType: 'number',
        popupDefaultEnabled: true
      });
      seen.add(propKey);
    }
  }

  return inferred;
};

export const getCoreMetricFields = () => coreMetricFields.map((field) => ({ ...field }));
export const getSyntheticPopupFields = () => syntheticPopupFields.map((field) => ({ ...field }));

export const isCoreMetricField = ({ propKey }) => coreMetricFieldMap.has(String(propKey ?? ''));
export const isSyntheticPopupField = ({ propKey }) => syntheticPopupFieldMap.has(String(propKey ?? ''));

export const getCoreMetricField = ({ propKey }) => {
  const field = coreMetricFieldMap.get(String(propKey ?? ''));
  return field ? { ...field } : null;
};

export const getSyntheticPopupField = ({ propKey }) => {
  const field = syntheticPopupFieldMap.get(String(propKey ?? ''));
  return field ? { ...field } : null;
};

export const getCoreMetricColumn = ({ propKey }) => coreMetricFieldMap.get(String(propKey ?? ''))?.coreColumn ?? null;

export const isPlottableField = ({ field }) => (
  Boolean(field)
  && field.source !== 'synthetic'
  && field.valueType !== 'string'
);

export const mergeMetricFields = ({ supportedFields }) => {
  const merged = [];
  const seen = new Set();

  for (const field of normalizeSupportedFields({ value: supportedFields }).filter((entry) => isPlottableField({ field: entry }))) {
    if (seen.has(field.propKey)) {
      continue;
    }

    merged.push({ ...field });
    seen.add(field.propKey);
  }

  return merged;
};

export const mergePopupFields = ({ supportedFields }) => {
  const merged = [];
  const seen = new Set();

  for (const field of normalizeSupportedFields({ value: supportedFields })) {
    if (seen.has(field.propKey)) {
      continue;
    }

    merged.push({ ...field });
    seen.add(field.propKey);

    if (field.valueType !== 'time') {
      continue;
    }

    const aggregateFieldPropKey = getAggregateTimePropKey({ value: field.propKey });
    if (!aggregateFieldPropKey || seen.has(aggregateFieldPropKey)) {
      continue;
    }

    merged.push({
      propKey: aggregateFieldPropKey,
      displayName: `${field.displayName} (Aggregate)`,
      source: 'synthetic',
      valueType: 'time',
      popupDefaultEnabled: field.popupDefaultEnabled
    });
    seen.add(aggregateFieldPropKey);
  }

  return merged;
};

const defaultSupportedFieldPropKeys = [
  'occurredAt',
  'latitude',
  'longitude',
  'altitudeMeters',
  'accuracy',
  aggregateDataCountPropKey
];

export const getDefaultSupportedFields = () => normalizeSupportedFields({
  value: defaultSupportedFieldPropKeys.map((propKey) => ({ propKey }))
});

export const getSupportedFieldRowValue = ({ field, row }) => {
  if (!field || !row || typeof row !== 'object') {
    return null;
  }

  switch (field.propKey) {
    case 'occurredAt':
      return row.occurredAt ?? row.receivedAt ?? null;
    case 'latitude':
      return row.latitude ?? null;
    case 'longitude':
      return row.longitude ?? null;
    case 'altitudeMeters':
      return row.altitudeMeters ?? null;
    case 'accuracy':
      return row.accuracy ?? null;
    default:
      return row.measurements?.[field.propKey]
        ?? row.measurementValues?.[field.propKey]
        ?? null;
  }
};

export const rowHasSupportedFieldValue = ({ field, row }) => {
  const value = getSupportedFieldRowValue({ field, row });
  if (value === null || value === undefined || value === '') {
    return false;
  }

  if (field?.valueType === 'string') {
    return String(value).trim().length > 0;
  }

  if (field?.valueType === 'time') {
    if (typeof value === 'number') {
      return Number.isFinite(value);
    }

    const parsed = new Date(value);
    return !Number.isNaN(parsed.getTime());
  }

  return Number.isFinite(Number(value));
};

export const coerceMeasurementValueMap = ({ input }) => {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return {};
  }

  const values = {};
  for (const [rawPropKey, rawValue] of Object.entries(input)) {
    const propKey = normalizePropKey({ value: rawPropKey });
    if (!propKey) {
      continue;
    }

    const numericValue = Number(rawValue);
    if (!Number.isFinite(numericValue)) {
      continue;
    }

    values[propKey] = numericValue;
  }

  return values;
};
