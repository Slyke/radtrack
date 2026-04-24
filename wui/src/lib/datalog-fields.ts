export type MetricField = {
  propKey: string;
  displayName: string;
  source: 'core' | 'measurement' | 'synthetic';
  valueType: 'number' | 'time' | 'string';
  popupDefaultEnabled: boolean;
  metricListEnabled: boolean;
};

const aggregateTimeSuffix = '_aggregated';
const aggregateTimeSuffixAliases = [aggregateTimeSuffix, '_aggregate'];
export const aggregateDataCountPropKey = 'radtrackDataCount';

export const coreMetricFields: MetricField[] = [
  { propKey: 'occurredAt', displayName: 'Time', source: 'core', valueType: 'time', popupDefaultEnabled: true, metricListEnabled: true },
  { propKey: 'latitude', displayName: 'Latitude', source: 'core', valueType: 'number', popupDefaultEnabled: true, metricListEnabled: true },
  { propKey: 'longitude', displayName: 'Longitude', source: 'core', valueType: 'number', popupDefaultEnabled: true, metricListEnabled: true },
  { propKey: 'altitudeMeters', displayName: 'Altitude', source: 'core', valueType: 'number', popupDefaultEnabled: true, metricListEnabled: true },
  { propKey: 'accuracy', displayName: 'Accuracy', source: 'core', valueType: 'number', popupDefaultEnabled: true, metricListEnabled: true }
];

export const syntheticPopupFields: MetricField[] = [
  {
    propKey: aggregateDataCountPropKey,
    displayName: 'Data Count',
    source: 'synthetic',
    valueType: 'number',
    popupDefaultEnabled: true,
    metricListEnabled: false
  },
  { propKey: 'radtrackCacheKey', displayName: 'Cache Key', source: 'synthetic', valueType: 'string', popupDefaultEnabled: false, metricListEnabled: false },
  { propKey: 'radtrackCacheSource', displayName: 'Cache Source', source: 'synthetic', valueType: 'string', popupDefaultEnabled: false, metricListEnabled: false },
  { propKey: 'radtrackCacheTtlSeconds', displayName: 'Cache TTL', source: 'synthetic', valueType: 'string', popupDefaultEnabled: false, metricListEnabled: false }
];

const coreMetricFieldMap = new Map(coreMetricFields.map((field) => [field.propKey, field] as const));
const syntheticPopupFieldMap = new Map(syntheticPopupFields.map((field) => [field.propKey, field] as const));

const splitAggregateTimeSuffix = (value: string | null | undefined) => {
  const rawValue = String(value ?? '').trim();
  const matchedSuffix = aggregateTimeSuffixAliases.find((suffix) => rawValue.toLowerCase().endsWith(suffix));

  return {
    hasAggregateSuffix: Boolean(matchedSuffix),
    baseValue: matchedSuffix
      ? rawValue.slice(0, -matchedSuffix.length)
      : rawValue
  };
};

const splitWords = (value: string) => value
  .trim()
  .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
  .replace(/[^a-zA-Z0-9]+/g, ' ')
  .split(/\s+/)
  .map((entry) => entry.trim())
  .filter(Boolean);

export const humanizePropKey = (value: string): string => {
  const aggregateBasePropKey = getAggregateTimeBasePropKey(value);
  if (aggregateBasePropKey) {
    return `${humanizePropKey(aggregateBasePropKey)} (Aggregate)`;
  }

  if (normalizePropKey(value) === aggregateDataCountPropKey) {
    return 'Data Count';
  }

  if (normalizePropKey(value) === 'occurredAt') {
    return 'Time';
  }

  return splitWords(value)
    .map((word) => `${word.charAt(0).toUpperCase()}${word.slice(1)}`)
    .join(' ');
};

export const normalizePropKey = (value: string | null | undefined) => {
  const { hasAggregateSuffix, baseValue } = splitAggregateTimeSuffix(value);
  const words = splitWords(baseValue).map((entry) => entry.toLowerCase());
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

export const getAggregateTimePropKey = (value: string | null | undefined) => {
  const normalizedPropKey = normalizePropKey(value);
  if (!normalizedPropKey) {
    return null;
  }

  return `${normalizedPropKey}${aggregateTimeSuffix}`;
};

export const getAggregateTimeBasePropKey = (value: string | null | undefined) => {
  const normalizedPropKey = normalizePropKey(value);
  if (!normalizedPropKey || !normalizedPropKey.endsWith(aggregateTimeSuffix)) {
    return null;
  }

  return normalizedPropKey.slice(0, -aggregateTimeSuffix.length);
};

export const isAggregateTimePropKey = (value: string | null | undefined) => (
  Boolean(getAggregateTimeBasePropKey(value))
);

const getAggregateTimeSyntheticField = (propKey: string) => {
  const aggregateBasePropKey = getAggregateTimeBasePropKey(propKey);
  if (!aggregateBasePropKey) {
    return null;
  }

  return {
    propKey,
    displayName: `${humanizePropKey(aggregateBasePropKey)} (Aggregate)`,
    source: 'synthetic' as const,
    valueType: 'time' as const,
    popupDefaultEnabled: false,
    metricListEnabled: false
  };
};

export const normalizeSupportedFields = (value: unknown): MetricField[] => {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set<string>();
  const normalized: MetricField[] = [];

  for (const entry of value) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
      continue;
    }

    const record = entry as Record<string, unknown>;
    const propKey = normalizePropKey(String(record.propKey ?? record.key ?? record.name ?? ''));
    if (!propKey || seen.has(propKey)) {
      continue;
    }

    const coreField = coreMetricFieldMap.get(propKey);
    const syntheticField = syntheticPopupFieldMap.get(propKey);
    const aggregateTimeField = getAggregateTimeSyntheticField(propKey);
    const defaultField = aggregateTimeField ?? syntheticField ?? coreField ?? null;
    const hasExplicitPopupDefault = Object.prototype.hasOwnProperty.call(record, 'popupDefaultEnabled');
    const hasExplicitMetricList = Object.prototype.hasOwnProperty.call(record, 'metricListEnabled');
    const normalizedSource = defaultField?.source ?? 'measurement';
    const normalizedValueType = defaultField?.valueType
      ?? (record.valueType === 'time' || record.valueType === 'timestamp'
        ? 'time'
        : (record.valueType === 'string' ? 'string' : 'number'));
    const displayName = String(
      record.displayName
      ?? record.label
      ?? defaultField?.displayName
      ?? humanizePropKey(propKey)
    ).trim();
    if (!displayName) {
      continue;
    }

    normalized.push({
      propKey,
      displayName,
      source: normalizedSource,
      valueType: normalizedValueType,
      popupDefaultEnabled: hasExplicitPopupDefault
        ? record.popupDefaultEnabled !== false
        : (defaultField?.popupDefaultEnabled ?? true),
      metricListEnabled: hasExplicitMetricList
        ? record.metricListEnabled !== false
        : (defaultField?.metricListEnabled ?? (normalizedSource !== 'synthetic' && normalizedValueType !== 'string'))
    });
    seen.add(propKey);
  }

  return normalized;
};

export const mergeMetricFields = (supportedFields: MetricField[]): MetricField[] => {
  const merged: MetricField[] = [];
  const seen = new Set<string>();

  for (const field of supportedFields.filter((entry) => isPlottableField(entry) && entry.metricListEnabled !== false)) {
    if (seen.has(field.propKey)) {
      continue;
    }

    merged.push(field);
    seen.add(field.propKey);
  }

  return merged;
};

export const mergePopupFields = (supportedFields: MetricField[]): MetricField[] => {
  const normalizedFields = normalizeSupportedFields(supportedFields);
  const merged: MetricField[] = [];
  const seen = new Set<string>();

  for (const field of normalizedFields) {
    if (seen.has(field.propKey)) {
      continue;
    }

    merged.push(field);
    seen.add(field.propKey);
  }

  for (const field of normalizedFields) {
    if (field.valueType !== 'time' || isAggregateTimePropKey(field.propKey)) {
      continue;
    }

    const aggregateFieldPropKey = getAggregateTimePropKey(field.propKey);
    if (!aggregateFieldPropKey || seen.has(aggregateFieldPropKey)) {
      continue;
    }

    merged.push({
      propKey: aggregateFieldPropKey,
      displayName: `${field.displayName} (Aggregate)`,
      source: 'synthetic',
      valueType: 'time',
      popupDefaultEnabled: field.popupDefaultEnabled,
      metricListEnabled: false
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

export const getDefaultSupportedFields = (): MetricField[] => normalizeSupportedFields(
  defaultSupportedFieldPropKeys.map((propKey) => ({ propKey }))
);

export const isSyntheticPopupField = (propKey: string | null | undefined) => syntheticPopupFieldMap.has(String(propKey ?? ''));

export const isPlottableField = (field: MetricField | null | undefined) => {
  if (!field) {
    return false;
  }

  return field.source !== 'synthetic' && field.valueType !== 'string';
};

export const getMetricOptionLabels = (fields: MetricField[]) => {
  const labelCounts = new Map<string, number>();
  for (const field of fields) {
    labelCounts.set(field.displayName, (labelCounts.get(field.displayName) ?? 0) + 1);
  }

  return new Map(
    fields.map((field) => [
      field.propKey,
      (labelCounts.get(field.displayName) ?? 0) > 1
        ? `${field.propKey}.${field.displayName}`
        : field.displayName
    ])
  );
};
