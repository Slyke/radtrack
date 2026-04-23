import { browser } from '$app/environment';
import { normalizePropKey } from '$lib/datalog-fields';

export type MapFieldVisibilityRecord = Record<string, boolean>;
export type MapFieldOrder = string[];

const storageKeyPrefix = 'radtrack.map.field-visibility';
const orderStorageKeyPrefix = 'radtrack.map.field-order';

const getStorageKey = ({ userId }: { userId?: string | null }) => `${storageKeyPrefix}.${userId ?? 'default'}`;
const getOrderStorageKey = ({ userId }: { userId?: string | null }) => `${orderStorageKeyPrefix}.${userId ?? 'default'}`;

const normalizeFieldKey = (value: string | null | undefined) => normalizePropKey(value ?? '');

const normalizeRecord = (value: unknown): MapFieldVisibilityRecord => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value)
      .map(([rawPropKey, rawEnabled]) => {
        const propKey = normalizeFieldKey(rawPropKey);
        if (!propKey) {
          return null;
        }

        return [propKey, rawEnabled !== false] as const;
      })
      .filter((entry): entry is readonly [string, boolean] => Boolean(entry))
  );
};

const normalizeOrder = (value: unknown): MapFieldOrder => {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set<string>();
  const normalized: string[] = [];

  for (const entry of value) {
    const propKey = normalizeFieldKey(String(entry ?? ''));
    if (!propKey || seen.has(propKey)) {
      continue;
    }

    normalized.push(propKey);
    seen.add(propKey);
  }

  return normalized;
};

export const loadMapFieldVisibility = ({ userId }: { userId?: string | null }) => {
  if (!browser) {
    return {};
  }

  const rawValue = window.localStorage.getItem(getStorageKey({ userId }));
  if (!rawValue) {
    return {};
  }

  try {
    return normalizeRecord(JSON.parse(rawValue));
  } catch {
    return {};
  }
};

export const persistMapFieldVisibility = ({
  userId,
  visibility
}: {
  userId?: string | null;
  visibility: MapFieldVisibilityRecord;
}) => {
  if (!browser) {
    return;
  }

  window.localStorage.setItem(
    getStorageKey({ userId }),
    JSON.stringify(normalizeRecord(visibility))
  );
};

export const loadMapFieldOrder = ({ userId }: { userId?: string | null }) => {
  if (!browser) {
    return [];
  }

  const rawValue = window.localStorage.getItem(getOrderStorageKey({ userId }));
  if (!rawValue) {
    return [];
  }

  try {
    return normalizeOrder(JSON.parse(rawValue));
  } catch {
    return [];
  }
};

export const persistMapFieldOrder = ({
  userId,
  order
}: {
  userId?: string | null;
  order: MapFieldOrder;
}) => {
  if (!browser) {
    return;
  }

  window.localStorage.setItem(
    getOrderStorageKey({ userId }),
    JSON.stringify(normalizeOrder(order))
  );
};

export const orderMapFields = <T extends { propKey: string }>({
  fields,
  order
}: {
  fields: T[];
  order: MapFieldOrder;
}) => {
  const indexByPropKey = new Map(
    normalizeOrder(order).map((propKey, index) => [propKey, index] as const)
  );

  return [...fields]
    .map((field, index) => ({
      field,
      index,
      orderIndex: indexByPropKey.get(normalizeFieldKey(field.propKey) ?? field.propKey) ?? Number.MAX_SAFE_INTEGER
    }))
    .sort((left, right) => (
      left.orderIndex - right.orderIndex
      || left.index - right.index
    ))
    .map((entry) => entry.field);
};

export const moveMapFieldOrder = ({
  fields,
  order,
  propKey,
  direction
}: {
  fields: Array<{ propKey: string }>;
  order: MapFieldOrder;
  propKey: string;
  direction: 'up' | 'down';
}) => {
  const normalizedPropKey = normalizeFieldKey(propKey);
  if (!normalizedPropKey) {
    return normalizeOrder(order);
  }

  const normalizedFieldKeys = orderMapFields({
    fields,
    order
  }).map((field) => normalizeFieldKey(field.propKey)).filter((value): value is string => Boolean(value));
  const currentIndex = normalizedFieldKeys.indexOf(normalizedPropKey);
  if (currentIndex < 0) {
    return normalizedFieldKeys;
  }

  const swapIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1;
  if (swapIndex < 0 || swapIndex >= normalizedFieldKeys.length) {
    return normalizedFieldKeys;
  }

  const nextOrder = [...normalizedFieldKeys];
  [nextOrder[currentIndex], nextOrder[swapIndex]] = [nextOrder[swapIndex], nextOrder[currentIndex]];
  return nextOrder;
};

export const isMapFieldVisible = ({
  visibility,
  propKey
}: {
  visibility: MapFieldVisibilityRecord;
  propKey: string | null | undefined;
}) => {
  const normalizedPropKey = normalizeFieldKey(propKey ?? '');
  if (!normalizedPropKey) {
    return true;
  }

  return visibility[normalizedPropKey] !== false;
};
