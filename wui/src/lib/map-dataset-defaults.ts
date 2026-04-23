import { browser } from '$app/environment';

export type MapDatasetDefaultRecord = Record<string, boolean>;
export type MapDatasetOrder = string[];

const storageKeyPrefix = 'radtrack.map.dataset-defaults';
const orderStorageKeyPrefix = 'radtrack.map.dataset-order';

const getStorageKey = ({ userId }: { userId?: string | null }) => `${storageKeyPrefix}.${userId ?? 'default'}`;
const getOrderStorageKey = ({ userId }: { userId?: string | null }) => `${orderStorageKeyPrefix}.${userId ?? 'default'}`;

const normalizeRecord = (value: unknown): MapDatasetDefaultRecord => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value)
      .map(([datasetId, rawEnabled]) => {
        const normalizedDatasetId = String(datasetId ?? '').trim();
        if (!normalizedDatasetId) {
          return null;
        }

        return [normalizedDatasetId, rawEnabled !== false] as const;
      })
      .filter((entry): entry is readonly [string, boolean] => Boolean(entry))
  );
};

const normalizeOrder = (value: unknown): MapDatasetOrder => {
  if (!Array.isArray(value)) {
    return [];
  }

  const seen = new Set<string>();
  const normalized: string[] = [];

  for (const entry of value) {
    const datasetId = String(entry ?? '').trim();
    if (!datasetId || seen.has(datasetId)) {
      continue;
    }

    normalized.push(datasetId);
    seen.add(datasetId);
  }

  return normalized;
};

export const loadMapDatasetDefaults = ({ userId }: { userId?: string | null }) => {
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

export const persistMapDatasetDefaults = ({
  userId,
  defaults
}: {
  userId?: string | null;
  defaults: MapDatasetDefaultRecord;
}) => {
  if (!browser) {
    return;
  }

  window.localStorage.setItem(
    getStorageKey({ userId }),
    JSON.stringify(normalizeRecord(defaults))
  );
};

export const loadMapDatasetOrder = ({ userId }: { userId?: string | null }) => {
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

export const persistMapDatasetOrder = ({
  userId,
  order
}: {
  userId?: string | null;
  order: MapDatasetOrder;
}) => {
  if (!browser) {
    return;
  }

  window.localStorage.setItem(
    getOrderStorageKey({ userId }),
    JSON.stringify(normalizeOrder(order))
  );
};

export const orderMapDatasets = <T extends { id: string }>({
  datasets,
  order
}: {
  datasets: T[];
  order: MapDatasetOrder;
}) => {
  const indexById = new Map(
    normalizeOrder(order).map((datasetId, index) => [datasetId, index] as const)
  );

  return [...datasets]
    .map((dataset, index) => ({
      dataset,
      index,
      orderIndex: indexById.get(String(dataset.id ?? '').trim()) ?? Number.MAX_SAFE_INTEGER
    }))
    .sort((left, right) => (
      left.orderIndex - right.orderIndex
      || left.index - right.index
    ))
    .map((entry) => entry.dataset);
};

export const moveMapDatasetOrder = ({
  datasets,
  order,
  datasetId,
  direction
}: {
  datasets: Array<{ id: string }>;
  order: MapDatasetOrder;
  datasetId: string;
  direction: 'up' | 'down';
}) => {
  const normalizedDatasetId = String(datasetId ?? '').trim();
  if (!normalizedDatasetId) {
    return normalizeOrder(order);
  }

  const orderedDatasetIds = orderMapDatasets({
    datasets,
    order
  }).map((dataset) => String(dataset.id ?? '').trim()).filter(Boolean);
  const currentIndex = orderedDatasetIds.indexOf(normalizedDatasetId);
  if (currentIndex < 0) {
    return orderedDatasetIds;
  }

  const swapIndex = direction === 'up' ? currentIndex - 1 : currentIndex + 1;
  if (swapIndex < 0 || swapIndex >= orderedDatasetIds.length) {
    return orderedDatasetIds;
  }

  const nextOrder = [...orderedDatasetIds];
  [nextOrder[currentIndex], nextOrder[swapIndex]] = [nextOrder[swapIndex], nextOrder[currentIndex]];
  return nextOrder;
};

export const isMapDatasetDefaultEnabled = ({
  defaults,
  datasetId
}: {
  defaults: MapDatasetDefaultRecord;
  datasetId: string | null | undefined;
}) => {
  const normalizedDatasetId = String(datasetId ?? '').trim();
  if (!normalizedDatasetId) {
    return true;
  }

  return defaults[normalizedDatasetId] !== false;
};
