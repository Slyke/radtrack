import { createOpaqueId, sha256Hex } from '../utils/ids.js';
import { canModerateUsers } from './permissions.js';

const auditLimitOptions = new Set(['10', '20', '50', '100', '250', 'all']);
const defaultAuditLimitValue = '20';

const isRecord = (value) => Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const normalizeLimitInput = ({ value }) => (
  Array.isArray(value) ? value[0] : value
);

const parseAuditLimitValue = ({ value }) => {
  const normalized = String(normalizeLimitInput({ value }) ?? '').trim().toLowerCase();
  return auditLimitOptions.has(normalized) ? normalized : defaultAuditLimitValue;
};

const resolveListLimit = ({ value, fallback = 20 }) => {
  const normalized = normalizeLimitInput({ value });
  if (normalized === undefined || normalized === null || normalized === '') {
    return fallback;
  }

  if (String(normalized).trim().toLowerCase() === 'all') {
    return null;
  }

  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }

  return Math.max(1, Math.min(Math.trunc(parsed), 500));
};

const buildAuditIndexFilter = ({ value }) => [
  0,
  value === 'all' ? -1 : Number.parseInt(value, 10)
];

const toEntityLabel = ({ entityType, entityId }) => `${entityType}${entityId ? `:${entityId}` : ''}`;

const extractAuditMetadata = ({ payload }) => {
  if (!isRecord(payload) || !isRecord(payload.metadata)) {
    return null;
  }

  return Object.keys(payload.metadata).length ? payload.metadata : null;
};

const buildAccessibleAuditWhere = ({ user }) => {
  if (!user || canModerateUsers({ user })) {
    return {
      params: [],
      whereClause: ''
    };
  }

  return {
    params: [user.id],
    whereClause: `WHERE (
      audit.scope_user_id = $1
      OR audit.actor_user_id = $1
      OR (audit.scope_user_id IS NULL AND audit.actor_user_id IS NULL)
    )`
  };
};

const mapAuditRow = (row) => ({
  id: row.id,
  actorUserId: row.actor_user_id,
  actorUsername: row.actor_username,
  scopeUserId: row.scope_user_id,
  eventType: row.event_type,
  entityType: row.entity_type,
  entityId: row.entity_id,
  payload: row.payload_json,
  createdAt: row.created_at
});

const toAuditExportEntry = ({ entry, previousHash }) => ({
  time: entry.createdAt,
  author: entry.actorUsername ?? 'System',
  authorUserId: entry.actorUserId,
  scopeUserId: entry.scopeUserId,
  event: entry.eventType,
  entityType: entry.entityType,
  entityId: entry.entityId,
  entity: toEntityLabel({
    entityType: entry.entityType,
    entityId: entry.entityId
  }),
  payload: entry.payload,
  metadata: extractAuditMetadata({ payload: entry.payload }),
  previousHash
});

const hashAuditExportEntry = ({ entry }) => sha256Hex({
  value: JSON.stringify(entry)
});

export const createAuditService = ({ db }) => {
  const record = async ({
    actorUserId = null,
    scopeUserId = actorUserId,
    eventType,
    entityType,
    entityId = null,
    payload = {}
  }) => {
    await db.query(
      `INSERT INTO audit_events (id, actor_user_id, scope_user_id, event_type, entity_type, entity_id, payload_json, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)`,
      [
        createOpaqueId(),
        actorUserId,
        scopeUserId,
        eventType,
        entityType,
        entityId,
        JSON.stringify(payload),
        new Date().toISOString()
      ]
    );
  };

  const countVisible = async ({ user } = {}) => {
    const { params, whereClause } = buildAccessibleAuditWhere({ user });
    const result = await db.query(
      `SELECT COUNT(*)::integer AS total
       FROM audit_events audit
       ${whereClause}`,
      params
    );

    return Number(result.rows[0]?.total ?? 0);
  };

  const listVisible = async ({
    user,
    limit = null,
    descending = true
  } = {}) => {
    const { params, whereClause } = buildAccessibleAuditWhere({ user });
    const queryParams = [...params];
    const limitClause = limit === null
      ? ''
      : `LIMIT $${queryParams.push(limit)}`;

    const result = await db.query(
      `SELECT
         audit.id,
         audit.actor_user_id,
         audit.scope_user_id,
         audit.event_type,
         audit.entity_type,
         audit.entity_id,
         audit.payload_json,
         audit.created_at,
         actor.username AS actor_username
       FROM audit_events audit
       LEFT JOIN users actor ON actor.id = audit.actor_user_id
       ${whereClause}
       ORDER BY audit.created_at ${descending ? 'DESC' : 'ASC'}, audit.id ${descending ? 'DESC' : 'ASC'}
       ${limitClause}`,
      queryParams
    );

    return result.rows.map(mapAuditRow);
  };

  const listRecent = async ({ user, limit = 20 } = {}) => {
    const safeLimit = resolveListLimit({ value: limit, fallback: 20 });
    return await listVisible({
      user,
      limit: safeLimit,
      descending: true
    });
  };

  const listPage = async ({ user, limitValue } = {}) => {
    const parsedLimitValue = parseAuditLimitValue({ value: limitValue });
    const limit = resolveListLimit({
      value: parsedLimitValue,
      fallback: Number.parseInt(defaultAuditLimitValue, 10)
    });
    const [entries, totalEntries] = await Promise.all([
      listVisible({
        user,
        limit,
        descending: true
      }),
      countVisible({ user })
    ]);

    return {
      limitValue: parsedLimitValue,
      totalEntries,
      hasMore: limit !== null && totalEntries > entries.length,
      filters: {
        indexes: buildAuditIndexFilter({ value: parsedLimitValue })
      },
      entries
    };
  };

  const exportEntries = async ({
    user,
    limitValue,
    buildLabel
  } = {}) => {
    const parsedLimitValue = parseAuditLimitValue({ value: limitValue });
    const allEntriesAscending = await listVisible({
      user,
      limit: null,
      descending: false
    });

    let previousHash = null;
    const chainedEntries = allEntriesAscending.map((entry) => {
      const exportEntry = toAuditExportEntry({
        entry,
        previousHash
      });
      const entryHash = hashAuditExportEntry({ entry: exportEntry });
      previousHash = entryHash;
      return {
        exportEntry,
        entryHash
      };
    });

    const selectedEntries = parsedLimitValue === 'all'
      ? chainedEntries
      : chainedEntries.slice(-Number.parseInt(parsedLimitValue, 10));

    return {
      exportTime: new Date().toISOString(),
      build: buildLabel,
      filters: {
        indexes: buildAuditIndexFilter({ value: parsedLimitValue }),
        limit: parsedLimitValue,
        totalEntries: chainedEntries.length,
        returnedEntries: selectedEntries.length
      },
      title: 'Radiacode',
      type: 'audit log',
      integrity: {
        verified: true,
        algorithm: 'sha256',
        hashInput: 'sha256(JSON.stringify(entry))',
        totalEntriesVerified: chainedEntries.length,
        latestHash: chainedEntries[chainedEntries.length - 1]?.entryHash ?? null
      },
      data: selectedEntries
        .slice()
        .reverse()
        .map(({ exportEntry }) => exportEntry)
    };
  };

  return {
    record,
    listRecent,
    listPage,
    exportEntries
  };
};
