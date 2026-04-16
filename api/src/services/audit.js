import { createOpaqueId } from '../utils/ids.js';

export const createAuditService = ({ db }) => {
  const record = async ({
    actorUserId = null,
    eventType,
    entityType,
    entityId = null,
    payload = {}
  }) => {
    await db.query(
      `INSERT INTO audit_events (id, actor_user_id, event_type, entity_type, entity_id, payload_json, created_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
      [
        createOpaqueId(),
        actorUserId,
        eventType,
        entityType,
        entityId,
        JSON.stringify(payload),
        new Date().toISOString()
      ]
    );
  };

  const listRecent = async ({ limit = 20 } = {}) => {
    const result = await db.query(
      `SELECT id, actor_user_id, event_type, entity_type, entity_id, payload_json, created_at
       FROM audit_events
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );

    return result.rows.map((row) => ({
      id: row.id,
      actorUserId: row.actor_user_id,
      eventType: row.event_type,
      entityType: row.entity_type,
      entityId: row.entity_id,
      payload: row.payload_json,
      createdAt: row.created_at
    }));
  };

  return {
    record,
    listRecent
  };
};
