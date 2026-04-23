import { createAppError } from '../lib/errors.js';
import { mergeMetricFields, mergePopupFields, normalizeSupportedFields } from '../utils/datalog-fields.js';
import { createOpaqueId } from '../utils/ids.js';
import { canShare } from './permissions.js';

const datasetAccessCase = ({ roleParamIndex, userParamIndex }) => `
  CASE
    WHEN $${roleParamIndex} = 'admin' THEN 'edit'
    WHEN d.owner_user_id = $${userParamIndex} THEN 'edit'
    WHEN EXISTS (
      SELECT 1
      FROM dataset_shares share_edit
      WHERE share_edit.dataset_id = d.id
        AND share_edit.target_user_id = $${userParamIndex}
        AND share_edit.access_level = 'edit'
    ) THEN 'edit'
    WHEN EXISTS (
      SELECT 1
      FROM dataset_shares share_view
      WHERE share_view.dataset_id = d.id
        AND share_view.target_user_id = $${userParamIndex}
        AND share_view.access_level IN ('edit', 'view')
    ) THEN 'view'
    ELSE NULL
  END
`;

const datasetWhereClause = ({ roleParamIndex, userParamIndex }) => `
  (
    $${roleParamIndex} = 'admin'
    OR d.owner_user_id = $${userParamIndex}
    OR EXISTS (
      SELECT 1
      FROM dataset_shares ds
      WHERE ds.dataset_id = d.id
        AND ds.target_user_id = $${userParamIndex}
    )
  )
`;

export const createDatasetService = ({ db, audit }) => {
  const listDatasets = async ({ user }) => {
    const result = await db.query(
      `SELECT
         d.*,
         ${datasetAccessCase({ roleParamIndex: 1, userParamIndex: 2 })} AS access_level,
         (SELECT COUNT(*) FROM datalogs t WHERE t.dataset_id = d.id) AS datalog_count,
         (SELECT COUNT(*) FROM readings r JOIN datalogs t ON t.id = r.datalog_id WHERE t.dataset_id = d.id) AS reading_count
       FROM datasets d
       WHERE ${datasetWhereClause({ roleParamIndex: 1, userParamIndex: 2 })}
       ORDER BY d.updated_at DESC`,
      [user.role, user.id]
    );

    return result.rows.map((row) => ({
      id: row.id,
      ownerUserId: row.owner_user_id,
      name: row.name,
      description: row.description,
      accessLevel: row.access_level,
      datalogCount: Number(row.datalog_count ?? 0),
      readingCount: Number(row.reading_count ?? 0),
      createdAt: row.created_at,
      updatedAt: row.updated_at
    }));
  };

  const getDatasetAccess = async ({ datasetId, user, correlationId = null }) => {
    const result = await db.query(
      `SELECT d.*, ${datasetAccessCase({ roleParamIndex: 2, userParamIndex: 3 })} AS access_level
       FROM datasets d
       WHERE d.id = $1`,
      [datasetId, user.role, user.id]
    );
    const dataset = result.rows[0] ?? null;

    if (!dataset) {
      throw createAppError({
        caller: 'datasets::getDatasetAccess',
        reason: 'Dataset was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    if (!dataset.access_level) {
      throw createAppError({
        caller: 'datasets::getDatasetAccess',
        reason: 'Dataset is not visible to the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    return dataset;
  };

  const getDatasetDetail = async ({ datasetId, user, correlationId = null }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    const [tracksResult, sharesResult, excludeAreasResult, focusPointResult] = await Promise.all([
      db.query(
        `SELECT *
         FROM datalogs
         WHERE dataset_id = $1
         ORDER BY created_at DESC`,
        [datasetId]
      ),
      db.query(
        `SELECT ds.*, u.username
         FROM dataset_shares ds
         JOIN users u ON u.id = ds.target_user_id
         WHERE ds.dataset_id = $1
         ORDER BY u.username`,
        [datasetId]
      ),
      db.query(
        `SELECT *
         FROM exclude_areas
         WHERE dataset_id = $1
         ORDER BY created_at DESC`,
        [datasetId]
      ),
      db.query(
        `SELECT
           r.latitude,
           r.longitude
         FROM datalogs t
         JOIN readings r ON r.datalog_id = t.id
         WHERE t.dataset_id = $1
           AND r.latitude IS NOT NULL
           AND r.longitude IS NOT NULL
         ORDER BY t.created_at DESC, r.row_number ASC, r.created_at ASC
         LIMIT 1`,
        [datasetId]
      )
    ]);
    const focusPoint = focusPointResult.rows[0] ?? null;

    return {
      id: dataset.id,
      ownerUserId: dataset.owner_user_id,
      name: dataset.name,
      description: dataset.description,
      accessLevel: dataset.access_level,
      createdAt: dataset.created_at,
      updatedAt: dataset.updated_at,
      datalogs: tracksResult.rows.map((row) => {
        const supportedFields = normalizeSupportedFields({
          value: row.supported_fields_json
        });

        return ({
        id: row.id,
        rawFileId: row.raw_file_id,
        sourceType: row.source_type,
        ingestDatalogId: row.ingest_datalog_id,
        datalogName: row.datalog_name,
        deviceIdentifierRaw: row.device_identifier_raw,
        supportedFields,
        metricFields: mergeMetricFields({ supportedFields }),
        rowCount: row.row_count,
        validRowCount: row.valid_row_count,
        warningCount: row.warning_count,
        errorCount: row.error_count,
        skippedRowCount: row.skipped_row_count,
        startedAt: row.started_at,
        endedAt: row.ended_at,
        createdAt: row.created_at
      });
      }),
      shares: sharesResult.rows.map((row) => ({
        id: row.id,
        targetUserId: row.target_user_id,
        username: row.username,
        accessLevel: row.access_level,
        createdBy: row.created_by,
        createdAt: row.created_at
      })),
      excludeAreas: excludeAreasResult.rows.map((row) => ({
        id: row.id,
        datasetId: row.dataset_id,
        shapeType: row.shape_type,
        effectType: row.effect_type,
        compressMinPoints: row.compress_min_points,
        compressMaxPoints: row.compress_max_points,
        geometry: row.geometry_json,
        label: row.label,
        applyByDefaultOnExport: row.apply_by_default_on_export,
        createdByUserId: row.created_by_user_id,
        createdAt: row.created_at
      })),
      mapFocusPoint: focusPoint
        ? {
            latitude: focusPoint.latitude,
            longitude: focusPoint.longitude
          }
        : null
    };
  };

  const listFieldInventory = async ({ user }) => {
    const result = await db.query(
      `SELECT
         d.id AS dataset_id,
         d.name AS dataset_name,
         log.id AS datalog_id,
         log.datalog_name,
         log.supported_fields_json
       FROM datasets d
       JOIN datalogs log ON log.dataset_id = d.id
       WHERE ${datasetWhereClause({ roleParamIndex: 1, userParamIndex: 2 })}
       ORDER BY d.updated_at DESC, log.created_at DESC`,
      [user.role, user.id]
    );

    const byPropKey = new Map();

    for (const row of result.rows) {
      for (const field of mergePopupFields({
        supportedFields: row.supported_fields_json
      })) {
        const current = byPropKey.get(field.propKey) ?? {
          propKey: field.propKey,
          source: field.source,
          valueType: field.valueType,
          displayNames: new Set(),
          datasetsById: new Map(),
          datalogs: []
        };

        current.displayNames.add(field.displayName);
        current.datasetsById.set(row.dataset_id, {
          id: row.dataset_id,
          name: row.dataset_name
        });
        current.datalogs.push({
          id: row.datalog_id,
          name: row.datalog_name,
          datasetId: row.dataset_id,
          datasetName: row.dataset_name,
          displayName: field.displayName
        });
        byPropKey.set(field.propKey, current);
      }
    }

    return [...byPropKey.values()]
      .map((entry) => ({
        propKey: entry.propKey,
        source: entry.source,
        valueType: entry.valueType,
        displayNames: [...entry.displayNames].sort((left, right) => left.localeCompare(right)),
        datasets: [...entry.datasetsById.values()].sort((left, right) => left.name.localeCompare(right.name)),
        datalogs: entry.datalogs.sort((left, right) => (
          left.datasetName.localeCompare(right.datasetName)
          || String(left.name ?? '').localeCompare(String(right.name ?? ''))
        ))
      }))
      .sort((left, right) => left.propKey.localeCompare(right.propKey));
  };

  const createDataset = async ({ user, name, description = null, correlationId = null }) => {
    if (!name || typeof name !== 'string') {
      throw createAppError({
        caller: 'datasets::createDataset',
        reason: 'Dataset name is required.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const datasetId = createOpaqueId();
    const now = new Date().toISOString();
    await db.query(
      `INSERT INTO datasets (id, owner_user_id, name, description, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $5)`,
      [datasetId, user.id, name.trim(), description, now]
    );

    await audit.record({
      actorUserId: user.id,
      eventType: 'dataset.created',
      entityType: 'dataset',
      entityId: datasetId,
      payload: { name: name.trim() }
    });

    return getDatasetDetail({ datasetId, user, correlationId });
  };

  const updateDataset = async ({ datasetId, user, name, description, correlationId = null }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::updateDataset',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query(
      `UPDATE datasets
       SET name = COALESCE($2, name),
           description = COALESCE($3, description),
           updated_at = $4
       WHERE id = $1`,
      [datasetId, name ?? null, description ?? null, new Date().toISOString()]
    );

    await audit.record({
      actorUserId: user.id,
      eventType: 'dataset.updated',
      entityType: 'dataset',
      entityId: datasetId,
      payload: { name, description }
    });

    return getDatasetDetail({ datasetId, user, correlationId });
  };

  const deleteDataset = async ({ datasetId, user, correlationId = null }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::deleteDataset',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query('DELETE FROM datasets WHERE id = $1', [datasetId]);
    await audit.record({
      actorUserId: user.id,
      eventType: 'dataset.deleted',
      entityType: 'dataset',
      entityId: datasetId,
      payload: { name: dataset.name }
    });
  };

  const upsertShare = async ({ datasetId, user, targetUserId, accessLevel, correlationId = null }) => {
    if (!canShare({ user })) {
      throw createAppError({
        caller: 'datasets::upsertShare',
        reason: 'This user is not allowed to share datasets.',
        errorKey: 'AUTH_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::upsertShare',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    if (!['view', 'edit'].includes(accessLevel)) {
      throw createAppError({
        caller: 'datasets::upsertShare',
        reason: 'Share access level must be view or edit.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const shareId = createOpaqueId();
    await db.query(
      `INSERT INTO dataset_shares (id, dataset_id, target_user_id, access_level, created_by, created_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (dataset_id, target_user_id) DO UPDATE
       SET access_level = EXCLUDED.access_level,
           created_by = EXCLUDED.created_by,
           created_at = EXCLUDED.created_at`,
      [shareId, datasetId, targetUserId, accessLevel, user.id, new Date().toISOString()]
    );

    await audit.record({
      actorUserId: user.id,
      eventType: 'dataset.share_upserted',
      entityType: 'dataset',
      entityId: datasetId,
      payload: { targetUserId, accessLevel }
    });
  };

  const removeShare = async ({ datasetId, shareId, user, correlationId = null }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::removeShare',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query('DELETE FROM dataset_shares WHERE id = $1 AND dataset_id = $2', [shareId, datasetId]);
    await audit.record({
      actorUserId: user.id,
      eventType: 'dataset.share_removed',
      entityType: 'dataset',
      entityId: datasetId,
      payload: { shareId }
    });
  };

  const listCombinedDatasets = async ({ user }) => {
    const result = await db.query(
      `SELECT
         cd.*,
         (
           SELECT COUNT(*)
           FROM combined_dataset_members member
           WHERE member.combined_dataset_id = cd.id
         ) AS member_count
       FROM combined_datasets cd
       WHERE cd.owner_user_id = $1 OR $2 = 'admin'
       ORDER BY cd.updated_at DESC`,
      [user.id, user.role]
    );

    return result.rows.map((row) => ({
      id: row.id,
      ownerUserId: row.owner_user_id,
      name: row.name,
      description: row.description,
      memberCount: Number(row.member_count ?? 0),
      createdAt: row.created_at,
      updatedAt: row.updated_at
    }));
  };

  const getCombinedDataset = async ({ combinedDatasetId, user, correlationId = null }) => {
    const combinedResult = await db.query(
      `SELECT * FROM combined_datasets WHERE id = $1`,
      [combinedDatasetId]
    );
    const combined = combinedResult.rows[0] ?? null;
    if (!combined) {
      throw createAppError({
        caller: 'datasets::getCombinedDataset',
        reason: 'Combined dataset was not found.',
        errorKey: 'DATASET_NOT_FOUND',
        correlationId,
        status: 404
      });
    }

    if (combined.owner_user_id !== user.id && user.role !== 'admin') {
      throw createAppError({
        caller: 'datasets::getCombinedDataset',
        reason: 'Combined dataset is not visible to the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    const membersResult = await db.query(
      `SELECT d.id, d.name
       FROM combined_dataset_members cdm
       JOIN datasets d ON d.id = cdm.dataset_id
       WHERE cdm.combined_dataset_id = $1
       ORDER BY d.name`,
      [combinedDatasetId]
    );

    for (const member of membersResult.rows) {
      await getDatasetAccess({ datasetId: member.id, user, correlationId });
    }

    return {
      id: combined.id,
      ownerUserId: combined.owner_user_id,
      name: combined.name,
      description: combined.description,
      createdAt: combined.created_at,
      updatedAt: combined.updated_at,
      members: membersResult.rows.map((row) => ({
        datasetId: row.id,
        name: row.name
      }))
    };
  };

  const createCombinedDataset = async ({ user, name, description = null, datasetIds, correlationId = null }) => {
    if (!Array.isArray(datasetIds) || !datasetIds.length) {
      throw createAppError({
        caller: 'datasets::createCombinedDataset',
        reason: 'Combined dataset requires at least one member dataset.',
        errorKey: 'COMBINED_DATASET_INVALID',
        correlationId,
        status: 400
      });
    }

    if (!name || typeof name !== 'string') {
      throw createAppError({
        caller: 'datasets::createCombinedDataset',
        reason: 'Combined dataset name is required.',
        errorKey: 'COMBINED_DATASET_INVALID',
        correlationId,
        status: 400
      });
    }

    for (const datasetId of datasetIds) {
      await getDatasetAccess({ datasetId, user, correlationId });
    }

    const combinedDatasetId = createOpaqueId();
    const now = new Date().toISOString();
    await db.withTransaction(async (client) => {
      await client.query(
        `INSERT INTO combined_datasets (id, owner_user_id, name, description, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $5)`,
        [combinedDatasetId, user.id, name, description, now]
      );

      for (const datasetId of datasetIds) {
        await client.query(
          `INSERT INTO combined_dataset_members (id, combined_dataset_id, dataset_id)
           VALUES ($1, $2, $3)`,
          [createOpaqueId(), combinedDatasetId, datasetId]
        );
      }
    });

    await audit.record({
      actorUserId: user.id,
      eventType: 'combined_dataset.created',
      entityType: 'combined_dataset',
      entityId: combinedDatasetId,
      payload: { datasetIds }
    });

    return getCombinedDataset({ combinedDatasetId, user, correlationId });
  };

  const updateCombinedDataset = async ({ combinedDatasetId, user, name, description, datasetIds, correlationId = null }) => {
    const combined = await getCombinedDataset({ combinedDatasetId, user, correlationId });
    if (datasetIds) {
      for (const datasetId of datasetIds) {
        await getDatasetAccess({ datasetId, user, correlationId });
      }
    }

    await db.withTransaction(async (client) => {
      await client.query(
        `UPDATE combined_datasets
         SET name = COALESCE($2, name),
             description = COALESCE($3, description),
             updated_at = $4
         WHERE id = $1`,
        [combinedDatasetId, name ?? null, description ?? null, new Date().toISOString()]
      );

      if (datasetIds) {
        await client.query(
          `DELETE FROM combined_dataset_members WHERE combined_dataset_id = $1`,
          [combinedDatasetId]
        );
        for (const datasetId of datasetIds) {
          await client.query(
            `INSERT INTO combined_dataset_members (id, combined_dataset_id, dataset_id)
             VALUES ($1, $2, $3)`,
            [createOpaqueId(), combinedDatasetId, datasetId]
          );
        }
      }
    });

    await audit.record({
      actorUserId: user.id,
      eventType: 'combined_dataset.updated',
      entityType: 'combined_dataset',
      entityId: combinedDatasetId,
      payload: { name, description, datasetIds }
    });

    return getCombinedDataset({ combinedDatasetId, user, correlationId });
  };

  const deleteCombinedDataset = async ({ combinedDatasetId, user, correlationId = null }) => {
    await getCombinedDataset({ combinedDatasetId, user, correlationId });
    await db.query('DELETE FROM combined_datasets WHERE id = $1', [combinedDatasetId]);
    await audit.record({
      actorUserId: user.id,
      eventType: 'combined_dataset.deleted',
      entityType: 'combined_dataset',
      entityId: combinedDatasetId,
      payload: {}
    });
  };

  const createExcludeArea = async ({
    datasetId,
    user,
    shapeType,
    geometry,
    label = null,
    applyByDefaultOnExport = false,
    effectType = 'hard_remove',
    compressMinPoints = null,
    compressMaxPoints = null,
    correlationId = null
  }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::createExcludeArea',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    if (!['polygon', 'circle'].includes(shapeType)) {
      throw createAppError({
        caller: 'datasets::createExcludeArea',
        reason: 'Exclude area shape must be polygon or circle.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    if (!['hard_remove', 'compress'].includes(effectType)) {
      throw createAppError({
        caller: 'datasets::createExcludeArea',
        reason: 'Exclude area effect type must be hard_remove or compress.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const normalizedCompressMinPoints = compressMinPoints === null || compressMinPoints === undefined
      ? null
      : Number(compressMinPoints);
    const normalizedCompressMaxPoints = compressMaxPoints === null || compressMaxPoints === undefined
      ? null
      : Number(compressMaxPoints);

    if (effectType === 'compress') {
      if (
        !Number.isInteger(normalizedCompressMinPoints)
        || !Number.isInteger(normalizedCompressMaxPoints)
        || normalizedCompressMinPoints < 0
        || normalizedCompressMaxPoints < normalizedCompressMinPoints
      ) {
        throw createAppError({
          caller: 'datasets::createExcludeArea',
          reason: 'Compress exclude areas require a valid min/max point range.',
          errorKey: 'REQUEST_INVALID',
          correlationId,
          status: 400
        });
      }
    }

    const excludeAreaId = createOpaqueId();
    await db.query(
      `INSERT INTO exclude_areas (
        id,
        dataset_id,
        created_by_user_id,
        shape_type,
        effect_type,
        compress_min_points,
        compress_max_points,
        geometry_json,
        label,
        apply_by_default_on_export,
        created_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11)`,
      [
        excludeAreaId,
        datasetId,
        user.id,
        shapeType,
        effectType,
        effectType === 'compress' ? normalizedCompressMinPoints : null,
        effectType === 'compress' ? normalizedCompressMaxPoints : null,
        JSON.stringify(geometry),
        label,
        applyByDefaultOnExport,
        new Date().toISOString()
      ]
    );

    await audit.record({
      actorUserId: user.id,
      eventType: 'exclude_area.created',
      entityType: 'exclude_area',
      entityId: excludeAreaId,
      payload: {
        datasetId,
        shapeType,
        effectType,
        compressMinPoints: effectType === 'compress' ? normalizedCompressMinPoints : null,
        compressMaxPoints: effectType === 'compress' ? normalizedCompressMaxPoints : null,
        label
      }
    });
  };

  const deleteExcludeArea = async ({ datasetId, excludeAreaId, user, correlationId = null }) => {
    const dataset = await getDatasetAccess({ datasetId, user, correlationId });
    if (dataset.access_level !== 'edit') {
      throw createAppError({
        caller: 'datasets::deleteExcludeArea',
        reason: 'Dataset is not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query('DELETE FROM exclude_areas WHERE id = $1 AND dataset_id = $2', [excludeAreaId, datasetId]);
    await audit.record({
      actorUserId: user.id,
      eventType: 'exclude_area.deleted',
      entityType: 'exclude_area',
      entityId: excludeAreaId,
      payload: { datasetId }
    });
  };

  const hideReadings = async ({ user, readingIds, reason = null, correlationId = null }) => {
    if (!Array.isArray(readingIds) || !readingIds.length) {
      throw createAppError({
        caller: 'datasets::hideReadings',
        reason: 'At least one reading id is required.',
        errorKey: 'REQUEST_INVALID',
        correlationId,
        status: 400
      });
    }

    const result = await db.query(
      `SELECT r.id, d.id AS dataset_id, ${datasetAccessCase({ roleParamIndex: 2, userParamIndex: 3 })} AS access_level
       FROM readings r
       JOIN datalogs t ON t.id = r.datalog_id
       JOIN datasets d ON d.id = t.dataset_id
       WHERE r.id = ANY($1::text[])`,
      [readingIds, user.role, user.id]
    );

    if (result.rows.some((row) => row.access_level !== 'edit')) {
      throw createAppError({
        caller: 'datasets::hideReadings',
        reason: 'One or more readings are not editable by the current user.',
        errorKey: 'DATASET_FORBIDDEN',
        correlationId,
        status: 403
      });
    }

    await db.query(
      `UPDATE readings
       SET is_hidden = TRUE,
           hidden_reason = $2,
           hidden_by_user_id = $3,
           hidden_at = $4
       WHERE id = ANY($1::text[])`,
      [readingIds, reason, user.id, new Date().toISOString()]
    );

    await audit.record({
      actorUserId: user.id,
      eventType: 'reading.hidden',
      entityType: 'reading',
      entityId: readingIds[0],
      payload: {
        readingIds,
        reason
      }
    });
  };

  return {
    listDatasets,
    listFieldInventory,
    getDatasetAccess,
    getDatasetDetail,
    createDataset,
    updateDataset,
    deleteDataset,
    upsertShare,
    removeShare,
    listCombinedDatasets,
    getCombinedDataset,
    createCombinedDataset,
    updateCombinedDataset,
    deleteCombinedDataset,
    createExcludeArea,
    deleteExcludeArea,
    hideReadings
  };
};
