import { Pool } from 'pg';
import { createAppError } from './errors.js';

const migrationStatements = [
  `CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL,
    password_hash TEXT,
    must_change_password BOOLEAN NOT NULL DEFAULT TRUE,
    is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS auth_identities (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_type TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    provider_subject_or_principal TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(provider_type, provider_subject_or_principal)
  )`,
  `CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    auth_method TEXT NOT NULL,
    csrf_token TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS auth_requests (
    state TEXT PRIMARY KEY,
    code_verifier TEXT NOT NULL,
    redirect_to TEXT,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS user_devices (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_identifier_raw TEXT NOT NULL,
    device_model TEXT,
    device_serial TEXT,
    nickname TEXT,
    preferred_display_unit TEXT,
    calibration_json JSONB,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE(user_id, device_identifier_raw)
  )`,
  `CREATE TABLE IF NOT EXISTS upload_batches (
    id TEXT PRIMARY KEY,
    uploader_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    source_type TEXT NOT NULL,
    original_filename TEXT,
    checksum TEXT,
    size_bytes BIGINT,
    uploaded_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb
  )`,
  `CREATE TABLE IF NOT EXISTS raw_files (
    id TEXT PRIMARY KEY,
    upload_batch_id TEXT NOT NULL REFERENCES upload_batches(id) ON DELETE CASCADE,
    parent_raw_file_id TEXT REFERENCES raw_files(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    checksum TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    blob_data BYTEA NOT NULL,
    source_type TEXT NOT NULL,
    provenance_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    parse_status TEXT NOT NULL,
    parse_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS datasets (
    id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS dataset_shares (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    access_level TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(dataset_id, target_user_id)
  )`,
  `CREATE TABLE IF NOT EXISTS datalogs (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    raw_file_id TEXT REFERENCES raw_files(id) ON DELETE RESTRICT,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    device_identifier_raw TEXT,
    device_model TEXT,
    device_serial TEXT,
    datalog_name TEXT NOT NULL,
    raw_header_line TEXT NOT NULL,
    raw_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    header_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    supported_fields_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_type TEXT NOT NULL DEFAULT 'import',
    ingest_datalog_id TEXT,
    semantic_dedupe_key TEXT,
    row_count INTEGER NOT NULL DEFAULT 0,
    valid_row_count INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    skipped_row_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS datalog_ingest_keys (
    id TEXT PRIMARY KEY,
    datalog_id TEXT NOT NULL REFERENCES datalogs(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL,
    key_prefix TEXT NOT NULL,
    label TEXT NOT NULL,
    notes TEXT,
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    revoked_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL,
    last_used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
  )`,
  `CREATE TABLE IF NOT EXISTS datalog_shares (
    id TEXT PRIMARY KEY,
    datalog_id TEXT NOT NULL REFERENCES datalogs(id) ON DELETE CASCADE,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    access_level TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(datalog_id, target_user_id)
  )`,
  `CREATE TABLE IF NOT EXISTS readings (
    id TEXT PRIMARY KEY,
    datalog_id TEXT NOT NULL REFERENCES datalogs(id) ON DELETE CASCADE,
    raw_timestamp TEXT,
    parsed_time_text TEXT,
    occurred_at TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude_meters DOUBLE PRECISION,
    accuracy DOUBLE PRECISION,
    comment TEXT,
    custom_text TEXT,
    row_number INTEGER NOT NULL,
    is_hidden BOOLEAN NOT NULL DEFAULT FALSE,
    is_modified BOOLEAN NOT NULL DEFAULT FALSE,
    hidden_reason TEXT,
    modified_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    modified_at TIMESTAMPTZ,
    hidden_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    hidden_at TIMESTAMPTZ,
    device_id TEXT,
    device_name TEXT,
    device_type TEXT,
    device_calibration TEXT,
    firmware_version TEXT,
    source_reading_id TEXT,
    ingest_key_id TEXT REFERENCES datalog_ingest_keys(id) ON DELETE SET NULL,
    warning_flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    extra_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS reading_numeric_values (
    id TEXT PRIMARY KEY,
    reading_id TEXT NOT NULL REFERENCES readings(id) ON DELETE CASCADE,
    datalog_id TEXT NOT NULL REFERENCES datalogs(id) ON DELETE CASCADE,
    prop_key TEXT NOT NULL,
    numeric_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(reading_id, prop_key)
  )`,
  `CREATE TABLE IF NOT EXISTS exclude_areas (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    shape_type TEXT NOT NULL,
    effect_type TEXT NOT NULL DEFAULT 'hard_remove',
    compress_min_points INTEGER,
    compress_max_points INTEGER,
    geometry_json JSONB NOT NULL,
    label TEXT,
    apply_by_default_on_export BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS combined_datasets (
    id TEXT PRIMARY KEY,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS combined_dataset_members (
    id TEXT PRIMARY KEY,
    combined_dataset_id TEXT NOT NULL REFERENCES combined_datasets(id) ON DELETE CASCADE,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    UNIQUE(combined_dataset_id, dataset_id)
  )`,
  `CREATE TABLE IF NOT EXISTS audit_events (
    id TEXT PRIMARY KEY,
    actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    scope_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS app_settings (
    key_name TEXT PRIMARY KEY,
    value_json JSONB NOT NULL,
    source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS user_settings (
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key_name TEXT NOT NULL,
    value_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, key_name)
  )`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS effect_type TEXT`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS compress_min_points INTEGER`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS compress_max_points INTEGER`,
  `UPDATE exclude_areas SET effect_type = 'hard_remove' WHERE effect_type IS NULL`,
  `ALTER TABLE exclude_areas ALTER COLUMN effect_type SET DEFAULT 'hard_remove'`,
  `ALTER TABLE exclude_areas ALTER COLUMN effect_type SET NOT NULL`,
  `ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS scope_user_id TEXT REFERENCES users(id) ON DELETE SET NULL`,
  `UPDATE audit_events SET scope_user_id = COALESCE(scope_user_id, actor_user_id) WHERE scope_user_id IS NULL`,
  `CREATE INDEX IF NOT EXISTS idx_user_settings_key_name ON user_settings(key_name)`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_datalog_id ON readings(datalog_id)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_occurred_at ON readings(occurred_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_received_at ON readings(received_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_latitude ON readings(latitude)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_longitude ON readings(longitude)`,
  `CREATE INDEX IF NOT EXISTS idx_datalogs_dataset_id ON datalogs(dataset_id)`,
  `CREATE INDEX IF NOT EXISTS idx_datalogs_owner_semantic_dedupe_key
   ON datalogs(owner_user_id, semantic_dedupe_key)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_datalogs_owner_ingest_datalog_id
   ON datalogs(owner_user_id, ingest_datalog_id)
   WHERE ingest_datalog_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_datalog_ingest_keys_datalog_id ON datalog_ingest_keys(datalog_id)`,
  `CREATE INDEX IF NOT EXISTS idx_datalog_shares_target_user_id ON datalog_shares(target_user_id)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_datalog_ingest_keys_key_hash ON datalog_ingest_keys(key_hash)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_datalog_source_reading_id
   ON readings(datalog_id, source_reading_id)
   WHERE source_reading_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_reading_numeric_values_reading_id ON reading_numeric_values(reading_id)`,
  `CREATE INDEX IF NOT EXISTS idx_reading_numeric_values_datalog_prop_key
   ON reading_numeric_values(datalog_id, prop_key)`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_shares_target_user_id ON dataset_shares(target_user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_raw_files_checksum ON raw_files(checksum)`,
  `CREATE INDEX IF NOT EXISTS idx_raw_files_upload_batch_id ON raw_files(upload_batch_id)`,
  `CREATE INDEX IF NOT EXISTS idx_upload_batches_checksum ON upload_batches(checksum)`,
  `CREATE INDEX IF NOT EXISTS idx_upload_batches_uploader_user_id ON upload_batches(uploader_user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events(created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_events_scope_user_id ON audit_events(scope_user_id)`
];

const appResetStatements = [
  `DROP TABLE IF EXISTS reading_numeric_values CASCADE`,
  `DROP TABLE IF EXISTS readings CASCADE`,
  `DROP TABLE IF EXISTS datalog_ingest_keys CASCADE`,
  `DROP TABLE IF EXISTS datalog_shares CASCADE`,
  `DROP TABLE IF EXISTS datalogs CASCADE`,
  `DROP TABLE IF EXISTS track_ingest_keys CASCADE`,
  `DROP TABLE IF EXISTS track_shares CASCADE`,
  `DROP TABLE IF EXISTS tracks CASCADE`
];

const compatibleColumnsByTable = {
  datalogs: [
    'dataset_id',
    'owner_user_id',
    'datalog_name',
    'supported_fields_json',
    'source_type',
    'ingest_datalog_id',
    'semantic_dedupe_key'
  ],
  datalog_ingest_keys: [
    'datalog_id',
    'key_hash',
    'key_prefix',
    'label'
  ],
  datalog_shares: [
    'datalog_id',
    'target_user_id',
    'access_level'
  ],
  readings: [
    'datalog_id',
    'received_at',
    'altitude_meters',
    'accuracy',
    'custom_text',
    'source_reading_id',
    'ingest_key_id',
    'extra_json'
  ],
  reading_numeric_values: [
    'reading_id',
    'datalog_id',
    'prop_key',
    'numeric_value'
  ]
};

const loadExistingColumns = async ({ pool, tableNames }) => {
  if (!tableNames.length) {
    return new Map();
  }

  const result = await pool.query(
    `SELECT table_name, column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = ANY($1::text[])
     ORDER BY table_name, column_name`,
    [tableNames]
  );

  const columnsByTable = new Map();
  for (const row of result.rows) {
    if (!columnsByTable.has(row.table_name)) {
      columnsByTable.set(row.table_name, new Set());
    }

    columnsByTable.get(row.table_name).add(row.column_name);
  }

  return columnsByTable;
};

const getIncompatibleAppSchemaReason = async ({ pool }) => {
  const legacyTableResult = await pool.query(
    `SELECT
       to_regclass('public.tracks') AS legacy_tracks,
       to_regclass('public.track_ingest_keys') AS legacy_track_ingest_keys,
       to_regclass('public.track_shares') AS legacy_track_shares`
  );
  const legacyTables = legacyTableResult.rows[0] ?? {};

  if (legacyTables.legacy_tracks || legacyTables.legacy_track_ingest_keys || legacyTables.legacy_track_shares) {
    return 'legacy track tables detected';
  }

  const columnsByTable = await loadExistingColumns({
    pool,
    tableNames: Object.keys(compatibleColumnsByTable)
  });

  for (const [tableName, requiredColumns] of Object.entries(compatibleColumnsByTable)) {
    const existingColumns = columnsByTable.get(tableName);
    if (!existingColumns) {
      continue;
    }

    const missingColumns = requiredColumns.filter((columnName) => !existingColumns.has(columnName));
    if (missingColumns.length) {
      return `${tableName} is missing columns: ${missingColumns.join(', ')}`;
    }
  }

  return null;
};

const resetIncompatibleAppSchemaIfNeeded = async ({ pool, logger, correlationId }) => {
  const incompatibleReason = await getIncompatibleAppSchemaReason({ pool });
  if (!incompatibleReason) {
    return;
  }

  logger.warn({
    caller: 'db::resetIncompatibleAppSchemaIfNeeded',
    message: 'Resetting incompatible app schema before migrations.',
    correlationId,
    errorKey: 'DB_MIGRATION_FAILED',
    context: {
      reason: incompatibleReason
    }
  });

  for (const statement of appResetStatements) {
    await pool.query(statement);
  }
};

export const createDatabase = async ({ runtimeConfig, logger, correlationId }) => {
  const pool = new Pool({
    host: runtimeConfig.database.postgres.host,
    port: runtimeConfig.database.postgres.port,
    database: runtimeConfig.database.postgres.database,
    user: runtimeConfig.database.postgres.user,
    password: runtimeConfig.database.postgres.password,
    max: 20
  });

  try {
    await pool.query('SELECT 1');
  } catch (cause) {
    throw createAppError({
      caller: 'db::createDatabase',
      reason: 'Failed connecting to Postgres.',
      errorKey: 'DB_CONNECT_FAILED',
      correlationId,
      cause
    });
  }

  try {
    await resetIncompatibleAppSchemaIfNeeded({ pool, logger, correlationId });
    for (const statement of migrationStatements) {
      try {
        await pool.query(statement);
      } catch (cause) {
        throw createAppError({
          caller: 'db::createDatabase',
          reason: 'Failed running a database migration statement.',
          errorKey: 'DB_MIGRATION_FAILED',
          correlationId,
          context: {
            statement
          },
          cause
        });
      }
    }
  } catch (cause) {
    if (cause?.name === 'AppError') {
      throw cause;
    }

    throw createAppError({
      caller: 'db::createDatabase',
      reason: 'Failed running database migrations.',
      errorKey: 'DB_MIGRATION_FAILED',
      correlationId,
      cause
    });
  }

  logger.info({
    caller: 'db::createDatabase',
    message: 'Postgres schema is ready.',
    correlationId,
    context: {
      host: runtimeConfig.database.postgres.host,
      database: runtimeConfig.database.postgres.database
    }
  });

  const withClient = async (run) => {
    const client = await pool.connect();
    try {
      return await run(client);
    } finally {
      client.release();
    }
  };

  const withTransaction = async (run) => withClient(async (client) => {
    await client.query('BEGIN');

    try {
      const result = await run(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
  });

  return {
    pool,
    query: (text, params = []) => pool.query(text, params),
    withClient,
    withTransaction,
    close: async () => {
      await pool.end();
    },
    cleanupSessions: async () => {
      await pool.query('DELETE FROM sessions WHERE expires_at < NOW()');
      await pool.query(`DELETE FROM auth_requests WHERE created_at < NOW() - INTERVAL '10 minutes'`);
    }
  };
};
