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
  `CREATE TABLE IF NOT EXISTS tracks (
    id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    raw_file_id TEXT NOT NULL REFERENCES raw_files(id) ON DELETE RESTRICT,
    owner_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    device_identifier_raw TEXT,
    device_model TEXT,
    device_serial TEXT,
    track_name TEXT NOT NULL,
    raw_header_line TEXT NOT NULL,
    raw_columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    header_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    row_count INTEGER NOT NULL DEFAULT 0,
    valid_row_count INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    skipped_row_count INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS track_ingest_keys (
    id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
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
  `CREATE TABLE IF NOT EXISTS track_shares (
    id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    target_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    access_level TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE(track_id, target_user_id)
  )`,
  `CREATE TABLE IF NOT EXISTS readings (
    id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    raw_timestamp TEXT,
    parsed_time_text TEXT,
    occurred_at TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    accuracy DOUBLE PRECISION,
    dose_rate DOUBLE PRECISION,
    count_rate DOUBLE PRECISION,
    comment TEXT,
    row_number INTEGER NOT NULL,
    is_hidden BOOLEAN NOT NULL DEFAULT FALSE,
    is_modified BOOLEAN NOT NULL DEFAULT FALSE,
    hidden_reason TEXT,
    modified_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    modified_at TIMESTAMPTZ,
    hidden_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    hidden_at TIMESTAMPTZ,
    warning_flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    extra_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL
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
  `ALTER TABLE tracks ALTER COLUMN raw_file_id DROP NOT NULL`,
  `ALTER TABLE tracks ADD COLUMN IF NOT EXISTS source_type TEXT`,
  `ALTER TABLE tracks ADD COLUMN IF NOT EXISTS ingest_track_id TEXT`,
  `ALTER TABLE tracks ADD COLUMN IF NOT EXISTS semantic_dedupe_key TEXT`,
  `UPDATE tracks SET source_type = 'import' WHERE source_type IS NULL`,
  `ALTER TABLE tracks ALTER COLUMN source_type SET DEFAULT 'import'`,
  `ALTER TABLE tracks ALTER COLUMN source_type SET NOT NULL`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS altitude_meters DOUBLE PRECISION`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS temperature_c DOUBLE PRECISION`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS humidity_pct DOUBLE PRECISION`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS pressure_hpa DOUBLE PRECISION`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS battery_pct DOUBLE PRECISION`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS device_id TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS device_name TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS device_type TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS device_calibration TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS firmware_version TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS source_reading_id TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS custom_text TEXT`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS ingest_key_id TEXT REFERENCES track_ingest_keys(id) ON DELETE SET NULL`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS is_modified BOOLEAN`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS modified_by_user_id TEXT REFERENCES users(id) ON DELETE SET NULL`,
  `ALTER TABLE readings ADD COLUMN IF NOT EXISTS modified_at TIMESTAMPTZ`,
  `UPDATE readings SET is_modified = FALSE WHERE is_modified IS NULL`,
  `ALTER TABLE readings ALTER COLUMN is_modified SET DEFAULT FALSE`,
  `ALTER TABLE readings ALTER COLUMN is_modified SET NOT NULL`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS effect_type TEXT`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS compress_min_points INTEGER`,
  `ALTER TABLE exclude_areas ADD COLUMN IF NOT EXISTS compress_max_points INTEGER`,
  `UPDATE exclude_areas SET effect_type = 'hard_remove' WHERE effect_type IS NULL`,
  `ALTER TABLE exclude_areas ALTER COLUMN effect_type SET DEFAULT 'hard_remove'`,
  `ALTER TABLE exclude_areas ALTER COLUMN effect_type SET NOT NULL`,
  `UPDATE readings SET received_at = COALESCE(received_at, created_at) WHERE received_at IS NULL`,
  `ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS scope_user_id TEXT REFERENCES users(id) ON DELETE SET NULL`,
  `UPDATE audit_events SET scope_user_id = COALESCE(scope_user_id, actor_user_id) WHERE scope_user_id IS NULL`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_track_id ON readings(track_id)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_occurred_at ON readings(occurred_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_received_at ON readings(received_at)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_latitude ON readings(latitude)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_longitude ON readings(longitude)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_dose_rate ON readings(dose_rate)`,
  `CREATE INDEX IF NOT EXISTS idx_readings_count_rate ON readings(count_rate)`,
  `CREATE INDEX IF NOT EXISTS idx_tracks_dataset_id ON tracks(dataset_id)`,
  `CREATE INDEX IF NOT EXISTS idx_tracks_owner_semantic_dedupe_key
   ON tracks(owner_user_id, semantic_dedupe_key)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_tracks_owner_ingest_track_id
   ON tracks(owner_user_id, ingest_track_id)
   WHERE ingest_track_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_track_ingest_keys_track_id ON track_ingest_keys(track_id)`,
  `CREATE INDEX IF NOT EXISTS idx_track_shares_target_user_id ON track_shares(target_user_id)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_track_ingest_keys_key_hash ON track_ingest_keys(key_hash)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_readings_track_source_reading_id
   ON readings(track_id, source_reading_id)
   WHERE source_reading_id IS NOT NULL`,
  `CREATE INDEX IF NOT EXISTS idx_dataset_shares_target_user_id ON dataset_shares(target_user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_raw_files_checksum ON raw_files(checksum)`,
  `CREATE INDEX IF NOT EXISTS idx_raw_files_upload_batch_id ON raw_files(upload_batch_id)`,
  `CREATE INDEX IF NOT EXISTS idx_upload_batches_checksum ON upload_batches(checksum)`,
  `CREATE INDEX IF NOT EXISTS idx_upload_batches_uploader_user_id ON upload_batches(uploader_user_id)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events(created_at)`,
  `CREATE INDEX IF NOT EXISTS idx_audit_events_scope_user_id ON audit_events(scope_user_id)`
];

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
    for (const statement of migrationStatements) {
      await pool.query(statement);
    }
  } catch (cause) {
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
