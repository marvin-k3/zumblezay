use crate::bedrock::{create_bedrock_client, BedrockClientTrait};
use crate::openai::{real::maybe_create_openai_client, OpenAIClientTrait};
use anyhow::Result;
use base64::Engine;
use lru::LruCache;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rand::rngs::OsRng;
use rand::RngCore;
use rusqlite::Connection;
use rusqlite_migration::{Migrations, M};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::instrument;
use tracing::warn;

pub mod app;
pub mod bedrock;
pub mod bedrock_spend;
pub mod hybrid_search;
pub mod investigation;
pub mod openai;
pub mod process_events;
pub mod prompt_context;
pub mod prompts;
pub mod schema_registry;
pub mod storyboard;
pub mod summary;
#[cfg(test)]
pub mod summary_test;
pub mod time_util;
pub mod transcription;
pub mod transcripts;

pub mod test_utils;

use crate::storyboard::StoryboardData;

// ServiceStats struct for both main app and testing
#[derive(Debug)]
pub struct ServiceStats {
    pub processed_count: AtomicU64,
    pub error_count: AtomicU64,
    pub total_processing_time_ms: AtomicU64,
}

impl Default for ServiceStats {
    fn default() -> Self {
        Self::new()
    }
}
// Structures for database records
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: String,
    pub type_: String,
    pub camera_id: String,
    pub camera_name: Option<String>,
    pub start: f64,
    pub end: f64,
    pub path: Option<String>,
}

impl ServiceStats {
    pub fn new() -> Self {
        Self {
            processed_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            total_processing_time_ms: AtomicU64::new(0),
        }
    }
}

// Define the AppState struct for both main app and testing
pub struct AppState {
    pub events_db: Pool<SqliteConnectionManager>,
    pub zumblezay_db: Pool<SqliteConnectionManager>,
    pub cache_db: Pool<SqliteConnectionManager>,
    pub whisper_url: String,
    pub last_processed_time: Arc<Mutex<f64>>,
    pub stats: ServiceStats,
    pub active_tasks: Arc<Mutex<HashMap<String, String>>>,
    pub semaphore: Arc<tokio::sync::Semaphore>,
    pub openai_client: Option<Arc<dyn OpenAIClientTrait>>,
    pub bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
    pub bedrock_region: Option<String>,
    pub runpod_api_key: Option<String>,
    pub transcription_service: String,
    pub enable_embedding_updates: bool,
    pub camera_name_cache: Arc<Mutex<HashMap<String, String>>>,
    pub storyboard_cache: Arc<RwLock<LruCache<String, StoryboardData>>>,
    pub default_summary_model: String,
    pub investigation_model: String,
    pub video_path_original_prefix: String,
    pub video_path_replacement_prefix: String,
    pub timezone: chrono_tz::Tz,
    // Track in-progress storyboard generations
    pub in_progress_storyboards:
        Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
    pub signing_secret: String,
    // Holds temporary data for prompt context.
    pub prompt_context_store: Arc<prompt_context::Store>,
    pub shutdown_token: CancellationToken,
    pub backfill_tasks: Arc<Mutex<Vec<(i64, JoinHandle<()>)>>>,
    // Add fields to track temp files
    #[allow(dead_code)]
    temp_events_path: Option<tempfile::NamedTempFile>,
    #[allow(dead_code)]
    temp_zumblezay_path: Option<tempfile::NamedTempFile>,
    #[allow(dead_code)]
    temp_cache_db_path: Option<tempfile::NamedTempFile>,
}

impl AppState {
    pub fn new_for_testing() -> Self {
        Self::new_for_testing_with_clients(None, None)
    }
    // Create a new AppState for testing with minimal configuration
    pub fn new_for_testing_with_openai_client(
        openai_client: Option<Arc<dyn OpenAIClientTrait>>,
    ) -> Self {
        Self::new_for_testing_with_clients(openai_client, None)
    }

    pub fn new_for_testing_with_clients(
        openai_client: Option<Arc<dyn OpenAIClientTrait>>,
        bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
    ) -> Self {
        // Create temporary files for SQLite databases
        let temp_events_file = tempfile::NamedTempFile::new()
            .expect("Failed to create temporary events database file");
        let temp_zumblezay_file = tempfile::NamedTempFile::new()
            .expect("Failed to create temporary zumblezay database file");
        let temp_cache_db_file = tempfile::NamedTempFile::new()
            .expect("Failed to create temporary cache database file");

        // Get paths as strings
        let events_path = temp_events_file
            .path()
            .to_str()
            .expect("Failed to get events temp file path")
            .to_string();
        let zumblezay_path = temp_zumblezay_file
            .path()
            .to_str()
            .expect("Failed to get zumblezay temp file path")
            .to_string();
        let cache_path = temp_cache_db_file
            .path()
            .to_str()
            .expect("Failed to get cache temp file path")
            .to_string();

        // Create connection managers with the temp file paths
        let events_manager = SqliteConnectionManager::file(&events_path);
        let events_pool =
            Pool::new(events_manager).expect("Failed to create events pool");

        let zumblezay_manager = SqliteConnectionManager::file(&zumblezay_path);
        let zumblezay_pool = Pool::new(zumblezay_manager)
            .expect("Failed to create zumblezay pool");

        let cache_manager = SqliteConnectionManager::file(&cache_path);
        let cache_pool =
            Pool::new(cache_manager).expect("Failed to create cache pool");

        let mut zumblezay_conn =
            zumblezay_pool.get().expect("Failed to get connection");
        init_zumblezay_db(&mut zumblezay_conn)
            .expect("Failed to initialize transcript db");

        let mut events_conn =
            events_pool.get().expect("Failed to get connection");
        init_events_db_for_testing(&mut events_conn)
            .expect("Failed to initialize events db");

        let mut cache_conn =
            cache_pool.get().expect("Failed to get connection");
        init_cache_db(&mut cache_conn).expect("Failed to initialize cache db");

        Self {
            events_db: events_pool,
            zumblezay_db: zumblezay_pool,
            cache_db: cache_pool,
            whisper_url: "http://localhost:9000/asr".to_string(),
            last_processed_time: Arc::new(Mutex::new(0.0)),
            stats: ServiceStats::new(),
            active_tasks: Arc::new(Mutex::new(HashMap::new())),
            semaphore: Arc::new(tokio::sync::Semaphore::new(3)),
            openai_client,
            bedrock_client: bedrock_client
                .or_else(|| Some(create_bedrock_client(None))),
            bedrock_region: None,
            runpod_api_key: None,
            transcription_service: "whisper-local".to_string(),
            enable_embedding_updates: true,
            camera_name_cache: Arc::new(Mutex::new(HashMap::new())),
            storyboard_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(10).unwrap(),
            ))),
            default_summary_model: "anthropic-claude-haiku".to_string(),
            investigation_model: "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
                .to_string(),
            video_path_original_prefix: "/data".to_string(),
            video_path_replacement_prefix: "/path/to/replacement".to_string(),
            // Use Adelaide timezone for tests because it is an odd timezone,
            // being offset from UTC by 9.5 hours
            timezone: chrono_tz::Australia::Adelaide,
            // Track in-progress storyboard generations
            in_progress_storyboards: Arc::new(Mutex::new(HashMap::new())),
            signing_secret: "test_secret".to_string(),
            prompt_context_store: Arc::new(prompt_context::Store::new()),
            shutdown_token: CancellationToken::new(),
            backfill_tasks: Arc::new(Mutex::new(Vec::new())),
            // Store temp files so they're cleaned up when AppState is dropped
            temp_events_path: Some(temp_events_file),
            temp_zumblezay_path: Some(temp_zumblezay_file),
            temp_cache_db_path: Some(temp_cache_db_file),
        }
    }
}

pub fn init_events_db_for_testing(conn: &mut Connection) -> Result<()> {
    // CREATE TABLE events(id PRIMARY KEY, type, camera_id, start REAL, end REAL);
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            camera_id TEXT NOT NULL,
            start REAL NOT NULL,
            end REAL NOT NULL
        )",
        [],
    )?;

    // CREATE TABLE backups(id REFERENCES events(id) ON DELETE CASCADE, remote, path, PRIMARY KEY (id, remote));
    conn.execute(
        "CREATE TABLE IF NOT EXISTS backups (
            id TEXT NOT NULL,
            remote TEXT NOT NULL,
            path TEXT NOT NULL,
            PRIMARY KEY (id, remote),
            FOREIGN KEY (id) REFERENCES events(id) ON DELETE CASCADE
        )",
        [],
    )?;

    Ok(())
}

// Create a config struct to hold AppState configuration
pub struct AppConfig {
    pub events_pool: Pool<SqliteConnectionManager>,
    pub zumblezay_pool: Pool<SqliteConnectionManager>,
    pub cache_pool: Pool<SqliteConnectionManager>,
    pub whisper_url: String,
    pub max_concurrent_tasks: usize,
    pub openai_api_key: Option<String>,
    pub openai_api_base: Option<String>,
    pub bedrock_region: Option<String>,
    pub runpod_api_key: Option<String>,
    pub signing_secret: Option<String>,
    pub transcription_service: String,
    pub enable_embedding_updates: bool,
    pub default_summary_model: String,
    pub investigation_model: String,
    pub video_path_original_prefix: String,
    pub video_path_replacement_prefix: String,
    pub timezone_str: Option<String>,
}

// Function to create AppState from parameters
pub fn create_app_state(config: AppConfig) -> Arc<AppState> {
    // Determine the timezone to use
    let timezone =
        time_util::get_local_timezone(config.timezone_str.as_deref());

    let openai_client = match maybe_create_openai_client(
        config.openai_api_key,
        config.openai_api_base,
    ) {
        Ok(client) => Some(client),
        Err(e) => {
            warn!("Failed to create OpenAI client: {}", e);
            None
        }
    };

    let signing_secret = config.signing_secret.unwrap_or_else(|| {
        info!("No secret provided, generating random one");
        let mut key = [0u8; 32]; // 256-bit secret
        OsRng.fill_bytes(&mut key);
        // URL-safe base64 encoded string
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key)
    });

    let bedrock_region = config.bedrock_region.clone();

    Arc::new(AppState {
        events_db: config.events_pool,
        zumblezay_db: config.zumblezay_pool,
        cache_db: config.cache_pool,
        whisper_url: config.whisper_url,
        last_processed_time: Arc::new(Mutex::new(0.0)),
        stats: ServiceStats::new(),
        active_tasks: Arc::new(Mutex::new(HashMap::new())),
        semaphore: Arc::new(tokio::sync::Semaphore::new(
            config.max_concurrent_tasks,
        )),
        openai_client,
        bedrock_client: Some(create_bedrock_client(config.bedrock_region)),
        bedrock_region,
        runpod_api_key: config.runpod_api_key,
        transcription_service: config.transcription_service,
        enable_embedding_updates: config.enable_embedding_updates,
        camera_name_cache: Arc::new(Mutex::new(HashMap::new())),
        storyboard_cache: Arc::new(RwLock::new(LruCache::new(
            NonZeroUsize::new(100).unwrap(),
        ))),
        default_summary_model: config.default_summary_model,
        investigation_model: config.investigation_model,
        video_path_original_prefix: config.video_path_original_prefix,
        video_path_replacement_prefix: config.video_path_replacement_prefix,
        timezone,
        in_progress_storyboards: Arc::new(Mutex::new(HashMap::new())),
        signing_secret,
        prompt_context_store: Arc::new(prompt_context::Store::new()),
        shutdown_token: CancellationToken::new(),
        backfill_tasks: Arc::new(Mutex::new(Vec::new())),
        temp_events_path: None,
        temp_zumblezay_path: None,
        temp_cache_db_path: None,
    })
}

fn zumblezay_migration_steps() -> Vec<M<'static>> {
    vec![
        M::up(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                event_start REAL NOT NULL,
                event_end REAL NOT NULL,
                event_type TEXT NOT NULL,
                camera_id TEXT NOT NULL,
                video_path TEXT
            );

            CREATE TABLE IF NOT EXISTS transcriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                transcription_type TEXT NOT NULL,
                url TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                raw_response TEXT NOT NULL,
                FOREIGN KEY(event_id) REFERENCES events(event_id),
                UNIQUE(event_id, transcription_type)
            );

            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,                   -- Date in YYYY-MM-DD format
                created_at INTEGER NOT NULL,          -- Unix timestamp of creation
                model TEXT NOT NULL,                  -- Model used for generation (e.g., 'anthropic-claude-haiku')
                prompt_name TEXT NOT NULL,            -- Name/identifier of the prompt used
                summary_type TEXT NOT NULL,           -- Type of summary (e.g., 'text', 'json', 'markdown')
                content TEXT NOT NULL,                -- The actual summary content
                duration_ms INTEGER NOT NULL,         -- Time taken to generate the summary
                UNIQUE(date, model, prompt_name, summary_type)
            );

            CREATE TABLE IF NOT EXISTS corrupted_files (
                event_id TEXT PRIMARY KEY,
                video_path TEXT NOT NULL,
                first_failure_at INTEGER NOT NULL,
                last_attempt_at INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 1,
                last_error TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'failed'
            );
            "#,
        ),
        M::up(
            r#"
            CREATE INDEX IF NOT EXISTS idx_events_camera_start_end
                ON events(camera_id, event_start, event_end);
            "#,
        ),
        M::up(
            r#"
            CREATE INDEX IF NOT EXISTS idx_events_start_event
                ON events(event_start DESC, event_id DESC);
            "#,
        ),
        M::up(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS transcript_search
                USING fts5(
                    event_id UNINDEXED,
                    content,
                    tokenize = 'unicode61'
                );

            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        ),
        // Repair step for older deployments where the migration ledger
        // may be ahead of actual schema objects.
        M::up(
            r#"
            CREATE INDEX IF NOT EXISTS idx_events_start_event
                ON events(event_start DESC, event_id DESC);
            "#,
        ),
        M::up(
            r#"
            CREATE TABLE IF NOT EXISTS bedrock_pricing (
                model_id TEXT PRIMARY KEY,
                input_cost_per_1k_usd REAL NOT NULL,
                output_cost_per_1k_usd REAL NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bedrock_spend_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                category TEXT NOT NULL,
                operation TEXT NOT NULL,
                model_id TEXT NOT NULL,
                request_id TEXT,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                estimated_cost_usd REAL NOT NULL
            );
            "#,
        ),
        M::up(
            r#"
            INSERT OR IGNORE INTO bedrock_pricing (
                model_id,
                input_cost_per_1k_usd,
                output_cost_per_1k_usd,
                updated_at
            ) VALUES
                (
                    'us.anthropic.claude-sonnet-4-5-20250929-v1:0',
                    0.003,
                    0.015,
                    strftime('%s', 'now')
                ),
                (
                    'anthropic.claude-sonnet-4-5-20250929-v1:0',
                    0.003,
                    0.015,
                    strftime('%s', 'now')
                );
            "#,
        ),
    ]
}

fn apply_zumblezay_migrations(conn: &mut Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;

    let migrations = Migrations::new(zumblezay_migration_steps());
    migrations.to_latest(conn)?;

    Ok(())
}

// Database initialization
#[instrument]
pub fn init_zumblezay_db(conn: &mut Connection) -> Result<()> {
    info!("Initializing zumblezay database");
    apply_zumblezay_migrations(conn)?;
    crate::hybrid_search::ensure_schema(conn)?;
    crate::transcripts::ensure_transcript_search_index(conn)?;
    Ok(())
}

pub fn init_cache_db(conn: &mut Connection) -> Result<()> {
    // Enable WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")?;

    // Create cache table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS storyboard_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            version TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            generation_duration_ms INTEGER NOT NULL,
            vtt TEXT NOT NULL,
            image BLOB NOT NULL,
            UNIQUE(event_id, version)
        )",
        [],
    )?;

    Ok(())
}

#[cfg(test)]
mod migration_tests {
    use super::{init_zumblezay_db, zumblezay_migration_steps};
    use anyhow::Result;
    use rusqlite::{Connection, OptionalExtension};
    use rusqlite_migration::Migrations;

    fn has_table(conn: &Connection, name: &str) -> Result<bool> {
        Ok(conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    fn has_index(conn: &Connection, name: &str) -> Result<bool> {
        Ok(conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    #[test]
    fn migrations_apply_on_fresh_database() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;

        init_zumblezay_db(&mut conn)?;

        assert!(has_table(&conn, "events")?);
        assert!(has_table(&conn, "transcriptions")?);
        assert!(has_table(&conn, "transcript_search")?);
        assert!(has_table(&conn, "daily_summaries")?);
        assert!(has_table(&conn, "corrupted_files")?);
        assert!(has_table(&conn, "metadata")?);
        assert!(has_table(&conn, "bedrock_pricing")?);
        assert!(has_table(&conn, "bedrock_spend_ledger")?);
        assert!(has_table(&conn, "embedding_models")?);
        assert!(has_table(&conn, "transcript_units")?);
        assert!(has_table(&conn, "unit_embeddings")?);
        assert!(has_table(&conn, "embedding_jobs")?);
        assert!(has_table(&conn, "search_config")?);
        assert!(has_index(&conn, "idx_events_camera_start_end")?);

        Ok(())
    }

    #[test]
    fn migrations_upgrade_existing_schema() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;

        // Apply only the first migration to simulate an older database.
        let mut partial_steps = zumblezay_migration_steps();
        let first_step = vec![partial_steps.remove(0)];
        Migrations::new(first_step).to_latest(&mut conn)?;

        assert!(!has_index(&conn, "idx_events_camera_start_end")?);

        init_zumblezay_db(&mut conn)?;

        assert!(has_index(&conn, "idx_events_camera_start_end")?);
        assert!(has_table(&conn, "transcriptions")?);
        assert!(has_table(&conn, "transcript_search")?);
        assert!(has_table(&conn, "metadata")?);
        assert!(has_table(&conn, "bedrock_pricing")?);
        assert!(has_table(&conn, "bedrock_spend_ledger")?);
        assert!(has_table(&conn, "embedding_models")?);
        assert!(has_table(&conn, "transcript_units")?);
        assert!(has_table(&conn, "unit_embeddings")?);
        assert!(has_table(&conn, "embedding_jobs")?);
        assert!(has_table(&conn, "search_config")?);

        Ok(())
    }
}

#[cfg(test)]
mod app_state_tests {
    use super::{create_app_state, AppConfig};
    use r2d2::Pool;
    use r2d2_sqlite::SqliteConnectionManager;
    use tempfile::NamedTempFile;

    fn temp_pool() -> (Pool<SqliteConnectionManager>, NamedTempFile) {
        let temp_file = NamedTempFile::new().expect("temp sqlite file");
        let manager = SqliteConnectionManager::file(
            temp_file.path().to_str().expect("temp path"),
        );
        let pool = Pool::new(manager).expect("pool");
        (pool, temp_file)
    }

    #[test]
    fn create_app_state_generates_signing_secret_when_missing() {
        let (events_pool, _events_file) = temp_pool();
        let (zumblezay_pool, _zumblezay_file) = temp_pool();
        let (cache_pool, _cache_file) = temp_pool();

        let config = AppConfig {
            events_pool,
            zumblezay_pool,
            cache_pool,
            whisper_url: "http://localhost:9000/asr".to_string(),
            max_concurrent_tasks: 2,
            openai_api_key: None,
            openai_api_base: None,
            bedrock_region: None,
            runpod_api_key: None,
            signing_secret: None,
            transcription_service: "whisper-local".to_string(),
            enable_embedding_updates: true,
            default_summary_model: "test-model".to_string(),
            investigation_model: "test-bedrock-model".to_string(),
            video_path_original_prefix: "/data".to_string(),
            video_path_replacement_prefix: "/tmp".to_string(),
            timezone_str: Some("America/Los_Angeles".to_string()),
        };

        let state = create_app_state(config);
        assert!(!state.signing_secret.is_empty());
        assert!(state.openai_client.is_none());
        assert_eq!(state.timezone, chrono_tz::America::Los_Angeles);
    }
}
