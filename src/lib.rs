use anyhow::Result;
use lru::LruCache;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tracing::info;
use tracing::instrument;

pub mod app;
pub mod prompts;
pub mod storyboard;
pub mod summary;
pub mod time_util;
pub mod transcription;
pub mod transcripts;

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
    pub openai_api_key: Option<String>,
    pub openai_api_base: Option<String>,
    pub runpod_api_key: Option<String>,
    pub transcription_service: String,
    pub camera_name_cache: Arc<Mutex<HashMap<String, String>>>,
    pub storyboard_cache: Arc<RwLock<LruCache<String, StoryboardData>>>,
    pub default_summary_model: String,
    pub video_path_original_prefix: String,
    pub video_path_replacement_prefix: String,
    pub timezone: chrono_tz::Tz,
    // Track in-progress storyboard generations
    pub in_progress_storyboards:
        Arc<Mutex<HashMap<String, Arc<tokio::sync::Notify>>>>,
    // Add fields to track temp files
    #[allow(dead_code)]
    temp_events_path: Option<tempfile::NamedTempFile>,
    #[allow(dead_code)]
    temp_zumblezay_path: Option<tempfile::NamedTempFile>,
    #[allow(dead_code)]
    temp_cache_db_path: Option<tempfile::NamedTempFile>,
}

impl AppState {
    // Create a new AppState for testing with minimal configuration
    pub fn new_for_testing() -> Self {
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
            openai_api_key: None,
            openai_api_base: Some("https://localhost:1234/v1".to_string()),
            runpod_api_key: None,
            transcription_service: "whisper-local".to_string(),
            camera_name_cache: Arc::new(Mutex::new(HashMap::new())),
            storyboard_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(10).unwrap(),
            ))),
            default_summary_model: "anthropic-claude-haiku".to_string(),
            video_path_original_prefix: "/data".to_string(),
            video_path_replacement_prefix: "/path/to/replacement".to_string(),
            // Use Adelaide timezone for tests because it is an odd timezone,
            // being offset from UTC by 9.5 hours
            timezone: chrono_tz::Australia::Adelaide,
            // Track in-progress storyboard generations
            in_progress_storyboards: Arc::new(Mutex::new(HashMap::new())),
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
    pub runpod_api_key: Option<String>,
    pub transcription_service: String,
    pub default_summary_model: String,
    pub video_path_original_prefix: String,
    pub video_path_replacement_prefix: String,
    pub timezone_str: Option<String>,
}

// Function to create AppState from parameters
pub fn create_app_state(config: AppConfig) -> Arc<AppState> {
    // Determine the timezone to use
    let timezone =
        time_util::get_local_timezone(config.timezone_str.as_deref());

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
        openai_api_key: config.openai_api_key,
        openai_api_base: config.openai_api_base,
        runpod_api_key: config.runpod_api_key,
        transcription_service: config.transcription_service,
        camera_name_cache: Arc::new(Mutex::new(HashMap::new())),
        storyboard_cache: Arc::new(RwLock::new(LruCache::new(
            NonZeroUsize::new(100).unwrap(),
        ))),
        default_summary_model: config.default_summary_model,
        video_path_original_prefix: config.video_path_original_prefix,
        video_path_replacement_prefix: config.video_path_replacement_prefix,
        timezone,
        in_progress_storyboards: Arc::new(Mutex::new(HashMap::new())),
        temp_events_path: None,
        temp_zumblezay_path: None,
        temp_cache_db_path: None,
    })
}

// Database initialization
#[instrument]
pub fn init_zumblezay_db(conn: &mut Connection) -> Result<()> {
    info!("Initializing zumblezay database");

    // Enable WAL mode
    conn.pragma_update(None, "journal_mode", "WAL")?;

    // Create events table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            event_start REAL NOT NULL,
            event_end REAL NOT NULL,
            event_type TEXT NOT NULL,
            camera_id TEXT NOT NULL,
            video_path TEXT
        )",
        [],
    )?;

    // Create transcriptions table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS transcriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            transcription_type TEXT NOT NULL,
            url TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            raw_response TEXT NOT NULL,
            FOREIGN KEY(event_id) REFERENCES events(event_id),
            UNIQUE(event_id, transcription_type)
        )",
        [],
    )?;

    // Create daily summaries table
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                   -- Date in YYYY-MM-DD format
            created_at INTEGER NOT NULL,          -- Unix timestamp of creation
            model TEXT NOT NULL,                  -- Model used for generation (e.g., 'anthropic-claude-haiku')
            prompt_name TEXT NOT NULL,            -- Name/identifier of the prompt used
            summary_type TEXT NOT NULL,           -- Type of summary (e.g., 'text', 'json', 'markdown')
            content TEXT NOT NULL,                -- The actual summary content
            duration_ms INTEGER NOT NULL,         -- Time taken to generate the summary
            UNIQUE(date, model, prompt_name, summary_type)
        )",
        [],
    )?;

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
