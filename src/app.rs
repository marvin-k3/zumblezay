use super::storyboard;
use crate::process_events;
use crate::prompt_context;
use crate::prompt_context::PromptContextError;
use crate::prompts::{SUMMARY_USER_PROMPT, SUMMARY_USER_PROMPT_JSON};
use crate::summary;
use crate::time_util;
use crate::transcripts;
use crate::AppState;
use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Json, Router,
};
use chrono::Utc;
use clap::Parser;
use fs2::FileExt as Fs2FileExt;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::SeekFrom;
use std::process::Stdio;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tera::{Context as TeraContext, Tera};
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::process::Command;
use tokio::time;
use tokio_util::io::ReaderStream;
use tower_http::compression::predicate::{
    NotForContentType, Predicate, SizeAbove,
};
use tower_http::compression::CompressionLayer;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, instrument, warn};
use tracing_subscriber::{prelude::*, Registry};
use tracing_tree::HierarchicalLayer;

// Add build-time information
pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    last_processed_time: f64,
    active_tasks: HashMap<String, String>,
    stats: StatusStats,
}

#[derive(Debug, Serialize)]
struct StatusStats {
    processed_count: u64,
    error_count: u64,
    total_processing_time_ms: u64,
    average_processing_time_ms: f64,
}

#[derive(Debug)]
struct DbAttacher {
    events_db: String,
}

impl r2d2::CustomizeConnection<Connection, rusqlite::Error> for DbAttacher {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), rusqlite::Error> {
        if self.events_db.is_empty() {
            return Err(rusqlite::Error::InvalidPath(
                std::path::PathBuf::from("events_db path is empty"),
            ));
        }
        conn.execute("ATTACH DATABASE ? AS ubnt", params![&self.events_db])?;
        Ok(())
    }
}

#[instrument(skip(state), err)]
pub async fn cache_camera_names(state: &AppState) -> Result<()> {
    let conn = state.zumblezay_db.get()?;

    let mut stmt = conn.prepare(
        "SELECT DISTINCT 
    camera_id,
    substr(video_path, instr(video_path, '/data/') + 6,
        instr(substr(video_path, instr(video_path, '/data/') + 6), '/') - 1) as camera_name
        FROM events",
    )?;

    let camera_names = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?, // camera_id
            row.get::<_, String>(1)?, // camera_name
        ))
    })?;

    let camera_names: Result<HashMap<_, _>, _> = camera_names.collect();
    let camera_names = camera_names
        .map_err(|e| anyhow::anyhow!("Failed to fetch camera names: {}", e))?;
    info!("Camera names: {:?}", camera_names);
    let mut camera_name_cache = state.camera_name_cache.lock().await;
    camera_name_cache.clear();
    camera_name_cache.extend(camera_names);
    Ok(())
}

// Health check endpoint
#[instrument]
pub async fn health_check() -> &'static str {
    debug!("Health check requested");
    "OK"
}

#[instrument(level = "debug")]
fn check_file_is_writable(path: &str, file_type: &str) -> Result<()> {
    let file_path = std::path::Path::new(path);
    if let Some(parent) = file_path.parent() {
        if !parent.exists() {
            return Err(anyhow::anyhow!(
                "Directory for {} at '{}' does not exist. Please create it manually.",
                file_type,
                parent.display()
            ));
        }
    }
    let file_exists = file_path.exists();
    let file = if file_exists {
        std::fs::OpenOptions::new().write(true).open(file_path)
    } else {
        std::fs::OpenOptions::new()
            .write(true)
            .create(false)
            .truncate(false)
            .open(file_path)
    };
    if let Err(e) = file {
        return Err(anyhow::anyhow!(
            "Cannot write to {} at '{}': {}. Please check file permissions.",
            file_type,
            path,
            e
        ));
    }

    Ok(())
}

// Add this new helper function
fn get_build_info() -> String {
    fn clean(value: Option<String>) -> Option<String> {
        value
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty() && v != "unknown")
    }

    let clean_env = |key: &str| clean(env::var(key).ok());
    let clean_opt = |value: Option<&str>| clean(value.map(|v| v.to_string()));

    let mut parts = Vec::new();
    parts.push(format!("Version {}", built_info::PKG_VERSION));

    if let Some(tag) = clean_env("APP_BUILD_TAG") {
        parts.push(format!("Image {}", tag));
    }

    let branch =
        clean_env("APP_BUILD_BRANCH").or_else(|| {
            clean(built_info::GIT_HEAD_REF.map(|r| {
                r.strip_prefix("refs/heads/").unwrap_or(r).to_string()
            }))
        });
    if let Some(branch) = branch {
        parts.push(format!("Branch {}", branch));
    }

    let env_commit = clean_env("APP_BUILD_COMMIT");
    let full_commit = env_commit
        .clone()
        .or_else(|| clean(built_info::GIT_COMMIT_HASH.map(|s| s.to_string())));

    if let Some(commit) = full_commit {
        let short: String = commit.chars().take(12).collect();
        if commit.len() > short.len() {
            parts.push(format!("Commit {}… (full: {})", short, commit));
        } else {
            parts.push(format!("Commit {}", commit));
        }
    } else if let Some(commit) =
        clean(built_info::GIT_COMMIT_HASH_SHORT.map(|s| s.to_string()))
    {
        parts.push(format!("Commit {}", commit));
    }

    if let Some(dirty) = built_info::GIT_DIRTY {
        if dirty {
            parts.push("workspace dirty".to_string());
        }
    }

    if let Some(time) = clean(Some(built_info::BUILT_TIME_UTC.to_string())) {
        parts.push(format!("Built {}", time));
    }

    if let Some(profile) = clean(Some(built_info::PROFILE.to_string())) {
        parts.push(format!("Profile {}", profile));
    }

    if let Some(target) = clean(Some(built_info::TARGET.to_string())) {
        parts.push(format!("Target {}", target));
    }

    if let Some(ci_platform) = clean_opt(built_info::CI_PLATFORM) {
        parts.push(format!("CI {}", ci_platform));
    }

    if let Some(rustc_version) =
        clean(Some(built_info::RUSTC_VERSION.to_string()))
    {
        parts.push(format!("Rustc {}", rustc_version));
    }

    parts.join(" • ")
}

static TEMPLATES: OnceLock<Tera> = OnceLock::new();

fn init_templates() -> Tera {
    let mut tera = Tera::default();
    tera.add_raw_template("base.html", include_str!("templates/base.html"))
        .unwrap();
    tera.add_raw_template("status.html", include_str!("templates/status.html"))
        .unwrap();
    tera.add_raw_template("events.html", include_str!("templates/events.html"))
        .unwrap();
    tera.add_raw_template(
        "summary.html",
        include_str!("templates/summary.html"),
    )
    .unwrap();
    tera.add_raw_template(
        "transcript.html",
        include_str!("templates/transcript.html"),
    )
    .unwrap();
    tera
}

pub fn ensure_templates() {
    TEMPLATES.get_or_init(init_templates);
}

#[axum::debug_handler]
async fn get_status_page(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut context = TeraContext::new();
    context.insert("build_info", &get_build_info());
    context.insert("request_path", &"/status");

    let timezone_str = state.timezone.to_string().replace("::", "/");
    context.insert("timezone", &timezone_str);

    let rendered = TEMPLATES
        .get()
        .unwrap()
        .render("status.html", &context)
        .unwrap_or_else(|e| format!("Template error: {}", e));

    Html(rendered)
}

#[axum::debug_handler]
async fn get_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let last_processed_time = *state.last_processed_time.lock().await;
    let active_tasks = state.active_tasks.lock().await.clone();

    let processed_count = state.stats.processed_count.load(Ordering::Relaxed);
    let total_time =
        state.stats.total_processing_time_ms.load(Ordering::Relaxed);
    let average_time = if processed_count > 0 {
        total_time as f64 / processed_count as f64
    } else {
        0.0
    };

    let status = StatusResponse {
        last_processed_time,
        active_tasks,
        stats: StatusStats {
            processed_count,
            error_count: state.stats.error_count.load(Ordering::Relaxed),
            total_processing_time_ms: total_time,
            average_processing_time_ms: average_time,
        },
    };

    Json(status).into_response()
}

// Add these new endpoint handlers
#[derive(Debug, Deserialize)]
struct EventFilters {
    date: Option<String>,
    date_start: Option<String>,
    date_end: Option<String>,
    camera_id: Option<String>,
    time_start: Option<String>,
    time_end: Option<String>,
    q: Option<String>,
    cursor_start: Option<f64>,
    cursor_event_id: Option<String>,
    limit: Option<usize>,
}

fn prepare_search_match(term: &str) -> String {
    let tokens: Vec<String> = term
        .split_whitespace()
        .filter(|token| !token.is_empty())
        .map(|token| {
            let escaped = token.replace('"', "\"\"");
            format!("\"{}\"", escaped)
        })
        .collect();

    if tokens.is_empty() {
        let escaped = term.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        tokens.join(" AND ")
    }
}

#[axum::debug_handler]
async fn get_completed_events(
    State(state): State<Arc<AppState>>,
    Query(filters): Query<EventFilters>,
) -> Result<Json<EventPage>, (StatusCode, String)> {
    let camera_names: HashMap<String, String> =
        state.camera_name_cache.lock().await.clone();
    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (date_start, date_end) =
        if filters.date_start.is_some() || filters.date_end.is_some() {
            let start = filters
                .date_start
                .clone()
                .or_else(|| filters.date_end.clone())
                .ok_or((
                    StatusCode::BAD_REQUEST,
                    "Date start or end is required".to_string(),
                ))?;
            let end = filters.date_end.clone().unwrap_or_else(|| start.clone());
            (start, end)
        } else if let Some(date) = filters.date.clone() {
            (date.clone(), date)
        } else {
            let today = Utc::now()
                .with_timezone(&state.timezone)
                .date_naive()
                .format("%Y-%m-%d")
                .to_string();
            (today.clone(), today)
        };

    let transcription_type = state.transcription_service.clone();

    let search_term = filters
        .q
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(prepare_search_match);

    let (start_ts, end_ts) =
        time_util::parse_local_date_range_to_utc_range_with_time(
            &date_start,
            &date_end,
            &filters.time_start,
            &filters.time_end,
            state.timezone,
        )
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let mut query: String = String::from("");
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    let has_search = search_term.is_some();
    let limit = filters.limit.unwrap_or(200).clamp(1, 500);
    let query_limit = limit.saturating_add(1);
    if has_search {
        query.push_str(
            "WITH matching AS (
                SELECT ts.rowid AS ts_rowid, ts.event_id
                FROM transcript_search ts
                WHERE ts MATCH ?
             ),
             filtered AS (
                SELECT e1.event_id,
                       e1.camera_id,
                       e1.event_start,
                       e1.event_end,
                       m.ts_rowid
                FROM events e1
                JOIN matching m ON m.event_id = e1.event_id
                WHERE e1.event_start BETWEEN ? AND ?",
        );

        if let Some(query_string) = search_term.as_ref() {
            params.push(Box::new(query_string.clone()));
        }
        params.push(Box::new(start_ts));
        params.push(Box::new(end_ts));

        if let Some(camera) = filters.camera_id.clone() {
            query.push_str(" AND e1.camera_id = ?");
            params.push(Box::new(camera));
        }

        let cursor_start = filters.cursor_start;
        let cursor_event_id = filters.cursor_event_id.clone();
        if cursor_start.is_some() ^ cursor_event_id.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "cursor_start and cursor_event_id must be provided together"
                    .to_string(),
            ));
        }

        if let (Some(cursor_start), Some(cursor_event_id)) =
            (cursor_start, cursor_event_id)
        {
            query.push_str(
                " AND (e1.event_start < ? OR (e1.event_start = ? AND e1.event_id < ?))",
            );
            params.push(Box::new(cursor_start));
            params.push(Box::new(cursor_start));
            params.push(Box::new(cursor_event_id));
        }

        query.push_str(
            " ORDER BY e1.event_start DESC, e1.event_id DESC LIMIT ?",
        );
        params.push(Box::new(query_limit as i64));

        query.push_str(
            ")
            SELECT f.event_id,
                   f.camera_id,
                   f.event_start,
                   f.event_end,
                   EXISTS(
                      SELECT 1
                      FROM transcriptions t
                      WHERE t.event_id = f.event_id
                        AND t.transcription_type = ?
                   ) AS has_transcript,
                   snippet(
                      ts,
                      1,
                      '[[H]]',
                      '[[/H]]',
                      '…',
                      12
                   ) AS snippet
            FROM filtered f
            JOIN transcript_search ts ON ts.rowid = f.ts_rowid
            ORDER BY f.event_start DESC, f.event_id DESC",
        );
        params.push(Box::new(transcription_type));
    } else {
        query.push_str(
            "
          SELECT e1.event_id,
                 e1.camera_id,
                 e1.event_start,
                 e1.event_end,
                 EXISTS(
                    SELECT 1
                    FROM transcriptions t
                    WHERE t.event_id = e1.event_id
                      AND t.transcription_type = ?
                 ) AS has_transcript,
                 NULL AS snippet
          FROM events e1
          WHERE e1.event_start BETWEEN ? AND ?
            AND NOT EXISTS (
              SELECT 1
              FROM events e2
              WHERE e2.camera_id = e1.camera_id
                AND e2.event_start <= e1.event_start
                AND e2.event_end >= e1.event_end
                AND (e2.event_start < e1.event_start OR e2.event_end > e1.event_end)
                AND e2.event_start BETWEEN ? AND ?
            )",
        );
        params.push(Box::new(transcription_type));
        params.push(Box::new(start_ts));
        params.push(Box::new(end_ts));
        params.push(Box::new(start_ts));
        params.push(Box::new(end_ts));
    }
    if !has_search {
        if let Some(camera) = filters.camera_id {
            query.push_str(" AND (e1.camera_id = ?)");
            params.push(Box::new(camera));
        }
    }
    if !has_search {
        let cursor_start = filters.cursor_start;
        let cursor_event_id = filters.cursor_event_id.clone();
        if cursor_start.is_some() ^ cursor_event_id.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "cursor_start and cursor_event_id must be provided together"
                    .to_string(),
            ));
        }
        if let (Some(cursor_start), Some(cursor_event_id)) =
            (cursor_start, cursor_event_id)
        {
            query.push_str(
                " AND (e1.event_start < ? OR (e1.event_start = ? AND e1.event_id < ?))",
            );
            params.push(Box::new(cursor_start));
            params.push(Box::new(cursor_start));
            params.push(Box::new(cursor_event_id));
        }
    }
    if !has_search {
        query.push_str(" ORDER BY e1.event_start DESC, e1.event_id DESC");

        query.push_str(" LIMIT ?");
        params.push(Box::new(query_limit as i64));
    }

    let mut stmt = conn
        .prepare(&query)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p.as_ref()).collect();

    let transcripts = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok(TranscriptListItem {
                event_id: row.get(0)?,
                camera_id: row.get(1)?,
                camera_name: camera_names
                    .get(&row.get::<_, String>(1)?)
                    .cloned(),
                event_start: row.get(2)?,
                event_end: row.get(3)?,
                has_transcript: row.get(4)?,
                snippet: if has_search { row.get(5)? } else { None },
            })
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let transcripts: Result<Vec<_>, _> = transcripts.collect();
    let mut transcripts = transcripts
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut next_cursor = None;
    if transcripts.len() > limit {
        transcripts.pop();
        if let Some(last) = transcripts.last() {
            next_cursor = Some(EventCursor {
                event_start: last.event_start,
                event_id: last.event_id.clone(),
            });
        }
    }

    Ok(Json(EventPage {
        events: transcripts,
        next_cursor,
    }))
}

#[axum::debug_handler]
async fn get_event(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let camera_names: HashMap<String, String> =
        state.camera_name_cache.lock().await.clone();
    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // First get the event details
    let event = conn
        .query_row(
            "SELECT 
            event_id, created_at, event_start, event_end,
            event_type, camera_id, video_path
         FROM events 
         WHERE event_id = ?",
            params![event_id],
            |row| {
                let camera_id: String = row.get(5)?;
                Ok(json!({
                    "event_id": row.get::<_, String>(0)?,
                    "created_at": row.get::<_, i64>(1)?,
                    "event_start": row.get::<_, f64>(2)?,
                    "event_end": row.get::<_, f64>(3)?,
                    "event_type": row.get::<_, String>(4)?,
                    "camera_id": camera_id.clone(),
                    "camera_name": camera_names.get(&camera_id).cloned(),
                    "video_path": row.get::<_, String>(6)?,
                    "has_transcript": false
                }))
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => (
                StatusCode::NOT_FOUND,
                format!("Event {} not found", event_id),
            ),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        })?;

    // Then try to get transcription details if they exist
    let transcript = conn
        .query_row(
            "SELECT 
            transcription_type, duration_ms, raw_response, url
         FROM transcriptions 
         WHERE event_id = ? AND transcription_type = ?
         ORDER BY created_at DESC
         LIMIT 1",
            params![event_id, state.transcription_service.as_str()],
            |row| {
                Ok(json!({
                    "transcription_type": row.get::<_, String>(0)?,
                    "transcription_duration_ms": row.get::<_, i64>(1)?,
                    "raw_response": row.get::<_, String>(2)?,
                    "url": row.get::<_, String>(3)?
                }))
            },
        )
        .ok();

    // Combine event and transcript data
    let mut response = event.as_object().unwrap().clone();
    if let Some(transcript_data) = transcript {
        response.insert("has_transcript".to_string(), json!(true));
        response.extend(transcript_data.as_object().unwrap().clone());
    }

    Ok(Json(Value::Object(response)))
}

#[axum::debug_handler]
async fn stream_video(
    Path(event_id): Path<String>,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Get video path from database in a separate scope
    let video_path: String = {
        let conn = state
            .zumblezay_db
            .get()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        conn.query_row(
            "SELECT video_path FROM events WHERE event_id = ?",
            params![event_id],
            |row| row.get(0),
        )
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?
    };

    // Validate video path
    if !video_path.starts_with(&state.video_path_original_prefix) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Invalid video path format: {}", video_path),
        ));
    }

    let modified_path = video_path.replace(
        &state.video_path_original_prefix,
        &state.video_path_replacement_prefix,
    );
    debug!("Attempting to open video file at: {}", modified_path);

    const CHUNK_SIZE: u64 = 1024 * 1024; // 1MB chunks, changed to u64

    let file = File::open(&modified_path).map_err(|e| {
        error!("Failed to open video at {}: {}", modified_path, e);
        (
            StatusCode::NOT_FOUND,
            format!("Video file not found: {}", e),
        )
    })?;

    let file_size = file
        .metadata()
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get file size: {}", e),
            )
        })?
        .len();

    // Parse range header if present
    let (start, end) = if let Some(range) = headers.get(header::RANGE) {
        let range_str = range.to_str().map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Invalid range header: {}", e),
            )
        })?;

        // Parse range header of the form "bytes=start-end"
        let captures = regex::Regex::new(r"bytes=(\d*)-(\d*)")
            .unwrap()
            .captures(range_str)
            .ok_or_else(|| {
                (StatusCode::BAD_REQUEST, "Invalid range format".to_string())
            })?;

        let start = captures
            .get(1)
            .and_then(|m| m.as_str().parse::<u64>().ok())
            .unwrap_or(0);
        let end = captures
            .get(2)
            .and_then(|m| m.as_str().parse::<u64>().ok())
            .unwrap_or(file_size - 1)
            .min(file_size - 1);

        if start > end {
            return Err((
                StatusCode::RANGE_NOT_SATISFIABLE,
                "Invalid range".to_string(),
            ));
        }

        (start, end)
    } else {
        (0_u64, (CHUNK_SIZE - 1).min(file_size - 1))
    };

    let content_length = end - start + 1;

    // Calculate next chunk range for prefetch hint
    let next_start = end + 1;
    let next_end = (next_start + CHUNK_SIZE - 1).min(file_size - 1);
    let range_hint = if next_start < file_size {
        format!("bytes={}-{}", next_start, next_end)
    } else {
        String::new()
    };

    // Seek to the start position
    let mut file = tokio::fs::File::from_std(file);
    if start > 0 {
        if let Err(e) = file.seek(SeekFrom::Start(start)).await {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to seek: {}", e),
            ));
        }
    }

    // Create buffered reader with optimal buffer size
    let buf_reader = BufReader::with_capacity(CHUNK_SIZE as usize, file);
    let limited_reader =
        tokio::io::AsyncReadExt::take(buf_reader, content_length);

    // Use larger buffer size for streaming
    let stream =
        ReaderStream::with_capacity(limited_reader, CHUNK_SIZE as usize);

    let mut response_builder = axum::response::Response::builder()
        .status(if start == 0 && end == file_size - 1 {
            StatusCode::OK
        } else {
            StatusCode::PARTIAL_CONTENT
        })
        .header(header::CONTENT_TYPE, "video/mp4")
        .header(header::CONTENT_LENGTH, content_length)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size),
        );

    // Add caching headers
    response_builder = response_builder
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .header(header::ETAG, format!("\"{}\"", file_size));

    // Add range hint for next chunk if available
    if !range_hint.is_empty() {
        response_builder = response_builder.header("Range-Hint", range_hint);
    }

    let response = response_builder
        .body(axum::body::Body::from_stream(stream))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(response)
}

// Add this new struct
#[derive(Debug, Serialize)]
struct TranscriptListItem {
    event_id: String,
    camera_id: String,
    camera_name: Option<String>,
    event_start: f64,
    event_end: f64,
    has_transcript: bool,
    snippet: Option<String>,
}

#[derive(Debug, Serialize)]
struct EventCursor {
    event_start: f64,
    event_id: String,
}

#[derive(Debug, Serialize)]
struct EventPage {
    events: Vec<TranscriptListItem>,
    next_cursor: Option<EventCursor>,
}

// Update the events page handlers
#[axum::debug_handler]
async fn events_latest_page() -> Html<String> {
    let templates = TEMPLATES.get().unwrap();
    let mut context = TeraContext::new();
    context.insert("build_info", &get_build_info());
    context.insert("request_path", &"/events/latest");
    context.insert("page_title", &"Latest Events");
    context.insert("mode", &"latest");

    let rendered = templates
        .render("events.html", &context)
        .unwrap_or_else(|e| format!("Template error: {}", e));

    Html(rendered)
}

#[axum::debug_handler]
async fn events_search_page() -> Html<String> {
    let templates = TEMPLATES.get().unwrap();
    let mut context = TeraContext::new();
    context.insert("build_info", &get_build_info());
    context.insert("request_path", &"/events/search");
    context.insert("page_title", &"Search Events");
    context.insert("mode", &"search");

    let rendered = templates
        .render("events.html", &context)
        .unwrap_or_else(|e| format!("Template error: {}", e));

    Html(rendered)
}

#[axum::debug_handler]
async fn events_redirect() -> impl IntoResponse {
    axum::response::Redirect::to("/events/latest")
}

fn create_app_lock() -> Result<File> {
    let lock_file = File::create("/tmp/whisper_asr.lock")?;
    lock_file
        .try_lock_exclusive()
        .map_err(|_| anyhow::anyhow!("Another instance is already running"))?;
    Ok(lock_file)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// URL of the Whisper ASR API endpoint
    #[arg(
        long,
        env = "WHISPER_URL",
        default_value = "http://localhost:9000/asr"
    )]
    whisper_url: String,

    /// Path to the events database
    #[arg(long)]
    events_db: String,

    /// Path to the transcripts database
    #[arg(long, default_value = "data/zumblezay.db")]
    zumblezay_db: String,

    /// Path to the cache database
    #[arg(long, default_value = "data/cache.db")]
    cache_db: String,

    /// Maximum concurrent transcription tasks
    #[arg(long, default_value_t = 3)]
    max_concurrent_tasks: usize,

    // Should create lock file to prevent multiple instances from running
    #[arg(long, default_value_t = true)]
    create_lock_file: bool,

    /// Port to listen on
    #[arg(long, default_value_t = 3010)]
    port: u16,

    /// Host address to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Enable background transcription task (enabled by default)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_transcription: bool,

    // Transcription service
    #[arg(long, default_value = "whisper-local")]
    transcription_service: String,

    /// OpenAI API key for summary generation
    #[arg(long, env = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,

    /// OpenAI API base URL
    #[arg(long, env = "OPENAI_API_BASE")]
    openai_api_base: Option<String>,

    /// RunPod API key for transcription
    #[arg(long, env = "RUNPOD_API_KEY")]
    runpod_api_key: Option<String>,

    /// Signing secret for prompt context
    #[arg(long, env = "SIGNING_SECRET")]
    signing_secret: Option<String>,

    /// Enable event sync task (enabled by default)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_sync: bool,

    /// Interval in seconds between event sync attempts
    #[arg(long, default_value_t = 10)]
    sync_interval: u64,

    /// Default model to use for summary generation
    #[arg(long, default_value = "anthropic-claude-haiku")]
    default_summary_model: String,

    /// Original path prefix to replace in video paths
    #[arg(long, default_value = "/data")]
    video_path_original_prefix: String,

    /// Replacement path prefix for video paths
    #[arg(long, default_value = "/path/to/replacement")]
    video_path_replacement_prefix: String,

    /// Timezone to use for date/time conversions (e.g., "Australia/Adelaide")
    /// If not specified, the system timezone will be used
    #[arg(long)]
    timezone: Option<String>,
}

// Update get_transcripts_csv to use the new function
#[axum::debug_handler]
async fn get_transcripts_csv(
    State(state): State<Arc<AppState>>,
    Path(date): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let (csv_content, _event_ids) =
        transcripts::get_formatted_transcripts_for_date(&state, &date)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Create the response with appropriate headers
    let headers = [
        (header::CONTENT_TYPE, "text/csv"),
        (
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"transcripts.csv\"",
        ),
    ];

    Ok((headers, csv_content))
}

// Get a summary of transcripts for a specific date
#[derive(Debug, Deserialize)]
struct SummaryParams {
    model: Option<String>,
}

#[axum::debug_handler]
async fn get_transcripts_summary(
    State(state): State<Arc<AppState>>,
    Path(date): Path<String>,
    Query(params): Query<SummaryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Determine which model to use
    let model = params
        .model
        .as_deref()
        .unwrap_or(&state.default_summary_model)
        .to_string();

    // Check if we already have a cached summary
    if let Ok(Some(cached_summary)) = summary::get_daily_summary(
        &state,
        &date,
        Some(&model),
        Some("default"),
        "markdown",
    )
    .await
    {
        info!(
            "Using cached summary for date {} with model {}",
            date, model
        );
        // Return the cached summary as HTML
        let html_summary = markdown::to_html(&cached_summary.content);
        return Ok(([(header::CONTENT_TYPE, "text/html")], html_summary));
    }

    // If no cached summary, generate a new one
    info!(
        "No cached summary found, generating new summary for date {} with model {}",
        date, model
    );

    // Generate the summary
    let summary = summary::generate_summary(
        &state,
        &date,
        &model,
        "markdown",
        SUMMARY_USER_PROMPT,
    )
    .await
    .map_err(|e| {
        error!("Failed to generate summary: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    // Return the summary as HTML
    let html_summary = markdown::to_html(&summary);
    Ok(([(header::CONTENT_TYPE, "text/html")], html_summary))
}

#[axum::debug_handler]
async fn get_transcripts_summary_json(
    State(state): State<Arc<AppState>>,
    Path(date): Path<String>,
    Query(params): Query<SummaryParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Determine which model to use
    let model = params
        .model
        .as_deref()
        .unwrap_or(&state.default_summary_model)
        .to_string();

    // Check if we already have a cached JSON summary
    if let Ok(Some(cached_summary)) = summary::get_daily_summary(
        &state,
        &date,
        Some(&model),
        Some("default"),
        "json",
    )
    .await
    {
        info!(
            "Using cached JSON summary for date {} with model {}",
            date, model
        );
        return Ok((
            [(header::CONTENT_TYPE, "application/json")],
            cached_summary.content,
        ));
    }

    // If no cached summary, generate a new one
    info!(
        "No cached JSON summary found, generating new summary for date {} with model {}",
        date, model
    );

    // Generate the JSON summary
    let json_summary = summary::generate_summary(
        &state,
        &date,
        &model,
        "json",
        SUMMARY_USER_PROMPT_JSON,
    )
    .await
    .map_err(|e| {
        error!("Failed to generate summary: {}", e);
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    // Return the JSON summary
    Ok(([(header::CONTENT_TYPE, "application/json")], json_summary))
}

#[axum::debug_handler]
async fn get_vtt_captions(
    State(state): State<Arc<AppState>>,
    Path(event_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let raw_response: String = conn
        .query_row(
            "SELECT raw_response FROM transcriptions WHERE event_id = ? AND transcription_type = ?",
            params![event_id, state.transcription_service.as_str()],
            |row| row.get(0),
        )
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    // Parse the Whisper JSON response
    let whisper_response: Value =
        serde_json::from_str(&raw_response).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Invalid JSON: {}", e),
            )
        })?;

    // Generate VTT content
    let mut vtt_content = String::from("WEBVTT\n\n");

    if let Some(segments) = whisper_response["segments"].as_array() {
        for segment in segments {
            let start = segment["start"].as_f64().unwrap_or(0.0);
            let end = segment["end"].as_f64().unwrap_or(0.0);
            let text = segment["text"].as_str().unwrap_or("").trim();

            if !text.is_empty() {
                // Convert seconds to HH:MM:SS.mmm format
                let format_time = |secs: f64| {
                    let hours = (secs as i64) / 3600;
                    let minutes = ((secs as i64) % 3600) / 60;
                    let seconds = secs % 60.0;
                    format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds)
                };

                vtt_content.push_str(&format!(
                    "{} --> {}\n{}\n\n",
                    format_time(start),
                    format_time(end),
                    text
                ));
            }
        }
    }

    Ok(([(header::CONTENT_TYPE, "text/vtt")], vtt_content))
}

#[axum::debug_handler]
async fn get_storyboard_image(
    Path(event_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let storyboard_data =
        storyboard::get_or_create_storyboard(&state, &event_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to get storyboard: {}", e),
                )
            })?;

    Ok((
        [(header::CONTENT_TYPE, "image/jpeg")],
        storyboard_data.image,
    ))
}

#[axum::debug_handler]
async fn get_storyboard_vtt(
    Path(event_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let storyboard_data =
        storyboard::get_or_create_storyboard(&state, &event_id)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to get storyboard VTT: {}", e),
                )
            })?;

    Ok(([(header::CONTENT_TYPE, "text/vtt")], storyboard_data.vtt))
}

#[axum::debug_handler]
async fn stream_audio(
    Path(event_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Get video path from database
    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let video_path: String = conn
        .query_row(
            "SELECT video_path FROM events WHERE event_id = ?",
            params![event_id],
            |row| row.get(0),
        )
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let modified_path = video_path.replace(
        &state.video_path_original_prefix,
        &state.video_path_replacement_prefix,
    );
    debug!("Extracting audio from video file at: {}", modified_path);

    // Set up FFmpeg command with more explicit audio conversion parameters
    let mut command = Command::new("ffmpeg");
    let command = command
        .arg("-i")
        .arg(&modified_path)
        .arg("-vn") // Disable video
        .arg("-acodec") // Force audio codec
        .arg("pcm_s16le") // Use 16-bit PCM
        .arg("-ac") // Set audio channels
        .arg("1") // Mono
        .arg("-ar") // Set sample rate
        .arg("16000") // 16kHz
        .arg("-af") // Audio filters
        .arg("aresample=async=1") // Handle async audio samples
        .arg("-f") // Force format
        .arg("wav") // WAV output
        .arg("-bitexact") // Ensure deterministic output
        .arg("-y") // Overwrite output
        .arg("pipe:1") // Output to stdout
        .stdout(Stdio::piped())
        .stderr(Stdio::piped()); // Capture stderr for error handling

    debug!("Running FFmpeg command: {:?}", command);

    // Spawn FFmpeg process
    let mut child = command.spawn().map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to spawn FFmpeg: {}", e),
        )
    })?;

    // Get stdout and stderr handles
    let stdout = child.stdout.take().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to capture FFmpeg stdout".to_string(),
        )
    })?;

    let stderr = child.stderr.take().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to capture FFmpeg stderr".to_string(),
        )
    })?;

    // Monitor stderr in background task
    let stderr_handle = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(stderr);
        let mut line = String::new();
        while let Ok(n) = reader.read_line(&mut line).await {
            if n == 0 {
                break;
            }
            if line.contains("Error") || line.contains("error") {
                error!("FFmpeg error: {}", line.trim());
            } else {
                debug!("FFmpeg output: {}", line.trim());
            }
            line.clear();
        }
    });

    // Create buffered reader with optimal buffer size
    let reader = tokio::io::BufReader::with_capacity(8192, stdout);

    // Create stream from reader
    let stream = ReaderStream::new(reader);

    // Spawn task to wait for process completion and handle stderr
    tokio::spawn(async move {
        let status = child.wait().await;
        stderr_handle.abort(); // Stop stderr monitoring
        if let Err(e) = status {
            error!("FFmpeg process error: {}", e);
        }
    });

    // Build and return response
    let response = axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "audio/wav")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}.wav\"", event_id),
        )
        .body(axum::body::Body::from_stream(stream))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(response)
}

// Add this new helper function
#[instrument(skip(state))]
async fn sync_missing_events(state: &AppState) -> Result<()> {
    let conn = state.zumblezay_db.get()?;

    info!("Syncing missing events from events database");

    // Insert missing events directly using ATTACH database
    let rows_affected = conn.execute(
        "INSERT OR IGNORE INTO events (
            event_id, created_at, event_start, event_end,
            event_type, camera_id, video_path
        )
        SELECT 
            ue.id,
            unixepoch(), -- current timestamp
            ue.start,
            ue.end,
            ue.type,
            ue.camera_id,
            b.path
        FROM ubnt.events ue
        JOIN ubnt.backups b ON ue.id = b.id
        LEFT JOIN events e ON ue.id = e.event_id
        WHERE e.event_id IS NULL",
        [],
    )?;

    info!("Synced {} missing events", rows_affected);

    Ok(())
}

#[instrument(skip(state))]
async fn sync_events(
    state: Arc<AppState>,
    interval: Duration,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    info!(
        "Starting event sync loop with {}s interval",
        interval.as_secs()
    );

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutting down sync task");
                break;
            }
            _ = async {
                if let Err(e) = sync_missing_events(&state).await {
                    error!("Failed to sync missing events: {}", e);
                }
                time::sleep(interval).await;
            } => {}
        }
    }

    info!("Sync task terminated");
}

#[axum::debug_handler]
async fn list_daily_summaries(
    State(state): State<Arc<AppState>>,
    Path(date): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut stmt = conn
        .prepare(
            "SELECT id, date, created_at, model, prompt_name, summary_type, duration_ms
             FROM daily_summaries
             WHERE date = ?
             ORDER BY created_at DESC",
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rows = stmt
        .query_map(params![date], |row| {
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "date": row.get::<_, String>(1)?,
                "created_at": row.get::<_, i64>(2)?,
                "model": row.get::<_, String>(3)?,
                "prompt_name": row.get::<_, String>(4)?,
                "summary_type": row.get::<_, String>(5)?,
                "duration_ms": row.get::<_, i64>(6)?,
                "url": format!("/api/transcripts/summary/{}/type/{}/model/{}/prompt/{}",
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?
                )
            }))
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let summaries: Result<Vec<_>, _> = rows.collect();
    let summaries = summaries
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(json!({
        "date": date,
        "summaries": summaries
    })))
}

#[derive(Debug, Deserialize)]
struct SummaryPathParams {
    date: String,
    type_: String,
    model: String,
    prompt: String,
}

#[axum::debug_handler]
async fn get_specific_summary(
    State(state): State<Arc<AppState>>,
    Path(params): Path<SummaryPathParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Get the specific summary from the database
    let summary = summary::get_daily_summary(
        &state,
        &params.date,
        Some(&params.model),
        Some(&params.prompt),
        &params.type_,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!(
                "Summary not found for date {} with model {}, prompt {}, and type {}",
                params.date, params.model, params.prompt, params.type_
            ),
        )
    })?;

    // Return the summary with appropriate content type
    match params.type_.as_str() {
        "json" => Ok((
            [(header::CONTENT_TYPE, "application/json")],
            summary.content,
        )),
        "markdown" => Ok((
            [(header::CONTENT_TYPE, "text/html")],
            markdown::to_html(&summary.content),
        )),
        _ => Ok(([(header::CONTENT_TYPE, "text/plain")], summary.content)),
    }
}

#[axum::debug_handler]
async fn summary_page(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut context = TeraContext::new();
    context.insert("build_info", &get_build_info());
    context.insert("request_path", &"/transcripts");

    let timezone_str = state.timezone.to_string().replace("::", "/");
    context.insert("timezone", &timezone_str);

    let rendered = TEMPLATES
        .get()
        .unwrap()
        .render("summary.html", &context)
        .unwrap_or_else(|e| format!("Template error: {}", e));

    Html(rendered)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModelsResponse {
    pub models: Vec<summary::ModelInfo>,
}

#[axum::debug_handler]
async fn get_available_models(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ModelsResponse>, (StatusCode, String)> {
    let models = summary::get_available_models(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ModelsResponse { models }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Camera {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CamerasResponse {
    pub cameras: Vec<Camera>,
}

pub fn create_signed_request(
    state: &AppState,
    entry_key: &prompt_context::Key,
    offset: usize,
    duration: Duration,
) -> Result<prompt_context::sign::SignedRequestParams, PromptContextError> {
    let hmac = prompt_context::sign::sign_request_with_duration(
        state.signing_secret.as_ref(),
        duration,
        entry_key,
        offset,
    )?;
    Ok(hmac)
}

pub fn verify_signed_request(
    state: &AppState,
    entry_key: &prompt_context::Key,
    offset: usize,
    signed_request: &prompt_context::sign::SignedRequestParams,
) -> Result<(), PromptContextError> {
    prompt_context::sign::verify_request(
        state.signing_secret.as_ref(),
        signed_request.expires,
        entry_key,
        offset,
        &signed_request.hmac,
    )
}
#[derive(Debug, Deserialize)]
pub struct OptionalSignedRequestParams {
    hmac: Option<String>,
    expires: Option<u64>,
}

impl OptionalSignedRequestParams {
    pub fn into_signed_request(
        self,
    ) -> Result<prompt_context::sign::SignedRequestParams, PromptContextError>
    {
        let hmac = self.hmac.unwrap_or_default();
        let expires = self.expires.unwrap_or_default();
        let params =
            prompt_context::sign::SignedRequestParams { hmac, expires };
        params.validate()?;
        Ok(params)
    }
}

#[axum::debug_handler]
async fn get_prompt_context(
    State(state): State<Arc<AppState>>,
    Path((key, offset)): Path<(String, usize)>,
    Query(params): Query<OptionalSignedRequestParams>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let params = params.into_signed_request().map_err(|_| {
        (
            StatusCode::FORBIDDEN,
            "Missing or invalid HMAC parameters".to_string(),
        )
    })?;

    verify_signed_request(&state, &key, offset, &params).map_err(|e| {
        error!("hmac verification failed: {}", e);
        (
            StatusCode::FORBIDDEN,
            "hmac verification failed".to_string(),
        )
    })?;

    let prompt_context = state.prompt_context_store.get(key, offset).await;

    match prompt_context {
        Ok(prompt_context) => Ok(prompt_context),
        Err(e) => match e {
            PromptContextError::NotFound => {
                Err((StatusCode::NOT_FOUND, e.to_string()))
            }
            PromptContextError::OffsetOutOfRange => {
                Err((StatusCode::BAD_REQUEST, e.to_string()))
            }
            _ => {
                error!("Failed to get prompt context: {}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error: Failed to get prompt context"
                        .to_string(),
                ))
            }
        },
    }
}

async fn get_cameras(
    State(state): State<Arc<AppState>>,
) -> Result<Json<CamerasResponse>, (StatusCode, String)> {
    let camera_names: HashMap<String, String> =
        state.camera_name_cache.lock().await.clone();

    let cameras = camera_names
        .into_iter()
        .map(|(id, name)| Camera { id, name })
        .collect::<Vec<_>>();

    Ok(Json(CamerasResponse { cameras }))
}

// Add transcript page handler
#[axum::debug_handler]
async fn transcript_page(State(state): State<Arc<AppState>>) -> Html<String> {
    let mut context = TeraContext::new();
    context.insert("build_info", &get_build_info());
    context.insert("request_path", &"/transcript");

    let timezone_str = state.timezone.to_string().replace("::", "/");
    context.insert("timezone", &timezone_str);

    let rendered = TEMPLATES
        .get()
        .unwrap()
        .render("transcript.html", &context)
        .unwrap_or_else(|e| format!("Template error: {}", e));

    Html(rendered)
}

// Add a new API endpoint for transcripts with event IDs
#[axum::debug_handler]
async fn get_transcripts_json(
    State(state): State<Arc<AppState>>,
    Path(date): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let transcript_entries =
        transcripts::get_transcripts_with_event_ids(&state, &date)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Create the response
    Ok(Json(json!({
        "date": date,
        "transcripts": transcript_entries
    })))
}

pub fn routes(state: Arc<AppState>) -> Router {
    let predicate = SizeAbove::new(32)
        // still don't compress gRPC
        .and(NotForContentType::GRPC)
        // still don't compress images
        .and(NotForContentType::IMAGES)
        // also don't compress JSON
        .and(NotForContentType::const_new("video/mp4"));

    let compression_layer = CompressionLayer::new()
        .br(true)
        .deflate(true)
        .gzip(true)
        .zstd(true)
        .compress_when(predicate);

    Router::new()
        .route("/", get(events_latest_page))
        .route("/events", get(events_redirect))
        .route("/events/latest", get(events_latest_page))
        .route("/events/search", get(events_search_page))
        .route("/health", get(health_check))
        .route("/status", get(get_status_page))
        .route("/summary", get(summary_page))
        .route("/transcript", get(transcript_page))
        .route("/api/events", get(get_completed_events))
        .route("/api/event/{event_id}", get(get_event))
        .route("/api/models", get(get_available_models))
        .route("/api/cameras", get(get_cameras))
        .route("/video/{event_id}", get(stream_video))
        .route("/api/status", get(get_status))
        .route("/api/transcripts/csv/{date}", get(get_transcripts_csv))
        .route("/api/transcripts/json/{date}", get(get_transcripts_json))
        .route(
            "/api/transcripts/summary/{date}",
            get(get_transcripts_summary),
        )
        .route(
            "/api/transcripts/summary/{date}/json",
            get(get_transcripts_summary_json),
        )
        .route(
            "/api/transcripts/summary/{date}/list",
            get(list_daily_summaries),
        )
        .route(
            "/api/transcripts/summary/{date}/type/{type_}/model/{model}/prompt/{prompt}",
            get(get_specific_summary),
        )
        .route("/api/captions/{event_id}", get(get_vtt_captions))
        .route(
            "/api/storyboard/image/{event_id}",
            get(get_storyboard_image),
        )
        .route("/api/storyboard/vtt/{event_id}", get(get_storyboard_vtt))
        .route("/audio/{event_id}/wav", get(stream_audio))
        .route("/api/prompt_context/{key}/{offset}", get(get_prompt_context))
        .layer(compression_layer)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn serve() -> Result<()> {
    // Initialize logging with tracing
    let subscriber = Registry::default()
        .with(
            HierarchicalLayer::new(2)
                .with_targets(true)
                .with_bracketed_fields(true),
        )
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug".into()),
        );

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Parse command line arguments
    let args = Args::parse();

    if args.create_lock_file && args.enable_transcription {
        let _lock_file = create_app_lock().map_err(|e| {
            error!("Failed to create lock file: {}", e);
            e
        })?;
    }

    info!("Starting whisper_asr service");

    // Initialize database connection pools
    info!("Creating database connection pools");

    // Check if events_db path is empty
    if args.events_db.is_empty() {
        return Err(anyhow::anyhow!("events_db path cannot be empty. Please provide a valid path using --events-db"));
    }

    // Check if zumblezay_db file exists and is writable
    info!("Checking if zumblezay database is writable");
    check_file_is_writable(&args.zumblezay_db, "zumblezay database")?;

    let events_manager = SqliteConnectionManager::file(&args.events_db)
        .with_flags(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);
    let events_pool = Pool::new(events_manager)?;

    // Create zumblezay pool with connection customization
    let zumblezay_manager = SqliteConnectionManager::file(&args.zumblezay_db);
    let zumblezay_pool = Pool::builder()
        .connection_customizer(Box::new(DbAttacher {
            events_db: args.events_db.clone(),
        }))
        .build(zumblezay_manager)?;

    // Initialize transcripts database schema
    {
        let mut conn = zumblezay_pool.get()?;
        crate::init_zumblezay_db(&mut conn)?;
    }

    // Check if cache_db file exists and is writable
    info!("Checking if cache database is writable");
    check_file_is_writable(&args.cache_db, "cache database")?;

    let cache_manager = SqliteConnectionManager::file(&args.cache_db);
    let cache_pool = Pool::new(cache_manager)?;

    // Initialize cache database schema
    {
        let mut conn = cache_pool.get()?;
        crate::init_cache_db(&mut conn)?;
    }

    info!("Using Whisper API URL: {}", args.whisper_url);

    let state = crate::create_app_state(crate::AppConfig {
        events_pool,
        zumblezay_pool,
        cache_pool,
        whisper_url: args.whisper_url,
        max_concurrent_tasks: args.max_concurrent_tasks,
        openai_api_key: args.openai_api_key,
        openai_api_base: args.openai_api_base,
        runpod_api_key: args.runpod_api_key,
        signing_secret: args.signing_secret,
        transcription_service: args.transcription_service,
        default_summary_model: args.default_summary_model,
        video_path_original_prefix: args.video_path_original_prefix,
        video_path_replacement_prefix: args.video_path_replacement_prefix,
        timezone_str: args.timezone,
    });

    // Create a channel for shutdown coordination
    let (shutdown_tx, mut shutdown_rx) =
        tokio::sync::broadcast::channel::<()>(1);

    // Set up ctrl-c handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received CTRL-C, initiating shutdown");
            let _ = shutdown_tx_clone.send(());
        }
    });

    // Cache camera names
    if let Err(e) = cache_camera_names(&state).await {
        error!("Failed to cache camera names: {}", e);
    }

    // Start background sync task if enabled
    let sync_handle = if args.enable_sync {
        info!("Starting background sync task");
        let sync_state = state.clone();
        let sync_interval = Duration::from_secs(args.sync_interval);
        let sync_shutdown_rx = shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            sync_events(sync_state, sync_interval, sync_shutdown_rx).await;
        }))
    } else {
        info!("Background sync task disabled");
        None
    };

    // Start background processing only if transcription is enabled
    let processing_handle = if args.enable_transcription {
        info!("Starting background processing task");
        let processing_state = state.clone();
        Some(tokio::spawn(async move {
            process_events::process_events(processing_state).await;
        }))
    } else {
        info!("Background transcription task disabled");
        None
    };

    // Initialize templates
    ensure_templates();

    // Start web server
    let app = routes(state);
    let addr = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Server running on http://{}", addr);

    let server = axum::serve(listener, app);

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
        _ = shutdown_rx.recv() => {
            info!("Shutdown signal received, waiting for background tasks to complete...");
        }
    }

    // Wait for both handles with timeout
    for (task_name, handle) in
        [("sync", sync_handle), ("processing", processing_handle)]
    {
        if let Some(handle) = handle {
            match tokio::time::timeout(Duration::from_secs(30), handle).await {
                Ok(_) => info!("Background {} completed gracefully", task_name),
                Err(_) => {
                    warn!("Background {} timed out during shutdown", task_name)
                }
            }
        }
    }

    info!("Server shutdown complete");
    Ok(())
}
