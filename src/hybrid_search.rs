use crate::bedrock::{BedrockClientTrait, BedrockUsage};
use crate::bedrock_spend::{
    fetch_bedrock_pricing_from_aws, is_missing_pricing_error,
    record_bedrock_spend, upsert_bedrock_pricing, SpendLogRequest,
};
use anyhow::{Context, Result};
use rand::Rng;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub const ACTIVE_MODEL_KEY: &str = "nova2_text_v1";
pub const ACTIVE_PROVIDER_MODEL_ID: &str =
    "amazon.nova-2-multimodal-embeddings-v1:0";
pub const ACTIVE_EMBEDDING_KIND: &str = "text";
pub const ACTIVE_SEGMENT_STRATEGY: &str = "asr_window_v1";
pub const DEFAULT_EMBEDDING_DIM: usize = 256;
pub const DEFAULT_RRF_K: f64 = 60.0;

const SEGMENT_WINDOW_MS: i64 = 30_000;
const SEGMENT_OVERLAP_MS: i64 = 10_000;
const RETRY_BASE_SECS: i64 = 60;
const MAX_ATTEMPTS: i64 = 5;
const JOB_TYPE_BACKFILL_EVENT_PREFIX: &str = "backfill_event:";
const EMBEDDING_SPEND_CATEGORY: &str = "transcript_embedding";
const EMBEDDING_SPEND_OPERATION_INDEX: &str = "embed_transcript_unit";
const EMBEDDING_SPEND_OPERATION_QUERY: &str = "embed_search_query";

#[derive(Debug, Clone)]
pub struct SearchConfig {
    pub rrf_k: f64,
    pub bm25_weight: f64,
    pub vector_weight: f64,
}

#[derive(Debug, Clone)]
pub struct HybridSearchHit {
    pub event_id: String,
    pub snippet: String,
    pub score: f64,
    pub unit_type: Option<String>,
    pub segment_start_ms: Option<i64>,
    pub segment_end_ms: Option<i64>,
    pub match_sources: Vec<String>,
    pub bm25_rank: Option<usize>,
    pub vector_rank: Option<usize>,
    pub bm25_rrf_score: Option<f64>,
    pub vector_rrf_score: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Hybrid,
    Bm25,
    Vector,
}

impl SearchMode {
    pub fn parse(value: Option<&str>) -> Option<Self> {
        match value
            .unwrap_or("hybrid")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "hybrid" => Some(Self::Hybrid),
            "bm25" | "keyword" => Some(Self::Bm25),
            "vector" | "semantic" => Some(Self::Vector),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventFilters {
    pub transcription_type: String,
    pub start_ts: f64,
    pub end_ts: f64,
    pub camera_id: Option<String>,
    pub cursor_start: Option<f64>,
    pub cursor_event_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BackfillRangeParams {
    pub start_ts: f64,
    pub end_ts: f64,
    pub transcription_type: String,
    pub shutdown_token: Option<CancellationToken>,
}

#[derive(Debug, Clone)]
pub struct BackfillSummary {
    pub scanned_rows: usize,
    pub indexed_rows: usize,
    pub parse_failures: usize,
    pub processed_jobs: usize,
}

#[derive(Debug, Clone)]
struct TranscriptUnitDraft {
    unit_id: String,
    unit_type: &'static str,
    segment_strategy: Option<&'static str>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    content_text: String,
    content_hash: String,
}

#[derive(Debug, Clone)]
struct SourceCandidate {
    event_id: String,
    snippet: String,
    unit_type: Option<String>,
    segment_start_ms: Option<i64>,
    segment_end_ms: Option<i64>,
}

pub fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS embedding_models (
            model_key TEXT PRIMARY KEY,
            provider_model_id TEXT NOT NULL,
            modality TEXT NOT NULL,
            embedding_dim INTEGER NOT NULL,
            version_tag TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transcript_units (
            unit_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            unit_type TEXT NOT NULL,
            segment_strategy TEXT,
            start_ms INTEGER,
            end_ms INTEGER,
            content_text TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            content_version INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_transcript_units_event_type
            ON transcript_units(event_id, unit_type);
        CREATE INDEX IF NOT EXISTS idx_transcript_units_event_strategy_start
            ON transcript_units(event_id, segment_strategy, start_ms);
        CREATE INDEX IF NOT EXISTS idx_transcript_units_content_hash
            ON transcript_units(content_hash);

        CREATE TABLE IF NOT EXISTS unit_embeddings (
            embedding_id TEXT PRIMARY KEY,
            unit_id TEXT NOT NULL,
            model_key TEXT NOT NULL,
            embedding_kind TEXT NOT NULL,
            embedding_dim INTEGER NOT NULL,
            vector_blob BLOB NOT NULL,
            content_version INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unit_embeddings_unique_version
            ON unit_embeddings(unit_id, model_key, embedding_kind, content_version);
        CREATE INDEX IF NOT EXISTS idx_unit_embeddings_model_kind
            ON unit_embeddings(model_key, embedding_kind);
        CREATE INDEX IF NOT EXISTS idx_unit_embeddings_unit
            ON unit_embeddings(unit_id);

        -- Lightweight in-db mirror for vector retrieval in environments
        -- where sqlite-vec is not available.
        CREATE TABLE IF NOT EXISTS unit_embedding_vec_index (
            embedding_id TEXT PRIMARY KEY,
            vector_blob BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS embedding_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            unit_id TEXT,
            job_type TEXT NOT NULL,
            priority INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_embedding_jobs_status_priority_created
            ON embedding_jobs(status, priority, created_at);
        CREATE INDEX IF NOT EXISTS idx_embedding_jobs_event_status
            ON embedding_jobs(event_id, status);

        CREATE TABLE IF NOT EXISTS search_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS embedding_backfill_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_start TEXT NOT NULL,
            date_end TEXT NOT NULL,
            timezone TEXT NOT NULL,
            transcription_type TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at INTEGER NOT NULL,
            finished_at INTEGER,
            scanned_rows INTEGER NOT NULL DEFAULT 0,
            indexed_rows INTEGER NOT NULL DEFAULT 0,
            parse_failures INTEGER NOT NULL DEFAULT 0,
            processed_jobs INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_embedding_backfill_runs_status
            ON embedding_backfill_runs(status, created_at DESC);
        "#,
    )?;

    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT OR IGNORE INTO embedding_models (
            model_key, provider_model_id, modality, embedding_dim,
            version_tag, is_active, created_at
        ) VALUES (?, ?, ?, ?, ?, 1, ?)",
        params![
            ACTIVE_MODEL_KEY,
            ACTIVE_PROVIDER_MODEL_ID,
            "multimodal",
            DEFAULT_EMBEDDING_DIM as i64,
            "v1",
            now
        ],
    )?;

    conn.execute(
        "INSERT OR IGNORE INTO search_config (key, value, updated_at)
         VALUES (?, ?, ?)",
        params!["hybrid.rrf.k", DEFAULT_RRF_K.to_string(), now],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO search_config (key, value, updated_at)
         VALUES (?, ?, ?)",
        params!["hybrid.weight.bm25", "1.0", now],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO search_config (key, value, updated_at)
         VALUES (?, ?, ?)",
        params!["hybrid.weight.vector", "1.0", now],
    )?;

    Ok(())
}

pub fn load_search_config(conn: &Connection) -> Result<SearchConfig> {
    let rrf_k = get_config_f64(conn, "hybrid.rrf.k", DEFAULT_RRF_K)?;
    let bm25_weight = get_config_f64(conn, "hybrid.weight.bm25", 1.0)?;
    let vector_weight = get_config_f64(conn, "hybrid.weight.vector", 1.0)?;
    Ok(SearchConfig {
        rrf_k,
        bm25_weight,
        vector_weight,
    })
}

fn get_config_f64(conn: &Connection, key: &str, default: f64) -> Result<f64> {
    let raw = conn
        .query_row(
            "SELECT value FROM search_config WHERE key = ?",
            params![key],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(raw
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default))
}

pub fn index_transcript_units_for_event(
    conn: &Connection,
    event_id: &str,
    raw_json: &Value,
    event_start_ts: f64,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let drafts = build_transcript_units(event_id, raw_json);

    let mut changed_units = Vec::new();
    let mut active_segment_ids: HashSet<String> = HashSet::new();
    for draft in &drafts {
        if draft.unit_type == "segment" {
            active_segment_ids.insert(draft.unit_id.clone());
        }

        let existing = conn
            .query_row(
                "SELECT content_hash, content_version
                 FROM transcript_units WHERE unit_id = ?",
                params![draft.unit_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;

        match existing {
            None => {
                conn.execute(
                    "INSERT INTO transcript_units (
                        unit_id, event_id, unit_type, segment_strategy,
                        start_ms, end_ms, content_text, content_hash,
                        content_version, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                    params![
                        draft.unit_id,
                        event_id,
                        draft.unit_type,
                        draft.segment_strategy,
                        draft.start_ms,
                        draft.end_ms,
                        draft.content_text,
                        draft.content_hash,
                        now,
                        now
                    ],
                )?;
                changed_units.push((draft.unit_id.clone(), draft.unit_type));
            }
            Some((existing_hash, existing_version)) => {
                if existing_hash == draft.content_hash {
                    conn.execute(
                        "UPDATE transcript_units
                         SET updated_at = ?
                         WHERE unit_id = ?",
                        params![now, draft.unit_id],
                    )?;
                } else {
                    conn.execute(
                        "UPDATE transcript_units
                         SET segment_strategy = ?,
                             start_ms = ?,
                             end_ms = ?,
                             content_text = ?,
                             content_hash = ?,
                             content_version = ?,
                             updated_at = ?
                         WHERE unit_id = ?",
                        params![
                            draft.segment_strategy,
                            draft.start_ms,
                            draft.end_ms,
                            draft.content_text,
                            draft.content_hash,
                            existing_version + 1,
                            now,
                            draft.unit_id
                        ],
                    )?;
                    changed_units
                        .push((draft.unit_id.clone(), draft.unit_type));
                }
            }
        }
    }

    let mut stale_stmt = conn.prepare(
        "SELECT unit_id FROM transcript_units
         WHERE event_id = ? AND unit_type = 'segment' AND segment_strategy = ?",
    )?;
    let stale_rows = stale_stmt
        .query_map(params![event_id, ACTIVE_SEGMENT_STRATEGY], |row| {
            row.get::<_, String>(0)
        })?;
    let stale_ids: Vec<String> = stale_rows
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|unit_id| !active_segment_ids.contains(unit_id))
        .collect();
    drop(stale_stmt);

    for unit_id in stale_ids {
        delete_unit_and_embeddings(conn, &unit_id)?;
    }

    for (unit_id, unit_type) in changed_units {
        let job_type = if unit_type == "segment" {
            "segment"
        } else {
            "full_clip"
        };

        let age_secs =
            (chrono::Utc::now().timestamp() as f64 - event_start_ts).max(0.0);
        let priority = if age_secs <= 86_400.0 {
            100
        } else if age_secs <= 7.0 * 86_400.0 {
            50
        } else {
            10
        };

        conn.execute(
            "INSERT INTO embedding_jobs (
                event_id, unit_id, job_type, priority, status,
                attempt_count, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?, ?)",
            params![event_id, unit_id, job_type, priority, now, now],
        )?;
    }

    Ok(())
}

fn delete_unit_and_embeddings(conn: &Connection, unit_id: &str) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT embedding_id FROM unit_embeddings WHERE unit_id = ?",
    )?;
    let rows =
        stmt.query_map(params![unit_id], |row| row.get::<_, String>(0))?;
    let embedding_ids: Vec<String> = rows.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    for embedding_id in embedding_ids {
        conn.execute(
            "DELETE FROM unit_embedding_vec_index WHERE embedding_id = ?",
            params![embedding_id],
        )?;
    }
    conn.execute(
        "DELETE FROM unit_embeddings WHERE unit_id = ?",
        params![unit_id],
    )?;
    conn.execute(
        "DELETE FROM transcript_units WHERE unit_id = ?",
        params![unit_id],
    )?;
    Ok(())
}

pub fn process_pending_embedding_jobs(
    conn: &Connection,
    max_jobs: usize,
) -> Result<usize> {
    process_pending_embedding_jobs_with_client_and_concurrency(
        conn, max_jobs, 1, None,
    )
}

pub fn process_pending_embedding_jobs_with_client(
    conn: &Connection,
    max_jobs: usize,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    process_pending_embedding_jobs_with_client_and_concurrency(
        conn,
        max_jobs,
        1,
        bedrock_client,
    )
}

pub fn process_pending_embedding_jobs_with_client_and_concurrency(
    conn: &Connection,
    max_jobs: usize,
    embedding_concurrency: usize,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    if max_jobs == 0 {
        return Ok(0);
    }

    let embedding_concurrency = embedding_concurrency.max(1);
    let mut processed = process_embedding_jobs_parallel(
        conn,
        max_jobs,
        embedding_concurrency,
        bedrock_client.as_ref(),
    )?;
    if processed < max_jobs {
        processed += process_backfill_jobs_sequential(
            conn,
            max_jobs - processed,
            bedrock_client.as_ref(),
        )?;
    }
    Ok(processed)
}

pub fn process_pending_embedding_jobs_embeddings_only_with_client_and_concurrency(
    conn: &Connection,
    max_jobs: usize,
    embedding_concurrency: usize,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    if max_jobs == 0 {
        return Ok(0);
    }
    process_embedding_jobs_parallel(
        conn,
        max_jobs,
        embedding_concurrency.max(1),
        bedrock_client.as_ref(),
    )
}

pub fn process_pending_embedding_jobs_backfill_only_with_client(
    conn: &Connection,
    max_jobs: usize,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    if max_jobs == 0 {
        return Ok(0);
    }
    process_backfill_jobs_sequential(conn, max_jobs, bedrock_client.as_ref())
}

#[derive(Debug, Clone)]
struct ClaimedEmbeddingJob {
    job_id: i64,
    unit_id: Option<String>,
    attempts: i64,
    content_text: Option<String>,
    content_version: Option<i64>,
    embedding_dim: i64,
}

#[derive(Debug)]
struct EmbeddingJobComputationWithMeta {
    embedding_id: String,
    content_version: i64,
    embedding: Vec<f32>,
    usage: Option<BedrockUsage>,
    request_id: Option<String>,
}

fn process_embedding_jobs_parallel(
    conn: &Connection,
    max_jobs: usize,
    embedding_concurrency: usize,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    let now = chrono::Utc::now().timestamp();
    let mut stmt = conn.prepare(
        "SELECT job_id
         FROM embedding_jobs
         WHERE status = 'pending'
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
           AND job_type IN ('full_clip', 'segment')
         ORDER BY
           priority DESC,
           created_at ASC
         LIMIT ?",
    )?;
    let job_rows = stmt
        .query_map(params![now, max_jobs as i64], |row| row.get::<_, i64>(0))?;
    let job_ids: Vec<i64> = job_rows.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut claimed_jobs = Vec::new();
    for job_id in job_ids {
        let updated = conn.execute(
            "UPDATE embedding_jobs
             SET status = 'running',
                 attempt_count = attempt_count + 1,
                 updated_at = ?
             WHERE job_id = ? AND status = 'pending'",
            params![now, job_id],
        )?;
        if updated == 0 {
            continue;
        }

        let row = conn
            .query_row(
                "SELECT ej.unit_id, ej.attempt_count, tu.content_text, tu.content_version
                 FROM embedding_jobs ej
                 LEFT JOIN transcript_units tu ON tu.unit_id = ej.unit_id
                 WHERE ej.job_id = ?",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                    ))
                },
            )
            .optional()?;

        let Some((unit_id, attempts, content_text, content_version)) = row
        else {
            continue;
        };

        let embedding_dim: i64 = conn
            .query_row(
                "SELECT embedding_dim FROM embedding_models
                 WHERE model_key = ?",
                params![ACTIVE_MODEL_KEY],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(DEFAULT_EMBEDDING_DIM as i64);

        claimed_jobs.push(ClaimedEmbeddingJob {
            job_id,
            unit_id,
            attempts,
            content_text,
            content_version,
            embedding_dim,
        });
    }

    let mut processed = 0usize;
    for batch in claimed_jobs.chunks(embedding_concurrency) {
        let mut handles = Vec::with_capacity(batch.len());
        for job in batch {
            let job_for_thread = job.clone();
            let client = bedrock_client.cloned();
            handles.push(thread::spawn(move || {
                compute_embedding_for_job(job_for_thread, client)
            }));
        }

        for handle in handles {
            let computation = handle.join().map_err(|_| {
                anyhow::anyhow!("embedding computation thread panicked")
            })?;
            if finalize_embedding_job(conn, computation, now)?.is_ok() {
                processed += 1;
            }
        }
    }

    Ok(processed)
}

fn process_backfill_jobs_sequential(
    conn: &Connection,
    max_jobs: usize,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<usize> {
    if max_jobs == 0 {
        return Ok(0);
    }

    let now = chrono::Utc::now().timestamp();
    let mut stmt = conn.prepare(
        "SELECT job_id
         FROM embedding_jobs
         WHERE status = 'pending'
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
           AND job_type LIKE 'backfill_event:%'
         ORDER BY priority DESC, created_at ASC
         LIMIT ?",
    )?;
    let rows = stmt
        .query_map(params![now, max_jobs as i64], |row| row.get::<_, i64>(0))?;
    let job_ids: Vec<i64> = rows.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut processed = 0usize;
    for job_id in job_ids {
        if process_single_job(conn, job_id, bedrock_client).is_ok() {
            processed += 1;
        }
    }
    Ok(processed)
}

fn compute_embedding_for_job(
    job: ClaimedEmbeddingJob,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> (
    ClaimedEmbeddingJob,
    Result<Option<EmbeddingJobComputationWithMeta>>,
) {
    let Some(unit_id) = job.unit_id.clone() else {
        return (job, Err(anyhow::anyhow!("embedding job missing unit_id")));
    };

    let Some(content_text) = job.content_text.clone() else {
        return (job, Ok(None));
    };

    let Some(content_version) = job.content_version else {
        return (job, Ok(None));
    };

    let dim =
        usize::try_from(job.embedding_dim).unwrap_or(DEFAULT_EMBEDDING_DIM);
    let result = embed_text(&content_text, dim, bedrock_client.as_ref()).map(
        |embedding_result| {
            let embedding_id = deterministic_embedding_id(
                &unit_id,
                ACTIVE_MODEL_KEY,
                ACTIVE_EMBEDDING_KIND,
                content_version,
            );
            Some(EmbeddingJobComputationWithMeta {
                embedding_id,
                content_version,
                embedding: embedding_result.embedding,
                usage: embedding_result.usage,
                request_id: embedding_result.request_id,
            })
        },
    );

    (job, result)
}

fn finalize_embedding_job(
    conn: &Connection,
    computation: (
        ClaimedEmbeddingJob,
        Result<Option<EmbeddingJobComputationWithMeta>>,
    ),
    now: i64,
) -> Result<Result<()>> {
    let (job, result) = computation;
    match result {
        Ok(Some(mut payload)) => {
            let unit_id = job
                .unit_id
                .as_deref()
                .context("embedding job missing unit_id")?;
            let dim = i64::try_from(payload.embedding.len())
                .context("embedding vector too large for i64")?;
            let blob = serialize_embedding(&payload.embedding);
            conn.execute(
                "INSERT OR REPLACE INTO unit_embeddings (
                    embedding_id, unit_id, model_key, embedding_kind,
                    embedding_dim, vector_blob, content_version, created_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    payload.embedding_id,
                    unit_id,
                    ACTIVE_MODEL_KEY,
                    ACTIVE_EMBEDDING_KIND,
                    dim,
                    blob,
                    payload.content_version,
                    now
                ],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO unit_embedding_vec_index (
                    embedding_id, vector_blob
                 ) VALUES (?, ?)",
                params![
                    payload.embedding_id.clone(),
                    serialize_embedding(&payload.embedding)
                ],
            )?;
            let embedding_values = std::mem::take(&mut payload.embedding);
            maybe_record_embedding_spend(
                conn,
                &EmbeddingCallResult {
                    embedding: embedding_values,
                    usage: payload.usage,
                    request_id: payload.request_id,
                },
                EMBEDDING_SPEND_OPERATION_INDEX,
            )?;
            conn.execute(
                "UPDATE embedding_jobs
                 SET status = 'done',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     updated_at = ?
                 WHERE job_id = ?",
                params![now, job.job_id],
            )?;
            Ok(Ok(()))
        }
        Ok(None) => {
            conn.execute(
                "UPDATE embedding_jobs
                 SET status = 'done',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     updated_at = ?
                 WHERE job_id = ?",
                params![now, job.job_id],
            )?;
            Ok(Ok(()))
        }
        Err(err) => {
            let attempts = job.attempts.max(1);
            if attempts >= MAX_ATTEMPTS {
                conn.execute(
                    "UPDATE embedding_jobs
                     SET status = 'failed',
                         last_error = ?,
                         updated_at = ?
                     WHERE job_id = ?",
                    params![err.to_string(), now, job.job_id],
                )?;
            } else {
                let mut rng = rand::thread_rng();
                let jitter: i64 = rng.gen_range(0..=15);
                let delay = RETRY_BASE_SECS * (1i64 << (attempts - 1)) + jitter;
                conn.execute(
                    "UPDATE embedding_jobs
                     SET status = 'pending',
                         next_attempt_at = ?,
                         last_error = ?,
                         updated_at = ?
                     WHERE job_id = ?",
                    params![now + delay, err.to_string(), now, job.job_id],
                )?;
            }
            Ok(Err(err))
        }
    }
}

pub fn backfill_date_range(
    conn: &Connection,
    params: &BackfillRangeParams,
) -> Result<BackfillSummary> {
    info!(
        "Embedding backfill scan starting: start_ts={} end_ts={} transcription_type={}",
        params.start_ts, params.end_ts, params.transcription_type
    );
    if params
        .shutdown_token
        .as_ref()
        .is_some_and(|token| token.is_cancelled())
    {
        return Err(anyhow::anyhow!("backfill cancelled due to shutdown"));
    }
    let mut stmt = conn.prepare(
        "SELECT e.event_id, e.event_start
         FROM events e
         JOIN transcriptions t ON t.event_id = e.event_id
         WHERE e.event_start BETWEEN ? AND ?
           AND t.transcription_type = ?
         ORDER BY e.event_start ASC",
    )?;
    let rows = stmt.query_map(
        params![
            params.start_ts,
            params.end_ts,
            params.transcription_type.as_str()
        ],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?)),
    )?;
    let rows: Vec<(String, f64)> = rows.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);

    let mut scanned_rows = 0usize;
    let mut indexed_rows = 0usize;
    let parse_failures = 0usize;
    let now = chrono::Utc::now().timestamp();
    let job_type = format!(
        "{}{}",
        JOB_TYPE_BACKFILL_EVENT_PREFIX, params.transcription_type
    );
    for row in rows {
        if params
            .shutdown_token
            .as_ref()
            .is_some_and(|token| token.is_cancelled())
        {
            return Err(anyhow::anyhow!("backfill cancelled due to shutdown"));
        }
        let (event_id, event_start) = row;
        scanned_rows += 1;

        let age_secs =
            (chrono::Utc::now().timestamp() as f64 - event_start).max(0.0);
        let priority = if age_secs <= 86_400.0 {
            100
        } else if age_secs <= 7.0 * 86_400.0 {
            50
        } else {
            10
        };

        conn.execute(
            "INSERT INTO embedding_jobs (
                event_id, unit_id, job_type, priority, status,
                attempt_count, next_attempt_at, created_at, updated_at
             ) VALUES (?, NULL, ?, ?, 'pending', 0, NULL, ?, ?)",
            params![event_id, job_type, priority, now, now],
        )?;
        indexed_rows += 1;
        if scanned_rows.is_multiple_of(500) {
            info!(
                "Embedding backfill progress: scanned_rows={} indexed_rows={} parse_failures={}",
                scanned_rows, indexed_rows, parse_failures
            );
        }
    }

    // Backfill only scans/indexes and enqueues embedding work.
    // The background drain loop is the sole embedding writer.
    let processed_jobs = 0usize;

    info!(
        "Embedding backfill complete: scanned_rows={} indexed_rows={} parse_failures={} processed_jobs={}",
        scanned_rows, indexed_rows, parse_failures, processed_jobs
    );
    Ok(BackfillSummary {
        scanned_rows,
        indexed_rows,
        parse_failures,
        processed_jobs,
    })
}

fn process_single_job(
    conn: &Connection,
    job_id: i64,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<()> {
    let now = chrono::Utc::now().timestamp();
    let changed = conn.execute(
        "UPDATE embedding_jobs
         SET status = 'running',
             attempt_count = attempt_count + 1,
             updated_at = ?
         WHERE job_id = ? AND status = 'pending'",
        params![now, job_id],
    )?;
    if changed == 0 {
        return Ok(());
    }

    let job_row = conn
        .query_row(
            "SELECT event_id, unit_id, attempt_count, job_type
             FROM embedding_jobs WHERE job_id = ?",
            params![job_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()?;

    let Some((event_id, unit_id_opt, attempts, job_type)) = job_row else {
        return Ok(());
    };

    let result = (|| -> Result<()> {
        if let Some(transcription_type) =
            job_type.strip_prefix(JOB_TYPE_BACKFILL_EVENT_PREFIX)
        {
            let row = conn
                .query_row(
                    "SELECT e.event_start, t.raw_response
                     FROM events e
                     JOIN transcriptions t ON t.event_id = e.event_id
                     WHERE e.event_id = ? AND t.transcription_type = ?
                     LIMIT 1",
                    params![event_id, transcription_type],
                    |row| Ok((row.get::<_, f64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;

            let Some((event_start, raw_response)) = row else {
                return Ok(());
            };

            let raw_json: Value = serde_json::from_str(&raw_response)?;
            upsert_transcript_search_content(conn, &event_id, &raw_json)?;
            index_transcript_units_for_event(
                conn,
                &event_id,
                &raw_json,
                event_start,
            )?;
            Ok(())
        } else {
            let unit_id =
                unit_id_opt.context("embedding job missing unit_id")?;
            upsert_current_embedding_for_unit(conn, &unit_id, bedrock_client)
        }
    })();

    match result {
        Ok(()) => {
            conn.execute(
                "UPDATE embedding_jobs
                 SET status = 'done',
                     next_attempt_at = NULL,
                     last_error = NULL,
                     updated_at = ?
                 WHERE job_id = ?",
                params![now, job_id],
            )?;
            Ok(())
        }
        Err(err) => {
            let attempts = attempts.max(1);
            if attempts >= MAX_ATTEMPTS {
                conn.execute(
                    "UPDATE embedding_jobs
                     SET status = 'failed',
                         last_error = ?,
                         updated_at = ?
                     WHERE job_id = ?",
                    params![err.to_string(), now, job_id],
                )?;
            } else {
                let mut rng = rand::thread_rng();
                let jitter: i64 = rng.gen_range(0..=15);
                let delay = RETRY_BASE_SECS * (1i64 << (attempts - 1)) + jitter;
                conn.execute(
                    "UPDATE embedding_jobs
                     SET status = 'pending',
                         next_attempt_at = ?,
                         last_error = ?,
                         updated_at = ?
                     WHERE job_id = ?",
                    params![now + delay, err.to_string(), now, job_id],
                )?;
            }
            Err(err)
        }
    }
}

fn upsert_current_embedding_for_unit(
    conn: &Connection,
    unit_id: &str,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<()> {
    let row = conn
        .query_row(
            "SELECT content_text, content_version
             FROM transcript_units
             WHERE unit_id = ?",
            params![unit_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;

    let Some((content_text, content_version)) = row else {
        return Ok(());
    };

    let dim: i64 = conn
        .query_row(
            "SELECT embedding_dim FROM embedding_models WHERE model_key = ?",
            params![ACTIVE_MODEL_KEY],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(DEFAULT_EMBEDDING_DIM as i64);

    let embedding_result =
        embed_text(&content_text, dim as usize, bedrock_client)?;
    maybe_record_embedding_spend(
        conn,
        &embedding_result,
        EMBEDDING_SPEND_OPERATION_INDEX,
    )?;

    let embedding = embedding_result.embedding;
    let embedding_id = deterministic_embedding_id(
        unit_id,
        ACTIVE_MODEL_KEY,
        ACTIVE_EMBEDDING_KIND,
        content_version,
    );
    let now = chrono::Utc::now().timestamp();
    let blob = serialize_embedding(&embedding);

    conn.execute(
        "INSERT OR REPLACE INTO unit_embeddings (
            embedding_id, unit_id, model_key, embedding_kind,
            embedding_dim, vector_blob, content_version, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            embedding_id,
            unit_id,
            ACTIVE_MODEL_KEY,
            ACTIVE_EMBEDDING_KIND,
            dim,
            blob,
            content_version,
            now
        ],
    )?;

    conn.execute(
        "INSERT OR REPLACE INTO unit_embedding_vec_index (embedding_id, vector_blob)
         VALUES (?, ?)",
        params![embedding_id, serialize_embedding(&embedding)],
    )?;

    Ok(())
}

pub fn search_events_hybrid(
    conn: &Connection,
    query: &str,
    filters: &EventFilters,
    limit: usize,
) -> Result<Vec<HybridSearchHit>> {
    search_events(conn, query, filters, limit, SearchMode::Hybrid)
}

pub fn search_events(
    conn: &Connection,
    query: &str,
    filters: &EventFilters,
    limit: usize,
    mode: SearchMode,
) -> Result<Vec<HybridSearchHit>> {
    search_events_with_client(conn, query, filters, limit, mode, None)
}

pub fn search_events_with_client(
    conn: &Connection,
    query: &str,
    filters: &EventFilters,
    limit: usize,
    mode: SearchMode,
    bedrock_client: Option<Arc<dyn BedrockClientTrait>>,
) -> Result<Vec<HybridSearchHit>> {
    let cfg = load_search_config(conn)?;

    let bm25_candidates = if mode == SearchMode::Vector {
        Vec::new()
    } else {
        search_bm25(conn, query, filters, limit.saturating_mul(4))?
    };
    let vector_candidates = if mode == SearchMode::Bm25 {
        Vec::new()
    } else {
        match search_vector(
            conn,
            query,
            filters,
            limit.saturating_mul(4),
            bedrock_client.as_ref(),
        ) {
            Ok(candidates) => candidates,
            Err(error) if mode == SearchMode::Hybrid => {
                warn!(
                    "Hybrid vector search failed; falling back to BM25-only: {}",
                    error
                );
                Vec::new()
            }
            Err(error) => return Err(error),
        }
    };

    let mut bm25_ranks: HashMap<String, usize> = HashMap::new();
    let mut vector_ranks: HashMap<String, usize> = HashMap::new();
    let mut best_source: HashMap<String, SourceCandidate> = HashMap::new();

    for (idx, candidate) in bm25_candidates.iter().enumerate() {
        bm25_ranks
            .entry(candidate.event_id.clone())
            .or_insert(idx + 1);
        best_source
            .entry(candidate.event_id.clone())
            .or_insert_with(|| candidate.clone());
    }
    for (idx, candidate) in vector_candidates.iter().enumerate() {
        vector_ranks
            .entry(candidate.event_id.clone())
            .or_insert(idx + 1);
        best_source
            .entry(candidate.event_id.clone())
            .and_modify(|existing| {
                if existing.unit_type.is_none() {
                    *existing = candidate.clone();
                }
            })
            .or_insert(candidate.clone());
    }

    let mut all_event_ids = HashSet::new();
    all_event_ids.extend(bm25_ranks.keys().cloned());
    all_event_ids.extend(vector_ranks.keys().cloned());

    let mut scored = Vec::new();
    for event_id in all_event_ids {
        let bm25_rrf_score = bm25_ranks
            .get(&event_id)
            .map(|rank| cfg.bm25_weight / (cfg.rrf_k + *rank as f64));
        let vector_rrf_score = vector_ranks
            .get(&event_id)
            .map(|rank| cfg.vector_weight / (cfg.rrf_k + *rank as f64));

        let mut score = 0.0;
        if let Some(v) = bm25_rrf_score {
            score += v;
        }
        if let Some(v) = vector_rrf_score {
            score += v;
        }
        if let Some(source) = best_source.get(&event_id) {
            let mut match_sources = Vec::new();
            if bm25_ranks.contains_key(&event_id) {
                match_sources.push("bm25".to_string());
            }
            if vector_ranks.contains_key(&event_id) {
                match_sources.push("vector".to_string());
            }
            scored.push(HybridSearchHit {
                event_id: event_id.clone(),
                snippet: source.snippet.clone(),
                score,
                unit_type: source.unit_type.clone(),
                segment_start_ms: source.segment_start_ms,
                segment_end_ms: source.segment_end_ms,
                match_sources,
                bm25_rank: bm25_ranks.get(&event_id).copied(),
                vector_rank: vector_ranks.get(&event_id).copied(),
                bm25_rrf_score,
                vector_rrf_score,
            });
        }
    }

    // Fallback to BM25-only if vector path has no candidates in hybrid mode.
    if mode == SearchMode::Hybrid && vector_candidates.is_empty() {
        scored = bm25_candidates
            .iter()
            .enumerate()
            .map(|(idx, source)| HybridSearchHit {
                event_id: source.event_id.clone(),
                snippet: source.snippet.clone(),
                score: cfg.bm25_weight / (cfg.rrf_k + (idx + 1) as f64),
                unit_type: source.unit_type.clone(),
                segment_start_ms: source.segment_start_ms,
                segment_end_ms: source.segment_end_ms,
                match_sources: vec!["bm25".to_string()],
                bm25_rank: Some(idx + 1),
                vector_rank: None,
                bm25_rrf_score: Some(
                    cfg.bm25_weight / (cfg.rrf_k + (idx + 1) as f64),
                ),
                vector_rrf_score: None,
            })
            .collect();
    }

    scored.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
    });
    scored.truncate(limit);
    Ok(scored)
}

fn search_bm25(
    conn: &Connection,
    query: &str,
    filters: &EventFilters,
    limit: usize,
) -> Result<Vec<SourceCandidate>> {
    let match_query = prepare_search_match(query);

    let mut sql = String::from(
        "SELECT e.event_id,
                snippet(transcript_search, 1, '[[H]]', '[[/H]]', '…', 12) AS snippet
         FROM transcript_search
         JOIN events e ON e.event_id = transcript_search.event_id
         JOIN transcriptions t ON t.event_id = e.event_id
         WHERE transcript_search MATCH ?
           AND t.transcription_type = ?
           AND e.event_start BETWEEN ? AND ?",
    );

    let mut params: Vec<rusqlite::types::Value> = vec![
        match_query.into(),
        filters.transcription_type.clone().into(),
        filters.start_ts.into(),
        filters.end_ts.into(),
    ];

    if let Some(camera_id) = &filters.camera_id {
        sql.push_str(" AND e.camera_id = ?");
        params.push(camera_id.clone().into());
    }

    if let (Some(cursor_start), Some(cursor_event_id)) =
        (filters.cursor_start, filters.cursor_event_id.as_ref())
    {
        sql.push_str(
            " AND (e.event_start < ? OR (e.event_start = ? AND e.event_id < ?))",
        );
        params.push(cursor_start.into());
        params.push(cursor_start.into());
        params.push(cursor_event_id.clone().into());
    }

    sql.push_str(" ORDER BY bm25(transcript_search) ASC LIMIT ?");
    params.push((limit as i64).into());

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok(SourceCandidate {
            event_id: row.get::<_, String>(0)?,
            snippet: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
            unit_type: None,
            segment_start_ms: None,
            segment_end_ms: None,
        })
    })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn search_vector(
    conn: &Connection,
    query: &str,
    filters: &EventFilters,
    limit: usize,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<Vec<SourceCandidate>> {
    let dim: i64 = conn
        .query_row(
            "SELECT embedding_dim FROM embedding_models
             WHERE model_key = ? AND is_active = 1",
            params![ACTIVE_MODEL_KEY],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(DEFAULT_EMBEDDING_DIM as i64);
    let query_embedding_result =
        embed_text(query, dim as usize, bedrock_client)?;
    maybe_record_embedding_spend(
        conn,
        &query_embedding_result,
        EMBEDDING_SPEND_OPERATION_QUERY,
    )?;
    let query_embedding = query_embedding_result.embedding;

    let mut sql = String::from(
        "SELECT e.event_id,
                tu.content_text,
                tu.unit_type,
                tu.start_ms,
                tu.end_ms,
                v.vector_blob
         FROM unit_embeddings ue
         JOIN unit_embedding_vec_index v ON v.embedding_id = ue.embedding_id
         JOIN transcript_units tu ON tu.unit_id = ue.unit_id
         JOIN events e ON e.event_id = tu.event_id
         JOIN transcriptions t ON t.event_id = e.event_id
         WHERE ue.model_key = ?
           AND ue.embedding_kind = ?
           AND t.transcription_type = ?
           AND e.event_start BETWEEN ? AND ?",
    );

    let mut params: Vec<rusqlite::types::Value> = vec![
        ACTIVE_MODEL_KEY.to_string().into(),
        ACTIVE_EMBEDDING_KIND.to_string().into(),
        filters.transcription_type.clone().into(),
        filters.start_ts.into(),
        filters.end_ts.into(),
    ];

    if let Some(camera_id) = &filters.camera_id {
        sql.push_str(" AND e.camera_id = ?");
        params.push(camera_id.clone().into());
    }
    if let (Some(cursor_start), Some(cursor_event_id)) =
        (filters.cursor_start, filters.cursor_event_id.as_ref())
    {
        sql.push_str(
            " AND (e.event_start < ? OR (e.event_start = ? AND e.event_id < ?))",
        );
        params.push(cursor_start.into());
        params.push(cursor_start.into());
        params.push(cursor_event_id.clone().into());
    }

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Vec<u8>>(5)?,
        ))
    })?;

    let mut ranked: Vec<(SourceCandidate, f64)> = Vec::new();
    for row in rows {
        let (event_id, content, unit_type, start_ms, end_ms, vector_blob) =
            row?;
        let candidate_embedding = deserialize_embedding(&vector_blob)
            .context("failed to decode embedding blob")?;
        let score = cosine_similarity(&query_embedding, &candidate_embedding);
        ranked.push((
            SourceCandidate {
                event_id,
                snippet: content.chars().take(220).collect::<String>(),
                unit_type: Some(unit_type),
                segment_start_ms: start_ms,
                segment_end_ms: end_ms,
            },
            score,
        ));
    }

    ranked.sort_by(|left, right| {
        right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal)
    });

    let mut dedup: HashMap<String, (SourceCandidate, f64)> = HashMap::new();
    for (candidate, score) in ranked {
        match dedup.get(&candidate.event_id) {
            Some((_existing, existing_score)) if *existing_score >= score => {}
            _ => {
                dedup.insert(candidate.event_id.clone(), (candidate, score));
            }
        }
    }

    let mut deduped: Vec<(SourceCandidate, f64)> =
        dedup.into_values().collect();
    deduped.sort_by(|left, right| {
        right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal)
    });

    Ok(deduped
        .into_iter()
        .take(limit)
        .map(|(candidate, _)| candidate)
        .collect())
}

fn deterministic_embedding_id(
    unit_id: &str,
    model_key: &str,
    embedding_kind: &str,
    content_version: i64,
) -> String {
    let input = format!(
        "{}::{}::{}::{}",
        unit_id, model_key, embedding_kind, content_version
    );
    format!("emb:{}", hash_hex(&input))
}

fn serialize_embedding(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn deserialize_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        anyhow::bail!("embedding blob length must be multiple of 4");
    }
    let mut out = Vec::with_capacity(bytes.len() / 4);
    for chunk in bytes.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(out)
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let x = *x as f64;
        let y = *y as f64;
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a.sqrt() * norm_b.sqrt())
}

fn embed_text_deterministic(text: &str, dim: usize) -> Vec<f32> {
    let mut vec = vec![0.0f32; dim];
    if text.trim().is_empty() {
        return vec;
    }

    for token in text
        .to_lowercase()
        .split_whitespace()
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let digest = hasher.finalize();
        let idx = (u64::from_le_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5],
            digest[6], digest[7],
        ]) % dim as u64) as usize;
        let sign = if digest[8] % 2 == 0 { 1.0 } else { -1.0 };
        vec[idx] += sign;
    }

    let norm = vec
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt();
    if norm > 0.0 {
        for value in &mut vec {
            *value /= norm as f32;
        }
    }
    vec
}

fn embed_text(
    text: &str,
    dim: usize,
    bedrock_client: Option<&Arc<dyn BedrockClientTrait>>,
) -> Result<EmbeddingCallResult> {
    let Some(client) = bedrock_client else {
        return Ok(EmbeddingCallResult {
            embedding: embed_text_deterministic(text, dim),
            usage: None,
            request_id: None,
        });
    };

    if dim == 0 {
        return Ok(EmbeddingCallResult {
            embedding: Vec::new(),
            usage: None,
            request_id: None,
        });
    }

    let model_id = ACTIVE_PROVIDER_MODEL_ID.to_string();
    let text_owned = text.to_string();
    let client = Arc::clone(client);
    let dim_i32 =
        i32::try_from(dim).context("embedding dimension too large")?;

    let run_call = move || -> Result<EmbeddingCallResult> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("create tokio runtime for bedrock embedding call")?;
        let response = runtime.block_on(client.embed_text(
            &model_id,
            &text_owned,
            dim_i32,
        ))?;
        if response.embedding.len() != dim {
            return Err(anyhow::anyhow!(
                "bedrock embedding dimension mismatch: expected {}, got {}",
                dim,
                response.embedding.len()
            ));
        }
        Ok(EmbeddingCallResult {
            embedding: response.embedding,
            usage: Some(response.usage),
            request_id: response.request_id,
        })
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        let handle = std::thread::spawn(run_call);
        return handle.join().map_err(|_| {
            anyhow::anyhow!("bedrock embedding thread panicked")
        })?;
    }

    run_call()
}

#[derive(Debug, Clone)]
struct EmbeddingCallResult {
    embedding: Vec<f32>,
    usage: Option<BedrockUsage>,
    request_id: Option<String>,
}

fn maybe_record_embedding_spend(
    conn: &Connection,
    result: &EmbeddingCallResult,
    operation: &'static str,
) -> Result<()> {
    let Some(usage) = result.usage.clone() else {
        return Ok(());
    };

    let spend_request = SpendLogRequest {
        category: EMBEDDING_SPEND_CATEGORY,
        operation,
        model_id: ACTIVE_PROVIDER_MODEL_ID,
        request_id: result.request_id.as_deref(),
        usage,
    };

    match record_bedrock_spend(conn, spend_request.clone()) {
        Ok(_) => Ok(()),
        Err(error) if is_missing_pricing_error(&error) => {
            let aws_region = std::env::var("AWS_REGION").ok();
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                if let Ok(Some(rate)) =
                    runtime.block_on(fetch_bedrock_pricing_from_aws(
                        ACTIVE_PROVIDER_MODEL_ID,
                        aws_region.as_deref(),
                    ))
                {
                    upsert_bedrock_pricing(
                        conn,
                        ACTIVE_PROVIDER_MODEL_ID,
                        rate,
                    )?;
                    record_bedrock_spend(conn, spend_request)?;
                    return Ok(());
                }
            }

            warn!(
                model_id = ACTIVE_PROVIDER_MODEL_ID,
                error = %error,
                "Skipping embedding spend log due to missing Bedrock pricing"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn build_transcript_units(
    event_id: &str,
    raw_json: &Value,
) -> Vec<TranscriptUnitDraft> {
    let full_text = collect_full_text(raw_json);
    let mut drafts = Vec::new();

    if !full_text.trim().is_empty() {
        drafts.push(TranscriptUnitDraft {
            unit_id: format!("{}:full_clip", event_id),
            unit_type: "full_clip",
            segment_strategy: None,
            start_ms: None,
            end_ms: None,
            content_hash: hash_hex(&full_text),
            content_text: full_text,
        });
    }

    for segment in build_segments(raw_json) {
        if segment.content_text.trim().is_empty() {
            continue;
        }
        drafts.push(TranscriptUnitDraft {
            unit_id: format!(
                "{}:segment:{}:{}:{}",
                event_id,
                ACTIVE_SEGMENT_STRATEGY,
                segment.start_ms,
                segment.end_ms
            ),
            unit_type: "segment",
            segment_strategy: Some(ACTIVE_SEGMENT_STRATEGY),
            start_ms: Some(segment.start_ms),
            end_ms: Some(segment.end_ms),
            content_hash: hash_hex(&segment.content_text),
            content_text: segment.content_text,
        });
    }

    drafts
}

fn collect_full_text(raw_json: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(text) = raw_json.get("text").and_then(Value::as_str) {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            parts.push(trimmed.to_string());
        }
    }

    if let Some(segments) = raw_json.get("segments").and_then(Value::as_array) {
        for segment in segments {
            if let Some(text) = segment.get("text").and_then(Value::as_str) {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
            }
        }
    }

    parts.join(" ")
}

fn upsert_transcript_search_content(
    conn: &Connection,
    event_id: &str,
    raw_json: &Value,
) -> Result<()> {
    conn.execute(
        "DELETE FROM transcript_search WHERE event_id = ?",
        params![event_id],
    )?;
    let content = collect_full_text(raw_json);
    if !content.trim().is_empty() {
        conn.execute(
            "INSERT INTO transcript_search (event_id, content) VALUES (?, ?)",
            params![event_id, content],
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct SegmentDraft {
    start_ms: i64,
    end_ms: i64,
    content_text: String,
}

fn build_segments(raw_json: &Value) -> Vec<SegmentDraft> {
    let mut timed_segments = Vec::new();
    if let Some(segments) = raw_json.get("segments").and_then(Value::as_array) {
        for segment in segments {
            let start =
                segment.get("start").and_then(Value::as_f64).unwrap_or(0.0);
            let end = segment
                .get("end")
                .and_then(Value::as_f64)
                .unwrap_or(start + 1.0)
                .max(start + 0.1);
            let text = segment
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();
            if text.is_empty() {
                continue;
            }
            timed_segments.push((
                (start * 1000.0).round() as i64,
                (end * 1000.0).round() as i64,
                text,
            ));
        }
    }

    if timed_segments.is_empty() {
        let full_text = collect_full_text(raw_json);
        if full_text.trim().is_empty() {
            return Vec::new();
        }
        return vec![SegmentDraft {
            start_ms: 0,
            end_ms: SEGMENT_WINDOW_MS,
            content_text: full_text,
        }];
    }

    let mut max_end = 0i64;
    for (_, end_ms, _) in &timed_segments {
        max_end = max_end.max(*end_ms);
    }
    if max_end <= 0 {
        return Vec::new();
    }

    let step = SEGMENT_WINDOW_MS - SEGMENT_OVERLAP_MS;
    let mut windows = Vec::new();
    let mut start = 0i64;
    while start <= max_end {
        let end = (start + SEGMENT_WINDOW_MS).max(start + 1);
        let mut window_parts = Vec::new();
        for (seg_start, seg_end, text) in &timed_segments {
            if *seg_end > start && *seg_start < end {
                window_parts.push(text.clone());
            }
        }
        if !window_parts.is_empty() {
            windows.push(SegmentDraft {
                start_ms: start,
                end_ms: end,
                content_text: window_parts.join(" "),
            });
        }
        if end >= max_end {
            break;
        }
        start += step;
    }

    windows
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

fn hash_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    format!("{:x}", digest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_zumblezay_db;

    fn sample_transcript_json() -> Value {
        serde_json::json!({
            "text": "alpha beta gamma delta epsilon",
            "segments": [
                {"start": 0.0, "end": 8.0, "text": "alpha beta"},
                {"start": 8.0, "end": 20.0, "text": "gamma delta"},
                {"start": 25.0, "end": 41.0, "text": "epsilon zeta"}
            ]
        })
    }

    #[test]
    fn segment_generation_uses_30s_window_10s_overlap() {
        let raw = serde_json::json!({
            "segments": [
                {"start": 0.0, "end": 6.0, "text": "a"},
                {"start": 22.0, "end": 28.0, "text": "b"},
                {"start": 35.0, "end": 42.0, "text": "c"}
            ]
        });

        let segments = build_segments(&raw);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].start_ms, 0);
        assert_eq!(segments[0].end_ms, 30_000);
        assert_eq!(segments[1].start_ms, 20_000);
        assert_eq!(segments[1].end_ms, 50_000);
    }

    #[test]
    fn transcript_units_upsert_is_deterministic_and_idempotent() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_zumblezay_db(&mut conn)?;

        let raw = sample_transcript_json();
        index_transcript_units_for_event(&conn, "evt-1", &raw, 0.0)?;
        index_transcript_units_for_event(&conn, "evt-1", &raw, 0.0)?;

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM transcript_units WHERE event_id = ?",
            params!["evt-1"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 3); // 1 full + 2 overlapped segments for this input

        let version_sum: i64 = conn.query_row(
            "SELECT SUM(content_version) FROM transcript_units WHERE event_id = ?",
            params!["evt-1"],
            |row| row.get(0),
        )?;
        assert_eq!(version_sum, count);

        Ok(())
    }

    #[test]
    fn unit_embeddings_unique_by_version() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_zumblezay_db(&mut conn)?;

        let raw = sample_transcript_json();
        index_transcript_units_for_event(&conn, "evt-emb", &raw, 0.0)?;
        process_pending_embedding_jobs(&conn, 100)?;

        let first_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM unit_embeddings",
            [],
            |row| row.get(0),
        )?;
        assert!(first_count > 0);

        index_transcript_units_for_event(
            &conn,
            "evt-emb",
            &serde_json::json!({
                "text": "completely different",
                "segments": [
                    {"start": 0.0, "end": 5.0, "text": "completely different"}
                ]
            }),
            0.0,
        )?;
        process_pending_embedding_jobs(&conn, 100)?;

        let second_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM unit_embeddings",
            [],
            |row| row.get(0),
        )?;
        assert!(second_count > first_count);

        Ok(())
    }

    #[test]
    fn rrf_fusion_and_ties_work() {
        let cfg = SearchConfig {
            rrf_k: 60.0,
            bm25_weight: 1.0,
            vector_weight: 1.0,
        };
        let mut bm25: HashMap<String, usize> = HashMap::new();
        let mut vector: HashMap<String, usize> = HashMap::new();

        bm25.insert("a".to_string(), 1);
        bm25.insert("b".to_string(), 2);
        vector.insert("b".to_string(), 1);
        vector.insert("a".to_string(), 2);

        let score_a = cfg.bm25_weight / (cfg.rrf_k + 1.0)
            + cfg.vector_weight / (cfg.rrf_k + 2.0);
        let score_b = cfg.bm25_weight / (cfg.rrf_k + 2.0)
            + cfg.vector_weight / (cfg.rrf_k + 1.0);
        assert!((score_a - score_b).abs() < 1e-9);
    }

    #[test]
    fn embedding_job_retry_transitions_status() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_zumblezay_db(&mut conn)?;

        let now = chrono::Utc::now().timestamp();
        conn.execute(
            "INSERT INTO embedding_jobs (
                event_id, unit_id, job_type, priority, status,
                attempt_count, next_attempt_at, created_at, updated_at
             ) VALUES ('evt-retry', NULL, 'segment', 10, 'pending', 0, NULL, ?, ?)",
            params![now, now],
        )?;

        let processed = process_pending_embedding_jobs(&conn, 10)?;
        assert_eq!(processed, 0);

        let status: String = conn.query_row(
            "SELECT status FROM embedding_jobs WHERE event_id = 'evt-retry'",
            [],
            |row| row.get(0),
        )?;
        assert_eq!(status, "pending");

        let next_attempt: Option<i64> = conn.query_row(
            "SELECT next_attempt_at FROM embedding_jobs WHERE event_id = 'evt-retry'",
            [],
            |row| row.get(0),
        )?;
        assert!(next_attempt.is_some());

        Ok(())
    }

    #[test]
    fn freshness_path_indexes_new_transcript() -> Result<()> {
        let mut conn = Connection::open_in_memory()?;
        init_zumblezay_db(&mut conn)?;

        conn.execute(
            "INSERT INTO events (
                event_id, created_at, event_start, event_end,
                event_type, camera_id, video_path
             ) VALUES ('evt-fresh', 0, 0.0, 30.0, 'motion', 'cam-a', '/tmp/a.mp4')",
            [],
        )?;
        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
             ) VALUES ('evt-fresh', 0, 'whisper-local', 'u', 10, '{\"text\":\"child lunch\"}')",
            [],
        )?;
        conn.execute(
            "INSERT INTO transcript_search (event_id, content)
             VALUES ('evt-fresh', 'child lunch')",
            [],
        )?;

        index_transcript_units_for_event(
            &conn,
            "evt-fresh",
            &serde_json::json!({"text": "child lunch"}),
            0.0,
        )?;
        process_pending_embedding_jobs(&conn, 20)?;

        let filters = EventFilters {
            transcription_type: "whisper-local".to_string(),
            start_ts: -1.0,
            end_ts: 1000.0,
            camera_id: None,
            cursor_start: None,
            cursor_event_id: None,
        };
        let hits = search_events_hybrid(&conn, "child lunch", &filters, 10)?;
        assert!(!hits.is_empty());
        assert_eq!(hits[0].event_id, "evt-fresh");

        Ok(())
    }
}
