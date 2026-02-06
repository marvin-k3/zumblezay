use crate::AppState;
use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};

const DEFAULT_JOB_LIMIT: usize = 10;
const DEFAULT_LEASE_SECONDS: i64 = 300;
const RETRY_BASE_DELAY_SECONDS: i64 = 60;

#[derive(Debug, Serialize)]
pub struct VisionJobClaim {
    pub job_id: i64,
    pub event_id: String,
    pub analysis_type: String,
    pub depends_on: Option<String>,
    pub attempt_count: i32,
    pub video_url: String,
}

#[derive(Debug, Deserialize)]
pub struct ClaimJobsRequest {
    pub worker_id: String,
    pub max_jobs: Option<usize>,
    pub lease_seconds: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ClaimJobsResponse {
    pub lease_seconds: i64,
    pub jobs: Vec<VisionJobClaim>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitResultRequest {
    pub worker_id: String,
    pub job_id: i64,
    pub event_id: String,
    pub analysis_type: String,
    pub duration_ms: i64,
    pub raw_response: serde_json::Value,
    pub model: Option<String>,
    pub version: Option<String>,
    pub hash: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct SubmitFailureRequest {
    pub worker_id: String,
    pub job_id: i64,
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct EnqueueTestJobsRequest {
    pub date: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct EnqueueTestJobsResponse {
    pub date: Option<String>,
    pub requested: usize,
    pub enqueued: usize,
}

pub fn enqueue_missing_jobs(
    state: &AppState,
    analysis_type: &str,
    depends_on: Option<&str>,
) -> Result<usize> {
    let conn = state.zumblezay_db.get()?;
    let rows = conn.execute(
        "INSERT INTO vision_jobs (
            event_id, analysis_type, depends_on, status, created_at, priority
        )
        SELECT e.event_id, ?, ?, 'pending', unixepoch(), 0
        FROM events e
        LEFT JOIN vision_jobs v
            ON v.event_id = e.event_id
           AND v.analysis_type = ?
        LEFT JOIN vision_results r
            ON r.event_id = e.event_id
           AND r.analysis_type = ?
        WHERE v.id IS NULL AND r.id IS NULL",
        params![analysis_type, depends_on, analysis_type, analysis_type],
    )?;
    Ok(rows)
}

pub fn enqueue_test_jobs(
    state: &AppState,
    request: &EnqueueTestJobsRequest,
) -> Result<EnqueueTestJobsResponse> {
    let limit = request.limit.unwrap_or(3).max(1);
    let conn = state.zumblezay_db.get()?;

    let (sql, params): (&str, Vec<Box<dyn rusqlite::ToSql>>) = if let Some(
        date,
    ) =
        request.date.clone()
    {
        (
                "INSERT INTO vision_jobs (
                    event_id, analysis_type, depends_on, status, created_at, priority
                )
                SELECT e.event_id, 'yolo_boxes', NULL, 'pending', unixepoch(), 0
                FROM events e
                LEFT JOIN vision_jobs v
                    ON v.event_id = e.event_id
                   AND v.analysis_type = 'yolo_boxes'
                LEFT JOIN vision_results r
                    ON r.event_id = e.event_id
                   AND r.analysis_type = 'yolo_boxes'
                WHERE date(e.event_start, 'unixepoch') = ?
                  AND e.video_path IS NOT NULL
                  AND v.id IS NULL
                  AND r.id IS NULL
                ORDER BY random()
                LIMIT ?",
                vec![Box::new(date), Box::new(limit as i64)],
            )
    } else {
        (
                "INSERT INTO vision_jobs (
                    event_id, analysis_type, depends_on, status, created_at, priority
                )
                SELECT e.event_id, 'yolo_boxes', NULL, 'pending', unixepoch(), 0
                FROM events e
                LEFT JOIN vision_jobs v
                    ON v.event_id = e.event_id
                   AND v.analysis_type = 'yolo_boxes'
                LEFT JOIN vision_results r
                    ON r.event_id = e.event_id
                   AND r.analysis_type = 'yolo_boxes'
                WHERE e.video_path IS NOT NULL
                  AND v.id IS NULL
                  AND r.id IS NULL
                ORDER BY random()
                LIMIT ?",
                vec![Box::new(limit as i64)],
            )
    };

    let mut stmt = conn.prepare(sql)?;
    let enqueued = stmt.execute(rusqlite::params_from_iter(params))?;

    Ok(EnqueueTestJobsResponse {
        date: request.date.clone(),
        requested: limit,
        enqueued,
    })
}

pub fn claim_jobs(
    state: &AppState,
    request: &ClaimJobsRequest,
) -> Result<ClaimJobsResponse> {
    let max_jobs = request.max_jobs.unwrap_or(DEFAULT_JOB_LIMIT).max(1);
    let lease_seconds = request
        .lease_seconds
        .unwrap_or(DEFAULT_LEASE_SECONDS)
        .max(30);

    let mut conn = state.zumblezay_db.get()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let ids: Vec<i64> = {
        let mut stmt = tx.prepare(
            "SELECT v.id
             FROM vision_jobs v
             LEFT JOIN vision_results r
                ON r.event_id = v.event_id
               AND r.analysis_type = v.depends_on
             WHERE (
                   v.status = 'pending'
                   OR (v.status = 'in_progress'
                       AND v.locked_at <= unixepoch() - ?)
               )
               AND (v.depends_on IS NULL OR r.event_id IS NOT NULL)
               AND (v.next_retry_at IS NULL OR v.next_retry_at <= unixepoch())
             ORDER BY v.priority DESC, v.created_at ASC
             LIMIT ?",
        )?;
        let rows = stmt
            .query_map(params![lease_seconds, max_jobs as i64], |row| {
                row.get(0)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows
    };

    if ids.is_empty() {
        tx.commit()?;
        return Ok(ClaimJobsResponse {
            lease_seconds,
            jobs: Vec::new(),
        });
    }

    {
        let mut updated = tx.prepare(
            "UPDATE vision_jobs
             SET status = 'in_progress',
                 locked_at = unixepoch(),
                 locked_by = ?,
                 attempt_count = attempt_count + 1,
                 next_retry_at = NULL
             WHERE id = ?
               AND (
                    status = 'pending'
                    OR (status = 'in_progress'
                        AND locked_at <= unixepoch() - ?)
               )",
        )?;

        for id in &ids {
            updated.execute(params![request.worker_id, id, lease_seconds])?;
        }
    }

    let mut jobs = Vec::with_capacity(ids.len());
    {
        let mut job_stmt = tx.prepare(
            "SELECT id, event_id, analysis_type, depends_on, attempt_count
             FROM vision_jobs
             WHERE id = ?",
        )?;

        for id in ids {
            let job = job_stmt
                .query_row(params![id], |row| {
                    Ok(VisionJobClaim {
                        job_id: row.get(0)?,
                        event_id: row.get(1)?,
                        analysis_type: row.get(2)?,
                        depends_on: row.get(3)?,
                        attempt_count: row.get(4)?,
                        video_url: format!(
                            "/video/{}",
                            row.get::<_, String>(1)?
                        ),
                    })
                })
                .with_context(|| format!("missing vision job {}", id))?;
            jobs.push(job);
        }
    }

    tx.commit()?;

    Ok(ClaimJobsResponse {
        lease_seconds,
        jobs,
    })
}

pub fn submit_result(
    state: &AppState,
    request: &SubmitResultRequest,
) -> Result<()> {
    let mut conn = state.zumblezay_db.get()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let raw_response = serde_json::to_string(&request.raw_response)
        .context("failed to serialize vision raw_response to string")?;
    let metadata = match &request.metadata {
        Some(value) => Some(
            serde_json::to_string(value)
                .context("failed to serialize vision metadata to string")?,
        ),
        None => None,
    };

    tx.execute(
        "INSERT INTO vision_results (
            event_id, analysis_type, created_at, duration_ms,
            raw_response, model, version, hash, metadata
        ) VALUES (?, ?, unixepoch(), ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, analysis_type, model, version)
        DO UPDATE SET
            created_at = excluded.created_at,
            duration_ms = excluded.duration_ms,
            raw_response = excluded.raw_response,
            hash = excluded.hash,
            metadata = excluded.metadata",
        params![
            request.event_id,
            request.analysis_type,
            request.duration_ms,
            raw_response,
            request.model,
            request.version,
            request.hash,
            metadata,
        ],
    )?;

    let updated = tx.execute(
        "UPDATE vision_jobs
         SET status = 'completed',
             locked_at = NULL,
             locked_by = NULL,
             last_error = NULL
         WHERE id = ? AND locked_by = ?",
        params![request.job_id, request.worker_id],
    )?;

    if updated == 0 {
        let job_exists: Option<i64> = tx
            .query_row(
                "SELECT id FROM vision_jobs WHERE id = ?",
                params![request.job_id],
                |row| row.get(0),
            )
            .optional()?;
        if job_exists.is_none() {
            return Err(anyhow::anyhow!(
                "vision job {} not found",
                request.job_id
            ));
        }
        return Err(anyhow::anyhow!(
            "vision job {} not locked by {}",
            request.job_id,
            request.worker_id
        ));
    }

    tx.commit()?;
    Ok(())
}

pub fn submit_failure(
    state: &AppState,
    request: &SubmitFailureRequest,
) -> Result<()> {
    let mut conn = state.zumblezay_db.get()?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let attempt_count: i32 = tx.query_row(
        "SELECT attempt_count FROM vision_jobs WHERE id = ?",
        params![request.job_id],
        |row| row.get(0),
    )?;

    let backoff =
        RETRY_BASE_DELAY_SECONDS * (1i64 << attempt_count.saturating_sub(1));

    let updated = tx.execute(
        "UPDATE vision_jobs
         SET status = 'pending',
             locked_at = NULL,
             locked_by = NULL,
             last_error = ?,
             next_retry_at = unixepoch() + ?
         WHERE id = ? AND locked_by = ?",
        params![request.error, backoff, request.job_id, request.worker_id],
    )?;

    if updated == 0 {
        return Err(anyhow::anyhow!(
            "vision job {} not locked by {}",
            request.job_id,
            request.worker_id
        ));
    }

    tx.commit()?;
    Ok(())
}
