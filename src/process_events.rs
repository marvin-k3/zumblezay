use crate::transcription;
use crate::transcripts;
use crate::AppState;
use crate::Event;
use anyhow::Result;
use async_trait::async_trait;
use rusqlite::params;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, instrument};

const MAX_RETRY_ATTEMPTS: i32 = 5;
const INITIAL_RETRY_DELAY_SECS: i64 = 60; // 1 minute

#[derive(Debug, Clone)]
struct EventRetry {
    retry_attempt: Option<i32>,
    last_attempt: Option<i64>,
}

// Define a trait for transcription service

#[async_trait]
pub trait TranscriptionService: Send + Sync {
    async fn get_transcript(
        &self,
        event: &Event,
        state: &AppState,
    ) -> Result<(String, i64)>;

    fn as_str(&self) -> &'static str;
}

// Implement the trait for the real service
#[derive(Clone)]
pub struct RealTranscriptionService;
#[async_trait]
impl TranscriptionService for RealTranscriptionService {
    async fn get_transcript(
        &self,
        event: &Event,
        state: &AppState,
    ) -> Result<(String, i64)> {
        transcription::get_transcript(event, state).await
    }

    fn as_str(&self) -> &'static str {
        "whisper"
    }
}

// Add a factory function to get the appropriate service
pub fn get_transcription_service(
    service_type: &str,
) -> Arc<dyn TranscriptionService> {
    match service_type {
        "whisper-local" | "runpod" => Arc::new(RealTranscriptionService),
        unknown => panic!("Unknown transcription service: {}", unknown),
    }
}

#[instrument(skip(state))]
pub async fn process_events(state: Arc<AppState>) {
    info!("Starting event processing loop");

    // Create a channel for shutdown signal
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1);

    // Clone the sender for the signal handler
    let shutdown_tx_clone = shutdown_tx.clone();

    // Set up ctrl-c handler
    tokio::spawn(async move {
        if let Ok(()) = tokio::signal::ctrl_c().await {
            info!("Received shutdown signal");
            let _ = shutdown_tx_clone.send(());
        }
    });

    // Create a JoinSet to track running tasks
    let mut tasks = tokio::task::JoinSet::new();

    // Create the transcription service once
    let transcription_service =
        get_transcription_service(&state.transcription_service);

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutting down event processing loop - waiting for current tasks to complete");
                break;
            }
            _ = async {
                match fetch_new_events(&state).await {
                    Ok(events) => {
                        for event in events {
                            let state = state.clone();
                            let event = event.clone();
                            let transcription_service = transcription_service.clone();

                            // Acquire semaphore permit before spawning task
                            let permit = state.semaphore.clone().acquire_owned().await.unwrap();

                            tasks.spawn(async move {
                                let _permit = permit; // Keep permit alive for duration of task
                                if let Err(e) = process_single_event(&state, event, transcription_service.as_ref()).await {
                                    error!("Error processing event: {}", e);
                                }
                            });
                        }
                    }
                    Err(e) => error!("Error fetching events: {}", e),
                }
                time::sleep(Duration::from_secs(30)).await;
            } => {}
        }
    }

    // Wait for all tasks to complete with a timeout
    let shutdown_timeout = Duration::from_secs(30);
    let shutdown_deadline = tokio::time::Instant::now() + shutdown_timeout;

    while let Some(result) =
        tokio::time::timeout_at(shutdown_deadline, tasks.join_next())
            .await
            .unwrap_or(None)
    {
        if let Err(e) = result {
            error!("Task failed during shutdown: {}", e);
        }
    }

    info!("Event processing loop terminated");
}

// Modify process_single_event to use the new Event fields
#[instrument(skip(state, transcription_service), err)]
async fn process_single_event(
    state: &AppState,
    (event, retry_info): (Event, EventRetry),
    transcription_service: &dyn TranscriptionService,
) -> Result<()> {
    {
        let mut active_tasks = state.active_tasks.lock().await;
        active_tasks.insert(event.id.clone(), "Processing started".to_string());
    }
    debug!(
        "Processing event {} with retry attempt {:?} and last attempt {:?}",
        event.id, retry_info.retry_attempt, retry_info.last_attempt
    );

    // Process the event
    let result = match transcription_service.get_transcript(&event, state).await
    {
        Ok((raw_response, duration_ms)) => {
            // If successful and was previously marked as corrupted, update the status
            if retry_info.retry_attempt.is_some() {
                let conn = state.zumblezay_db.get()?;
                conn.execute(
                    "UPDATE corrupted_files SET status = 'resolved' WHERE event_id = ?",
                    params![event.id],
                )?;
            }

            transcripts::save_transcript(
                state,
                &event,
                &raw_response,
                duration_ms,
            )
            .await
            .map_err(|e| {
                error!("Error saving transcript for event {}: {}", event.id, e);
                state.stats.error_count.fetch_add(1, Ordering::Relaxed);
                e
            })?;

            state.stats.processed_count.fetch_add(1, Ordering::Relaxed);
            state
                .stats
                .total_processing_time_ms
                .fetch_add(duration_ms as u64, Ordering::Relaxed);

            let mut last_time = state.last_processed_time.lock().await;
            *last_time = event.start;
            info!(
                "Successfully processed event {} in {}ms",
                event.id, duration_ms
            );
            Ok(())
        }
        Err(e) => {
            error!("Error processing video for event {}: {}", event.id, e);
            state.stats.error_count.fetch_add(1, Ordering::Relaxed);

            // Update or insert into corrupted_files
            let now = chrono::Utc::now().timestamp();
            let attempt_count = retry_info.retry_attempt.unwrap_or(0) + 1;

            let conn = state.zumblezay_db.get()?;
            conn.execute(
                "INSERT INTO corrupted_files 
                (event_id, video_path, first_failure_at, last_attempt_at, attempt_count, last_error) 
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET 
                last_attempt_at = ?,
                attempt_count = ?,
                last_error = ?
                WHERE status = 'failed'",
                params![
                    event.id,
                    event.path,
                    now,
                    now,
                    attempt_count,
                    e.to_string(),
                    now,
                    attempt_count,
                    e.to_string()
                ],
            )?;

            Err(e)
        }
    };

    // Always remove the task from active tasks
    {
        let mut active_tasks = state.active_tasks.lock().await;
        active_tasks.remove(&event.id);
    }

    result
}

#[instrument(skip(state), err)]
async fn fetch_new_events(
    state: &AppState,
) -> Result<Vec<(Event, EventRetry)>> {
    let transcription_type = state.transcription_service.as_str();
    info!(
        "Fetching events without transcriptions for {}",
        transcription_type
    );
    let camera_names: HashMap<String, String> =
        state.camera_name_cache.lock().await.clone();

    let conn = state.zumblezay_db.get()?;
    let mut stmt = conn.prepare(
        "SELECT 
            e.event_id, e.event_type, e.camera_id, 
            e.event_start, e.event_end, e.video_path,
            cf.attempt_count, cf.last_attempt_at
         FROM events e
         LEFT JOIN transcriptions t ON 
            e.event_id = t.event_id 
            AND t.transcription_type = ?
         LEFT JOIN corrupted_files cf ON
            e.event_id = cf.event_id
            AND cf.status = 'failed'
         WHERE 
            t.event_id IS NULL
            AND (
                cf.event_id IS NULL
                OR (
                    cf.attempt_count < ?
                    AND cf.last_attempt_at < unixepoch() - (? * (1 << (cf.attempt_count - 1)))
                )
            )
         ORDER BY e.event_start DESC 
         LIMIT 20",
    )?;

    let events = stmt.query_map(
        params![
            transcription_type,
            MAX_RETRY_ATTEMPTS,
            INITIAL_RETRY_DELAY_SECS
        ],
        |row| {
            Ok((
                Event {
                    id: row.get(0)?,
                    type_: row.get(1)?,
                    camera_id: row.get(2)?,
                    camera_name: camera_names
                        .get(&row.get::<_, String>(2)?)
                        .cloned(),
                    start: row.get(3)?,
                    end: row.get(4)?,
                    path: row.get(5)?,
                },
                EventRetry {
                    retry_attempt: row.get(6)?,
                    last_attempt: row.get(7)?,
                },
            ))
        },
    )?;

    let events: Result<Vec<_>, _> = events.collect();
    let events = events?;

    info!("Found {} events needing transcription", events.len());
    Ok(events)
}

#[cfg(test)]
mod process_events_tests {
    use super::*;
    use crate::test_utils::init_test_logging;
    use crate::time_util;

    // Helper function to set up a test database with events and transcriptions
    async fn setup_test_db(state: &AppState) -> Result<(), anyhow::Error> {
        // Get the correct timestamp range for 2023-01-01
        let conn = state.zumblezay_db.get()?;
        let (start_ts, _) = time_util::parse_local_date_to_utc_range_with_time(
            "2023-01-01",
            &None,
            &None,
            state.timezone,
        )?;

        // Use timestamps within this range
        let event1_ts = start_ts + 3600.0; // 1 hour into the day
        let event2_ts = start_ts + 7200.0; // 2 hours into the day

        // Insert test events with timestamps in the correct range
        conn.execute(
            "INSERT INTO events (
                event_id, created_at, event_start, event_end, 
                event_type, camera_id, video_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                "test-event-1",
                start_ts,
                event1_ts,
                event1_ts + 10.0,
                "motion",
                "camera1",
                "/data/videos/test1.mp4",
            ],
        )?;

        conn.execute(
            "INSERT INTO events (
                event_id, created_at, event_start, event_end, 
                event_type, camera_id, video_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                "test-event-2",
                start_ts,
                event2_ts,
                event2_ts + 20.0,
                "person",
                "camera2",
                "/data/videos/test2.mp4",
            ],
        )?;

        // Add camera names to the cache
        {
            let mut camera_names = state.camera_name_cache.lock().await;
            camera_names
                .insert("camera1".to_string(), "Living Room".to_string());
            camera_names
                .insert("camera2".to_string(), "Front Door".to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_successful_transcription() -> Result<(), anyhow::Error> {
        init_test_logging();
        let state = AppState::new_for_testing();
        setup_test_db(&state).await?;

        // Create a test event
        let event = Event {
            id: "test-event-1".to_string(),
            type_: "motion".to_string(),
            camera_id: "camera1".to_string(),
            camera_name: Some("Living Room".to_string()),
            start: 1672531200.0, // 2023-01-01 00:00:00
            end: 1672531210.0,
            path: Some("/data/videos/test1.mp4".to_string()),
        };

        let retry_info = EventRetry {
            retry_attempt: None,
            last_attempt: None,
        };

        // Create a mock transcription service that succeeds
        struct MockTranscriptionService;
        #[async_trait]
        impl TranscriptionService for MockTranscriptionService {
            async fn get_transcript(
                &self,
                _event: &Event,
                _state: &AppState,
            ) -> Result<(String, i64)> {
                Ok((r#"{"text": "Test transcript"}"#.to_string(), 100))
            }

            fn as_str(&self) -> &'static str {
                "mock"
            }
        }

        // Process the event with the mock service
        process_single_event(
            &state,
            (event, retry_info),
            &MockTranscriptionService,
        )
        .await?;

        // Verify the transcript was saved
        let conn = state.zumblezay_db.get()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM transcriptions WHERE event_id = ?",
            params!["test-event-1"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        // Verify stats were updated
        assert_eq!(
            state
                .stats
                .processed_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            state
                .stats
                .error_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_transcription() -> Result<(), anyhow::Error> {
        init_test_logging();
        let state = AppState::new_for_testing();
        setup_test_db(&state).await?;

        // Create a test event
        let event = Event {
            id: "test-event-1".to_string(),
            type_: "motion".to_string(),
            camera_id: "camera1".to_string(),
            camera_name: Some("Living Room".to_string()),
            start: 1672531200.0,
            end: 1672531210.0,
            path: Some("/data/videos/test1.mp4".to_string()),
        };

        let retry_info = EventRetry {
            retry_attempt: None,
            last_attempt: None,
        };

        // Create a mock transcription service that fails
        struct MockTranscriptionService;
        #[async_trait]
        impl TranscriptionService for MockTranscriptionService {
            async fn get_transcript(
                &self,
                _event: &Event,
                _state: &AppState,
            ) -> Result<(String, i64)> {
                Err(anyhow::anyhow!("Transcription failed"))
            }

            fn as_str(&self) -> &'static str {
                "mock"
            }
        }

        // Process the event with the mock service
        process_single_event(
            &state,
            (event, retry_info),
            &MockTranscriptionService,
        )
        .await
        .expect_err("Expected transcription to fail");

        // Verify error was recorded in corrupted_files
        let conn = state.zumblezay_db.get()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM corrupted_files WHERE event_id = ? AND status = 'failed'",
            params!["test-event-1"],
            |row| row.get(0),
        )?;
        assert_eq!(count, 1);

        // Verify stats were updated
        assert_eq!(
            state
                .stats
                .processed_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            state
                .stats
                .error_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retry_resolution() -> Result<(), anyhow::Error> {
        init_test_logging();
        let state = AppState::new_for_testing();
        setup_test_db(&state).await?;

        // Create a test event that was previously failed
        let event = Event {
            id: "test-event-1".to_string(),
            type_: "motion".to_string(),
            camera_id: "camera1".to_string(),
            camera_name: Some("Living Room".to_string()),
            start: 1672531200.0,
            end: 1672531210.0,
            path: Some("/data/videos/test1.mp4".to_string()),
        };

        let retry_info = EventRetry {
            retry_attempt: Some(1),
            last_attempt: Some(1672531200),
        };

        // First insert a failed record
        let conn = state.zumblezay_db.get()?;
        conn.execute(
            "INSERT INTO corrupted_files (event_id, status, first_failure_at, last_attempt_at, attempt_count, video_path, last_error) VALUES (?, 'failed', ?, ?, ?, ?, ?)",
            params!["test-event-1", 1672531200, 1672531200, 1, "/data/videos/test1.mp4", "Previous error"],
        )?;

        // Create a mock transcription service that succeeds
        struct MockTranscriptionService;
        #[async_trait]
        impl TranscriptionService for MockTranscriptionService {
            async fn get_transcript(
                &self,
                _event: &Event,
                _state: &AppState,
            ) -> Result<(String, i64)> {
                Ok((r#"{"text": "Test transcript"}"#.to_string(), 100))
            }

            fn as_str(&self) -> &'static str {
                "mock"
            }
        }

        // Process the event with the mock service
        process_single_event(
            &state,
            (event, retry_info),
            &MockTranscriptionService,
        )
        .await?;

        // Verify the corrupted_files record was updated to resolved
        let status: String = conn.query_row(
            "SELECT status FROM corrupted_files WHERE event_id = ?",
            params!["test-event-1"],
            |row| row.get(0),
        )?;
        assert_eq!(status, "resolved");

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_new_events() -> Result<(), anyhow::Error> {
        init_test_logging();
        let state = AppState::new_for_testing();
        setup_test_db(&state).await?;

        // Fetch new events
        let events = fetch_new_events(&state).await?;

        // Verify we got the expected events
        assert_eq!(events.len(), 2);

        // Verify event details (ordered by event_start DESC)
        let event1 = &events[0].0;
        assert_eq!(event1.id, "test-event-2"); // Later event first
        assert_eq!(event1.camera_id, "camera2");
        assert_eq!(event1.camera_name, Some("Front Door".to_string()));

        let event2 = &events[1].0;
        assert_eq!(event2.id, "test-event-1"); // Earlier event second
        assert_eq!(event2.camera_id, "camera1");
        assert_eq!(event2.camera_name, Some("Living Room".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_new_events_with_retries() -> Result<(), anyhow::Error> {
        init_test_logging();
        let state = AppState::new_for_testing();
        setup_test_db(&state).await?;

        // Insert a failed event with retries
        let conn = state.zumblezay_db.get()?;
        conn.execute(
            "INSERT INTO corrupted_files (event_id, status, first_failure_at, last_attempt_at, attempt_count, video_path, last_error) VALUES (?, 'failed', ?, ?, ?, ?, ?)",
            params!["test-event-1", 1672531200, 1672531200, 1, "/data/videos/test1.mp4", "Previous error"],
        )?;

        // Fetch new events
        let events = fetch_new_events(&state).await?;

        // Verify we got the expected events
        assert_eq!(events.len(), 2);

        // Verify retry info for the failed event (ordered by event_start DESC)
        let event1_retry = &events[0].1;
        assert_eq!(event1_retry.retry_attempt, None); // test-event-2 has no retries

        let event2_retry = &events[1].1;
        assert_eq!(event2_retry.retry_attempt, Some(1)); // test-event-1 has retries
        assert_eq!(event2_retry.last_attempt, Some(1672531200));

        Ok(())
    }
}
