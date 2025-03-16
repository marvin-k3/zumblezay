use crate::AppState;
use anyhow::Result;
use rusqlite::params;
use rusqlite::OptionalExtension;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::{debug, info, instrument};

#[derive(Clone)]
pub struct StoryboardData {
    pub image: Vec<u8>,
    pub vtt: String,
}

#[instrument(skip(state))]
pub async fn get_storyboard_data_from_cache(
    state: &Arc<AppState>,
    event_id: &str,
) -> Result<Option<StoryboardData>, anyhow::Error> {
    let conn = state.cache_db.get()?;
    let mut stmt = conn
        .prepare(concat!(
            "SELECT image, vtt ",
            "FROM storyboard_cache ",
            "WHERE event_id = ? ",
            "ORDER BY created_at ",
            "DESC LIMIT 1"
        ))
        .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {}", e))?;

    let storyboard_data = stmt
        .query_row(params![event_id], |row| {
            let image: Vec<u8> = row.get(0)?;
            let vtt: String = row.get(1)?;
            Ok(StoryboardData { image, vtt })
        })
        .optional()
        .map_err(|e| {
            anyhow::anyhow!("Failed to query storyboard cache: {}", e)
        })?;

    Ok(storyboard_data)
}

#[instrument(err)]
async fn generate_storyboard(
    event_id: &str,
    video_path: &str,
    duration: f64,
) -> Result<StoryboardData, anyhow::Error> {
    // Calculate dimensions and layout
    let tile_width = 160;
    let tile_height = 90;
    let tiles_per_row = 10;
    let total_tiles = duration.ceil() as u32;
    let rows = ((total_tiles as f64) / (tiles_per_row as f64)).ceil() as u32;

    // Run ffmpeg with pipe output
    let mut command = tokio::process::Command::new("ffmpeg");
    let command = command
        .arg("-i")
        .arg(video_path)
        .arg("-vf")
        .arg(format!(
            "fps=1,scale={}:{},tile={}x{}",
            tile_width, tile_height, tiles_per_row, rows
        ))
        .arg("-frames:v")
        .arg("1")
        .arg("-f")
        .arg("image2pipe")
        .arg("-")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    debug!("Running FFmpeg command: {:?}", command);

    let mut child = command
        .spawn()
        .map_err(|e| anyhow::anyhow!("Failed to spawn FFmpeg: {}", e))?;

    let mut output = Vec::new();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
    let mut reader = tokio::io::BufReader::new(stdout);

    // Read in chunks
    let mut buffer = [0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => output.extend_from_slice(&buffer[..n]),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to read FFmpeg output: {}",
                    e
                ))
            }
        }
    }

    // Wait for the process to complete
    let status = child
        .wait()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to wait for FFmpeg: {}", e))?;

    if !status.success() {
        return Err(anyhow::anyhow!("FFmpeg failed with status {}", status));
    }

    if output.is_empty() {
        return Err(anyhow::anyhow!("FFmpeg produced no output"));
    }

    // Generate VTT content
    let mut vtt_content = String::from("WEBVTT\n\n");

    for i in 0..total_tiles {
        let start_time = i as f64;
        let end_time = (i + 1) as f64;

        let x = (i % tiles_per_row) * tile_width;
        let y = (i / tiles_per_row) * tile_height;

        vtt_content.push_str(&format!(
            "{:02}:{:02}:{:02}.000 --> {:02}:{:02}:{:02}.000\n",
            (start_time / 3600.0) as u32,
            ((start_time % 3600.0) / 60.0) as u32,
            (start_time % 60.0) as u32,
            (end_time / 3600.0) as u32,
            ((end_time % 3600.0) / 60.0) as u32,
            (end_time % 60.0) as u32,
        ));
        vtt_content.push_str(&format!(
            "/api/storyboard/image/{}#xywh={},{},{},{}\n\n",
            event_id, x, y, tile_width, tile_height
        ));
    }

    Ok(StoryboardData {
        image: output,
        vtt: vtt_content,
    })
}

// Helper function to get video path and duration for an event
#[instrument(skip(state), level = "debug", err)]
async fn get_video_info(
    state: &Arc<AppState>,
    event_id: &str,
) -> Result<(String, f64), anyhow::Error> {
    let conn = state.zumblezay_db.get().map_err(|e| {
        anyhow::anyhow!(
            "Failed to get database connection for event {}: {}",
            event_id,
            e
        )
    })?;

    let (video_path, event_start, event_end): (String, f64, f64) = conn
        .query_row(
            "SELECT video_path, event_start, event_end FROM events WHERE event_id = ?",
            params![event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|e| anyhow::anyhow!("Failed to get video info for event {}: {}", event_id, e))?;

    // Calculate duration and replace path prefix
    let duration = event_end - event_start;
    let modified_path = video_path.replace(
        &state.video_path_original_prefix,
        &state.video_path_replacement_prefix,
    );

    Ok((modified_path, duration))
}

// Helper function to save storyboard to cache
#[instrument(skip(state, storyboard_data), level = "debug", err)]
async fn save_to_cache(
    state: &Arc<AppState>,
    event_id: &str,
    storyboard_data: &StoryboardData,
    generation_duration_ms: u128,
) -> Result<(), anyhow::Error> {
    let conn = state.cache_db.get().map_err(|e| {
        anyhow::anyhow!(
            "Failed to get cache database connection for event {}: {}",
            event_id,
            e
        )
    })?;

    conn.execute(
        "INSERT INTO storyboard_cache (event_id, version, created_at, generation_duration_ms, vtt, image) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            "v1",
            chrono::Utc::now().timestamp(),
            generation_duration_ms as i64,
            storyboard_data.vtt,
            storyboard_data.image
        ],
    ).map_err(|e| anyhow::anyhow!("Failed to save storyboard to cache for event {}: {}", event_id, e))?;

    Ok(())
}

// Helper function to generate a storyboard (without caching)
#[instrument(skip(state), level = "debug", err)]
async fn generate_storyboard_for_event(
    state: &Arc<AppState>,
    event_id: &str,
) -> Result<(StoryboardData, std::time::Duration), anyhow::Error> {
    // Get video path and duration
    let (video_path, duration) = get_video_info(state, event_id).await?;

    // Generate the storyboard
    let start_time = std::time::Instant::now();
    let storyboard_data = generate_storyboard(event_id, &video_path, duration)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to generate storyboard for event {}: {}",
                event_id,
                e
            )
        })?;

    let generation_duration = start_time.elapsed();

    info!(
        "Generated storyboard for event {} in {} ms",
        event_id,
        generation_duration.as_millis()
    );

    Ok((storyboard_data, generation_duration))
}

// Helper function to handle the waiting path for non-first requesters
#[instrument(skip(state, notify), level = "debug", err)]
async fn wait_for_storyboard(
    state: &Arc<AppState>,
    event_id: &str,
    notify: &Arc<tokio::sync::Notify>,
) -> Result<StoryboardData, anyhow::Error> {
    // Wait for the notification that generation is complete
    notify.notified().await;

    // After being notified, check the cache again
    if let Some(storyboard_data) =
        get_storyboard_data_from_cache(state, event_id).await?
    {
        return Ok(storyboard_data);
    } else {
        // This should only happen if the generator encountered an error
        return Err(anyhow::anyhow!(
            "Storyboard generation for event {} was signaled as complete but not found in cache",
            event_id
        ));
    }
}

// Helper function to clean up and notify waiters
#[instrument(skip(state, notify), level = "debug")]
async fn cleanup_and_notify(
    state: &Arc<AppState>,
    event_id: &str,
    notify: &Arc<tokio::sync::Notify>,
) {
    // First remove the entry from the map, then notify waiters
    {
        let mut in_progress = state.in_progress_storyboards.lock().await;
        in_progress.remove(event_id);
    }

    // Now that the entry is removed, notify any waiters
    notify.notify_waiters();
}

#[instrument(skip(state), err)]
pub async fn get_or_create_storyboard(
    state: &Arc<AppState>,
    event_id: &str,
) -> Result<StoryboardData, anyhow::Error> {
    // ===== CONCURRENCY PATTERN OVERVIEW =====
    // This function implements a coalescing pattern for concurrent storyboard generation requests.
    // The goal is to ensure that when multiple requests for the same storyboard arrive
    // simultaneously, only one actually performs the expensive generation work.
    //
    // Key concurrency challenges addressed:
    // 1. Minimizing lock contention by holding locks for the shortest time possible
    // 2. Avoiding I/O operations while holding locks
    // 3. Preventing race conditions between waiters and generators
    // 4. Ensuring proper cleanup in all code paths
    // =========================================

    // STEP 1: Quick cache check without any locking
    if let Some(storyboard_data) =
        get_storyboard_data_from_cache(state, event_id).await?
    {
        return Ok(storyboard_data);
    }

    info!("Storyboard cache miss for event_id: {}", event_id);

    // STEP 2: Determine if we're the first requester or if generation is already in progress
    let (notify, is_first_requester) =
        check_or_register_in_progress(state, event_id).await;

    // STEP 3: Handle the waiting path for non-first requesters
    if !is_first_requester {
        return wait_for_storyboard(state, event_id, &notify).await;
    }

    // STEP 4: Extra safety check before generation (for first requester)
    if let Some(storyboard_data) =
        get_storyboard_data_from_cache(state, event_id).await?
    {
        // We found it in the cache, so we don't need to generate it
        cleanup_and_notify(state, event_id, &notify).await;
        return Ok(storyboard_data);
    }

    // STEP 5: Generate the storyboard (only the first requester gets here)
    let result = async {
        // Generate the storyboard
        let (storyboard_data, generation_duration) =
            generate_storyboard_for_event(state, event_id).await?;

        // Save to cache
        save_to_cache(
            state,
            event_id,
            &storyboard_data,
            generation_duration.as_millis(),
        )
        .await?;

        Ok(storyboard_data)
    }
    .await;

    // STEP 6: Clean up and notify waiters (regardless of success or failure)
    cleanup_and_notify(state, event_id, &notify).await;

    // Return the result (success or error)
    result
}

// Helper function to check if generation is in progress or register as the first requester
#[instrument(skip(state), level = "debug")]
async fn check_or_register_in_progress(
    state: &Arc<AppState>,
    event_id: &str,
) -> (Arc<tokio::sync::Notify>, bool) {
    let mut in_progress = state.in_progress_storyboards.lock().await;

    if let Some(existing_notify) = in_progress.get(event_id) {
        // Another request is already generating this storyboard
        info!("Storyboard generation already in progress for event_id: {}, waiting", event_id);
        (existing_notify.clone(), false)
    } else {
        // We're the first request for this storyboard
        let new_notify = Arc::new(tokio::sync::Notify::new());
        in_progress.insert(event_id.to_string(), new_notify.clone());
        (new_notify, true)
    }
}

#[cfg(test)]
mod storyboard_tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Once;
    use tracing::{debug, info};
    use tracing_subscriber;

    // Initialize logging once for all tests
    static INIT: Once = Once::new();

    // Helper function to initialize tracing for tests
    fn init_test_logging() {
        INIT.call_once(|| {
            // Initialize the tracing subscriber only once
            let subscriber = tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::from_default_env(),
                )
                .with_test_writer()
                .finish();

            // Set as global default
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set tracing subscriber");

            debug!("Test logging initialized");
        });
    }

    #[tokio::test]
    async fn test_generate_storyboard() {
        // Use the test file from testdata directory
        let test_video_path = "testdata/10s-bars.mp4";

        // Test case 1: Valid video file should succeed
        let result =
            generate_storyboard("test_event_id", test_video_path, 10.0).await;
        assert!(
            result.is_ok(),
            "Failed to generate storyboard: {:?}",
            result.err()
        );

        let storyboard_data = result.unwrap();

        // Verify the image data is not empty
        assert!(
            !storyboard_data.image.is_empty(),
            "Storyboard image should not be empty"
        );

        // Verify VTT content format
        assert!(
            storyboard_data.vtt.starts_with("WEBVTT\n\n"),
            "VTT should start with WEBVTT header"
        );
        assert!(
            storyboard_data
                .vtt
                .contains("/api/storyboard/image/test_event_id#xywh="),
            "VTT should contain image references with coordinates"
        );

        // Test case 2: Non-existent file should fail
        let non_existent_path = PathBuf::from("/path/does/not/exist.mp4")
            .to_str()
            .unwrap()
            .to_string();
        let result =
            generate_storyboard("test_event_id", &non_existent_path, 60.0)
                .await;
        assert!(result.is_err());

        // Test case 3: Empty duration should fail
        let result =
            generate_storyboard("test_event_id", test_video_path, 0.0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_coalescing_storyboard_requests() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_coalesce_id";
        let test_video_path = "testdata/10s-bars.mp4";

        info!("=== TEST: Coalescing Storyboard Requests ===");
        info!("Setting up test environment...");

        // Create counters to track generation attempts and completions
        let generation_attempts =
            Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let generation_completions =
            Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Create a barrier to ensure all requests are in-flight before any completes
        let num_concurrent_requests = 5;
        let barrier =
            Arc::new(tokio::sync::Barrier::new(num_concurrent_requests + 1)); // +1 for the test itself

        info!("Inserting test record in database...");
        // Insert a test record in the database
        {
            let conn = state.zumblezay_db.get().unwrap();
            conn.execute(
                "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    test_event_id,
                    chrono::Utc::now().timestamp(),
                    0.0,
                    10.0,
                    "motion",
                    "test_camera",
                    test_video_path
                ],
            ).unwrap();
        }

        info!(
            "Launching {} concurrent requests...",
            num_concurrent_requests
        );
        // TEST: Launch multiple concurrent requests for the same storyboard
        let handles: Vec<_> = (0..num_concurrent_requests)
            .map(|i| {
                let state = state.clone();
                let event_id = test_event_id.to_string();
                let attempts = generation_attempts.clone();
                let completions = generation_completions.clone();
                let barrier_clone = barrier.clone();

                tokio::spawn(async move {
                    debug!("[Request {}] Starting", i);

                    // Custom implementation of get_or_create_storyboard that tracks generation
                    let result = async {
                        // Check cache first
                        if let Some(data) = get_storyboard_data_from_cache(&state, &event_id).await? {
                            debug!("[Request {}] Found in cache", i);
                            return Ok(data);
                        }

                        debug!("[Request {}] Not in cache, checking if in progress", i);
                        // Get coordination objects
                        let (notify, is_first) = check_or_register_in_progress(&state, &event_id).await;

                        debug!("[Request {}] Waiting at barrier (is_first={})", i, is_first);
                        // Wait at the barrier to ensure all requests are in-flight
                        barrier_clone.wait().await;
                        debug!("[Request {}] Passed barrier", i);

                        if !is_first {
                            debug!("[Request {}] Waiting for first request to complete", i);
                            // Wait for the first request to complete
                            let result = wait_for_storyboard(&state, &event_id, &notify).await;
                            debug!("[Request {}] First request completed, got result", i);
                            return result;
                        }

                        debug!("[Request {}] I am the first request, doing extra safety check", i);
                        // Extra safety check
                        if let Some(data) = get_storyboard_data_from_cache(&state, &event_id).await? {
                            debug!("[Request {}] Found in cache during safety check", i);
                            cleanup_and_notify(&state, &event_id, &notify).await;
                            return Ok(data);
                        }

                        debug!("[Request {}] Generating storyboard...", i);
                        // Generate and cache
                        let result = async {
                            // Track generation attempt
                            let prev = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            debug!("[Request {}] Generation attempt #{}", i, prev + 1);

                            // Add a delay to simulate work and ensure other requests have time to wait
                            debug!("[Request {}] Sleeping to simulate work...", i);
                            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                            // Generate the storyboard
                            debug!("[Request {}] Calling generate_storyboard_for_event...", i);
                            let (data, duration) = generate_storyboard_for_event(&state, &event_id).await?;

                            // Track generation completion
                            let prev = completions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            debug!("[Request {}] Generation completion #{}", i, prev + 1);

                            // Save to cache
                            debug!("[Request {}] Saving to cache...", i);
                            save_to_cache(&state, &event_id, &data, duration.as_millis()).await?;

                            Ok(data)
                        }.await;

                        // Clean up
                        debug!("[Request {}] Cleaning up and notifying waiters", i);
                        cleanup_and_notify(&state, &event_id, &notify).await;
                        result
                    }.await;

                    debug!("[Request {}] Completed", i);
                    assert!(
                        result.is_ok(),
                        "Storyboard request {} should succeed", i
                    );
                    result
                })
            })
            .collect();

        info!("Main thread waiting at barrier...");
        // Wait at the barrier to ensure all requests are in-flight
        barrier.wait().await;
        info!("Main thread passed barrier, all requests are in-flight");

        // Wait for all requests to complete
        info!("Waiting for all requests to complete...");
        let mut results = Vec::new();
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap();
            results.push(result);
            debug!("Request {} joined", i);
        }

        info!("Verifying results...");
        // VERIFY: Check that all requests returned the same data
        let first_result = &results[0];
        for (i, result) in results.iter().enumerate().skip(1) {
            assert!(result.is_ok(), "Request {} should have succeeded", i);

            // Compare image data size (should be identical)
            assert_eq!(
                result.as_ref().unwrap().image.len(),
                first_result.as_ref().unwrap().image.len(),
                "All requests should return the same image data"
            );
        }

        // VERIFY: Check that the storyboard was created and cached
        let cache_result =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(
            cache_result.is_ok(),
            "Should be able to retrieve from cache"
        );
        assert!(
            cache_result.unwrap().is_some(),
            "Cache should contain the storyboard"
        );

        // VERIFY: Check that only one database entry was created
        {
            let conn = state.cache_db.get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM storyboard_cache WHERE event_id = ?",
                    params![test_event_id],
                    |row| row.get(0),
                )
                .unwrap();

            // If coalescing works, there should be exactly one entry
            assert_eq!(
                count, 1,
                "There should be exactly one cache entry despite {} concurrent requests",
                num_concurrent_requests
            );
        }

        // VERIFY: Check that generation was only attempted once
        let attempts =
            generation_attempts.load(std::sync::atomic::Ordering::SeqCst);
        let completions =
            generation_completions.load(std::sync::atomic::Ordering::SeqCst);

        info!(
            "Generation attempts: {}, completions: {}",
            attempts, completions
        );

        assert_eq!(
            attempts, 1,
            "Storyboard generation should only be attempted once, but was attempted {} times",
            attempts
        );

        assert_eq!(
            completions, 1,
            "Storyboard generation should only complete once, but completed {} times",
            completions
        );

        info!("Cleaning up test data...");
        // CLEANUP: Remove test data
        {
            let conn = state.zumblezay_db.get().unwrap();
            conn.execute(
                "DELETE FROM events WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }

        info!("Test completed successfully!");
    }

    #[tokio::test]
    async fn test_get_or_create_storyboard_end_to_end() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_e2e_storyboard";
        let test_video_path = "testdata/10s-bars.mp4";

        info!("=== TEST: get_or_create_storyboard End-to-End ===");
        info!("Setting up test environment...");

        // Insert a test record in the database
        {
            let conn = state.zumblezay_db.get().unwrap();
            conn.execute(
                "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    test_event_id,
                    chrono::Utc::now().timestamp(),
                    0.0,
                    10.0,
                    "motion",
                    "test_camera",
                    test_video_path
                ],
            ).unwrap();
        }

        // SCENARIO 1: First request (cache miss)
        info!("SCENARIO 1: Testing first request (cache miss)...");

        // Verify the storyboard is not in cache
        let cache_check =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(cache_check.is_ok(), "Cache check should succeed");
        assert!(
            cache_check.unwrap().is_none(),
            "Cache should be empty before first request"
        );

        // First request should generate the storyboard
        let start_time = std::time::Instant::now();
        let result1 = get_or_create_storyboard(&state, test_event_id).await;
        let first_request_duration = start_time.elapsed();

        info!("First request took {:?}", first_request_duration);
        assert!(result1.is_ok(), "First request should succeed");
        let storyboard1 = result1.unwrap();

        // Verify the storyboard data
        assert!(
            !storyboard1.image.is_empty(),
            "Storyboard image should not be empty"
        );
        assert!(
            storyboard1.vtt.starts_with("WEBVTT\n\n"),
            "VTT should start with WEBVTT header"
        );

        // Verify it was saved to cache
        let cache_check =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(cache_check.is_ok(), "Cache check should succeed");
        assert!(
            cache_check.unwrap().is_some(),
            "Cache should contain the storyboard after first request"
        );

        // SCENARIO 2: Second request (cache hit)
        info!("SCENARIO 2: Testing second request (cache hit)...");

        // Second request should retrieve from cache (much faster)
        let start_time = std::time::Instant::now();
        let result2 = get_or_create_storyboard(&state, test_event_id).await;
        let second_request_duration = start_time.elapsed();

        info!("Second request took {:?}", second_request_duration);
        assert!(result2.is_ok(), "Second request should succeed");
        let storyboard2 = result2.unwrap();

        // Verify the data is the same
        assert_eq!(
            storyboard1.image.len(),
            storyboard2.image.len(),
            "Both requests should return the same image data size"
        );
        assert_eq!(
            storyboard1.vtt, storyboard2.vtt,
            "Both requests should return the same VTT content"
        );

        // Verify the second request was faster (cache hit vs generation)
        assert!(
            second_request_duration < first_request_duration / 2,
            "Cache hit should be significantly faster than generation. First: {:?}, Second: {:?}",
            first_request_duration,
            second_request_duration
        );

        // SCENARIO 3: Concurrent requests
        info!("SCENARIO 3: Testing concurrent requests...");

        // Clear the cache to force regeneration
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();

            // Verify cache is empty
            let cache_check =
                get_storyboard_data_from_cache(&state, test_event_id).await;
            assert!(
                cache_check.unwrap().is_none(),
                "Cache should be empty after clearing"
            );
        }

        // Launch multiple concurrent requests
        let num_concurrent = 3;
        info!("Launching {} concurrent requests...", num_concurrent);

        let handles: Vec<_> = (0..num_concurrent)
            .map(|i| {
                let state = state.clone();
                let event_id = test_event_id.to_string();

                tokio::spawn(async move {
                    debug!("[Request {}] Starting", i);
                    let result =
                        get_or_create_storyboard(&state, &event_id).await;
                    debug!("[Request {}] Completed", i);
                    result
                })
            })
            .collect();

        // Wait for all requests to complete
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Verify all requests succeeded
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "Concurrent request {} should succeed", i);
        }

        // Verify only one cache entry was created
        {
            let conn = state.cache_db.get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM storyboard_cache WHERE event_id = ?",
                    params![test_event_id],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(
                count, 1,
                "There should be exactly one cache entry despite {} concurrent requests",
                num_concurrent
            );
        }

        info!("Cleaning up test data...");
        // CLEANUP: Remove test data
        {
            let conn = state.zumblezay_db.get().unwrap();
            conn.execute(
                "DELETE FROM events WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }

        info!("End-to-end test completed successfully!");
    }

    #[tokio::test]
    async fn test_get_storyboard_data_from_cache() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_cache_retrieval";

        info!("=== TEST: Storyboard Cache Retrieval ===");
        info!("Setting up test environment...");

        // SCENARIO 1: Empty cache should return None
        info!("SCENARIO 1: Testing empty cache...");
        let result =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(result.is_ok(), "Cache check should succeed even when empty");
        assert!(result.unwrap().is_none(), "Empty cache should return None");

        // SCENARIO 2: Populated cache should return data
        info!("SCENARIO 2: Testing populated cache...");

        // Create test data
        let test_image = vec![1, 2, 3, 4, 5]; // Simple test image data
        let test_vtt =
            "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nTest VTT content"
                .to_string();

        // Insert directly into the cache database
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "INSERT INTO storyboard_cache (event_id, version, created_at, generation_duration_ms, vtt, image) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    test_event_id,
                    "v1",
                    chrono::Utc::now().timestamp() - 100, // Older timestamp
                    100, // Dummy duration
                    test_vtt.clone(),
                    test_image.clone()
                ],
            ).unwrap();
        }

        // Retrieve from cache
        let result =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(result.is_ok(), "Cache retrieval should succeed");

        let storyboard_data = result.unwrap();
        assert!(
            storyboard_data.is_some(),
            "Cache should return data after insertion"
        );

        let data = storyboard_data.unwrap();
        assert_eq!(
            data.image, test_image,
            "Retrieved image should match inserted image"
        );
        assert_eq!(
            data.vtt, test_vtt,
            "Retrieved VTT should match inserted VTT"
        );

        // SCENARIO 3: Multiple entries should return the most recent
        info!("SCENARIO 3: Testing multiple entries (should return most recent)...");

        // Insert a newer entry with different data
        let newer_test_image = vec![6, 7, 8, 9, 10]; // Different test image
        let newer_test_vtt = "WEBVTT\n\nNewer VTT content".to_string();

        // Sleep briefly to ensure timestamp is different
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "INSERT INTO storyboard_cache (event_id, version, created_at, generation_duration_ms, vtt, image) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    test_event_id,
                    "v2", // Different version
                    chrono::Utc::now().timestamp(), // Current timestamp (newer)
                    50, // Different duration
                    newer_test_vtt.clone(),
                    newer_test_image.clone()
                ],
            ).unwrap();
        }

        // Retrieve from cache again
        let result =
            get_storyboard_data_from_cache(&state, test_event_id).await;
        assert!(result.is_ok(), "Cache retrieval should succeed");

        let storyboard_data = result.unwrap();
        assert!(storyboard_data.is_some(), "Cache should return data");

        let data = storyboard_data.unwrap();
        assert_eq!(
            data.image, newer_test_image,
            "Should retrieve the newer image"
        );
        assert_eq!(data.vtt, newer_test_vtt, "Should retrieve the newer VTT");

        // Verify we have two entries in the database
        {
            let conn = state.cache_db.get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM storyboard_cache WHERE event_id = ?",
                    params![test_event_id],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(count, 2, "There should be two cache entries");
        }

        // CLEANUP: Remove test data
        info!("Cleaning up test data...");
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }

        info!("Cache retrieval test completed successfully!");
    }

    #[tokio::test]
    async fn test_save_to_cache() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_cache_save";

        info!("=== TEST: Storyboard Cache Save ===");
        info!("Setting up test environment...");

        // Create test data
        let test_image = vec![10, 20, 30, 40, 50]; // Simple test image data
        let test_vtt =
            "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nTest save to cache"
                .to_string();
        let test_storyboard = StoryboardData {
            image: test_image.clone(),
            vtt: test_vtt.clone(),
        };

        // SCENARIO 1: Save new entry
        info!("SCENARIO 1: Testing saving new entry...");

        // Save to cache
        let result = save_to_cache(
            &state,
            test_event_id,
            &test_storyboard,
            150, // Test duration in ms
        )
        .await;

        assert!(result.is_ok(), "Saving to cache should succeed");

        // Verify it was saved correctly
        {
            let conn = state.cache_db.get().unwrap();
            let row = conn.query_row(
                "SELECT event_id, version, generation_duration_ms, vtt, image FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                    ))
                }
            ).unwrap();

            assert_eq!(row.0, test_event_id, "Event ID should match");
            assert_eq!(row.1, "v1", "Version should be v1");
            assert_eq!(row.2, 150, "Duration should match");
            assert_eq!(row.3, test_vtt, "VTT should match");
            assert_eq!(row.4, test_image, "Image data should match");
        }

        // SCENARIO 2: Save another entry for the same event with a different version
        info!("SCENARIO 2: Testing saving another entry with different version...");

        // Create different test data
        let test_image2 = vec![60, 70, 80, 90, 100]; // Different test image
        let test_vtt2 = "WEBVTT\n\nSecond entry test".to_string();

        // Modify the save_to_cache function to use a different version
        let conn = state.cache_db.get().unwrap();
        let result = conn.execute(
            "INSERT INTO storyboard_cache (event_id, version, created_at, generation_duration_ms, vtt, image) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                test_event_id,
                "v2", // Different version
                chrono::Utc::now().timestamp(),
                75, // Different duration
                test_vtt2.clone(),
                test_image2.clone()
            ],
        );

        assert!(
            result.is_ok(),
            "Saving second entry with different version should succeed"
        );

        // Verify we now have two entries
        {
            let conn = state.cache_db.get().unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM storyboard_cache WHERE event_id = ?",
                    params![test_event_id],
                    |row| row.get(0),
                )
                .unwrap();

            assert_eq!(
                count, 2,
                "There should be two cache entries after second save"
            );

            // Verify the entry with version v2 has the correct data
            let row = conn.query_row(
                "SELECT image, vtt, generation_duration_ms FROM storyboard_cache WHERE event_id = ? AND version = 'v2'",
                params![test_event_id],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                }
            ).unwrap();

            assert_eq!(
                row.0, test_image2,
                "Version v2 image should match second image"
            );
            assert_eq!(
                row.1, test_vtt2,
                "Version v2 VTT should match second VTT"
            );
            assert_eq!(
                row.2, 75,
                "Version v2 duration should match second duration"
            );
        }

        // CLEANUP: Remove test data
        info!("Cleaning up test data...");
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }

        info!("Cache save test completed successfully!");
    }

    #[tokio::test]
    async fn test_check_or_register_in_progress() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_in_progress";

        info!("=== TEST: Check or Register In Progress ===");

        // SCENARIO 1: First request should register as in progress
        info!("SCENARIO 1: Testing first request registration...");

        let (notify1, is_first1) =
            check_or_register_in_progress(&state, test_event_id).await;

        assert!(is_first1, "First request should be identified as first");

        // Verify entry exists in the map
        {
            let in_progress = state.in_progress_storyboards.lock().await;
            assert!(
                in_progress.contains_key(test_event_id),
                "Event should be registered as in progress"
            );
        }

        // SCENARIO 2: Second request should detect in progress
        info!("SCENARIO 2: Testing second request detection...");

        let (_notify2, is_first2) =
            check_or_register_in_progress(&state, test_event_id).await;

        assert!(
            !is_first2,
            "Second request should not be identified as first"
        );

        // SCENARIO 3: Different event ID should be treated as first
        info!("SCENARIO 3: Testing different event ID...");

        let different_event_id = "different_test_id";
        let (notify3, is_first3) =
            check_or_register_in_progress(&state, different_event_id).await;

        assert!(
            is_first3,
            "Request for different event should be identified as first"
        );

        // SCENARIO 4: After cleanup, new request should be first again
        info!("SCENARIO 4: Testing after cleanup...");

        // Clean up the first event
        cleanup_and_notify(&state, test_event_id, &notify1).await;

        // Check the map
        {
            let in_progress = state.in_progress_storyboards.lock().await;
            assert!(
                !in_progress.contains_key(test_event_id),
                "First event should be removed after cleanup"
            );
            assert!(
                in_progress.contains_key(different_event_id),
                "Different event should still be registered"
            );
        }

        // Try registering the first event again
        let (notify4, is_first4) =
            check_or_register_in_progress(&state, test_event_id).await;

        assert!(
            is_first4,
            "After cleanup, new request should be identified as first"
        );

        // Clean up all test data
        info!("Cleaning up test data...");
        cleanup_and_notify(&state, test_event_id, &notify4).await;
        cleanup_and_notify(&state, different_event_id, &notify3).await;

        info!("In-progress registration test completed successfully!");
    }

    #[tokio::test]
    async fn test_wait_and_notify_mechanism() {
        // Initialize tracing
        init_test_logging();

        // SETUP: Create a test environment
        let app_state = crate::AppState::new_for_testing();
        let state = Arc::new(app_state);
        let test_event_id = "test_wait_notify";

        info!("=== TEST: Wait and Notify Mechanism ===");
        info!("Setting up test environment...");

        // Create a notify object
        let notify = Arc::new(tokio::sync::Notify::new());

        // Register the event as in progress
        {
            let mut in_progress = state.in_progress_storyboards.lock().await;
            in_progress.insert(test_event_id.to_string(), notify.clone());
        }

        // SCENARIO 1: wait_for_storyboard should wait until notified and then check cache
        info!("SCENARIO 1: Testing wait mechanism...");

        // Create test data for the cache
        let test_image = vec![1, 2, 3, 4, 5];
        let test_vtt = "WEBVTT\n\nTest wait mechanism".to_string();

        // Spawn a task that will wait for the storyboard
        let state_clone = state.clone();
        let event_id_clone = test_event_id.to_string();
        let notify_clone = notify.clone();

        let wait_handle = tokio::spawn(async move {
            // This should block until notified

            wait_for_storyboard(&state_clone, &event_id_clone, &notify_clone)
                .await
        });

        // Wait a bit to ensure the task is waiting
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Now insert data into the cache
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "INSERT INTO storyboard_cache (event_id, version, created_at, generation_duration_ms, vtt, image) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    test_event_id,
                    "v1",
                    chrono::Utc::now().timestamp(),
                    100,
                    test_vtt.clone(),
                    test_image.clone()
                ],
            ).unwrap();
        }

        // Notify waiters
        info!("Notifying waiters...");
        cleanup_and_notify(&state, test_event_id, &notify).await;

        // Wait for the task to complete
        let wait_result = wait_handle.await.unwrap();

        // Verify the result
        assert!(
            wait_result.is_ok(),
            "wait_for_storyboard should succeed after notification"
        );
        let storyboard_data = wait_result.unwrap();
        assert_eq!(
            storyboard_data.image, test_image,
            "Retrieved image should match"
        );
        assert_eq!(storyboard_data.vtt, test_vtt, "Retrieved VTT should match");

        // SCENARIO 2: wait_for_storyboard should fail if cache is empty after notification
        info!("SCENARIO 2: Testing wait with empty cache...");

        // Register the event as in progress again
        let notify2 = Arc::new(tokio::sync::Notify::new());
        {
            let mut in_progress = state.in_progress_storyboards.lock().await;
            in_progress.insert(test_event_id.to_string(), notify2.clone());
        }

        // Clear the cache
        {
            let conn = state.cache_db.get().unwrap();
            conn.execute(
                "DELETE FROM storyboard_cache WHERE event_id = ?",
                params![test_event_id],
            )
            .unwrap();
        }

        // Spawn a task that will wait for the storyboard
        let state_clone = state.clone();
        let event_id_clone = test_event_id.to_string();
        let notify_clone = notify2.clone();

        let wait_handle = tokio::spawn(async move {
            // This should block until notified

            wait_for_storyboard(&state_clone, &event_id_clone, &notify_clone)
                .await
        });

        // Wait a bit to ensure the task is waiting
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Notify waiters without adding to cache
        info!("Notifying waiters without adding to cache...");
        cleanup_and_notify(&state, test_event_id, &notify2).await;

        // Wait for the task to complete
        let wait_result = wait_handle.await.unwrap();

        // Verify the result is an error
        assert!(wait_result.is_err(), "wait_for_storyboard should fail if cache is empty after notification");

        info!("Wait and notify test completed successfully!");
    }
}
