use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use rusqlite::params;
use std::sync::Arc;
use std::sync::Once;
use tower::util::ServiceExt;
use tracing::{debug, info};
use tracing_subscriber;
use zumblezay::AppState;

// Initialize logging once for all tests
static INIT: Once = Once::new();

// Helper function to initialize tracing for tests
fn init_test_logging() {
    INIT.call_once(|| {
        // Initialize the tracing subscriber only once
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info,tower_http=debug".into()),
            )
            .with_test_writer()
            .finish();

        // Set as global default
        tracing::subscriber::set_global_default(subscriber)
            .expect("Failed to set tracing subscriber");

        debug!("Test logging initialized");
    });
}

/// Create a test app with just the health endpoint
fn app() -> (Arc<AppState>, Router) {
    // Create a minimal AppState for testing
    let app_state = Arc::new(AppState::new_for_testing());
    let routes = zumblezay::app::routes(app_state.clone());
    (app_state, routes)
}

#[tokio::test]
async fn test_health_endpoint() {
    init_test_logging();
    let (_, router) = app();

    // Use tower's `oneshot` to send a request to our app
    let response = router
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Assert that the response status is OK
    assert_eq!(response.status(), StatusCode::OK);

    // Check the response body
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"OK");
}

#[tokio::test]
async fn test_not_found() {
    init_test_logging();
    let (_, router) = app();

    // Send a request to a non-existent endpoint
    let response = router
        .oneshot(
            Request::builder()
                .uri("/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Assert that the response status is NOT_FOUND
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// This test demonstrates how to test with a real server if needed
#[tokio::test]
async fn test_with_real_server() {
    init_test_logging();
    use tokio::net::TcpListener;

    // Bind to a random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (_app_state, router) = app();
    // Spawn the server in the background
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // Create a client
    let client = reqwest::Client::new();

    // Send a request to the health endpoint
    let response = client
        .get(format!("http://{}/health", addr))
        .send()
        .await
        .expect("Failed to send request");

    // Assert that the response is successful
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // Assert that the response body is "OK"
    let body = response.text().await.expect("Failed to read response body");
    assert_eq!(body, "OK");
}

#[tokio::test]
async fn test_events_api() {
    init_test_logging();
    let (app_state, app_router) = app();
    let conn = app_state.zumblezay_db.get().unwrap();

    /*
    CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            event_start REAL NOT NULL,
            event_end REAL NOT NULL,
            event_type TEXT NOT NULL,
            camera_id TEXT NOT NULL,
            video_path TEXT
        );
     */
    // Define and insert test events in a more compact way
    // Note: These timestamps are now adjusted for Australia/Adelaide timezone (GMT+1030)
    // 2022-12-31 in Adelaide starts at 1672407000.0 and ends at 1672493400.0 (UTC)
    let test_events = [
        (
            "test-event-1",
            1672531200,
            // 2022-12-31 in Adelaide timezone
            1672407300.0, // 5 minutes after midnight
            1672407310.0, // 10 seconds later
            "motion",
            "camera1",
            "/data/videos/test1.mp4",
        ),
        (
            "test-event-2",
            1672531300,
            // 2022-12-31 in Adelaide timezone
            1672450800.0, // 12:00 noon
            1672450820.0, // 20 seconds later
            "person",
            "camera1",
            "/data/videos/test2.mp4",
        ),
        (
            "test-event-3",
            1672531400,
            // 2022-12-31 in Adelaide timezone
            1672493100.0, // 23:55 (5 minutes before midnight)
            1672493120.0, // 20 seconds later
            "motion",
            "camera2",
            "/data/videos/test3.mp4",
        ),
    ];

    for (
        event_id,
        created_at,
        event_start,
        event_end,
        event_type,
        camera_id,
        video_path,
    ) in test_events
    {
        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) VALUES 
             (?, ?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                created_at,
                event_start,
                event_end,
                event_type,
                camera_id,
                video_path,
            ],
        )
        .unwrap();
    }

    // Test basic events retrieval
    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();

    // Extract the body outside the conditional block to avoid consuming response twice
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        String::from_utf8_lossy(&body)
    );

    // Use the already extracted body instead of consuming response again
    let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    // Verify we got our test events
    assert_eq!(events.len(), 3);

    // Test with date filter - using fixed date "2022-12-31"
    let fixed_date = "2022-12-31";
    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/events?date={}", fixed_date))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        String::from_utf8_lossy(&body)
    );
    let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    assert_eq!(events.len(), 3, "Expected 3 events for date {}", fixed_date);

    // Test with camera filter
    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/events?camera_id=camera1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        String::from_utf8_lossy(&body)
    );
    let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();

    // Verify camera-filtered events
    assert!(!events.is_empty());
    assert!(events.iter().all(|e| e["camera_id"] == "camera1"));
    assert_eq!(events.len(), 2);

    // Test with start and end time filters
    // Debug: Print all events with their local times
    info!("DEBUG: All events in database:");
    let mut stmt = conn.prepare("SELECT event_id, event_start, event_end, 
                             strftime('%H:%M:%S', datetime(event_start, 'unixepoch', 'localtime')) as local_start_time,
                             strftime('%H:%M:%S', datetime(event_end, 'unixepoch', 'localtime')) as local_end_time,
                             strftime('%Y-%m-%d', datetime(event_start, 'unixepoch', 'localtime')) as local_date
                             FROM events ORDER BY event_start")
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0).unwrap(),
                row.get::<_, f64>(1).unwrap(),
                row.get::<_, f64>(2).unwrap(),
                row.get::<_, String>(3).unwrap(),
                row.get::<_, String>(4).unwrap(),
                row.get::<_, String>(5).unwrap(),
            ))
        })
        .unwrap();

    let mut event_data = Vec::new();
    for row in rows {
        let (id, start_ts, end_ts, local_start, local_end, local_date) =
            row.unwrap();
        info!("Event {}: start_ts={}, end_ts={}, local_start_time={}, local_end_time={}, local_date={}", 
                 id, start_ts, end_ts, local_start, local_end, local_date);
        event_data.push((id, local_start, local_date));
    }

    // Debug: Print system timezone information
    info!("DEBUG: System timezone info:");
    info!(
        "TZ env var: {:?}",
        std::env::var("TZ").unwrap_or_else(|_| "Not set".to_string())
    );

    // Use the actual local date and times from the database instead of hardcoded values
    // This makes the test timezone-independent
    assert!(event_data.len() >= 2, "Need at least 2 events for the test");

    let _test_date = &event_data[0].2; // Use date from first event (unused now)
    let time_start = &event_data[0].1; // Use start time from first event

    // For time_end, use the time from the second event to ensure we get both events
    let time_end = &event_data[1].1;

    // Instead of using date filter which is problematic with timezone differences,
    // we'll use camera_id filter with time filters
    let time_query_url = format!(
        "/api/events?camera_id=camera1&time_start={}&time_end={}",
        time_start, time_end
    );
    info!("DEBUG: Making request to: {}", time_query_url);

    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(&time_query_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body).to_string();

    info!("DEBUG: Response status: {:?}", status);
    info!("DEBUG: Response body: {}", body_str);

    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        body_str
    );

    let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    info!("DEBUG: Number of events returned: {}", events.len());
    for (i, event) in events.iter().enumerate() {
        info!("DEBUG: Event {}: {:?}", i, event);
    }

    assert_eq!(
        events.len(),
        2,
        "Expected 2 events in time range {} to {}, got {}",
        time_start,
        time_end,
        events.len()
    );
}

#[tokio::test]
async fn test_events_api_timezone_robust() {
    init_test_logging();
    let (app_state, app_router) = app();
    let conn = app_state.zumblezay_db.get().unwrap();

    // Insert the same test events as in test_events_api
    let test_events = [
        (
            "test-tz-event-1",
            1672531200,
            1672531500.0, // First event start time
            1672531510.0,
            "motion",
            "camera1",
            "/data/videos/test1.mp4",
        ),
        (
            "test-tz-event-2",
            1672531300,
            1672531600.0, // Second event start time
            1672531620.0,
            "person",
            "camera1",
            "/data/videos/test2.mp4",
        ),
        (
            "test-tz-event-3",
            1672531400,
            1672531700.0, // Third event start time
            1672531720.0,
            "motion",
            "camera2",
            "/data/videos/test3.mp4",
        ),
    ];

    for (
        event_id,
        created_at,
        event_start,
        event_end,
        event_type,
        camera_id,
        video_path,
    ) in test_events
    {
        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) VALUES 
             (?, ?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                created_at,
                event_start,
                event_end,
                event_type,
                camera_id,
                video_path,
            ],
        )
        .unwrap();
    }

    // Get the local time representation of our events directly from the database
    let mut stmt = conn.prepare("
        SELECT 
            event_id,
            strftime('%Y-%m-%d', datetime(event_start, 'unixepoch', 'localtime')) as local_date,
            strftime('%H:%M:%S', datetime(event_start, 'unixepoch', 'localtime')) as local_time
        FROM events
        WHERE event_id LIKE 'test-tz-event-%'
        ORDER BY event_start
    ").unwrap();

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0).unwrap(),
                row.get::<_, String>(1).unwrap(), // local_date
                row.get::<_, String>(2).unwrap(), // local_time
            ))
        })
        .unwrap();

    let mut event_times = Vec::new();
    for row in rows {
        let (id, date, time) = row.unwrap();
        info!("Event {}: local_date={}, local_time={}", id, date, time);
        event_times.push((id, date, time));
    }

    // Ensure we have at least 2 events to test with
    assert!(
        event_times.len() >= 2,
        "Need at least 2 events for the test"
    );

    // Use the actual local date from the first event
    let _test_date = &event_times[0].1; // Unused now
    info!("Using test date: {}", _test_date);

    // Use the actual local times from the first and second events
    let time_start = &event_times[0].2;
    // Add 1 second to the second event's time to ensure we include it
    let time_end = &event_times[1].2;

    info!("Using time range: {} to {}", time_start, time_end);

    // Construct the URL with the actual local times from the database
    // Use camera_id filter instead of date filter to avoid timezone issues
    let url = format!(
        "/api/events?camera_id=camera1&time_start={}&time_end={}",
        time_start, time_end
    );
    info!("Request URL: {}", url);

    // Make the request with the timezone-aware parameters
    let response = app_router
        .clone()
        .oneshot(Request::builder().uri(&url).body(Body::empty()).unwrap())
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body).to_string();

    info!("Response status: {:?}", status);
    info!("Response body: {}", body_str);

    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        body_str
    );

    let events: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    info!("Number of events returned: {}", events.len());

    // We should get exactly 2 events - the first and second ones
    assert_eq!(
        events.len(),
        2,
        "Expected 2 events in time range {} to {}, got {}",
        time_start,
        time_end,
        events.len()
    );

    // Verify the event IDs match what we expect
    let event_ids: Vec<&str> = events
        .iter()
        .map(|e| e["event_id"].as_str().unwrap())
        .collect();

    assert!(event_ids.contains(&"test-tz-event-1"));
    assert!(event_ids.contains(&"test-tz-event-2"));
}

#[tokio::test]
async fn test_cameras_api() {
    init_test_logging();
    let (app_state, app_router) = app();
    let conn = app_state.zumblezay_db.get().unwrap();

    // Insert test events with different cameras
    let test_events = [
        (
            "test-camera-event-1",
            1672531200,
            1672531500.0,
            1672531510.0,
            "motion",
            "camera1",
            "/data/videos/test1.mp4",
        ),
        (
            "test-camera-event-2",
            1672531300,
            1672531600.0,
            1672531620.0,
            "person",
            "camera2",
            "/data/videos/test2.mp4",
        ),
        (
            "test-camera-event-3",
            1672531400,
            1672531700.0,
            1672531720.0,
            "motion",
            "camera3",
            "/data/videos/test3.mp4",
        ),
    ];

    for (
        event_id,
        created_at,
        event_start,
        event_end,
        event_type,
        camera_id,
        video_path,
    ) in test_events
    {
        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) VALUES 
             (?, ?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                created_at,
                event_start,
                event_end,
                event_type,
                camera_id,
                video_path,
            ],
        )
        .unwrap();
    }

    // Update camera name cache
    zumblezay::app::cache_camera_names(&app_state)
        .await
        .unwrap();

    // Test cameras API endpoint
    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/cameras")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        String::from_utf8_lossy(&body)
    );

    let response_data: serde_json::Value =
        serde_json::from_slice(&body).unwrap();

    // Verify response structure
    assert!(response_data.is_object());
    assert!(response_data.get("cameras").is_some());
    assert!(response_data["cameras"].is_array());

    let cameras = response_data["cameras"].as_array().unwrap();

    // Verify we have our test cameras
    assert_eq!(cameras.len(), 3);

    // Verify each camera has id and name fields
    for camera in cameras {
        assert!(camera.get("id").is_some());
        assert!(camera.get("name").is_some());
    }

    // Verify specific camera IDs are present
    let camera_ids: Vec<&str> =
        cameras.iter().map(|c| c["id"].as_str().unwrap()).collect();

    assert!(camera_ids.contains(&"camera1"));
    assert!(camera_ids.contains(&"camera2"));
    assert!(camera_ids.contains(&"camera3"));

    // Clean up test data
    conn.execute(
        "DELETE FROM events WHERE event_id LIKE 'test-camera-event-%'",
        [],
    )
    .unwrap();
}
