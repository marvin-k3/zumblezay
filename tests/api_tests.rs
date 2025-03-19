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
use tracing::debug;
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
                    .unwrap_or_else(|_| "debug".into()),
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

/// Helper function to make API requests and extract the response body
async fn make_api_request(router: Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let response = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();

    debug!("Response status: {:?}", status);
    debug!("Response body: {}", String::from_utf8_lossy(&body));

    (status, body)
}

/// Helper function to validate API response and extract events
fn validate_and_parse_events(
    status: StatusCode,
    body: &[u8],
) -> Vec<serde_json::Value> {
    let body_str = String::from_utf8_lossy(body).to_string();

    assert_eq!(
        status,
        StatusCode::OK,
        "Response status was: {:?}, body: {:?}",
        status,
        body_str
    );

    let events: Vec<serde_json::Value> = serde_json::from_slice(body).unwrap();

    debug!("Number of events returned: {}", events.len());
    for (i, event) in events.iter().enumerate() {
        debug!("Event {}: {:?}", i, event);
    }

    events
}

/// Helper function to extract event IDs from events
fn extract_event_ids(events: &[serde_json::Value]) -> Vec<&str> {
    events
        .iter()
        .map(|e| e["event_id"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn test_events_api() {
    init_test_logging();
    let (app_state, app_router) = app();
    assert_eq!(app_state.timezone, chrono_tz::Australia::Adelaide);
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
    // Note: These timestamps are for Australia/Adelaide timezone (GMT+1030)
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
            "camera2",
            "/data/videos/test2.mp4",
        ),
        (
            "test-event-3",
            1672531400,
            // 2022-12-31 in Adelaide timezone
            1672493100.0, // 23:55 (5 minutes before midnight)
            1672493120.0, // 20 seconds later
            "motion",
            "camera1",
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
    let (status, body) =
        make_api_request(app_router.clone(), "/api/events").await;
    let events = validate_and_parse_events(status, &body);

    // Verify we got our test events
    assert_eq!(events.len(), 3);
    let event_ids = extract_event_ids(&events);

    assert!(event_ids.contains(&"test-event-1"));
    assert!(event_ids.contains(&"test-event-2"));
    assert!(event_ids.contains(&"test-event-3"));

    // Test with date filter - using fixed date "2022-12-31"
    let fixed_date = "2022-12-31";
    let (status, body) = make_api_request(
        app_router.clone(),
        &format!("/api/events?date={}", fixed_date),
    )
    .await;
    let events = validate_and_parse_events(status, &body);

    assert_eq!(events.len(), 3, "Expected 3 events for date {}", fixed_date);
    let event_ids = extract_event_ids(&events);

    assert!(event_ids.contains(&"test-event-1"));
    assert!(event_ids.contains(&"test-event-2"));
    assert!(event_ids.contains(&"test-event-3"));

    // Test with camera filter
    let (status, body) =
        make_api_request(app_router.clone(), "/api/events?camera_id=camera1")
            .await;
    let events = validate_and_parse_events(status, &body);

    // Verify camera-filtered events
    assert!(!events.is_empty());
    assert!(events.iter().all(|e| e["camera_id"] == "camera1"));
    assert_eq!(events.len(), 2);
    let event_ids = extract_event_ids(&events);

    assert!(event_ids.contains(&"test-event-1"));
    assert!(event_ids.contains(&"test-event-3"));

    // Test with time range filter
    let time_query_url = format!(
        "/api/events?date={}&time_start={}&time_end={}",
        fixed_date, "00:04:59", "23:00:00"
    );
    debug!("Making request to: {}", time_query_url);

    let (status, body) =
        make_api_request(app_router.clone(), &time_query_url).await;
    let events = validate_and_parse_events(status, &body);
    let event_ids = extract_event_ids(&events);

    assert!(event_ids.contains(&"test-event-1"));
    assert!(event_ids.contains(&"test-event-2"));

    assert_eq!(
        events.len(),
        2,
        "Expected 2 events in time range 00:04:59 to 23:00:00, got {}",
        events.len()
    );

    // Test with combined filters (camera and time range)
    let time_query_url = format!(
        "/api/events?camera_id=camera1&date={}&time_start={}&time_end={}",
        fixed_date, "10:00:00", "23:59:00"
    );
    debug!("Making request to: {}", time_query_url);

    let (status, body) =
        make_api_request(app_router.clone(), &time_query_url).await;
    let events = validate_and_parse_events(status, &body);

    assert_eq!(
        events.len(),
        1,
        "Expected 1 event in time range 10:00:00 to 23:59:00, got {}",
        events.len()
    );
    let event_ids = extract_event_ids(&events);

    assert!(event_ids.contains(&"test-event-3"));
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
