use axum::{
    body::Body,
    http::{header, Request, StatusCode},
    Router,
};
use chrono::{LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use http_body_util::BodyExt;
use pretty_assertions::assert_eq;
use rusqlite::params;
use serde_json::json;
use std::env;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time;
use tokio_util::bytes;
use tower::util::ServiceExt;
use tracing::debug;
use zumblezay::openai::{fake::FakeOpenAIClient, OpenAIClientTrait};
use zumblezay::test_utils::init_test_logging;
use zumblezay::AppState;

/// Create a test app with just the health endpoint
fn app() -> (Arc<AppState>, Router) {
    // Create a minimal AppState for testing
    let app_state = Arc::new(AppState::new_for_testing());
    let routes = zumblezay::app::routes(app_state.clone());
    (app_state, routes)
}

fn app_with_custom_state<F>(customize: F) -> (Arc<AppState>, Router)
where
    F: FnOnce(&mut AppState),
{
    let mut state = AppState::new_for_testing();
    customize(&mut state);
    let app_state = Arc::new(state);
    let routes = zumblezay::app::routes(app_state.clone());
    (app_state, routes)
}

fn app_with_openai_client(
    openai_client: Arc<dyn OpenAIClientTrait>,
) -> (Arc<AppState>, Router) {
    let app_state = Arc::new(AppState::new_for_testing_with_openai_client(
        Some(openai_client),
    ));
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
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            eprintln!(
                "Skipping test_with_real_server because binding to a local port is not permitted: {}",
                e
            );
            return;
        }
        Err(e) => panic!("Failed to bind TcpListener: {}", e),
    };
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

fn local_time_to_epoch(
    state: &AppState,
    date: &str,
    time_str: &str,
) -> (f64, i64) {
    let naive_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("valid date");
    let naive_time =
        NaiveTime::parse_from_str(time_str, "%H:%M:%S").expect("valid time");
    let naive_dt = NaiveDateTime::new(naive_date, naive_time);

    let local_dt = match state.timezone.from_local_datetime(&naive_dt) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(first, second) => {
            panic!(
                "Ambiguous local time {} between {} and {}",
                naive_dt, first, second
            )
        }
        LocalResult::None => {
            panic!(
                "Local time {} does not exist in timezone {}",
                naive_dt, state.timezone
            )
        }
    };

    let utc_dt = local_dt.with_timezone(&Utc);
    (utc_dt.timestamp() as f64, utc_dt.timestamp())
}

fn insert_event_with_transcript(
    state: &AppState,
    event_id: &str,
    date: &str,
    time_str: &str,
    camera_id: &str,
    event_text: &str,
) {
    let (event_start, created_at) = local_time_to_epoch(state, date, time_str);
    let event_end = event_start + 15.0;

    let video_path = format!("/data/{}/{}.mp4", camera_id, event_id);
    let conn = state.zumblezay_db.get().unwrap();
    conn.execute(
        "INSERT INTO events (
            event_id, created_at, event_start, event_end,
            event_type, camera_id, video_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            created_at,
            event_start,
            event_end,
            "motion",
            camera_id,
            video_path,
        ],
    )
    .unwrap();

    let raw_response = json!({
        "text": event_text,
        "segments": [
            {"start": 0.0, "end": 5.0, "text": event_text}
        ]
    });

    conn.execute(
        "INSERT INTO transcriptions (
            event_id,
            created_at,
            transcription_type,
            url,
            duration_ms,
            raw_response
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            created_at,
            state.transcription_service.as_str(),
            format!("{}/{}", state.whisper_url.as_str(), event_id),
            1500,
            raw_response.to_string(),
        ],
    )
    .unwrap();
}

async fn refresh_camera_cache(state: &Arc<AppState>) {
    zumblezay::app::cache_camera_names(state)
        .await
        .expect("camera cache populates");
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

    conn.execute(
        "INSERT INTO transcriptions (event_id, created_at, transcription_type, url, duration_ms, raw_response)
         VALUES (?, ?, ?, ?, ?, ?)",
        params![
            "test-event-1",
            1672531200,
            "whisper-local",
            "https://example.com/transcript/test-event-1",
            1234,
            "{}"
        ],
    )
    .unwrap();

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

    let event_with_transcript = events
        .iter()
        .find(|e| e["event_id"] == "test-event-1")
        .expect("test-event-1 present");
    assert_eq!(event_with_transcript["has_transcript"], json!(true));
    assert!(
        events
            .iter()
            .filter(|e| e["event_id"] != "test-event-1")
            .all(|e| e["has_transcript"] == json!(false)),
        "Expected other events to have has_transcript=false"
    );

    // Test with camera filter
    let (status, body) = make_api_request(
        app_router.clone(),
        "/api/events?camera_id=camera1&date=2022-12-31",
    )
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

#[tokio::test]
async fn test_transcripts_json_endpoint_returns_expected_entries() {
    init_test_logging();
    let (app_state, app_router) = app();

    insert_event_with_transcript(
        &app_state,
        "event-alpha",
        "2024-03-15",
        "08:15:00",
        "camera-alpha",
        "Activity near the front door",
    );
    insert_event_with_transcript(
        &app_state,
        "event-bravo",
        "2024-03-15",
        "09:45:10",
        "camera-bravo",
        "Delivery arrived",
    );
    refresh_camera_cache(&app_state).await;

    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/transcripts/json/2024-03-15")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["date"], "2024-03-15");
    let transcripts = payload["transcripts"].as_array().unwrap();
    assert_eq!(transcripts.len(), 2);

    assert_eq!(transcripts[0]["event_id"], "event-alpha");
    assert_eq!(transcripts[0]["camera"], "camera-alpha");
    assert_eq!(transcripts[0]["text"], "Activity near the front door");
    assert_eq!(transcripts[0]["time"], "08:15:00");

    assert_eq!(transcripts[1]["event_id"], "event-bravo");
    assert_eq!(transcripts[1]["camera"], "camera-bravo");
    assert_eq!(transcripts[1]["text"], "Delivery arrived");
    assert_eq!(transcripts[1]["time"], "09:45:10");
}

#[tokio::test]
async fn test_transcripts_csv_endpoint_returns_expected_rows() {
    init_test_logging();
    let (app_state, app_router) = app();

    insert_event_with_transcript(
        &app_state,
        "event-charlie",
        "2024-05-01",
        "06:00:00",
        "camera-foyer",
        "Morning check-in",
    );
    insert_event_with_transcript(
        &app_state,
        "event-delta",
        "2024-05-01",
        "21:30:05",
        "camera-garden",
        "Motion detected at the gate",
    );
    refresh_camera_cache(&app_state).await;

    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/transcripts/csv/2024-05-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap()
        .to_string();
    assert_eq!(content_type, "text/csv");

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let csv = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        csv.starts_with("time,camera,transcription"),
        "CSV header missing: {}",
        csv
    );
    assert!(
        csv.contains("\"06:00:00\",\"camera-foyer\",\"Morning check-in\""),
        "First row missing: {}",
        csv
    );
    assert!(
        csv.contains(
            "\"21:30:05\",\"camera-garden\",\"Motion detected at the gate\""
        ),
        "Second row missing: {}",
        csv
    );
}

#[tokio::test]
async fn test_transcript_summary_endpoints_return_cached_content() {
    init_test_logging();
    let (app_state, app_router) = app();
    let conn = app_state.zumblezay_db.get().unwrap();

    let default_model = app_state.default_summary_model.clone();
    let now = chrono::Utc::now().timestamp();
    conn.execute(
        "INSERT INTO daily_summaries (
            date, created_at, model, prompt_name, summary_type, content, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            "2024-07-04",
            now,
            default_model.as_str(),
            "default",
            "markdown",
            "# Holiday Summary\n- Fireworks at night",
            1200,
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO daily_summaries (
            date, created_at, model, prompt_name, summary_type, content, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            "2024-07-04",
            now + 1,
            default_model.as_str(),
            "default",
            "json",
            r#"[{"highlight":"Fireworks at night"}]"#,
            800,
        ],
    )
    .unwrap();

    // Markdown summary endpoint should serve cached HTML
    let markdown_response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/transcripts/summary/2024-07-04")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(markdown_response.status(), StatusCode::OK);
    let markdown_body = markdown_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let markdown_html = String::from_utf8(markdown_body.to_vec()).unwrap();
    assert!(
        markdown_html.contains("Fireworks at night"),
        "Markdown summary missing expected bullet text: {}",
        markdown_html
    );
    assert!(
        !markdown_html.contains("# Holiday Summary"),
        "Markdown header was not converted to HTML: {}",
        markdown_html
    );

    // JSON summary endpoint should serve cached JSON body
    let json_response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/transcripts/summary/2024-07-04/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(json_response.status(), StatusCode::OK);
    let json_body = json_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let json_value: serde_json::Value =
        serde_json::from_slice(&json_body).unwrap();
    assert_eq!(
        json_value,
        serde_json::json!([{"highlight": "Fireworks at night"}])
    );

    // Listing endpoint should show both summaries with canonical URLs
    let list_response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/transcripts/summary/2024-07-04/list")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);
    let list_body = list_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let list_value: serde_json::Value =
        serde_json::from_slice(&list_body).unwrap();
    let summaries = list_value["summaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 2);
    let urls: Vec<&str> = summaries
        .iter()
        .map(|entry| entry["url"].as_str().unwrap())
        .collect();
    let markdown_url = format!(
        "/api/transcripts/summary/2024-07-04/type/markdown/model/{}/prompt/default",
        default_model
    );
    let json_url = format!(
        "/api/transcripts/summary/2024-07-04/type/json/model/{}/prompt/default",
        default_model
    );
    assert!(urls.iter().any(|url| url == &markdown_url));
    assert!(urls.iter().any(|url| url == &json_url));

    // Fetching a specific summary should respect the summary type
    let specific_markdown = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(markdown_url.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(specific_markdown.status(), StatusCode::OK);
    let markdown_specific = specific_markdown
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let markdown_specific_str =
        String::from_utf8(markdown_specific.to_vec()).unwrap();
    assert!(
        markdown_specific_str.contains("<li>Fireworks at night</li>"),
        "Markdown specific summary should render HTML list item: {}",
        markdown_specific_str
    );

    let specific_json = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri(json_url.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(specific_json.status(), StatusCode::OK);
    let specific_json_body = specific_json
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let specific_json_value: serde_json::Value =
        serde_json::from_slice(&specific_json_body).unwrap();
    assert_eq!(
        specific_json_value,
        serde_json::json!([{"highlight": "Fireworks at night"}])
    );
}

#[tokio::test]
async fn test_event_detail_and_captions_endpoints() {
    init_test_logging();
    let (app_state, app_router) = app();

    insert_event_with_transcript(
        &app_state,
        "event-echo",
        "2024-09-09",
        "13:05:30",
        "camera-porch",
        "Visitor chatted at the door",
    );
    refresh_camera_cache(&app_state).await;

    let event_response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/event/event-echo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(event_response.status(), StatusCode::OK);
    let event_body = event_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let event_json: serde_json::Value =
        serde_json::from_slice(&event_body).unwrap();

    assert_eq!(event_json["event_id"], "event-echo");
    assert_eq!(event_json["camera_id"], "camera-porch");
    assert_eq!(event_json["camera_name"], "camera-porch");
    assert_eq!(event_json["has_transcript"], true);
    assert_eq!(
        event_json["transcription_type"],
        app_state.transcription_service
    );
    assert!(event_json["raw_response"]
        .as_str()
        .unwrap()
        .contains("Visitor chatted at the door"));

    let captions_response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/captions/event-echo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(captions_response.status(), StatusCode::OK);
    let captions_body = captions_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let captions_text = String::from_utf8(captions_body.to_vec()).unwrap();
    assert!(
        captions_text.starts_with("WEBVTT"),
        "VTT output must start with WEBVTT"
    );
    assert!(
        captions_text.contains("Visitor chatted at the door"),
        "VTT output missing transcript text"
    );
}

#[tokio::test]
async fn test_video_streams_entire_file_without_range() {
    init_test_logging();
    let project_root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let video_dir = project_root.join("testdata");
    let video_filename = "10s-bars.mp4";
    let file_path = video_dir.join(video_filename);
    assert!(
        file_path.exists(),
        "expected test video at {}",
        file_path.display()
    );

    let replacement_prefix = video_dir.to_string_lossy().into_owned();
    let (app_state, app_router) = app_with_custom_state(|state| {
        state.video_path_original_prefix = "/data".to_string();
        state.video_path_replacement_prefix = replacement_prefix.clone();
    });

    let conn = app_state.zumblezay_db.get().unwrap();
    let video_path = format!(
        "{}/{}",
        app_state.video_path_original_prefix, video_filename
    );
    conn.execute(
        "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            "video-event",
            0_i64,
            0.0_f64,
            10.0_f64,
            "motion",
            "camera-test",
            video_path
        ],
    )
    .unwrap();

    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/video/video-event")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let expected = std::fs::read(&file_path).unwrap();

    assert_eq!(body.len(), expected.len());
    assert_eq!(body.as_ref(), expected.as_slice());
    assert_eq!(
        headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap(),
        "video/mp4"
    );
    assert_eq!(
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .unwrap(),
        expected.len().to_string()
    );
    let expected_range =
        format!("bytes 0-{:}/{}", expected.len() - 1, expected.len());
    assert_eq!(
        headers
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .unwrap(),
        expected_range
    );
}

#[tokio::test]
async fn test_status_api_reflects_runtime_metrics() {
    init_test_logging();
    let (app_state, app_router) = app();

    {
        let mut last_processed = app_state.last_processed_time.lock().await;
        *last_processed = 12345.5;
    }

    {
        let mut tasks = app_state.active_tasks.lock().await;
        tasks.insert("event-1".to_string(), "transcribing".to_string());
        tasks.insert("event-2".to_string(), "summarizing".to_string());
    }

    app_state
        .stats
        .processed_count
        .fetch_add(5, Ordering::Relaxed);
    app_state.stats.error_count.fetch_add(1, Ordering::Relaxed);
    app_state
        .stats
        .total_processing_time_ms
        .fetch_add(2500, Ordering::Relaxed);

    let response = app_router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["last_processed_time"].as_f64().unwrap(), 12345.5);
    let active_tasks = payload["active_tasks"].as_object().unwrap();
    assert_eq!(active_tasks.len(), 2);
    assert_eq!(active_tasks["event-1"], "transcribing");
    assert_eq!(active_tasks["event-2"], "summarizing");

    let stats = payload["stats"].as_object().unwrap();
    assert_eq!(stats["processed_count"], 5);
    assert_eq!(stats["error_count"], 1);
    assert_eq!(stats["total_processing_time_ms"], 2500);
    let average = stats["average_processing_time_ms"].as_f64().unwrap();
    assert!((average - 500.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_models_api() {
    init_test_logging();
    use async_openai::types::Model;
    use zumblezay::app::ModelsResponse;

    // Test data
    let test_models = vec![
        Model {
            id: "anthropic-claude-haiku".to_string(),
            object: "model".to_string(),
            created: 1716460800,
            owned_by: "anthropic".to_string(),
        },
        Model {
            id: "anthropic-claude-sonnet".to_string(),
            object: "model".to_string(),
            created: 1716450800,
            owned_by: "anthropic".to_string(),
        },
    ];
    let openai_client =
        Arc::new(FakeOpenAIClient::new().with_models(test_models.clone()));
    let (_, app_router) = app_with_openai_client(openai_client);

    let response = app_router
        .oneshot(
            Request::builder()
                .uri("/api/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Verify status and parse response
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let models_response: ModelsResponse =
        serde_json::from_slice(&body).unwrap();

    // Check response integrity
    assert_eq!(
        models_response.models.len(),
        test_models.len(),
        "Should return the correct number of models"
    );

    // Verify model data
    let expected_ids = ["anthropic-claude-haiku", "anthropic-claude-sonnet"];
    for expected_id in expected_ids {
        let model = models_response
            .models
            .iter()
            .find(|m| m.id == expected_id)
            .unwrap_or_else(|| {
                panic!("Missing expected model: {}", expected_id)
            });

        assert_eq!(
            model.provider, "anthropic",
            "Model {} should have provider 'anthropic'",
            expected_id
        );
        assert!(
            !model.name.is_empty(),
            "Model {} should have a name",
            expected_id
        );
    }
}

#[tokio::test]
async fn test_prompt_context_api() {
    init_test_logging();
    let (app_state, app_router) = app();

    // Insert test data
    app_state
        .prompt_context_store
        .insert(
            "test-key".to_string(),
            vec![bytes::Bytes::from("test-data")],
            time::Duration::from_secs(60),
        )
        .await;

    // Generate HMAC signature
    let signed_request = zumblezay::app::create_signed_request(
        &app_state,
        &"test-key".to_string(),
        0,
        time::Duration::from_secs(60),
    )
    .unwrap();

    // Build request with HMAC parameters
    let uri = format!(
        "/api/prompt_context/test-key/0?expires={}&hmac={}",
        signed_request.expires, signed_request.hmac
    );

    let response = app_router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"test-data");
}

#[tokio::test]
async fn test_prompt_context_api_not_found() {
    init_test_logging();
    let (app_state, app_router) = app();

    // Generate HMAC signature for non-existent key
    let signed_request = zumblezay::app::create_signed_request(
        &app_state,
        &"test-key".to_string(),
        0,
        time::Duration::from_secs(60),
    )
    .unwrap();

    // Build request with HMAC parameters
    let uri = format!(
        "/api/prompt_context/test-key/0?expires={}&hmac={}",
        signed_request.expires, signed_request.hmac
    );

    let response = app_router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_prompt_context_api_hmac_expired() {
    init_test_logging();
    let (app_state, app_router) = app();

    // Insert test data
    app_state
        .prompt_context_store
        .insert(
            "test-key".to_string(),
            vec![bytes::Bytes::from("test-data")],
            time::Duration::from_secs(60),
        )
        .await;

    // Generate HMAC signature with expired timestamp
    let signed_request = zumblezay::app::create_signed_request(
        &app_state,
        &"test-key".to_string(),
        0,
        time::Duration::from_secs(0), // Set to 0 to make it expired
    )
    .unwrap();

    // Build request with expired HMAC parameters
    let uri = format!(
        "/api/prompt_context/test-key/0?expires={}&hmac={}",
        signed_request.expires, signed_request.hmac
    );

    let response = app_router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_prompt_context_api_hmac_invalid() {
    init_test_logging();
    let (app_state, app_router) = app();

    // Insert test data
    app_state
        .prompt_context_store
        .insert(
            "test-key".to_string(),
            vec![bytes::Bytes::from("test-data")],
            time::Duration::from_secs(60),
        )
        .await;

    // Generate valid HMAC signature
    let signed_request = zumblezay::app::create_signed_request(
        &app_state,
        &"test-key".to_string(),
        0,
        time::Duration::from_secs(60),
    )
    .unwrap();

    // Build request with invalid HMAC (tampered with)
    let uri = format!(
        "/api/prompt_context/test-key/0?expires={}&hmac=tampered_hmac",
        signed_request.expires
    );

    let response = app_router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_prompt_context_api_hmac_missing() {
    init_test_logging();
    let (app_state, app_router) = app();

    // Insert test data
    app_state
        .prompt_context_store
        .insert(
            "test-key".to_string(),
            vec![bytes::Bytes::from("test-data")],
            time::Duration::from_secs(60),
        )
        .await;

    // Make request without HMAC parameters
    let uri = "/api/prompt_context/test-key/0";

    let response = app_router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
