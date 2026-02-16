use axum::{body::Body, http::Request, http::StatusCode};
use chrono::{LocalResult, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use http_body_util::BodyExt;
use rusqlite::params;
use std::sync::Arc;
use tower::ServiceExt;
use zumblezay::{app, hybrid_search, transcripts, AppState};

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
                "ambiguous local time {} between {} and {}",
                naive_dt, first, second
            )
        }
        LocalResult::None => {
            panic!(
                "local time {} does not exist in timezone {}",
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
    raw_response: serde_json::Value,
) {
    let (event_start, created_at) = local_time_to_epoch(state, date, time_str);
    let event_end = event_start + 60.0;

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
            format!("/data/{}/{}.mp4", camera_id, event_id)
        ],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO transcriptions (
            event_id, created_at, transcription_type, url,
            duration_ms, raw_response
         ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            created_at,
            state.transcription_service.as_str(),
            "https://example.test/transcript",
            1000,
            raw_response.to_string(),
        ],
    )
    .unwrap();

    transcripts::update_transcript_search(&conn, event_id, &raw_response)
        .unwrap();
}

async fn make_api_request(
    router: axum::Router,
    uri: &str,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json_value = serde_json::from_slice(&body).unwrap_or_else(|_| {
        panic!("non-json response body: {}", String::from_utf8_lossy(&body))
    });
    (status, json_value)
}

#[tokio::test]
async fn hybrid_search_returns_results_from_events_endpoint() {
    let state = Arc::new(AppState::new_for_testing());
    let router = app::routes(state.clone());

    let transcript = serde_json::json!({
        "text": "child had lunch in the kitchen",
        "segments": [
            {"start": 0.0, "end": 12.0, "text": "child had lunch"},
            {"start": 15.0, "end": 25.0, "text": "in the kitchen"}
        ]
    });
    insert_event_with_transcript(
        &state,
        "evt-hybrid-1",
        "2024-03-15",
        "12:00:00",
        "cam-kitchen",
        transcript.clone(),
    );

    let conn = state.zumblezay_db.get().unwrap();
    hybrid_search::index_transcript_units_for_event(
        &conn,
        "evt-hybrid-1",
        &transcript,
        0.0,
    )
    .unwrap();
    hybrid_search::process_pending_embedding_jobs(&conn, 100).unwrap();

    let (status, payload) =
        make_api_request(router, "/api/events?date=2024-03-15&q=child+lunch")
            .await;

    assert_eq!(status, StatusCode::OK);
    let events = payload["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["event_id"], "evt-hybrid-1");
}

#[tokio::test]
async fn bm25_fallback_works_when_vectors_are_missing() {
    let state = Arc::new(AppState::new_for_testing());
    let router = app::routes(state.clone());

    let transcript = serde_json::json!({
        "text": "nanny and child drawing with crayons",
        "segments": [{"start": 0.0, "end": 10.0, "text": "nanny and child drawing"}]
    });
    insert_event_with_transcript(
        &state,
        "evt-bm25-only",
        "2024-03-15",
        "09:00:00",
        "cam-playroom",
        transcript,
    );

    let (status, payload) =
        make_api_request(router, "/api/events?date=2024-03-15&q=child+drawing")
            .await;

    assert_eq!(status, StatusCode::OK);
    let events = payload["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["event_id"], "evt-bm25-only");
}

#[tokio::test]
async fn segment_evidence_metadata_is_returned() {
    let state = Arc::new(AppState::new_for_testing());
    let router = app::routes(state.clone());

    let transcript = serde_json::json!({
        "text": "intro smalltalk. then very specific keyword zebra appears.",
        "segments": [
            {"start": 0.0, "end": 10.0, "text": "intro smalltalk"},
            {"start": 35.0, "end": 45.0, "text": "very specific keyword zebra appears"}
        ]
    });
    insert_event_with_transcript(
        &state,
        "evt-segment-meta",
        "2024-03-15",
        "11:00:00",
        "cam-hall",
        transcript.clone(),
    );

    let conn = state.zumblezay_db.get().unwrap();
    hybrid_search::index_transcript_units_for_event(
        &conn,
        "evt-segment-meta",
        &transcript,
        0.0,
    )
    .unwrap();
    hybrid_search::process_pending_embedding_jobs(&conn, 100).unwrap();

    conn.execute(
        "DELETE FROM unit_embeddings
         WHERE unit_id IN (
            SELECT unit_id FROM transcript_units
            WHERE event_id = ? AND unit_type = 'full_clip'
         )",
        params!["evt-segment-meta"],
    )
    .unwrap();

    let (status, payload) =
        make_api_request(router, "/api/events?date=2024-03-15&q=zebra").await;

    assert_eq!(status, StatusCode::OK);
    let events = payload["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["event_id"], "evt-segment-meta");
    assert_eq!(events[0]["search_unit_type"], "segment");
    assert!(events[0]["search_segment_start_ms"].is_number());
    assert!(events[0]["search_segment_end_ms"].is_number());
}

#[tokio::test]
async fn pending_embedding_jobs_do_not_block_searchability() {
    let state = Arc::new(AppState::new_for_testing());
    let router = app::routes(state.clone());

    let transcript = serde_json::json!({
        "text": "child lunch happened near noon",
        "segments": [{"start": 0.0, "end": 12.0, "text": "child lunch happened"}]
    });
    insert_event_with_transcript(
        &state,
        "evt-pending",
        "2024-03-15",
        "12:30:00",
        "cam-dining",
        transcript.clone(),
    );

    let conn = state.zumblezay_db.get().unwrap();
    hybrid_search::index_transcript_units_for_event(
        &conn,
        "evt-pending",
        &transcript,
        0.0,
    )
    .unwrap();

    let pending_jobs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM embedding_jobs WHERE status = 'pending'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(pending_jobs > 0);

    let (status, payload) =
        make_api_request(router, "/api/events?date=2024-03-15&q=child+lunch")
            .await;

    assert_eq!(status, StatusCode::OK);
    let events = payload["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["event_id"], "evt-pending");
}
