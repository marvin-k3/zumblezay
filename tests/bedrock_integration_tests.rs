use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use http_body_util::BodyExt;
use rusqlite::params;
use serde_json::json;
use std::env;
use std::sync::Arc;
use tower::util::ServiceExt;
use zumblezay::bedrock::create_bedrock_client;
use zumblezay::test_utils::init_test_logging;
use zumblezay::AppState;

fn is_live_bedrock_enabled() -> bool {
    matches!(
        env::var("RUN_BEDROCK_INTEGRATION").ok().as_deref(),
        Some("1")
    )
}

#[tokio::test]
async fn test_live_bedrock_investigation_integration() {
    init_test_logging();
    if !is_live_bedrock_enabled() {
        return;
    }

    let model_id =
        env::var("BEDROCK_INVESTIGATION_MODEL").unwrap_or_else(|_| {
            "us.anthropic.claude-sonnet-4-5-20250929-v1:0".to_string()
        });
    let mut state = AppState::new_for_testing_with_clients(
        None,
        Some(create_bedrock_client(env::var("AWS_REGION").ok())),
    );
    state.investigation_model = model_id.clone();
    let app_state = Arc::new(state);
    let app = zumblezay::app::routes(app_state.clone());

    let conn = app_state.zumblezay_db.get().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO bedrock_pricing (
            model_id,
            input_cost_per_1k_usd,
            output_cost_per_1k_usd,
            updated_at
        ) VALUES (?, ?, ?, ?)",
        params![model_id, 0.003, 0.015, chrono::Utc::now().timestamp()],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO events (
            event_id, created_at, event_start, event_end, event_type, camera_id, video_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            "live-int-1",
            chrono::Utc::now().timestamp(),
            chrono::Utc::now().timestamp() as f64 - 3600.0,
            chrono::Utc::now().timestamp() as f64 - 3540.0,
            "motion",
            "integration-cam",
            "/data/integration/live-int-1.mp4",
        ],
    )
    .unwrap();

    let transcript = json!({
        "text": "The child is eating lunch in the kitchen.",
        "segments": [
            {"start": 5.0, "end": 12.0, "text": "The child is eating lunch in the kitchen."}
        ]
    });
    conn.execute(
        "INSERT INTO transcriptions (
            event_id, created_at, transcription_type, url, duration_ms, raw_response
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            "live-int-1",
            chrono::Utc::now().timestamp(),
            app_state.transcription_service.as_str(),
            "https://example.test/transcripts/live-int-1",
            1000,
            transcript.to_string(),
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO transcript_search (event_id, content) VALUES (?, ?)",
        params!["live-int-1", "The child is eating lunch in the kitchen."],
    )
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/investigate")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"question":"when did the child have lunch"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "investigate returned {} with body: {}",
        status,
        String::from_utf8_lossy(&body)
    );
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(!payload["answer"].as_str().unwrap_or("").is_empty());
    assert!(
        payload["usage"]["input_tokens"]
            .as_i64()
            .unwrap_or_default()
            > 0,
        "expected bedrock token usage in response"
    );
}

#[tokio::test]
async fn test_live_bedrock_investigation_streaming_integration() {
    init_test_logging();
    if !is_live_bedrock_enabled() {
        return;
    }

    let model_id =
        env::var("BEDROCK_INVESTIGATION_MODEL").unwrap_or_else(|_| {
            "us.anthropic.claude-sonnet-4-5-20250929-v1:0".to_string()
        });
    let mut state = AppState::new_for_testing_with_clients(
        None,
        Some(create_bedrock_client(env::var("AWS_REGION").ok())),
    );
    state.investigation_model = model_id.clone();
    let app_state = Arc::new(state);
    let app = zumblezay::app::routes(app_state.clone());

    let conn = app_state.zumblezay_db.get().unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO bedrock_pricing (
            model_id,
            input_cost_per_1k_usd,
            output_cost_per_1k_usd,
            updated_at
        ) VALUES (?, ?, ?, ?)",
        params![
            "anthropic.claude-sonnet-4-5-20250929-v1:0",
            0.003,
            0.015,
            chrono::Utc::now().timestamp()
        ],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO events (
            event_id, created_at, event_start, event_end, event_type, camera_id, video_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            "live-stream-1",
            chrono::Utc::now().timestamp(),
            chrono::Utc::now().timestamp() as f64 - 3600.0,
            chrono::Utc::now().timestamp() as f64 - 3540.0,
            "motion",
            "integration-cam",
            "/data/integration/live-stream-1.mp4",
        ],
    )
    .unwrap();

    let transcript = json!({
        "text": "The child is eating lunch in the kitchen.",
        "segments": [
            {"start": 5.0, "end": 12.0, "text": "The child is eating lunch in the kitchen."}
        ]
    });
    conn.execute(
        "INSERT INTO transcriptions (
            event_id, created_at, transcription_type, url, duration_ms, raw_response
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            "live-stream-1",
            chrono::Utc::now().timestamp(),
            app_state.transcription_service.as_str(),
            "https://example.test/transcripts/live-stream-1",
            1000,
            transcript.to_string(),
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO transcript_search (event_id, content) VALUES (?, ?)",
        params!["live-stream-1", "The child is eating lunch in the kitchen."],
    )
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/investigate/stream")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({"question":"when did the child have lunch"})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "stream endpoint returned {} with body: {}",
        status,
        String::from_utf8_lossy(&body)
    );
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("event: done"),
        "expected done event, got: {}",
        text
    );
    if !text.contains("event: answer_delta") {
        assert!(
            text.contains("\"answer\":"),
            "expected either answer deltas or a done payload with answer, got: {}",
            text
        );
    }
}
