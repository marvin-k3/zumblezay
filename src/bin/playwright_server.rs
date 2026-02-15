use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use axum::Router;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use chrono::{NaiveDate, NaiveTime, TimeZone};
use clap::Parser;
use rusqlite::params;
use serde_json::json;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::{sleep, Duration as TokioDuration};
use zumblezay::app;
use zumblezay::bedrock::{
    BedrockClientTrait, BedrockCompletionResponse, BedrockUsage,
};
use zumblezay::transcripts;
use zumblezay::AppState;

struct SeedEvent {
    id: &'static str,
    camera_id: &'static str,
    date: &'static str,
    time: &'static str,
    transcript: Option<&'static str>,
}

const SEED_EVENTS: &[SeedEvent] = &[
    SeedEvent {
        id: "event-alpha",
        camera_id: "camera-1",
        date: "2024-05-01",
        time: "08:15:00",
        transcript: Some("Visitor left a package on the porch."),
    },
    SeedEvent {
        id: "event-beta",
        camera_id: "camera-2",
        date: "2024-05-01",
        time: "13:30:00",
        transcript: None,
    },
    SeedEvent {
        id: "event-gamma",
        camera_id: "camera-3",
        date: "2024-05-02",
        time: "06:45:00",
        transcript: Some("Morning patrol completed without incidents."),
    },
];

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Launches the Zumblezay test server used by Playwright integration tests"
)]
struct Args {
    /// Host interface to bind
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// TCP port to bind
    #[arg(long, default_value_t = 4173)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_logging();
    let args = Args::parse();

    let bedrock_client: Arc<dyn BedrockClientTrait> =
        Arc::new(PlaywrightBedrockClient);
    let mut state =
        AppState::new_for_testing_with_clients(None, Some(bedrock_client));
    let video_root = prepare_video_assets(SEED_EVENTS)?;
    state.video_path_replacement_prefix = video_root
        .to_str()
        .ok_or_else(|| anyhow!("Video root path contains invalid UTF-8"))?
        .to_owned();

    let state = Arc::new(state);
    seed_databases(&state, SEED_EVENTS).await?;
    app::cache_camera_names(&state).await?;
    app::ensure_templates();

    let router: Router = app::routes(state.clone());
    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = TcpListener::bind(addr).await.with_context(|| {
        format!("failed to bind server listener on {}", addr)
    })?;
    let local_addr = listener.local_addr()?;
    println!("Playwright test server listening on {}", local_addr);

    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = signal::ctrl_c().await;
        })
        .await
        .context("server shutdown with error")?;

    Ok(())
}

struct PlaywrightBedrockClient;

#[async_trait]
impl BedrockClientTrait for PlaywrightBedrockClient {
    async fn complete_text(
        &self,
        _model_id: &str,
        system_prompt: &str,
        _user_prompt: &str,
        _max_tokens: i32,
    ) -> Result<BedrockCompletionResponse> {
        if system_prompt.contains("investigation planner") {
            return Ok(BedrockCompletionResponse {
                content: r#"{
                    "search_queries": ["package", "porch"],
                    "time_window_start_utc": "2024-01-01T00:00:00Z",
                    "time_window_end_utc": "2026-12-31T23:59:59Z",
                    "tool_calls": ["get_current_time"]
                }"#
                .to_string(),
                usage: BedrockUsage {
                    input_tokens: 25,
                    output_tokens: 12,
                },
                request_id: Some("playwright-plan".to_string()),
            });
        }

        Ok(BedrockCompletionResponse {
            content: json!({
                "answer": "### Findings\nThe visitor left a **package** on the porch.",
                "evidence_event_ids": ["event-alpha"]
            })
            .to_string(),
            usage: BedrockUsage {
                input_tokens: 40,
                output_tokens: 20,
            },
            request_id: Some("playwright-answer".to_string()),
        })
    }

    async fn complete_text_streaming(
        &self,
        _model_id: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _max_tokens: i32,
        on_delta: &mut (dyn FnMut(String) + Send),
    ) -> Result<BedrockCompletionResponse> {
        let chunks = [
            "{\"answer\":\"### Findings\\n",
            "The visitor left a **package** on the porch.\",",
            "\"evidence_event_ids\":[\"event-alpha\"]}",
        ];
        let mut content = String::new();
        for chunk in chunks {
            sleep(TokioDuration::from_millis(45)).await;
            on_delta(chunk.to_string());
            content.push_str(chunk);
        }
        Ok(BedrockCompletionResponse {
            content,
            usage: BedrockUsage {
                input_tokens: 42,
                output_tokens: 24,
            },
            request_id: Some("playwright-stream-answer".to_string()),
        })
    }
}

fn initialize_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .compact()
        .try_init();
}

fn prepare_video_assets(events: &[SeedEvent]) -> Result<PathBuf> {
    let project_root =
        std::env::current_dir().context("failed to get current dir")?;
    let source = project_root.join("testdata/10s-bars.mp4");
    if !source.exists() {
        return Err(anyhow!(
            "expected sample video at {}, but it is missing",
            source.display()
        ));
    }

    let video_root = project_root.join("target/playwright-videos");
    for event in events {
        let camera_dir = video_root.join(event.camera_id);
        fs::create_dir_all(&camera_dir).with_context(|| {
            format!("failed to create {}", camera_dir.display())
        })?;

        let destination = camera_dir.join(format!("{}.mp4", event.id));
        if !destination.exists() {
            fs::copy(&source, &destination).with_context(|| {
                format!(
                    "failed to copy video from {} to {}",
                    source.display(),
                    destination.display()
                )
            })?;
        }
    }

    Ok(video_root)
}

async fn seed_databases(
    state: &Arc<AppState>,
    events: &[SeedEvent],
) -> Result<()> {
    for event in events {
        let (event_start, created_at) =
            convert_local_to_epoch(state, event.date, event.time)?;
        let event_end = event_start + 20.0;

        insert_event(
            state,
            event.id,
            event.camera_id,
            event_start,
            event_end,
            created_at,
        )?;

        if let Some(transcript_text) = event.transcript {
            insert_transcript(state, event.id, created_at, transcript_text)
                .with_context(|| {
                    format!("failed to insert transcript for {}", event.id)
                })?;
        } else {
            remove_transcript(state, event.id).with_context(|| {
                format!("failed to remove transcript for {}", event.id)
            })?;
        }

        insert_storyboard(state, event.id).with_context(|| {
            format!("failed to seed storyboard for {}", event.id)
        })?;
    }

    Ok(())
}

fn convert_local_to_epoch(
    state: &Arc<AppState>,
    date: &str,
    time: &str,
) -> Result<(f64, i64)> {
    let naive_date =
        NaiveDate::parse_from_str(date, "%Y-%m-%d").with_context(|| {
            format!("invalid date {} – expected YYYY-MM-DD", date)
        })?;
    let naive_time =
        NaiveTime::parse_from_str(time, "%H:%M:%S").with_context(|| {
            format!("invalid time {} – expected HH:MM:SS", time)
        })?;
    let naive_dt = naive_date.and_time(naive_time);
    let local_dt = state
        .timezone
        .from_local_datetime(&naive_dt)
        .single()
        .ok_or_else(|| {
            anyhow!(
                "{} {} is ambiguous in timezone {}",
                date,
                time,
                state.timezone
            )
        })?;

    let utc_dt = local_dt.with_timezone(&chrono::Utc);
    Ok((utc_dt.timestamp() as f64, utc_dt.timestamp()))
}

fn insert_event(
    state: &Arc<AppState>,
    event_id: &str,
    camera_id: &str,
    event_start: f64,
    event_end: f64,
    created_at: i64,
) -> Result<()> {
    let video_path = format!("/data/{}/{}.mp4", camera_id, event_id);
    let conn = state.zumblezay_db.get()?;
    conn.execute(
        "INSERT OR REPLACE INTO events (
            event_id,
            created_at,
            event_start,
            event_end,
            event_type,
            camera_id,
            video_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            created_at,
            event_start,
            event_end,
            "motion",
            camera_id,
            video_path
        ],
    )
    .context("failed to insert event record")?;

    Ok(())
}

fn insert_transcript(
    state: &Arc<AppState>,
    event_id: &str,
    created_at: i64,
    text: &str,
) -> Result<()> {
    let conn = state.zumblezay_db.get()?;
    let segments = serde_json::json!([
        {
            "id": 0,
            "start": 0.0,
            "end": 3.0,
            "text": text
        },
        {
            "id": 1,
            "start": 3.5,
            "end": 7.0,
            "text": format!("{} (continued)", text)
        }
    ]);

    let raw_response = serde_json::json!({
        "text": text,
        "segments": segments
    });

    let raw_response_string = raw_response.to_string();
    conn.execute(
        "INSERT INTO transcriptions (
            event_id,
            created_at,
            transcription_type,
            url,
            duration_ms,
            raw_response
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, transcription_type) DO UPDATE SET
            created_at = excluded.created_at,
            url = excluded.url,
            duration_ms = excluded.duration_ms,
            raw_response = excluded.raw_response",
        params![
            event_id,
            created_at,
            state.transcription_service.as_str(),
            format!("{}/{}", state.whisper_url.as_str(), event_id),
            5000,
            raw_response_string,
        ],
    )
    .context("failed to insert transcription")?;

    transcripts::update_transcript_search_from_str(
        &conn,
        event_id,
        &raw_response_string,
    )
    .context("failed to update transcript search index")?;

    Ok(())
}

fn remove_transcript(state: &Arc<AppState>, event_id: &str) -> Result<()> {
    let conn = state.zumblezay_db.get()?;
    conn.execute(
        "DELETE FROM transcriptions WHERE event_id = ?",
        params![event_id],
    )
    .context("failed to remove transcription entry")?;

    Ok(())
}

fn insert_storyboard(state: &Arc<AppState>, event_id: &str) -> Result<()> {
    let conn = state.cache_db.get()?;
    conn.execute(
        "DELETE FROM storyboard_cache WHERE event_id = ?",
        params![event_id],
    )
    .context("failed to clear previous storyboard cache entry")?;

    let storyboard_vtt = format!(
        "WEBVTT\n\n00:00:00.000 --> 00:00:05.000\n/api/storyboard/image/{}#xywh=0,0,160,90\n",
        event_id
    );

    let storyboard_image = BASE64
        .decode(STORYBOARD_PLACEHOLDER)
        .context("failed to decode storyboard placeholder image")?;

    conn.execute(
        "INSERT INTO storyboard_cache (
            event_id,
            version,
            created_at,
            generation_duration_ms,
            vtt,
            image
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            event_id,
            "v1",
            chrono::Utc::now().timestamp(),
            250,
            storyboard_vtt,
            storyboard_image
        ],
    )
    .context("failed to insert storyboard cache")?;

    Ok(())
}

const STORYBOARD_PLACEHOLDER: &str = "\
/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxISEhURExIVFhUVFRUVFRUVFRUVFRUWFxUVFRUY\
HSggGBolGxUVITEhJSkrLi4uFx8zODMtNygtLisBCgoKDg0OGhAQGi0lHyUtLS0tLS0tLS0tLS0t\
LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLv/AABEIAKgBLAMBIgACEQEDEQH/xAAZ\
AAADAQEBAAAAAAAAAAAAAAABAgMBBAX/xAAVAQEBAAAAAAAAAAAAAAAAAAAEBf/EABUBAQEAAAAA\
AAAAAAAAAAAAAAAB/9oADAMBAAIRAxEAPwD/AP/Z";
