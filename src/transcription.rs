use crate::AppState;
use crate::Event;
use anyhow::{Context, Result};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with_macros::skip_serializing_none;
use std::io::Read;

use std::path::PathBuf;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, instrument};
use url::Url;

#[derive(Deserialize, Debug)]
struct RunpodResponse {
    id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct RunpodStatusResponse {
    id: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    output: Option<Value>,
}
// Add this new helper function to select the appropriate transcription service
pub async fn get_transcript(
    event: &Event,
    state: &AppState,
) -> Result<(String, i64)> {
    match state.transcription_service.as_str() {
        "whisper-local" => {
            get_local_whisper_transcript(event, &state.whisper_url, state).await
        }
        "runpod" => {
            let api_key = state.runpod_api_key.as_ref().ok_or_else(|| {
                anyhow::anyhow!("RunPod API key not configured")
            })?;
            get_runpod_whisper_transcript(event, api_key, &state.whisper_url)
                .await
        }
        unknown => Err(anyhow::anyhow!(
            "Unknown transcription service: {}",
            unknown
        )),
    }
}

#[instrument(err)]
async fn extract_audio(video_path: String) -> Result<Vec<u8>> {
    info!("Starting audio extraction for {}", video_path);

    let handle = tokio::task::spawn_blocking(move || {
        // Check if file exists first
        if !std::path::Path::new(&video_path).exists() {
            return Err(anyhow::anyhow!(
                "Video file does not exist: {}",
                video_path
            ));
        }

        let mut command = std::process::Command::new("ffmpeg");
        let command = command
            .arg("-i")
            .arg(&video_path)
            .arg("-vn")
            .arg("-acodec")
            .arg("pcm_s16le")
            .arg("-ac")
            .arg("1")
            .arg("-ar")
            .arg("16000")
            .arg("-f")
            .arg("wav")
            .arg("pipe:1")
            .stderr(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());

        debug!("Running FFmpeg command: {:?}", command);

        let mut child = command
            .spawn()
            .map_err(|e| anyhow::anyhow!("Failed to spawn FFmpeg: {}", e))?;

        let mut output = Vec::new();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let mut reader = std::io::BufReader::new(stdout);

        // Read in chunks
        let mut buffer = [0; 8192];
        loop {
            match reader.read(&mut buffer) {
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
            .map_err(|e| anyhow::anyhow!("Failed to wait for FFmpeg: {}", e))?;

        if !status.success() {
            let stderr = child
                .stderr
                .take()
                .map(|stderr| {
                    let mut s = String::new();
                    std::io::Read::read_to_string(
                        &mut std::io::BufReader::new(stderr),
                        &mut s,
                    )
                    .map(|_| s)
                    .unwrap_or_else(|_| "Could not read stderr".to_string())
                })
                .unwrap_or_else(|| "No stderr available".to_string());

            error!("FFmpeg failed with status {}: {}", status, stderr);
            return Err(anyhow::anyhow!("FFmpeg failed: {}", stderr));
        }

        if output.is_empty() {
            error!("FFmpeg produced no output data");
            return Err(anyhow::anyhow!("FFmpeg produced no output"));
        }

        debug!("Successfully extracted audio: {} bytes", output.len());
        Ok(output)
    });

    // Wait for the blocking task to complete
    match handle.await {
        Ok(result) => result,
        Err(e) => {
            if e.is_cancelled() {
                error!("Audio extraction was cancelled");
                Err(anyhow::anyhow!("Audio extraction cancelled"))
            } else {
                Err(anyhow::anyhow!("Audio extraction failed: {}", e))
            }
        }
    }
}

// Defaults: https://github.com/runpod-workers/worker-faster_whisper/blob/main/src/rp_schema.py
#[skip_serializing_none]
#[derive(Debug, Serialize)]
#[serde(default)]
#[derive(Default)]
pub struct RunpodFasterWhisperInput {
    // Default: None
    pub audio: Option<String>,
    // Default: None
    pub audio_base64: Option<String>,
    // Default: "base"
    pub model: Option<String>,
    // Default: "plain_text"
    pub transcription: Option<String>,
    // Default: false
    pub translate: Option<bool>,
    // Default: "plain_text"
    pub translation: Option<String>,
    // Default: None
    pub language: Option<String>,
    // Default: 0.0
    pub temperature: Option<f64>,
    // Default: 5
    pub best_of: Option<i32>,
    // Default: 5
    pub beam_size: Option<i32>,
    // Default: 1.0
    pub patience: Option<f64>,
    // Default: 0.0
    pub length_penalty: Option<f64>,
    // Default: "-1"
    pub suppress_tokens: Option<String>,
    // Default: None
    pub initial_prompt: Option<String>,
    // Default: true
    pub condition_on_previous_text: Option<bool>,
    // Default: 0.2
    pub temperature_increment_on_fallback: Option<f64>,
    // Default: 2.4
    pub compression_ratio_threshold: Option<f64>,
    // Default: -1.0
    pub logprob_threshold: Option<f64>,
    // Default: 0.6
    pub no_speech_threshold: Option<f64>,
    // Default: false
    pub enable_vad: Option<bool>,
    // Default: false
    pub word_timestamps: Option<bool>,
}

// Add Default implementation with all None values

#[derive(Debug, Serialize)]
pub struct RunPodFasterWhisperRequest {
    pub input: RunpodFasterWhisperInput,
}

#[instrument(err)]
async fn get_runpod_whisper_transcript(
    event: &Event,
    runpod_api_key: &str,
    _whisper_url: &str,
) -> Result<(String, i64)> {
    let start_time = std::time::Instant::now();
    let client = reqwest::Client::new();

    let mut input = RunpodFasterWhisperInput::default();
    input.audio = Some(format!(
        // TODO: Make this configurable, this should point back to this server.
        "https://user:pass@example.com/audio/{}/wav?key=$AUTH_TOKEN",
        event.id
    ));
    input.model = Some("large-v2".to_string());
    input.enable_vad = Some(true);
    input.language = Some("en".to_string());
    input.word_timestamps = Some(true);

    let request = RunPodFasterWhisperRequest { input };
    let request_json = serde_json::to_string(&request).unwrap();
    info!("Request JSON: {}", request_json);

    // TODO: Make this configurable, target is the runpod serverless endpoint.
    let target = "FIXME";

    // Initial job submission
    let response = client
        .post(&format!("https://api.runpod.ai/v2/{}/run", target))
        .header("Content-Type", "application/json")
        .header("Authorization", runpod_api_key)
        .json(&request)
        .send()
        .await?;

    // Get status code before consuming response with text()
    let status = response.status();
    if !status.is_success() {
        let error_text = response.text().await?;
        return Err(anyhow::anyhow!("API error {}: {}", status, error_text));
    }

    let response_data: RunpodResponse = response.json().await?;
    let job_id = response_data.id;
    info!("Started job: {} - {}", job_id, response_data.status);

    // Status polling with exponential backoff
    let mut delay = Duration::from_secs(1);
    const MAX_DELAY: Duration = Duration::from_secs(10);
    const MAX_ATTEMPTS: u32 = 30;

    for attempt in 0..MAX_ATTEMPTS {
        time::sleep(delay).await;

        let status_url =
            format!("https://api.runpod.ai/v2/{}/status/{}", target, job_id);
        let status_response = client
            .get(&status_url)
            .header("Authorization", runpod_api_key)
            .send()
            .await?;

        // Get status code before consuming response with text()
        let status = status_response.status();
        if !status.is_success() {
            let error_text = status_response.text().await?;
            return Err(anyhow::anyhow!(
                "Status API error {}: {}",
                status,
                error_text
            ));
        }
        let status_text = status_response.text().await?;
        info!("Status response: {}", status_text);

        let status_data: RunpodStatusResponse =
            serde_json::from_str(&status_text)?;

        match status_data.status.as_str() {
            "COMPLETED" => {
                info!("Job {} completed in {} attempts", job_id, attempt + 1);
                let duration_ms = start_time.elapsed().as_millis() as i64;
                if let Some(output) = status_data.output {
                    return Ok((output.to_string(), duration_ms));
                }
                return Err(anyhow::anyhow!(
                    "No output received from completed job"
                ));
            }
            "FAILED" => {
                let error_msg = status_data
                    .error
                    .unwrap_or_else(|| "Unknown error".to_string());
                return Err(anyhow::anyhow!(
                    "Job failed: id={} - {}",
                    status_data.id,
                    error_msg
                ));
            }
            _ => {
                debug!(
                    "Job {} status: {} (attempt {})",
                    job_id,
                    status_data.status,
                    attempt + 1
                );
                // Exponential backoff with max delay
                delay = std::cmp::min(delay * 2, MAX_DELAY);
            }
        }
    }

    Err(anyhow::anyhow!(
        "Job timed out after {} attempts",
        MAX_ATTEMPTS
    ))
}

// Function to process video through Whisper API
#[instrument(skip(whisper_url, state), err)]
async fn get_local_whisper_transcript(
    event: &Event,
    whisper_url: &str,
    state: &AppState,
) -> Result<(String, i64)> {
    info!("Processing video for event {}", event.id);
    let start_time = std::time::Instant::now();
    let client = reqwest::Client::new();
    let video_path =
        PathBuf::from(&event.path.clone().context("No path for event")?);

    let modified_path = video_path.to_string_lossy().replace(
        &state.video_path_original_prefix,
        &state.video_path_replacement_prefix,
    );
    debug!("Reading video file from path: {}", modified_path);

    // Extract audio in a blocking thread
    info!("Extracting audio from video");
    let wav_data = extract_audio(modified_path.to_string()).await?;

    // Send audio to Whisper
    let part = Part::bytes(wav_data)
        .file_name("audio.wav")
        .mime_str("audio/wav")?;

    let form = Form::new().part("audio_file", part);
    debug!("Sending request to Whisper API at {}", whisper_url);
    let mut url = Url::parse(whisper_url)?;
    url.query_pairs_mut().append_pair("output", "json");
    let url_string = url.to_string(); // Create a longer-lived string

    let response = client
        .post(url_string) // Use the longer-lived string instead of url.to_string()
        .multipart(form)
        .send()
        .await
        .context("Failed to send request to Whisper API")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_else(|e| {
            format!("Could not read error response: {}", e)
        });

        // Log the full error details
        error!(
            "1Runpod API returned error status {}: {}. ",
            status, error_text
        );

        return Err(anyhow::anyhow!(
            "2Runpod API returned error ({}): {}",
            status,
            error_text
        ));
    }

    let response_text = response
        .text()
        .await
        .context("Failed to get response text from Whisper API")?;

    debug!("Received response from Whisper API: {}", response_text);

    // Verify it's valid JSON but we don't need to parse it fully
    serde_json::from_str::<serde_json::Value>(&response_text)
        .context("Failed to validate Whisper API response as JSON")?;

    info!("Successfully transcribed video");
    let duration_ms = start_time.elapsed().as_millis() as i64;
    Ok((response_text, duration_ms))
}
