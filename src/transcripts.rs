use crate::time_util::parse_local_date_to_utc_range;
use crate::AppState;
use crate::Event;
use anyhow::Result;
use chrono::{TimeZone, Utc};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{info, instrument};

#[derive(Debug, Serialize, Deserialize)]
pub struct Transcript {
    event_id: String,
    created_at: i64,
    event_start: f64,
    event_end: f64,
    event_type: String,
    camera_id: String,
    camera_name: Option<String>,
    video_path: Option<String>,
    transcription_type: String,
    transcription_duration_ms: i64,
    raw_response: String,
    url: String,
}

#[instrument(skip(state, raw_response), err)]
pub async fn save_transcript(
    state: &AppState,
    event: &Event,
    raw_response: &str,
    duration_ms: i64,
) -> Result<()> {
    info!("Saving transcript for event {}", event.id);
    let conn = state.zumblezay_db.get()?;

    conn.execute(
        "INSERT INTO transcriptions (
            event_id, created_at, transcription_type, url,
            duration_ms, raw_response
        ) VALUES (?, ?, ?, ?, ?, ?)",
        params![
            event.id,
            chrono::Utc::now().timestamp(),
            state.transcription_service.as_str(),
            state.whisper_url,
            duration_ms,
            raw_response,
        ],
    )?;

    Ok(())
}

#[instrument(skip(state))]
pub async fn get_formatted_transcripts_for_date(
    state: &AppState,
    date: &str,
) -> Result<String, anyhow::Error> {
    let camera_names: HashMap<String, String> =
        state.camera_name_cache.lock().await.clone();
    let (start_timestamp, end_timestamp) =
        parse_local_date_to_utc_range(date, state.timezone)?;

    let conn = state
        .zumblezay_db
        .get()
        .map_err(|e| anyhow::anyhow!("Failed to get connection: {}", e))?;
    let mut stmt = conn
        .prepare(
            "SELECT 
            e.event_start,
            t.raw_response,
            e.camera_id
         FROM events e
         JOIN transcriptions t ON e.event_id = t.event_id
         WHERE 
            e.event_start >= ? 
            AND e.event_start < ? 
            AND t.transcription_type = ?
         ORDER BY e.event_start ASC",
        )
        .map_err(|e| anyhow::anyhow!("Failed to prepare statement: {}", e))?;

    let mut rows = stmt
        .query(params![
            start_timestamp,
            end_timestamp,
            state.transcription_service.as_str()
        ])
        .map_err(|e| anyhow::anyhow!("Failed to query transcripts: {}", e))?;

    // Start building CSV content with headers
    let mut csv_content = String::from("time,camera,transcription\n");

    while let Ok(Some(row)) = rows.next() {
        let timestamp: f64 = row
            .get(0)
            .map_err(|e| anyhow::anyhow!("Failed to get timestamp: {}", e))?;
        let raw_response: String = row.get(1).map_err(|e| {
            anyhow::anyhow!("Failed to get raw response: {}", e)
        })?;
        let camera_id: String = row
            .get(2)
            .map_err(|e| anyhow::anyhow!("Failed to get camera id: {}", e))?;

        // Parse the JSON response
        let json: Value = serde_json::from_str(&raw_response).map_err(|e| {
            anyhow::anyhow!("Failed to parse JSON response: {}", e)
        })?;

        // Extract the text field
        let text = json["text"].as_str().unwrap_or("").trim();

        // Skip empty transcriptions
        if text.is_empty() {
            continue;
        }

        // Convert UTC timestamp to local time for display
        let utc_datetime = Utc
            .timestamp_opt(timestamp as i64, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp"))?;
        let local_time = utc_datetime.with_timezone(&state.timezone);

        let formatted_time = local_time.format("%H:%M:%S");

        // Get camera display name (use name if available, otherwise ID)
        let camera_display = camera_names.get(&camera_id).unwrap_or(&camera_id);

        // Escape any quotes in the text and camera name, and wrap in quotes
        let escaped_text = text.replace("\"", "\"\"");
        let escaped_camera = camera_display.replace("\"", "\"\"");

        // Add the row to CSV
        csv_content.push_str(&format!(
            "\"{}\",\"{}\",\"{}\"\n",
            formatted_time, escaped_camera, escaped_text
        ));
    }

    Ok(csv_content)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Remove unused imports

    // Helper function to set up a test database with events and transcriptions
    async fn setup_test_db(state: &AppState) -> Result<(), anyhow::Error> {
        // Get the correct timestamp range for 2023-01-01
        let conn = state.zumblezay_db.get()?;
        let (start_ts, end_ts) =
            parse_local_date_to_utc_range("2023-01-01", state.timezone)?;
        println!(
            "Setting up test data with timestamps in range: {} to {}",
            start_ts, end_ts
        );

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

    // Helper function to verify CSV content
    fn verify_csv_contains(csv: &str, expected_text: &str) -> bool {
        csv.lines().any(|line| line.contains(expected_text))
    }

    #[tokio::test]
    async fn test_save_transcript() -> Result<(), anyhow::Error> {
        // Create a test AppState
        let state = AppState::new_for_testing();

        // Set up test database
        setup_test_db(&state).await?;

        // Get the first event ID and timestamp
        let conn = state.zumblezay_db.get()?;
        let (event_id, event_start, event_end): (String, f64, f64) = conn.query_row(
            "SELECT event_id, event_start, event_end FROM events WHERE camera_id = 'camera1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;

        // Create a test event
        let event = Event {
            id: event_id,
            type_: "motion".to_string(),
            camera_id: "camera1".to_string(),
            camera_name: Some("Living Room".to_string()),
            start: event_start,
            end: event_end,
            path: Some("/data/videos/test1.mp4".to_string()),
        };

        // Create a test transcript response
        let raw_response = r#"{"text": "This is a test transcript"}"#;
        let duration_ms = 500;

        // Save the transcript
        save_transcript(&state, &event, raw_response, duration_ms).await?;

        // Verify the transcript was saved correctly
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM transcriptions WHERE event_id = ?",
            params![event.id],
            |row| row.get(0),
        )?;

        assert_eq!(count, 1, "Transcript should be saved in the database");

        // Verify the transcript content
        let saved_response: String = conn.query_row(
            "SELECT raw_response FROM transcriptions WHERE event_id = ?",
            params![event.id],
            |row| row.get(0),
        )?;

        assert_eq!(
            saved_response, raw_response,
            "Saved transcript should match the original"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_formatted_transcripts_for_date(
    ) -> Result<(), anyhow::Error> {
        // Create a test AppState
        let state = AppState::new_for_testing();

        // Set up test database
        setup_test_db(&state).await?;

        // Get event IDs
        let conn = state.zumblezay_db.get()?;
        let event1_id: String = conn.query_row(
            "SELECT event_id FROM events WHERE camera_id = 'camera1'",
            [],
            |row| row.get(0),
        )?;

        let event2_id: String = conn.query_row(
            "SELECT event_id FROM events WHERE camera_id = 'camera2'",
            [],
            |row| row.get(0),
        )?;

        // Print the transcription service for debugging
        println!("Transcription service: {}", state.transcription_service);

        // Insert test transcriptions
        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                event1_id,
                chrono::Utc::now().timestamp(),
                state.transcription_service.as_str(),
                state.whisper_url,
                500,
                r#"{"text": "This is a test transcript from the living room"}"#,
            ],
        )?;

        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                event2_id,
                chrono::Utc::now().timestamp(),
                state.transcription_service.as_str(),
                state.whisper_url,
                600,
                r#"{"text": "Someone is at the front door"}"#,
            ],
        )?;

        // Verify the data is in the database
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events e JOIN transcriptions t ON e.event_id = t.event_id",
            [],
            |row| row.get(0),
        )?;
        println!("Number of joined records: {}", count);

        // Debug the date range conversion
        let (start_ts, end_ts) =
            parse_local_date_to_utc_range("2023-01-01", state.timezone)?;
        println!("Date range for 2023-01-01: {} to {}", start_ts, end_ts);

        // Check if our events fall within this range
        let events_in_range: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE event_start >= ? AND event_start < ?",
            params![start_ts, end_ts],
            |row| row.get(0),
        )?;
        println!("Events in date range: {}", events_in_range);

        // Get formatted transcripts for 2023-01-01
        let csv_content =
            get_formatted_transcripts_for_date(&state, "2023-01-01").await?;

        // Print the CSV content for debugging
        println!("CSV Content: {}", csv_content);

        // Verify the CSV content
        assert!(
            csv_content.contains("time,camera,transcription"),
            "CSV should have headers"
        );

        // Use more specific checks for camera names
        assert!(
            verify_csv_contains(&csv_content, "\"Living Room\""),
            "CSV should contain Living Room camera name"
        );
        assert!(
            verify_csv_contains(&csv_content, "\"Front Door\""),
            "CSV should contain Front Door camera name"
        );

        assert!(
            verify_csv_contains(
                &csv_content,
                "This is a test transcript from the living room"
            ),
            "CSV should contain transcript text"
        );
        assert!(
            verify_csv_contains(&csv_content, "Someone is at the front door"),
            "CSV should contain transcript text"
        );

        // Verify the format (should have 3 lines: header + 2 transcripts)
        let line_count = csv_content.lines().count();
        assert_eq!(
            line_count, 3,
            "CSV should have 3 lines (header + 2 transcripts)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_timezone_conversion_in_transcripts(
    ) -> Result<(), anyhow::Error> {
        let mut state = AppState::new_for_testing();
        setup_test_db(&state).await?;
        let conn = state.zumblezay_db.get()?;

        let event_id = "test-event-1";
        // Get the first event ID and timestamp
        let event_start: f64 = conn.query_row(
            "SELECT event_start FROM events WHERE event_id = ?",
            params![event_id],
            |row| row.get(0),
        )?;
        println!("Event start: {}", event_start);

        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                chrono::Utc::now().timestamp(),
                state.transcription_service.as_str(),
                state.whisper_url,
                500,
                r#"{"text": "This is a test with a fixed timestamp"}"#,
            ],
        )?;

        // Get formatted transcripts
        let csv_content =
            get_formatted_transcripts_for_date(&state, "2023-01-01").await?;

        println!("CSV Content with Adelaide timezone: {}", csv_content);

        let expected_time = "01:00:00";
        assert!(
            verify_csv_contains(&csv_content, expected_time),
            "CSV should contain the time formatted in Adelaide timezone ({})",
            expected_time
        );

        // Now change the timezone to a different one and verify the time changes
        state.timezone = chrono_tz::Australia::Sydney;

        // Use the same database connection
        let csv_content2 =
            get_formatted_transcripts_for_date(&state, "2023-01-01").await?;

        println!("CSV Content with Sydney timezone: {}", csv_content2);

        // Expected time in Sydney timezone (Sunday January 01, 2023 01:30:00)
        let expected_time2 = "01:30:00";

        assert!(
            verify_csv_contains(&csv_content2, expected_time2),
            "CSV should contain the time formatted in Sydney timezone ({})",
            expected_time2
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_formatted_transcripts_empty_result(
    ) -> Result<(), anyhow::Error> {
        // Create a test AppState
        let state = AppState::new_for_testing();

        // Set up test database but don't add any transcriptions
        setup_test_db(&state).await?;

        // Get formatted transcripts for a date with no transcriptions
        let csv_content =
            get_formatted_transcripts_for_date(&state, "2023-01-02").await?;

        // Verify the CSV content (should only have the header)
        assert_eq!(
            csv_content, "time,camera,transcription\n",
            "CSV should only have the header for empty results"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_transcription_text() -> Result<(), anyhow::Error> {
        // Create a test AppState
        let state = AppState::new_for_testing();

        // Set up test database
        setup_test_db(&state).await?;

        // Get the first event ID
        let conn = state.zumblezay_db.get()?;
        let event_id: String =
            conn.query_row("SELECT event_id FROM events LIMIT 1", [], |row| {
                row.get(0)
            })?;

        // Insert a test transcription with empty text
        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                chrono::Utc::now().timestamp(),
                state.transcription_service.as_str(),
                state.whisper_url,
                500,
                r#"{"text": ""}"#,
            ],
        )?;

        // Get formatted transcripts
        let csv_content =
            get_formatted_transcripts_for_date(&state, "2023-01-01").await?;

        // Verify the CSV content (should only have the header since empty transcriptions are skipped)
        assert_eq!(
            csv_content, "time,camera,transcription\n",
            "Empty transcriptions should be skipped"
        );

        // Debug the date range conversion
        let (start_ts, end_ts) =
            parse_local_date_to_utc_range("2023-01-01", state.timezone)?;
        println!("Date range for 2023-01-01: {} to {}", start_ts, end_ts);

        Ok(())
    }

    #[tokio::test]
    async fn test_transcript_with_quotes() -> Result<(), anyhow::Error> {
        // Create a test AppState
        let state = AppState::new_for_testing();

        // Set up test database
        setup_test_db(&state).await?;

        // Get the first event ID
        let conn = state.zumblezay_db.get()?;
        let event_id: String =
            conn.query_row("SELECT event_id FROM events LIMIT 1", [], |row| {
                row.get(0)
            })?;

        // Insert a test transcription with quotes in the text
        conn.execute(
            "INSERT INTO transcriptions (
                event_id, created_at, transcription_type, url,
                duration_ms, raw_response
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                event_id,
                chrono::Utc::now().timestamp(),
                state.transcription_service.as_str(),
                state.whisper_url,
                500,
                r#"{"text": "He said \"hello world\" loudly"}"#,
            ],
        )?;

        // Verify the data is in the database
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events e JOIN transcriptions t ON e.event_id = t.event_id",
            [],
            |row| row.get(0),
        )?;
        println!("Number of joined records for quotes test: {}", count);

        // Debug the date range conversion
        let (start_ts, end_ts) =
            parse_local_date_to_utc_range("2023-01-01", state.timezone)?;
        println!("Date range for 2023-01-01: {} to {}", start_ts, end_ts);

        // Check if our events fall within this range
        let events_in_range: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE event_start >= ? AND event_start < ?",
            params![start_ts, end_ts],
            |row| row.get(0),
        )?;
        println!("Events in date range: {}", events_in_range);

        // Get formatted transcripts
        let csv_content =
            get_formatted_transcripts_for_date(&state, "2023-01-01").await?;

        // Print the CSV content for debugging
        println!("CSV Content with quotes: {}", csv_content);

        // Check for properly escaped quotes in CSV
        assert!(
            verify_csv_contains(
                &csv_content,
                "He said \"\"hello world\"\" loudly"
            ),
            "Quotes should be properly escaped in CSV"
        );

        // Verify the format (should have 2 lines: header + 1 transcript)
        let line_count = csv_content.lines().count();
        assert_eq!(
            line_count, 2,
            "CSV should have 2 lines (header + 1 transcript)"
        );

        Ok(())
    }
}
