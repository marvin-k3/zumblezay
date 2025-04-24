use crate::AppState;
use anyhow::Result;
use rusqlite::{params_from_iter, Error as SqliteError, Row};
use std::sync::Arc;

#[derive(Debug)]
struct Event {
    event_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    event_start: chrono::DateTime<chrono::Utc>,
    event_end: chrono::DateTime<chrono::Utc>,
    event_type: String,
    camera_id: String,
    video_path: String,
}

impl Event {
    fn try_from_row(row: &Row) -> Result<Event, SqliteError> {
        let created_at_ts: f64 = row.get("created_at")?;
        let event_start_ts: f64 = row.get("event_start")?;
        let event_end_ts: f64 = row.get("event_end")?;

        let created_at =
            chrono::DateTime::from_timestamp(created_at_ts as i64, 0)
                .ok_or_else(|| {
                    SqliteError::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Real,
                        "Invalid timestamp for created_at".into(),
                    )
                })?;

        let event_start =
            chrono::DateTime::from_timestamp(event_start_ts as i64, 0)
                .ok_or_else(|| {
                    SqliteError::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Real,
                        "Invalid timestamp for event_start".into(),
                    )
                })?;

        let event_end =
            chrono::DateTime::from_timestamp(event_end_ts as i64, 0)
                .ok_or_else(|| {
                    SqliteError::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Real,
                        "Invalid timestamp for event_end".into(),
                    )
                })?;

        Ok(Event {
            event_id: row.get("event_id")?,
            created_at,
            event_start,
            event_end,
            event_type: row.get("event_type")?,
            camera_id: row.get("camera_id")?,
            video_path: row.get("video_path")?,
        })
    }
}

fn get_events(state: Arc<AppState>, event_ids: &[&str]) -> Result<Vec<Event>> {
    let conn = state.zumblezay_db.get()?;

    let query = format!(
        "SELECT event_id, created_at, event_start, event_end, event_type, camera_id, video_path \
         FROM events WHERE event_id IN ({})",
        event_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",")
    );

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(params_from_iter(event_ids), |row| {
        Event::try_from_row(row)
    })?;
    let events = rows.collect::<Result<Vec<_>, _>>()?;

    Ok(events)
}

pub fn understand_event(
    state: Arc<AppState>,
    event_id: &str,
) -> Result<String> {
    let events = get_events(state, &[event_id])?;
    let event = events
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Event not found: {}", event_id))?;

    Ok(format!("Understanding event: {}", event.event_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use chrono::Utc;
    use r2d2::PooledConnection;
    use r2d2_sqlite::SqliteConnectionManager;
    use rusqlite::params;
    use std::sync::Arc;

    fn insert_test_event(
        conn: &PooledConnection<SqliteConnectionManager>,
        event_id: &str,
        start_offset: i64,
    ) -> Result<()> {
        let now = Utc::now().timestamp() as f64; // Cast to f64 since test DB uses REAL
        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                event_id,
                now,
                now + (start_offset as f64),
                now + (start_offset as f64) + 60.0,  // 1 minute duration
                "test_type",
                "camera1",
                format!("/path/to/{}.mp4", event_id)
            ],
        )?;
        Ok(())
    }

    #[test]
    fn test_get_single_event() -> Result<()> {
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get()?;
        insert_test_event(&conn, "event1", 0)?;

        let events = get_events(state, &["event1"])?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "event1");
        assert_eq!(events[0].event_type, "test_type");
        assert_eq!(events[0].camera_id, "camera1");
        Ok(())
    }

    #[test]
    fn test_get_multiple_events() -> Result<()> {
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get()?;
        insert_test_event(&conn, "event1", 0)?;
        insert_test_event(&conn, "event2", 60)?;

        let events = get_events(state, &["event1", "event2"])?;
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|e| e.event_id == "event1"));
        assert!(events.iter().any(|e| e.event_id == "event2"));
        Ok(())
    }

    #[test]
    fn test_get_nonexistent_event() -> Result<()> {
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get()?;
        insert_test_event(&conn, "event1", 0)?;

        let events = get_events(state, &["nonexistent"])?;
        assert_eq!(events.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_mixed_existing_and_nonexistent() -> Result<()> {
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get()?;
        insert_test_event(&conn, "event1", 0)?;

        let events = get_events(state, &["event1", "nonexistent"])?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id, "event1");
        Ok(())
    }
}
