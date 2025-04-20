#![allow(dead_code)]
#![allow(unused)]

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Row;

#[derive(Debug)]
pub struct Dataset {
    pub dataset_id: i64,
    pub name: String,
    pub description: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

pub fn fetch_all_datasets(
    conn: &mut rusqlite::Connection,
) -> Result<Vec<Dataset>, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT * FROM eval_datasets")?;
    let dataset_iter = stmt.query_map([], |row| {
        Ok(Dataset {
            dataset_id: row.get(0)?,
            name: row.get(1)?,
            description: row.get(2)?,
            created_at: chrono::DateTime::from_timestamp(row.get(3)?, 0)
                .ok_or_else(|| {
                    rusqlite::Error::InvalidParameterName(
                        "Invalid timestamp".into(),
                    )
                })?,
        })
    })?;

    // Collect results into a Vec and return
    dataset_iter.collect::<Result<Vec<_>, _>>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use rusqlite::{params, Connection};

    #[test]
    fn test_fetch_all_datasets() {
        // Create a test state and fake OpenAI client
        let state = AppState::new_for_testing();
        let mut conn = state
            .zumblezay_db
            .get()
            .expect("Failed to get DB connection");

        // Check if the table exists
        let table_exists: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='eval_datasets'",
                [],
                |row| Ok(true),
            )
            .unwrap_or(false);

        assert!(table_exists, "The eval_datasets table does not exist in the database. Please create it before running the test.");

        // Insert test data
        let now = chrono::Utc::now();
        let timestamp = now.timestamp();

        conn.execute(
            "INSERT INTO eval_datasets (dataset_id, name, description, created_at) VALUES (?, ?, ?, ?)",
            params![1, "Test Dataset 1", "First test dataset", timestamp],
        ).expect("Failed to insert first test dataset");

        conn.execute(
            "INSERT INTO eval_datasets (dataset_id, name, description, created_at) VALUES (?, ?, ?, ?)",
            params![2, "Test Dataset 2", "Second test dataset", timestamp],
        ).expect("Failed to insert second test dataset");

        // Call the function being tested
        let datasets =
            fetch_all_datasets(&mut conn).expect("Failed to fetch datasets");

        // Verify results
        assert_eq!(datasets.len(), 2);
        assert_eq!(datasets[0].dataset_id, 1);
        assert_eq!(datasets[0].name, "Test Dataset 1");
        assert_eq!(datasets[0].description, "First test dataset");
        assert_eq!(datasets[0].created_at.timestamp(), timestamp);

        assert_eq!(datasets[1].dataset_id, 2);
        assert_eq!(datasets[1].name, "Test Dataset 2");
        assert_eq!(datasets[1].description, "Second test dataset");
        assert_eq!(datasets[1].created_at.timestamp(), timestamp);
    }
}
