#![allow(dead_code)]
#![allow(unused)]

use super::models;
use anyhow::Result;
use chrono::Utc;
use clap::{Parser, Subcommand};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Row;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Import AppConfig, create_app_state, and CommonArgs
use crate::{cli::CommonArgs, create_app_state, AppConfig};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[command(flatten)]
    pub common_args: CommonArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a specific evaluation
    Run {
        /// Name of evaluation to run
        #[arg(long)]
        name: String,

        /// Output format (text, json, csv)
        #[arg(long, default_value = "text")]
        format: String,
    },

    /// List available evaluations
    List,

    /// Generate a report from evaluation results
    Report {
        /// Path to output the report
        #[arg(long)]
        output: PathBuf,

        /// Type of report to generate
        #[arg(long, default_value = "summary")]
        report_type: String,
    },

    /// List all available datasets
    ListDatasets,

    /// Add a new dataset
    AddDataset {
        /// Name of the dataset
        #[arg(long)]
        name: String,

        /// Description of the dataset
        #[arg(long)]
        description: String,
    },

    /// Add a new task to an existing dataset
    AddDatasetTask {
        /// Dataset ID to add task to
        #[arg(long)]
        dataset_id: i64,

        /// Comma-separated list of event IDs
        #[arg(long)]
        event_ids: String,
    
        /// Notes for the task
        #[arg(long)]
        notes: Option<String>,
    },

    /// List all tasks for a dataset
    ListDatasetTasks {
        /// Name of the dataset
        #[arg(long)]
        dataset_name: String,
    },
}

pub async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    run_app().await
}

pub async fn run_app() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Create database connection pools
    let events_manager =
        SqliteConnectionManager::file(&cli.common_args.events_db)
            .with_flags(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY);
    let events_pool = Pool::new(events_manager)?;

    // Create zumblezay pool with connection customization
    let zumblezay_manager =
        SqliteConnectionManager::file(&cli.common_args.zumblezay_db);
    let zumblezay_pool = Pool::new(zumblezay_manager)?;

    // Create cache pool
    let cache_manager =
        SqliteConnectionManager::file(&cli.common_args.cache_db);
    let cache_pool = Pool::new(cache_manager)?;

    // Initialize zumblezay database schema
    {
        let mut conn = zumblezay_pool.get()?;
        crate::init_zumblezay_db(&mut conn)?;
    }

    // Initialize cache database schema
    {
        let mut conn = cache_pool.get()?;
        crate::init_cache_db(&mut conn)?;
    }

    // Create application state by reusing AppConfig and create_app_state
    let app_config = AppConfig {
        events_pool,
        zumblezay_pool,
        cache_pool,
        whisper_url: cli.common_args.whisper_url,
        max_concurrent_tasks: cli.common_args.max_concurrent_tasks,
        openai_api_key: cli.common_args.openai_api_key,
        openai_api_base: cli.common_args.openai_api_base,
        runpod_api_key: cli.common_args.runpod_api_key,
        signing_secret: cli.common_args.signing_secret,
        transcription_service: cli.common_args.transcription_service,
        default_summary_model: cli.common_args.default_summary_model,
        video_path_original_prefix: cli.common_args.video_path_original_prefix,
        video_path_replacement_prefix: cli
            .common_args
            .video_path_replacement_prefix,
        timezone_str: cli.common_args.timezone,
    };

    let state = create_app_state(app_config);

    // Handle commands
    match cli.command {
        Commands::Run { name, format } => {
            info!("Running evaluation: {} with format: {}", name, format);
            run_evaluation(state, &name, &format).await?;
        }
        Commands::List => {
            info!("Listing available evaluations");
            list_evaluations(state).await?;
        }
        Commands::Report {
            output,
            report_type,
        } => {
            info!("Generating {} report to {}", report_type, output.display());
            generate_report(state, &output, &report_type).await?;
        }
        Commands::ListDatasets => {
            info!("Listing available datasets");
            list_datasets(state).await?;
        }
        Commands::AddDataset { name, description } => {
            info!("Adding new dataset: {}", name);
            add_dataset(state, &name, &description).await?;
        }
        Commands::AddDatasetTask { dataset_id, event_ids, notes } => {
            info!("Adding new task to dataset: {} with event IDs: {}", dataset_id, event_ids);
            add_dataset_task(state, dataset_id, &event_ids, notes.as_deref()).await?;
        }
        Commands::ListDatasetTasks { dataset_name } => {
            info!("Listing tasks for dataset: {}", dataset_name);
            list_dataset_tasks(state, &dataset_name).await?;
        }
    }

    Ok(())
}

async fn run_evaluation(
    state: Arc<crate::AppState>,
    name: &str,
    format: &str,
) -> Result<()> {
    info!("Running evaluation {} with format {}", name, format);
    // TODO: Implement evaluation logic
    println!("Evaluation '{}' completed successfully", name);
    Ok(())
}

async fn list_evaluations(state: Arc<crate::AppState>) -> Result<()> {
    info!("Listing available evaluations");
    // TODO: Implement listing logic
    println!("Available evaluations:");
    println!("  - accuracy: Evaluate model accuracy");
    println!("  - performance: Evaluate system performance");
    println!("  - coherence: Evaluate output coherence");
    Ok(())
}

async fn generate_report(
    state: Arc<crate::AppState>,
    output: &PathBuf,
    report_type: &str,
) -> Result<()> {
    info!("Generating {} report to {}", report_type, output.display());
    // TODO: Implement report generation logic
    println!("Report generated successfully to {}", output.display());
    Ok(())
}

async fn list_datasets(state: Arc<crate::AppState>) -> Result<()> {
    info!("Listing available datasets");

    // Fetch datasets using the helper function
    let datasets = {
        let mut conn = state.zumblezay_db.get()?;
        models::fetch_all_datasets(&mut conn)?
    };

    println!("Available datasets:");
    for dataset in datasets {
        println!("  - {}: {}", dataset.name, dataset.description);
    }

    Ok(())
}

async fn add_dataset(
    state: Arc<crate::AppState>,
    name: &str,
    description: &str,
) -> Result<()> {
    info!(
        "Adding dataset '{}' with description '{}'",
        name, description
    );

    let now = Utc::now();
    let timestamp = now.timestamp();

    {
        let mut conn = state.zumblezay_db.get()?;
        conn.execute(
            "INSERT INTO eval_datasets (name, description, created_at) VALUES (?, ?, ?)",
            rusqlite::params![name, description, timestamp],
        )?;
    }

    println!("Dataset '{}' added successfully", name);
    Ok(())
}

async fn add_dataset_task(
    state: Arc<crate::AppState>,
    dataset_id: i64,
    event_ids: &str,
    notes: Option<&str>,
) -> Result<()> {
    info!("Adding new task to dataset: {} with event IDs: {}", dataset_id, event_ids);
    
    let mut conn = state.zumblezay_db.get()?;
    let tx = conn.transaction()?;
    
    // First verify the dataset exists
    let dataset_exists: bool = tx.query_row(
        "SELECT 1 FROM eval_datasets WHERE dataset_id = ?",
        rusqlite::params![dataset_id],
        |_| Ok(true)
    ).unwrap_or(false);

    if !dataset_exists {
        return Err(anyhow::anyhow!("Dataset {} not found", dataset_id));
    }

    // Parse and validate event IDs
    let event_id_list: Vec<&str> = event_ids.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if event_id_list.is_empty() {
        return Err(anyhow::anyhow!("No valid event IDs provided"));
    }

    // Verify all events exist in the events database
    let mut invalid_events = Vec::new();
    for event_id in &event_id_list {
        let exists: bool = tx.query_row(
            "SELECT 1 FROM events WHERE event_id = ?",
            rusqlite::params![event_id],
            |_| Ok(true)
        ).unwrap_or(false);

        if !exists {
            invalid_events.push(*event_id);
        }
    }

    if !invalid_events.is_empty() {
        return Err(anyhow::anyhow!(
            "The following event IDs were not found: {}",
            invalid_events.join(", ")
        ));
    }

    // Create JSON array of event IDs
    let events_json = serde_json::to_string(&event_id_list)?;
    let now = Utc::now().timestamp();

    // Insert the new task
    tx.execute(
        "INSERT INTO eval_dataset_tasks (
            dataset_id, events, created_at, updated_at, notes
        ) VALUES (?, ?, ?, ?, ?)",
        rusqlite::params![dataset_id, events_json, now, now, notes],
    )?;

    // Commit the transaction
    tx.commit()?;

    println!("Task added successfully to dataset: {}", dataset_id);
    Ok(())
}

async fn list_dataset_tasks(
    state: Arc<crate::AppState>,
    dataset_name: &str,
) -> Result<()> {
    info!("Listing tasks for dataset: {}", dataset_name);
    
    let mut conn = state.zumblezay_db.get()?;
    let tx = conn.transaction()?;

    // First get the dataset ID from the name
    let dataset_id: i64 = tx.query_row(
        "SELECT dataset_id FROM eval_datasets WHERE name = ?",
        rusqlite::params![dataset_name],
        |row| row.get(0)
    ).map_err(|_| anyhow::anyhow!("Dataset '{}' not found", dataset_name))?;

    // Now fetch all tasks for this dataset
    let mut stmt = tx.prepare(
        "SELECT task_id, events, created_at, updated_at, notes, evaluation_data 
         FROM eval_dataset_tasks 
         WHERE dataset_id = ? 
         ORDER BY created_at DESC"
    )?;

    let tasks = stmt.query_map(rusqlite::params![dataset_id], |row| {
        Ok((
            row.get::<_, i64>(0)?, // task_id
            row.get::<_, String>(1)?, // events (JSON)
            row.get::<_, i64>(2)?, // created_at
            row.get::<_, i64>(3)?, // updated_at
            row.get::<_, Option<String>>(4)?, // notes
            row.get::<_, Option<String>>(5)?, // evaluation_data
        ))
    })?;

    println!("Tasks for dataset '{}':", dataset_name);
    for task_result in tasks {
        let (task_id, events_json, created_at, updated_at, notes, eval_data) = task_result?;
        
        // Parse events JSON array
        let events: Vec<String> = serde_json::from_str(&events_json)?;
        
        // Format timestamps
        let created = chrono::DateTime::from_timestamp(created_at, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid created_at timestamp"))?;
        let updated = chrono::DateTime::from_timestamp(updated_at, 0)
            .ok_or_else(|| anyhow::anyhow!("Invalid updated_at timestamp"))?;

        println!("\nTask ID: {}", task_id);
        println!("  Events: {}", events.join(", "));
        println!("  Created: {}", created.format("%Y-%m-%d %H:%M:%S UTC"));
        println!("  Updated: {}", updated.format("%Y-%m-%d %H:%M:%S UTC"));
        
        if let Some(notes) = notes {
            println!("  Notes: {}", notes);
        }
        
        if let Some(eval_data) = eval_data {
            println!("  Has evaluation data: yes");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppState;
    use rusqlite::{params, Connection};

    #[test]
    fn list_datasets_test() {
        // Create a test state and fake OpenAI client
        let state = Arc::new(AppState::new_for_testing());

        // Set up test database
        {
            let mut conn = state
                .zumblezay_db
                .get()
                .expect("Failed to get DB connection");

            // Insert test data
            let now = Utc::now();
            let timestamp = now.timestamp();

            conn.execute(
                "INSERT INTO eval_datasets (name, description, created_at) VALUES (?, ?, ?)",
                params!["Test Dataset 1", "First test dataset", timestamp],
            ).expect("Failed to insert first test dataset");

            conn.execute(
                "INSERT INTO eval_datasets (name, description, created_at) VALUES (?, ?, ?)",
                params!["Test Dataset 2", "Second test dataset", timestamp],
            ).expect("Failed to insert second test dataset");
        }

        // Create a runtime for async test
        let rt =
            tokio::runtime::Runtime::new().expect("Failed to create runtime");

        // Run the list_datasets function
        rt.block_on(async {
            let result = list_datasets(state.clone()).await;
            assert!(result.is_ok(), "list_datasets should return Ok");

            // Verify datasets were fetched
            let conn = state
                .zumblezay_db
                .get()
                .expect("Failed to get DB connection");
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM eval_datasets", [], |row| {
                    row.get(0)
                })
                .expect("Failed to count datasets");

            assert_eq!(
                count, 2,
                "There should be exactly 2 datasets in the test database"
            );
        });
    }

    #[test]
    fn add_dataset_test() {
        // Create a test state
        let state = Arc::new(AppState::new_for_testing());

        // Make sure the database is properly initialized
        {
            let conn = state
                .zumblezay_db
                .get()
                .expect("Failed to get DB connection");
            
            // Check if the table exists, if not create it
            conn.execute(
                "CREATE TABLE IF NOT EXISTS eval_datasets (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )",
                [],
            ).expect("Failed to create eval_datasets table");
        }

        // Create a runtime for async test
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");

        // Test dataset details
        let test_name = "New Test Dataset";
        let test_description = "This is a test dataset added through the API";

        // Run the add_dataset function
        rt.block_on(async {
            // Add the dataset
            let result = add_dataset(state.clone(), test_name, test_description).await;
            assert!(result.is_ok(), "add_dataset should return Ok");

            // Verify dataset was added
            let conn = state
                .zumblezay_db
                .get()
                .expect("Failed to get DB connection");
            
            // Check the total count
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM eval_datasets", [], |row| {
                    row.get(0)
                })
                .expect("Failed to count datasets");
            
            assert_eq!(count, 1, "There should be exactly 1 dataset in the test database");
            
            // Verify the dataset details
            let dataset = conn
                .query_row(
                    "SELECT name, description FROM eval_datasets WHERE name = ?",
                    params![test_name],
                    |row| {
                        Ok((
                            row.get::<_, String>(0).expect("Failed to get name"),
                            row.get::<_, String>(1).expect("Failed to get description"),
                        ))
                    },
                )
                .expect("Failed to find added dataset");
            
            assert_eq!(dataset.0, test_name, "Dataset name should match");
            assert_eq!(dataset.1, test_description, "Dataset description should match");
        });
    }

    #[tokio::test]
    async fn test_add_dataset_task() {
        // Create a test state
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get().expect("Failed to get DB connection");

        // Set up test data
        let now = Utc::now();
        let timestamp = now.timestamp();

        // Create a test dataset
        conn.execute(
            "INSERT INTO eval_datasets (dataset_id, name, description, created_at) VALUES (?, ?, ?, ?)",
            params![1, "Test Dataset", "Test Description", timestamp],
        ).expect("Failed to create test dataset");

        // Create some test events
        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) 
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                "event1",
                timestamp,
                1000.0,
                1010.0,
                "motion",
                "camera1",
                "/path/to/video1"
            ],
        ).expect("Failed to create test event 1");

        conn.execute(
            "INSERT INTO events (event_id, created_at, event_start, event_end, event_type, camera_id, video_path) 
             VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![
                "event2",
                timestamp,
                1020.0,
                1030.0,
                "motion",
                "camera1",
                "/path/to/video2"
            ],
        ).expect("Failed to create test event 2");

        // Test successful case without notes
        let result = add_dataset_task(
            state.clone(),
            1,
            "event1,event2",
            None,
        ).await;
        assert!(result.is_ok(), "Failed to add dataset task: {:?}", result);

        // Test successful case with notes
        let result = add_dataset_task(
            state.clone(),
            1,
            "event1,event2",
            Some("Test notes"),
        ).await;
        assert!(result.is_ok(), "Failed to add dataset task with notes: {:?}", result);

        // Verify the tasks were created correctly
        let tasks: Vec<(i64, String, Option<String>)> = {
            let mut stmt = conn.prepare(
                "SELECT dataset_id, events, notes FROM eval_dataset_tasks WHERE dataset_id = 1 ORDER BY created_at"
            ).expect("Failed to prepare query");
            
            let task_iter = stmt.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                ))
            }).expect("Failed to execute query");
            
            task_iter.collect::<Result<Vec<_>, _>>().expect("Failed to collect tasks")
        };

        assert_eq!(tasks.len(), 2, "Expected two tasks to be created");
        
        // Check first task (without notes)
        assert_eq!(tasks[0].0, 1);
        let events: Vec<String> = serde_json::from_str(&tasks[0].1).expect("Failed to parse events JSON");
        assert_eq!(events, vec!["event1", "event2"]);
        assert_eq!(tasks[0].2, None);

        // Check second task (with notes)
        assert_eq!(tasks[1].0, 1);
        let events: Vec<String> = serde_json::from_str(&tasks[1].1).expect("Failed to parse events JSON");
        assert_eq!(events, vec!["event1", "event2"]);
        assert_eq!(tasks[1].2, Some("Test notes".to_string()));

        // Test error cases

        // Test non-existent dataset
        let result = add_dataset_task(
            state.clone(),
            999,
            "event1,event2",
            None,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Dataset 999 not found"));

        // Test non-existent event
        let result = add_dataset_task(
            state.clone(),
            1,
            "event1,nonexistent",
            None,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nonexistent"));

        // Test empty event list
        let result = add_dataset_task(
            state.clone(),
            1,
            "",
            None,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No valid event IDs provided"));

        // Test whitespace-only event list
        let result = add_dataset_task(
            state.clone(),
            1,
            "  ,  ,  ",
            None,
        ).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No valid event IDs provided"));
    }

    #[tokio::test]
    async fn test_list_dataset_tasks() {
        // Create a test state
        let state = Arc::new(AppState::new_for_testing());
        let conn = state.zumblezay_db.get().expect("Failed to get DB connection");

        // Set up test data
        let now = Utc::now();
        let timestamp = now.timestamp();

        // Create a test dataset
        conn.execute(
            "INSERT INTO eval_datasets (dataset_id, name, description, created_at) VALUES (?, ?, ?, ?)",
            params![1, "Test Dataset", "Test Description", timestamp],
        ).expect("Failed to create test dataset");

        // Create some test tasks
        let events1 = serde_json::to_string(&vec!["event1", "event2"]).unwrap();
        let events2 = serde_json::to_string(&vec!["event3", "event4"]).unwrap();

        conn.execute(
            "INSERT INTO eval_dataset_tasks (task_id, dataset_id, events, created_at, updated_at, notes) 
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                1,
                1,
                events1,
                timestamp,
                timestamp,
                "Test notes 1"
            ],
        ).expect("Failed to create first test task");

        conn.execute(
            "INSERT INTO eval_dataset_tasks (task_id, dataset_id, events, created_at, updated_at, notes) 
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                2,
                1,
                events2,
                timestamp + 100,
                timestamp + 100,
                "Test notes 2"
            ],
        ).expect("Failed to create second test task");

        // Test listing tasks for existing dataset
        let result = list_dataset_tasks(state.clone(), "Test Dataset").await;
        assert!(result.is_ok(), "Failed to list dataset tasks: {:?}", result);

        // Test listing tasks for non-existent dataset
        let result = list_dataset_tasks(state.clone(), "Nonexistent Dataset").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
