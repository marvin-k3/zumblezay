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
}

/// Helper struct to attach events db to zumblezay connection
#[derive(Debug)]
struct DbAttacher {
    events_db: PathBuf,
}

impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error>
    for DbAttacher
{
    fn on_acquire(
        &self,
        conn: &mut rusqlite::Connection,
    ) -> Result<(), rusqlite::Error> {
        let events_db_path = self.events_db.to_str().unwrap();
        conn.execute_batch(&format!(
            "ATTACH DATABASE '{}' AS events",
            events_db_path
        ))
    }
}

/// Represents a dataset in the system
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
    let zumblezay_pool = Pool::builder()
        .connection_customizer(Box::new(DbAttacher {
            events_db: cli.common_args.events_db.clone(),
        }))
        .build(zumblezay_manager)?;

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
}
