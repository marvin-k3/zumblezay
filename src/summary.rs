use crate::openai::OpenAIClientTrait;
use crate::prompts::SUMMARY_SYSTEM_PROMPT;
use crate::transcripts;
use crate::AppState;
use anyhow::Result;
use async_openai::types::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
    ChatCompletionRequestUserMessageArgs,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, instrument};

#[instrument(skip(state), err)]
pub async fn generate_summary(
    state: &AppState,
    date: &str,
    model: &str,
    summary_type: &str,
    user_prompt: &str,
) -> Result<String, anyhow::Error> {
    generate_summary_with_client(
        state,
        date,
        model,
        summary_type,
        user_prompt,
        state
            .openai_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("OpenAI client not configured"))?
            .clone(),
    )
    .await
}

pub async fn generate_summary_with_client(
    state: &AppState,
    date: &str,
    model: &str,
    summary_type: &str,
    user_prompt: &str,
    client: Arc<dyn OpenAIClientTrait>,
) -> Result<String, anyhow::Error> {
    // Get the CSV content and event IDs
    let (csv_content, event_ids) =
        transcripts::get_formatted_transcripts_for_date(state, date)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to get formatted transcripts: {}", e)
            })?;

    // Check if we have any events
    if event_ids.is_empty() {
        return Err(anyhow::anyhow!("No events found for the date"));
    }

    // Start timing the summary generation
    let start_time = std::time::Instant::now();

    info!("Building system message for {} summary", summary_type);
    let system_message = ChatCompletionRequestSystemMessageArgs::default()
        .content(SUMMARY_SYSTEM_PROMPT)
        .build()?;

    info!("Building user message for {} summary", summary_type);
    let user_content = user_prompt
        .replace("{date}", date)
        .replace("{transcript}", &csv_content);
    let user_message = ChatCompletionRequestUserMessageArgs::default()
        .content(user_content)
        .build()?;

    info!("Sending request for {} summary", summary_type);
    let response = client
        .chat_completion(
            model.to_string(),
            vec![
                ChatCompletionRequestMessage::System(system_message),
                ChatCompletionRequestMessage::User(user_message),
            ],
        )
        .await?;

    info!("{} summary received", summary_type);

    // Extract the content from the response
    let maybe_summary = response
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_ref())
        .map(String::from);

    // Handle the case where no content was generated
    let summary = match maybe_summary {
        Some(content) if !content.trim().is_empty() => content,
        Some(content) if summary_type == "json" && content.trim() == "[]" => {
            // Empty JSON array is valid for JSON summary type
            content
        }
        _ => {
            // Log the issue
            error!(
                "No meaningful summary content was generated for {}",
                summary_type
            );
            return Err(anyhow::anyhow!(
                "No meaningful summary content was generated"
            ));
        }
    };

    // Calculate duration
    let duration_ms = start_time.elapsed().as_millis() as i64;

    // For JSON summaries, validate that the response is valid JSON
    if summary_type == "json" {
        serde_json::from_str::<serde_json::Value>(&summary).map_err(|_| {
            anyhow::anyhow!("Generated summary is not valid JSON")
        })?;
    }

    // Save the summary to the database
    save_daily_summary(
        state,
        date,
        model,
        "default",
        summary_type,
        &summary,
        duration_ms,
    )
    .await
    .map_err(|e| {
        error!("Failed to save {} summary to database: {}", summary_type, e);
        anyhow::anyhow!(
            "Failed to save {} summary to database: {}",
            summary_type,
            e
        )
    })?;

    info!("{} summary saved to database", summary_type);

    Ok(summary)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DailySummary {
    pub id: Option<i64>,
    pub date: String,
    pub created_at: i64,
    pub model: String,
    pub prompt_name: String,
    pub summary_type: String,
    pub content: String,
    pub duration_ms: i64,
}

// Function to fetch a daily summary from the database
#[instrument(skip(state), err)]
pub async fn get_daily_summary(
    state: &AppState,
    date: &str,
    model: Option<&str>,
    prompt_name: Option<&str>,
    summary_type: &str,
) -> Result<Option<DailySummary>> {
    let conn = state.zumblezay_db.get()?;

    let mut query = String::from(
        "SELECT id, date, created_at, model, prompt_name, summary_type, content, duration_ms
         FROM daily_summaries
         WHERE date = ? AND summary_type = ?",
    );

    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
        Box::new(date.to_string()),
        Box::new(summary_type.to_string()),
    ];

    if let Some(model_val) = model {
        query.push_str(" AND model = ?");
        params.push(Box::new(model_val.to_string()));
    }

    if let Some(prompt_val) = prompt_name {
        query.push_str(" AND prompt_name = ?");
        params.push(Box::new(prompt_val.to_string()));
    }

    query.push_str(" ORDER BY created_at DESC LIMIT 1");

    let summary = conn.query_row(
        &query,
        params
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .as_slice(),
        |row| {
            Ok(DailySummary {
                id: row.get(0)?,
                date: row.get(1)?,
                created_at: row.get(2)?,
                model: row.get(3)?,
                prompt_name: row.get(4)?,
                summary_type: row.get(5)?,
                content: row.get(6)?,
                duration_ms: row.get(7)?,
            })
        },
    );

    match summary {
        Ok(s) => Ok(Some(s)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Database error: {}", e)),
    }
}

// Function to save a daily summary to the database
#[instrument(skip(state, content))]
pub async fn save_daily_summary(
    state: &AppState,
    date: &str,
    model: &str,
    prompt_name: &str,
    summary_type: &str,
    content: &str,
    duration_ms: i64,
) -> Result<i64> {
    let conn = state.zumblezay_db.get()?;
    let created_at = chrono::Utc::now().timestamp();

    // First, try to find an existing summary with the same date, model, prompt_name, and summary_type
    let existing = conn.query_row(
        "SELECT id FROM daily_summaries 
         WHERE date = ? AND model = ? AND prompt_name = ? AND summary_type = ?",
        params![date, model, prompt_name, summary_type],
        |row| row.get::<_, i64>(0),
    );

    match existing {
        Ok(id) => {
            // Update existing summary
            conn.execute(
                "UPDATE daily_summaries 
                 SET content = ?, created_at = ?, duration_ms = ? 
                 WHERE id = ?",
                params![content, created_at, duration_ms, id],
            )?;
            Ok(id)
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Insert new summary
            conn.execute(
                "INSERT INTO daily_summaries 
                 (date, created_at, model, prompt_name, summary_type, content, duration_ms) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![
                    date,
                    created_at,
                    model,
                    prompt_name,
                    summary_type,
                    content,
                    duration_ms
                ],
            )?;
            Ok(conn.last_insert_rowid())
        }
        Err(e) => Err(anyhow::anyhow!("Database error: {}", e)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    pub name: String,
    pub provider: String,
}

// For the get_available_models function, add a version with client injection
#[instrument(skip(state), err)]
pub async fn get_available_models(
    state: &AppState,
) -> Result<Vec<ModelInfo>, anyhow::Error> {
    let client = state
        .openai_client
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("OpenAI client not configured"))?;
    get_available_models_with_client(client.clone()).await
}

#[instrument(skip(client), err)]
pub async fn get_available_models_with_client(
    client: Arc<dyn OpenAIClientTrait>,
) -> Result<Vec<ModelInfo>, anyhow::Error> {
    let mut models = Vec::new();

    info!("Fetching OpenAI models");
    let openai_models = client.list_models().await?;

    for model in openai_models {
        models.push(ModelInfo {
            id: model.id.clone(),
            name: model.id.clone(),
            provider: model.owned_by.clone(),
        });
    }

    // Sort models by provider and id
    models.sort_by(|a, b| {
        let provider_cmp = a.provider.cmp(&b.provider);
        if provider_cmp == std::cmp::Ordering::Equal {
            a.id.cmp(&b.id)
        } else {
            provider_cmp
        }
    });

    Ok(models)
}
