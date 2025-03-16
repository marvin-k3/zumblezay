use crate::prompts::SUMMARY_SYSTEM_PROMPT;
use crate::transcripts;
use crate::AppState;
use anyhow::Result;
use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequestArgs,
    },
    Client,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tracing::{error, info, instrument};

pub async fn generate_summary(
    state: &AppState,
    date: &str,
    model: &str,
    summary_type: &str,
    user_prompt: &str,
) -> Result<String, anyhow::Error> {
    // Get the CSV content
    let csv_content =
        transcripts::get_formatted_transcripts_for_date(state, date)
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to get formatted transcripts: {}", e)
            })?;

    // Start timing the summary generation
    let start_time = std::time::Instant::now();

    // Get the OpenAI client
    let client = get_openai_client_from_state(state).await?;

    info!("Building system message for {} summary", summary_type);
    let system_message = ChatCompletionRequestMessage::System(
        ChatCompletionRequestSystemMessageArgs::default()
            .content(SUMMARY_SYSTEM_PROMPT)
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Failed to build system message: {}", e)
            })?,
    );

    info!("Building user message for {} summary", summary_type);
    let user_message = ChatCompletionRequestMessage::User(
        ChatCompletionRequestUserMessageArgs::default()
            .content(
                user_prompt
                    .replace("{date}", date)
                    .replace("{transcript}", &csv_content),
            )
            .build()
            .map_err(|e| {
                anyhow::anyhow!("Failed to build user message: {}", e)
            })?,
    );

    info!("Building request message");
    let request = CreateChatCompletionRequestArgs::default()
        .model(model)
        .messages([system_message, user_message])
        .build()
        .map_err(|e| {
            anyhow::anyhow!("Failed to build request message: {}", e)
        })?;

    let response = client.chat().create(request).await.map_err(|e| {
        anyhow::anyhow!("Failed to create chat completion: {}", e)
    })?;

    let summary = response
        .choices
        .first()
        .and_then(|choice| choice.message.content.as_ref())
        .map(String::from)
        .unwrap_or_else(|| {
            if summary_type == "json" {
                "[]".to_string()
            } else {
                "No summary generated".to_string()
            }
        });

    info!("{} summary received", summary_type);

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

async fn get_openai_client_from_state(
    state: &AppState,
) -> Result<Client<OpenAIConfig>, anyhow::Error> {
    let api_key = state
        .openai_api_key
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("OpenAI API key not configured"))?;

    let api_base = state
        .openai_api_base
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("OpenAI API base not configured"))?;

    let config = OpenAIConfig::new()
        .with_api_base(api_base)
        .with_api_key(api_key);
    Ok(Client::with_config(config))
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

    let mut stmt = conn.prepare(&query)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|p| p.as_ref()).collect();

    let result = stmt.query_row(param_refs.as_slice(), |row| {
        Ok(DailySummary {
            id: Some(row.get(0)?),
            date: row.get(1)?,
            created_at: row.get(2)?,
            model: row.get(3)?,
            prompt_name: row.get(4)?,
            summary_type: row.get(5)?,
            content: row.get(6)?,
            duration_ms: row.get(7)?,
        })
    });

    match result {
        Ok(summary) => Ok(Some(summary)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Database error: {}", e)),
    }
}

// Function to save a daily summary to the database
#[instrument(skip(state, content))]
async fn save_daily_summary(
    state: &AppState,
    date: &str,
    model: &str,
    prompt_name: &str,
    summary_type: &str,
    content: &str,
    duration_ms: i64,
) -> Result<i64> {
    info!("Saving {} summary for date {}", summary_type, date);
    let conn = state.zumblezay_db.get()?;

    // Insert the summary
    let id = conn.query_row(
        "INSERT INTO daily_summaries (
            date, created_at, model, prompt_name, 
            summary_type, content, duration_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, model, prompt_name, summary_type) 
        DO UPDATE SET 
            content = excluded.content,
            created_at = excluded.created_at,
            duration_ms = excluded.duration_ms
        RETURNING id",
        params![
            date,
            chrono::Utc::now().timestamp(),
            model,
            prompt_name,
            summary_type,
            content,
            duration_ms,
        ],
        |row| row.get(0),
    )?;

    Ok(id)
}

#[derive(Debug, Serialize)]
pub struct ModelInfo {
    pub id: String,
    pub name: String,
    pub provider: String,
}

#[instrument(skip(state), err)]
pub async fn get_available_models(
    state: &AppState,
) -> Result<Vec<ModelInfo>, anyhow::Error> {
    let mut models = Vec::new();

    // Get the OpenAI client
    let openai_client = get_openai_client_from_state(state).await?;

    info!("Fetching OpenAI models");
    let response =
        openai_client.models().list().await.map_err(|e| {
            anyhow::anyhow!("Failed to fetch OpenAI models: {}", e)
        })?;

    for model in response.data {
        models.push(ModelInfo {
            id: model.id.clone(),
            name: model.id.clone(),
            provider: model.owned_by.clone(),
        });
    }

    // Sort models by provider and name
    models.sort_by(|a, b| {
        if a.provider == b.provider {
            a.name.cmp(&b.name)
        } else {
            a.provider.cmp(&b.provider)
        }
    });

    Ok(models)
}
