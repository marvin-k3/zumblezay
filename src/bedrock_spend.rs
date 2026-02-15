use crate::bedrock::BedrockUsage;
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use tracing::info;

#[derive(Debug, Clone)]
pub struct SpendLogRequest<'a> {
    pub category: &'a str,
    pub operation: &'a str,
    pub model_id: &'a str,
    pub request_id: Option<&'a str>,
    pub usage: BedrockUsage,
}

pub fn record_bedrock_spend(
    conn: &Connection,
    request: SpendLogRequest<'_>,
) -> Result<f64> {
    let (input_cost_per_1k, output_cost_per_1k) =
        lookup_pricing(conn, request.model_id).ok_or_else(|| {
            anyhow!(
            "Missing bedrock pricing for model '{}' (or normalized aliases)",
            request.model_id
        )
        })?;

    let estimated_cost_usd = (request.usage.input_tokens as f64 / 1000.0)
        * input_cost_per_1k
        + (request.usage.output_tokens as f64 / 1000.0) * output_cost_per_1k;

    conn.execute(
        "INSERT INTO bedrock_spend_ledger (
            created_at,
            category,
            operation,
            model_id,
            request_id,
            input_tokens,
            output_tokens,
            estimated_cost_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            chrono::Utc::now().timestamp(),
            request.category,
            request.operation,
            request.model_id,
            request.request_id,
            request.usage.input_tokens,
            request.usage.output_tokens,
            estimated_cost_usd
        ],
    )?;

    info!(
        category = request.category,
        operation = request.operation,
        model_id = request.model_id,
        input_tokens = request.usage.input_tokens,
        output_tokens = request.usage.output_tokens,
        estimated_cost_usd,
        "recorded bedrock spend"
    );

    Ok(estimated_cost_usd)
}

fn lookup_pricing(conn: &Connection, model_id: &str) -> Option<(f64, f64)> {
    let mut candidates = vec![model_id.to_string()];
    if let Some(stripped) = normalized_pricing_model_id(model_id) {
        candidates.push(stripped);
    }

    for candidate in candidates {
        let lookup: Result<(f64, f64), _> = conn.query_row(
            "SELECT input_cost_per_1k_usd, output_cost_per_1k_usd
             FROM bedrock_pricing
             WHERE model_id = ?",
            params![candidate],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );
        if let Ok(costs) = lookup {
            return Some(costs);
        }
    }
    None
}

fn normalized_pricing_model_id(model_id: &str) -> Option<String> {
    let (prefix, rest) = model_id.split_once('.')?;
    if matches!(prefix, "us" | "eu" | "apac") {
        Some(rest.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory sqlite");
        conn.execute(
            "CREATE TABLE bedrock_pricing (
                model_id TEXT PRIMARY KEY,
                input_cost_per_1k_usd REAL NOT NULL,
                output_cost_per_1k_usd REAL NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )
        .expect("create bedrock_pricing");
        conn.execute(
            "CREATE TABLE bedrock_spend_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                category TEXT NOT NULL,
                operation TEXT NOT NULL,
                model_id TEXT NOT NULL,
                request_id TEXT,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                estimated_cost_usd REAL NOT NULL
            )",
            [],
        )
        .expect("create bedrock_spend_ledger");
        conn
    }

    #[test]
    fn record_spend_matches_exact_model_id() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO bedrock_pricing (
                model_id, input_cost_per_1k_usd, output_cost_per_1k_usd, updated_at
            ) VALUES (?, ?, ?, ?)",
            params![
                "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                0.003,
                0.015,
                chrono::Utc::now().timestamp()
            ],
        )
        .expect("insert pricing");

        let cost = record_bedrock_spend(
            &conn,
            SpendLogRequest {
                category: "video_investigation_search",
                operation: "test",
                model_id: "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                request_id: Some("req-1"),
                usage: BedrockUsage {
                    input_tokens: 1000,
                    output_tokens: 1000,
                },
            },
        )
        .expect("record spend");

        assert!((cost - 0.018).abs() < 1e-9);
    }

    #[test]
    fn record_spend_matches_normalized_inference_profile_id() {
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO bedrock_pricing (
                model_id, input_cost_per_1k_usd, output_cost_per_1k_usd, updated_at
            ) VALUES (?, ?, ?, ?)",
            params![
                "anthropic.claude-sonnet-4-5-20250929-v1:0",
                0.003,
                0.015,
                chrono::Utc::now().timestamp()
            ],
        )
        .expect("insert pricing");

        let cost = record_bedrock_spend(
            &conn,
            SpendLogRequest {
                category: "video_investigation_search",
                operation: "test",
                model_id: "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                request_id: Some("req-2"),
                usage: BedrockUsage {
                    input_tokens: 500,
                    output_tokens: 250,
                },
            },
        )
        .expect("record spend");

        assert!((cost - 0.00525).abs() < 1e-9);
    }
}
