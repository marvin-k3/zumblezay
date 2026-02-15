use crate::bedrock::BedrockUsage;
use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};
use serde_json::Value;
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

pub fn is_missing_pricing_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains("Missing bedrock pricing for model")
}

#[derive(Debug, Clone, Copy)]
pub struct BedrockPricingRate {
    pub input_cost_per_1k_usd: f64,
    pub output_cost_per_1k_usd: f64,
}

pub fn upsert_bedrock_pricing(
    conn: &Connection,
    model_id: &str,
    rate: BedrockPricingRate,
) -> Result<()> {
    let mut model_ids = vec![model_id.to_string()];
    if let Some(normalized) = normalized_pricing_model_id(model_id) {
        if normalized != model_id {
            model_ids.push(normalized);
        }
    }

    for id in model_ids {
        conn.execute(
            "INSERT OR REPLACE INTO bedrock_pricing (
                model_id,
                input_cost_per_1k_usd,
                output_cost_per_1k_usd,
                updated_at
            ) VALUES (?, ?, ?, ?)",
            params![
                id,
                rate.input_cost_per_1k_usd,
                rate.output_cost_per_1k_usd,
                chrono::Utc::now().timestamp()
            ],
        )?;
    }

    Ok(())
}

pub async fn fetch_bedrock_pricing_from_aws(
    model_id: &str,
    region_code: Option<&str>,
) -> Result<Option<BedrockPricingRate>> {
    let region_code = region_code
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("us-east-1");

    let hints = model_service_name_hints(model_id);
    if hints.is_empty() {
        return Ok(None);
    }

    let region_index_url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonBedrockFoundationModels/current/region_index.json";
    let region_index: Value =
        reqwest::get(region_index_url).await?.json().await?;

    let version_path = region_index
        .get("regions")
        .and_then(|value| value.get(region_code))
        .and_then(|value| value.get("currentVersionUrl"))
        .and_then(Value::as_str);

    let Some(version_path) = version_path else {
        return Ok(None);
    };
    let pricing_url =
        format!("https://pricing.us-east-1.amazonaws.com{version_path}");
    let pricing_doc: Value = reqwest::get(pricing_url).await?.json().await?;

    let products = pricing_doc
        .get("products")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            anyhow!("missing products object in AWS pricing response")
        })?;

    let mut input_skus = Vec::new();
    let mut output_skus = Vec::new();
    for (sku, product) in products {
        let Some(attributes) =
            product.get("attributes").and_then(Value::as_object)
        else {
            continue;
        };

        let service_name = attributes
            .get("servicename")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase();
        if !hints.iter().any(|hint| service_name.contains(hint)) {
            continue;
        }

        let usage_type = attributes
            .get("usagetype")
            .and_then(Value::as_str)
            .unwrap_or("");
        if !is_standard_token_rate_usage_type(usage_type) {
            continue;
        }

        if usage_type.contains("InputTokenCount-Units") {
            input_skus.push(sku.as_str());
        }
        if usage_type.contains("OutputTokenCount-Units") {
            output_skus.push(sku.as_str());
        }
    }

    let terms = pricing_doc
        .get("terms")
        .and_then(|value| value.get("OnDemand"))
        .and_then(Value::as_object)
        .ok_or_else(|| {
            anyhow!("missing OnDemand terms in AWS pricing response")
        })?;

    let input_rate = input_skus
        .into_iter()
        .find_map(|sku| extract_per_1k_rate_from_terms(terms, sku));
    let output_rate = output_skus
        .into_iter()
        .find_map(|sku| extract_per_1k_rate_from_terms(terms, sku));

    Ok(match (input_rate, output_rate) {
        (Some(input), Some(output)) => Some(BedrockPricingRate {
            input_cost_per_1k_usd: input,
            output_cost_per_1k_usd: output,
        }),
        _ => None,
    })
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

fn is_standard_token_rate_usage_type(usage_type: &str) -> bool {
    if !(usage_type.contains("InputTokenCount-Units")
        || usage_type.contains("OutputTokenCount-Units"))
    {
        return false;
    }

    !usage_type.contains("Cache")
        && !usage_type.contains("Batch")
        && !usage_type.contains("Global")
        && !usage_type.contains("LCtx")
        && !usage_type.contains("LatencyOptimized")
}

fn extract_per_1k_rate_from_terms(
    terms: &serde_json::Map<String, Value>,
    sku: &str,
) -> Option<f64> {
    let sku_terms = terms.get(sku)?.as_object()?;
    for term in sku_terms.values() {
        let price_dimensions =
            term.get("priceDimensions").and_then(Value::as_object)?;
        if let Some(dimension) = price_dimensions.values().next() {
            let usd = dimension
                .get("pricePerUnit")
                .and_then(|value| value.get("USD"))
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<f64>().ok())?;
            let description = dimension
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let per_1k = if description.contains("Billion") {
                usd / 1_000_000.0
            } else if description.contains("Million") {
                usd / 1000.0
            } else {
                usd
            };
            return Some(per_1k);
        }
    }
    None
}

fn model_service_name_hints(model_id: &str) -> Vec<String> {
    let normalized = normalized_pricing_model_id(model_id)
        .unwrap_or_else(|| model_id.to_string());
    let lower = normalized.to_lowercase();

    if lower.contains("claude-sonnet-4-5") {
        return vec!["claude sonnet 4.5".to_string()];
    }
    if lower.contains("claude-opus-4-5") {
        return vec!["claude opus 4.5".to_string()];
    }
    if lower.contains("claude-haiku-4-5") {
        return vec!["claude haiku 4.5".to_string()];
    }

    let mut hints = Vec::new();
    if lower.contains("claude") {
        if lower.contains("sonnet") {
            hints.push("claude sonnet".to_string());
        }
        if lower.contains("opus") {
            hints.push("claude opus".to_string());
        }
        if lower.contains("haiku") {
            hints.push("claude haiku".to_string());
        }
    }
    hints
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

    #[test]
    fn model_hint_generation_for_known_claude_model() {
        let hints = model_service_name_hints(
            "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        );
        assert!(hints.iter().any(|hint| hint == "claude sonnet 4.5"));
    }

    #[test]
    fn usage_type_filter_excludes_batch_and_cache() {
        assert!(is_standard_token_rate_usage_type(
            "USE1-MP:USE1_InputTokenCount-Units"
        ));
        assert!(!is_standard_token_rate_usage_type(
            "USE1-MP:USE1_InputTokenCount_Batch-Units"
        ));
        assert!(!is_standard_token_rate_usage_type(
            "USE1-MP:USE1_CacheReadInputTokenCount-Units"
        ));
    }
}
