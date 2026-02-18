use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::operation::RequestId;
use aws_sdk_bedrockruntime::primitives::Blob;
use aws_sdk_bedrockruntime::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};

const MODEL_INVOKE_MAX_ATTEMPTS: usize = 2;
const EMBEDDING_INVOKE_MAX_ATTEMPTS: usize = 1;
const MODEL_INVOKE_TIMEOUT: Duration = Duration::from_secs(20);
const EMBEDDING_INVOKE_TIMEOUT: Duration = Duration::from_secs(8);
const STREAM_RECV_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BedrockUsage {
    pub input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BedrockCompletionResponse {
    pub content: String,
    pub usage: BedrockUsage,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BedrockEmbeddingResponse {
    pub embedding: Vec<f32>,
    pub usage: BedrockUsage,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BedrockRerankResult {
    pub index: usize,
    pub relevance_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BedrockRerankResponse {
    pub results: Vec<BedrockRerankResult>,
    pub usage: BedrockUsage,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingPurpose {
    GenericIndex,
    TextRetrieval,
}

impl EmbeddingPurpose {
    fn as_nova_str(self) -> &'static str {
        match self {
            Self::GenericIndex => "GENERIC_INDEX",
            Self::TextRetrieval => "TEXT_RETRIEVAL",
        }
    }
}

#[async_trait]
pub trait BedrockClientTrait: Send + Sync {
    async fn complete_text(
        &self,
        model_id: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_tokens: i32,
    ) -> Result<BedrockCompletionResponse>;

    async fn complete_text_streaming(
        &self,
        model_id: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_tokens: i32,
        on_delta: &mut (dyn FnMut(String) + Send),
    ) -> Result<BedrockCompletionResponse>;

    async fn embed_text(
        &self,
        model_id: &str,
        text: &str,
        dimensions: i32,
        purpose: EmbeddingPurpose,
    ) -> Result<BedrockEmbeddingResponse>;

    async fn rerank_documents(
        &self,
        model_id: &str,
        query: &str,
        documents: &[String],
        top_n: usize,
    ) -> Result<BedrockRerankResponse>;
}

pub struct RealBedrockClient {
    region: Option<String>,
    client: OnceCell<Client>,
}

impl RealBedrockClient {
    pub fn new(region: Option<String>) -> Self {
        Self {
            region,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest());
                if let Some(region) = self.region.clone() {
                    loader = loader.region(aws_config::Region::new(region));
                }
                let config = loader.load().await;
                Ok(Client::new(&config))
            })
            .await
    }

    fn is_retryable_invoke_error(error: &str) -> bool {
        let normalized = error.to_ascii_lowercase();
        normalized.contains("timeout")
            || normalized.contains("throughputbelowminimum")
            || normalized.contains("dispatchfailure")
            || normalized.contains("connectorerror")
    }

    fn supports_count_tokens(model_id: &str) -> bool {
        !model_id
            .to_ascii_lowercase()
            .contains("nova-2-multimodal-embeddings")
    }
}

#[async_trait]
impl BedrockClientTrait for RealBedrockClient {
    async fn complete_text(
        &self,
        model_id: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_tokens: i32,
    ) -> Result<BedrockCompletionResponse> {
        let body = serde_json::json!({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": user_prompt
                }]
            }]
        });

        let body_bytes =
            serde_json::to_vec(&body).context("serialize bedrock body")?;
        let mut response_opt = None;
        let mut last_error = None;
        for attempt in 1..=MODEL_INVOKE_MAX_ATTEMPTS {
            let send_result = tokio::time::timeout(
                MODEL_INVOKE_TIMEOUT,
                self.client()
                    .await?
                    .invoke_model()
                    .model_id(model_id)
                    .content_type("application/json")
                    .accept("application/json")
                    .body(Blob::new(body_bytes.clone()))
                    .send(),
            )
            .await;
            match send_result {
                Ok(Ok(response)) => {
                    response_opt = Some(response);
                    break;
                }
                Ok(Err(error)) => {
                    let error_text = format!("{error:?}");
                    if attempt < MODEL_INVOKE_MAX_ATTEMPTS
                        && Self::is_retryable_invoke_error(&error_text)
                    {
                        sleep(Duration::from_millis(300 * attempt as u64))
                            .await;
                        continue;
                    }
                    last_error = Some(error_text);
                    break;
                }
                Err(_) => {
                    let error_text = format!(
                        "Timed out after {}s waiting for model invoke",
                        MODEL_INVOKE_TIMEOUT.as_secs()
                    );
                    if attempt < MODEL_INVOKE_MAX_ATTEMPTS {
                        sleep(Duration::from_millis(300 * attempt as u64))
                            .await;
                        continue;
                    }
                    last_error = Some(error_text);
                    break;
                }
            }
        }
        let response = if let Some(response) = response_opt {
            response
        } else {
            return Err(anyhow!(
                    "Bedrock invoke_model failed for model '{model_id}': {}",
                    last_error.unwrap_or_else(
                        || "unknown invoke_model error".to_string()
                    )
                ));
        };
        let request_id = response.request_id().map(str::to_string);

        let payload: Value = serde_json::from_slice(response.body().as_ref())
            .context("parse bedrock response body")?;

        let content = payload
            .get("content")
            .and_then(Value::as_array)
            .and_then(|blocks| {
                let mut text = String::new();
                for block in blocks {
                    if block.get("type").and_then(Value::as_str) == Some("text")
                    {
                        if let Some(chunk) =
                            block.get("text").and_then(Value::as_str)
                        {
                            text.push_str(chunk);
                        }
                    }
                }
                if text.trim().is_empty() {
                    None
                } else {
                    Some(text)
                }
            })
            .ok_or_else(|| {
                anyhow!("missing text content in bedrock response")
            })?;

        let usage = payload.get("usage").cloned().unwrap_or_else(|| {
            serde_json::json!({
                "input_tokens": 0,
                "output_tokens": 0
            })
        });
        let input_tokens = usage
            .get("input_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let output_tokens = usage
            .get("output_tokens")
            .and_then(Value::as_i64)
            .unwrap_or(0);

        Ok(BedrockCompletionResponse {
            content,
            usage: BedrockUsage {
                input_tokens,
                output_tokens,
            },
            request_id,
        })
    }

    async fn complete_text_streaming(
        &self,
        model_id: &str,
        system_prompt: &str,
        user_prompt: &str,
        max_tokens: i32,
        on_delta: &mut (dyn FnMut(String) + Send),
    ) -> Result<BedrockCompletionResponse> {
        let body = serde_json::json!({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": user_prompt
                }]
            }]
        });

        let body_bytes =
            serde_json::to_vec(&body).context("serialize bedrock body")?;
        let response = tokio::time::timeout(
            MODEL_INVOKE_TIMEOUT,
            self.client()
                .await?
                .invoke_model_with_response_stream()
                .model_id(model_id)
                .content_type("application/json")
                .accept("application/json")
                .body(Blob::new(body_bytes))
                .send(),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "Bedrock invoke_model_with_response_stream timed out for model '{model_id}' after {}s",
                MODEL_INVOKE_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| {
            anyhow!(
                "Bedrock invoke_model_with_response_stream failed for model '{model_id}': {error:?}"
            )
        })?;

        let request_id = response.request_id().map(str::to_string);
        let mut stream = response.body;
        let mut content = String::new();
        let mut usage = BedrockUsage {
            input_tokens: 0,
            output_tokens: 0,
        };

        while let Some(event) = tokio::time::timeout(STREAM_RECV_TIMEOUT, stream.recv())
            .await
            .map_err(|_| {
                anyhow!(
                    "Bedrock stream receive timed out for model '{model_id}' after {}s",
                    STREAM_RECV_TIMEOUT.as_secs()
                )
            })?
            .map_err(|error| {
                anyhow!(
                    "Bedrock stream receive failed for model '{model_id}': {error:?}"
                )
            })?
        {
            let payload_part = match event {
                aws_sdk_bedrockruntime::types::ResponseStream::Chunk(part) => part,
                _ => {
                    continue;
                }
            };

            let Some(bytes) = payload_part.bytes() else {
                continue;
            };
            let value: Value = match serde_json::from_slice(bytes.as_ref()) {
                Ok(value) => value,
                Err(_) => continue,
            };

            if let Some(input_tokens) = value
                .get("message")
                .and_then(|v| v.get("usage"))
                .and_then(|v| v.get("input_tokens"))
                .and_then(Value::as_i64)
            {
                usage.input_tokens = input_tokens;
            }
            if let Some(output_tokens) = value
                .get("usage")
                .and_then(|v| v.get("output_tokens"))
                .and_then(Value::as_i64)
            {
                usage.output_tokens = output_tokens;
            }
            if let Some(metrics) = value.get("amazon-bedrock-invocationMetrics")
            {
                if let Some(input_count) = metrics
                    .get("inputTokenCount")
                    .and_then(Value::as_i64)
                {
                    usage.input_tokens = input_count;
                }
                if let Some(output_count) = metrics
                    .get("outputTokenCount")
                    .and_then(Value::as_i64)
                {
                    usage.output_tokens = output_count;
                }
            }

            let delta_text = value
                .get("delta")
                .and_then(|v| v.get("text"))
                .and_then(Value::as_str)
                .or_else(|| value.get("completion").and_then(Value::as_str));
            if let Some(delta) = delta_text {
                if !delta.is_empty() {
                    content.push_str(delta);
                    on_delta(delta.to_string());
                }
            }
        }

        if content.trim().is_empty() {
            return Err(anyhow!(
                "missing text content in bedrock streaming response"
            ));
        }

        Ok(BedrockCompletionResponse {
            content,
            usage,
            request_id,
        })
    }

    async fn embed_text(
        &self,
        model_id: &str,
        text: &str,
        dimensions: i32,
        purpose: EmbeddingPurpose,
    ) -> Result<BedrockEmbeddingResponse> {
        let body = if model_id
            .to_ascii_lowercase()
            .contains("nova-2-multimodal-embeddings")
        {
            serde_json::json!({
                "taskType": "SINGLE_EMBEDDING",
                "singleEmbeddingParams": {
                    "embeddingPurpose": purpose.as_nova_str(),
                    "embeddingDimension": dimensions,
                    "text": {
                        "value": text,
                        "truncationMode": "END"
                    }
                }
            })
        } else {
            serde_json::json!({
                "inputText": text,
                "dimensions": dimensions,
                "normalize": true
            })
        };
        let body_bytes = serde_json::to_vec(&body)
            .context("serialize bedrock embedding body")?;
        let mut response_opt = None;
        let mut last_error = None;
        for attempt in 1..=EMBEDDING_INVOKE_MAX_ATTEMPTS {
            let send_result = tokio::time::timeout(
                EMBEDDING_INVOKE_TIMEOUT,
                self.client()
                    .await?
                    .invoke_model()
                    .model_id(model_id)
                    .content_type("application/json")
                    .accept("application/json")
                    .body(Blob::new(body_bytes.clone()))
                    .send(),
            )
            .await;
            match send_result {
                Ok(Ok(response)) => {
                    response_opt = Some(response);
                    break;
                }
                Ok(Err(error)) => {
                    let error_text = format!("{error:?}");
                    if attempt < EMBEDDING_INVOKE_MAX_ATTEMPTS
                        && Self::is_retryable_invoke_error(&error_text)
                    {
                        sleep(Duration::from_millis(300 * attempt as u64))
                            .await;
                        continue;
                    }
                    last_error = Some(error_text);
                    break;
                }
                Err(_) => {
                    let error_text = format!(
                        "Timed out after {}s waiting for embedding invoke",
                        EMBEDDING_INVOKE_TIMEOUT.as_secs()
                    );
                    if attempt < EMBEDDING_INVOKE_MAX_ATTEMPTS {
                        sleep(Duration::from_millis(300 * attempt as u64))
                            .await;
                        continue;
                    }
                    last_error = Some(error_text);
                    break;
                }
            }
        }

        let response = if let Some(response) = response_opt {
            response
        } else {
            return Err(anyhow!(
                "Bedrock invoke_model failed for embedding model '{model_id}': {}",
                last_error.unwrap_or_else(|| "unknown invoke_model error".to_string())
            ));
        };
        let request_id = response.request_id().map(str::to_string);

        let payload: Value =
            serde_json::from_slice(response.body().as_ref())
                .context("parse bedrock embedding response body")?;

        let embedding_values = payload
            .get("embedding")
            .and_then(Value::as_array)
            .or_else(|| {
                payload
                    .get("embeddingsByType")
                    .and_then(|value| value.get("text"))
                    .and_then(Value::as_array)
            })
            .or_else(|| {
                payload
                    .get("embeddings")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first())
                    .and_then(|item| item.get("embedding"))
                    .and_then(Value::as_array)
            })
            .ok_or_else(|| {
                anyhow!("missing embedding vector in bedrock response")
            })?;

        let mut embedding = Vec::with_capacity(embedding_values.len());
        for value in embedding_values {
            if let Some(number) = value.as_f64() {
                embedding.push(number as f32);
            }
        }
        if embedding.is_empty() {
            return Err(anyhow!(
                "bedrock embedding response had an empty/non-numeric embedding"
            ));
        }

        let mut input_tokens = payload
            .get("inputTextTokenCount")
            .and_then(Value::as_i64)
            .or_else(|| payload.get("tokenCount").and_then(Value::as_i64))
            .or_else(|| {
                payload
                    .get("usage")
                    .and_then(|v| v.get("input_tokens"))
                    .and_then(Value::as_i64)
            })
            .or_else(|| {
                payload
                    .get("usage")
                    .and_then(|v| v.get("inputTokens"))
                    .and_then(Value::as_i64)
            })
            .unwrap_or(0);
        if input_tokens == 0 && Self::supports_count_tokens(model_id) {
            let token_request =
                aws_sdk_bedrockruntime::types::InvokeModelTokensRequest::builder()
                    .body(Blob::new(body_bytes.clone()))
                    .build();
            if let Ok(token_request) = token_request {
                let count_tokens_result = tokio::time::timeout(
                    EMBEDDING_INVOKE_TIMEOUT,
                    self.client().await?.count_tokens().model_id(model_id).input(
                        aws_sdk_bedrockruntime::types::CountTokensInput::InvokeModel(
                            token_request,
                        ),
                    )
                    .send(),
                )
                .await;
                match count_tokens_result {
                    Ok(Ok(output)) => {
                        input_tokens = i64::from(output.input_tokens());
                    }
                    Ok(Err(error)) => {
                        warn!(model_id = model_id, error = ?error, "Bedrock count_tokens failed for embedding request");
                    }
                    Err(_) => {
                        warn!(
                            model_id = model_id,
                            timeout_secs = EMBEDDING_INVOKE_TIMEOUT.as_secs(),
                            "Bedrock count_tokens timed out for embedding request"
                        );
                    }
                }
            }
        }
        if input_tokens == 0 {
            let top_level_keys = payload
                .as_object()
                .map(|obj| obj.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let first_embedding_keys = payload
                .get("embeddings")
                .and_then(Value::as_array)
                .and_then(|items| items.first())
                .and_then(Value::as_object)
                .map(|obj| obj.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            debug!(
                model_id = model_id,
                top_level_keys = ?top_level_keys,
                usage = ?payload.get("usage"),
                token_count = ?payload.get("tokenCount"),
                input_text_token_count = ?payload.get("inputTextTokenCount"),
                first_embedding_keys = ?first_embedding_keys,
                "Bedrock embedding response missing token usage"
            );
            // Fallback estimator to avoid reporting zero spend when Bedrock
            // omits usage and count_tokens is unavailable for this model.
            input_tokens = estimate_embedding_input_tokens(text);
        }
        let output_tokens = payload
            .get("usage")
            .and_then(|v| v.get("output_tokens"))
            .and_then(Value::as_i64)
            .unwrap_or(0);

        Ok(BedrockEmbeddingResponse {
            embedding,
            usage: BedrockUsage {
                input_tokens,
                output_tokens,
            },
            request_id,
        })
    }

    async fn rerank_documents(
        &self,
        model_id: &str,
        query: &str,
        documents: &[String],
        top_n: usize,
    ) -> Result<BedrockRerankResponse> {
        if documents.is_empty() {
            return Ok(BedrockRerankResponse {
                results: Vec::new(),
                usage: BedrockUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                },
                request_id: None,
            });
        }

        let body = build_rerank_request_body(query, documents, top_n);
        let body_bytes = serde_json::to_vec(&body)
            .context("serialize bedrock rerank body")?;
        let response = tokio::time::timeout(
            MODEL_INVOKE_TIMEOUT,
            self.client()
                .await?
                .invoke_model()
                .model_id(model_id)
                .content_type("application/json")
                .accept("application/json")
                .body(Blob::new(body_bytes))
                .send(),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "Bedrock rerank invoke timed out for model '{model_id}' after {}s",
                MODEL_INVOKE_TIMEOUT.as_secs()
            )
        })?
        .map_err(|error| {
            anyhow!(
                "Bedrock rerank invoke failed for model '{model_id}': {error:?}"
            )
        })?;

        let request_id = response.request_id().map(str::to_string);
        let payload: Value = serde_json::from_slice(response.body().as_ref())
            .context("parse bedrock rerank response body")?;

        let mut results = Vec::new();
        if let Some(values) = payload.get("results").and_then(Value::as_array) {
            for value in values {
                let Some(index_u64) =
                    value.get("index").and_then(Value::as_u64)
                else {
                    continue;
                };
                let Some(index) = usize::try_from(index_u64).ok() else {
                    continue;
                };
                let relevance_score = value
                    .get("relevance_score")
                    .and_then(Value::as_f64)
                    .or_else(|| {
                        value.get("relevanceScore").and_then(Value::as_f64)
                    })
                    .unwrap_or(0.0);
                results.push(BedrockRerankResult {
                    index,
                    relevance_score,
                });
            }
        }

        let usage = BedrockUsage {
            input_tokens: payload
                .get("usage")
                .and_then(|value| value.get("input_tokens"))
                .and_then(Value::as_i64)
                .or_else(|| {
                    payload
                        .get("usage")
                        .and_then(|value| value.get("inputTokens"))
                        .and_then(Value::as_i64)
                })
                .unwrap_or(0),
            output_tokens: payload
                .get("usage")
                .and_then(|value| value.get("output_tokens"))
                .and_then(Value::as_i64)
                .or_else(|| {
                    payload
                        .get("usage")
                        .and_then(|value| value.get("outputTokens"))
                        .and_then(Value::as_i64)
                })
                .unwrap_or(0),
        };

        Ok(BedrockRerankResponse {
            results,
            usage,
            request_id,
        })
    }
}

fn estimate_embedding_input_tokens(text: &str) -> i64 {
    if text.trim().is_empty() {
        return 0;
    }
    // Heuristic token estimate (~4 chars/token) with a minimum of 1 token.
    ((text.chars().count() as i64 + 3) / 4).max(1)
}

fn build_rerank_request_body(
    query: &str,
    documents: &[String],
    top_n: usize,
) -> Value {
    serde_json::json!({
        "api_version": 2,
        "query": query,
        "documents": documents,
        "top_n": top_n.min(documents.len())
    })
}

pub fn create_bedrock_client(
    region: Option<String>,
) -> std::sync::Arc<dyn BedrockClientTrait> {
    std::sync::Arc::new(RealBedrockClient::new(region))
}

pub struct FakeBedrockClient {
    responses: Mutex<Vec<BedrockCompletionResponse>>,
}

impl FakeBedrockClient {
    pub fn new() -> Self {
        Self {
            responses: Mutex::new(Vec::new()),
        }
    }

    pub fn with_response(self, response: BedrockCompletionResponse) -> Self {
        self.responses
            .lock()
            .expect("responses lock")
            .push(response);
        self
    }
}

impl Default for FakeBedrockClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BedrockClientTrait for FakeBedrockClient {
    async fn complete_text(
        &self,
        _model_id: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _max_tokens: i32,
    ) -> Result<BedrockCompletionResponse> {
        let mut responses = self.responses.lock().expect("responses lock");
        if responses.is_empty() {
            return Err(anyhow!("no fake bedrock responses configured"));
        }
        Ok(responses.remove(0))
    }

    async fn complete_text_streaming(
        &self,
        _model_id: &str,
        _system_prompt: &str,
        _user_prompt: &str,
        _max_tokens: i32,
        on_delta: &mut (dyn FnMut(String) + Send),
    ) -> Result<BedrockCompletionResponse> {
        let response = self
            .complete_text(_model_id, _system_prompt, _user_prompt, _max_tokens)
            .await?;
        on_delta(response.content.clone());
        Ok(response)
    }

    async fn embed_text(
        &self,
        _model_id: &str,
        text: &str,
        dimensions: i32,
        _purpose: EmbeddingPurpose,
    ) -> Result<BedrockEmbeddingResponse> {
        let dim = usize::try_from(dimensions)
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(256);
        let embedding = embed_text_deterministic(text, dim);
        let usage = BedrockUsage {
            input_tokens: estimate_token_count(text),
            output_tokens: 0,
        };
        Ok(BedrockEmbeddingResponse {
            embedding,
            usage,
            request_id: Some("fake-bedrock-embed".to_string()),
        })
    }

    async fn rerank_documents(
        &self,
        _model_id: &str,
        query: &str,
        documents: &[String],
        top_n: usize,
    ) -> Result<BedrockRerankResponse> {
        let query_tokens: Vec<String> = query
            .to_ascii_lowercase()
            .split_whitespace()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        let mut scored: Vec<BedrockRerankResult> = documents
            .iter()
            .enumerate()
            .map(|(index, doc)| {
                let lower = doc.to_ascii_lowercase();
                let overlap = query_tokens
                    .iter()
                    .filter(|token| lower.contains(token.as_str()))
                    .count() as f64;
                let denom = query_tokens.len().max(1) as f64;
                BedrockRerankResult {
                    index,
                    relevance_score: overlap / denom,
                }
            })
            .collect();
        scored.sort_by(|left, right| {
            right
                .relevance_score
                .partial_cmp(&left.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(top_n.min(scored.len()));

        Ok(BedrockRerankResponse {
            results: scored,
            usage: BedrockUsage {
                input_tokens: estimate_token_count(query),
                output_tokens: 0,
            },
            request_id: Some("fake-bedrock-rerank".to_string()),
        })
    }
}

fn estimate_token_count(text: &str) -> i64 {
    let approx = text.split_whitespace().count().max(1);
    i64::try_from(approx).unwrap_or(1)
}

fn embed_text_deterministic(text: &str, dim: usize) -> Vec<f32> {
    if dim == 0 {
        return Vec::new();
    }

    let mut values = vec![0.0f32; dim];
    let normalized = text.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return values;
    }

    for token in normalized.split_whitespace() {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let digest = hasher.finalize();
        for (idx, byte) in digest.iter().enumerate() {
            let position = (idx * 31 + usize::from(*byte)) % dim;
            let centered = (f32::from(*byte) / 255.0) - 0.5;
            values[position] += centered;
        }
    }

    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut values {
            *value /= norm;
        }
    }
    values
}

#[cfg(test)]
mod tests {
    use super::build_rerank_request_body;

    #[test]
    fn rerank_request_matches_expected_bedrock_schema() {
        let documents = vec!["doc one".to_string(), "doc two".to_string()];
        let body =
            build_rerank_request_body("where is the package", &documents, 99);

        assert_eq!(body.get("api_version").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(
            body.get("query").and_then(|v| v.as_str()),
            Some("where is the package")
        );
        assert_eq!(body.get("documents"), Some(&serde_json::json!(documents)));
        assert_eq!(body.get("top_n").and_then(|v| v.as_u64()), Some(2));
        assert!(
            body.get("return_documents").is_none(),
            "request body must not include deprecated return_documents field"
        );
    }
}
