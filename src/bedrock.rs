use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::operation::RequestId;
use aws_sdk_bedrockruntime::primitives::Blob;
use aws_sdk_bedrockruntime::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Mutex;
use tokio::sync::OnceCell;

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

        let response = self
            .client()
            .await?
            .invoke_model()
            .model_id(model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(
                serde_json::to_vec(&body).context("serialize bedrock body")?,
            ))
            .send()
            .await
            .map_err(|error| {
                anyhow!("Bedrock invoke_model failed for model '{model_id}': {error:?}")
            })?;
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

        let response = self
            .client()
            .await?
            .invoke_model_with_response_stream()
            .model_id(model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(
                serde_json::to_vec(&body).context("serialize bedrock body")?,
            ))
            .send()
            .await
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

        while let Some(event) = stream.recv().await.map_err(|error| {
            anyhow!(
                "Bedrock stream receive failed for model '{model_id}': {error:?}"
            )
        })? {
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
}
