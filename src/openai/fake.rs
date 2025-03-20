use anyhow::Result;
use async_openai::types::{
    ChatChoice, ChatCompletionRequestMessage, ChatCompletionResponseMessage,
    CompletionUsage, CreateChatCompletionResponse, FinishReason, Model, Role,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Mutex;

use crate::openai::{ModelRequest, OpenAIClientTrait};

/// A fake implementation of the OpenAI client for testing
///
/// This fake client allows tests to control exactly what responses are returned,
/// without making any real API calls. It provides a builder pattern for configuration
/// and tracks requests for verification in tests.
///
/// # Example
///
/// ```
/// use zumblezay::openai::{OpenAIClientTrait, ROLE_USER};
/// use zumblezay::openai::fake::FakeOpenAIClient;
/// use async_openai::types::{ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs};
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     // Create a fake client with predetermined responses
///     let client = FakeOpenAIClient::new()
///         .with_response("First response");
///     
///     // Create messages for the API call
///     let user_msg = ChatCompletionRequestUserMessageArgs::default()
///         .content("Hello")
///         .build()?;
///     let messages = vec![ChatCompletionRequestMessage::User(user_msg)];
///     
///     // Call the fake client as you would the real one
///     let response = client.chat_completion("gpt-4".to_string(), messages).await?;
///     
///     // Extract the content from the response
///     let content = response.choices.first()
///         .and_then(|choice| choice.message.content.as_ref())
///         .map(String::from)
///         .unwrap_or_default();
///         
///     // Assert on the result in tests
///     assert_eq!(content, "First response");
///     Ok(())
/// }
/// ```
pub struct FakeOpenAIClient {
    responses: Mutex<Vec<Option<String>>>,
    models: Vec<Model>,
    // Track requests for verification in tests
    pub requests: Mutex<Vec<ModelRequest>>,
}

impl Default for FakeOpenAIClient {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeOpenAIClient {
    pub fn new() -> Self {
        Self {
            responses: Mutex::new(vec![]),
            models: vec![],
            requests: Mutex::new(vec![]),
        }
    }

    /// Add a response to be returned by the fake client
    pub fn with_response(self, response: &str) -> Self {
        self.responses
            .lock()
            .unwrap()
            .push(Some(response.to_string()));
        self
    }

    /// Configure the client to return a response with None content
    pub fn with_none_content_response(self) -> Self {
        self.responses.lock().unwrap().push(None);
        self
    }

    /// Add multiple responses to be returned by the fake client in sequence
    pub fn with_responses(self, responses: Vec<&str>) -> Self {
        let responses_to_add: Vec<Option<String>> =
            responses.into_iter().map(|s| Some(s.to_string())).collect();

        for response in responses_to_add {
            self.responses.lock().unwrap().push(response);
        }
        self
    }

    /// Configure the models to be returned by the fake client
    pub fn with_models(mut self, models: Vec<Model>) -> Self {
        self.models = models;
        self
    }

    /// Configure standard test models to be returned by the fake client
    pub fn with_standard_test_models(self) -> Self {
        let models = vec![
            Model {
                id: "gpt-4".to_string(),
                created: 0,
                object: "model".to_string(),
                owned_by: "openai".to_string(),
            },
            Model {
                id: "gpt-3.5-turbo".to_string(),
                created: 0,
                object: "model".to_string(),
                owned_by: "openai".to_string(),
            },
        ];
        self.with_models(models)
    }

    /// Helper method to create a simplified model from id and provider
    pub fn create_model(id: &str, provider: &str) -> Model {
        Model {
            id: id.to_string(),
            created: 0,
            object: "model".to_string(),
            owned_by: provider.to_string(),
        }
    }
}

#[async_trait]
impl OpenAIClientTrait for FakeOpenAIClient {
    #[allow(deprecated)]
    async fn chat_completion(
        &self,
        model: String,
        _messages: Vec<ChatCompletionRequestMessage>,
    ) -> Result<CreateChatCompletionResponse, anyhow::Error> {
        // Store the request for later verification
        self.requests.lock().unwrap().push(ModelRequest {
            model_name: model.clone(),
        });

        let mut responses = self.responses.lock().unwrap();
        let content_option = if responses.is_empty() {
            Some("Fake default response".to_string())
        } else {
            responses.remove(0)
        };

        // Check JSON validity if applicable
        if let Some(content) = &content_option {
            // If the model requested a json summary and the content isn't valid JSON,
            // we should return an error if that's what we're testing
            if model.contains("json")
                && !content.is_empty()
                && content != "[]"
                && serde_json::from_str::<Value>(content).is_err()
            {
                return Err(anyhow::anyhow!(
                    "Generated summary is not valid JSON"
                ));
            }
        }

        // Create the response message
        let message = ChatCompletionResponseMessage {
            role: Role::Assistant,
            content: content_option,
            #[allow(deprecated)]
            function_call: None,
            tool_calls: None,
            #[allow(deprecated)]
            refusal: None,
            audio: None,
        };

        // Create the chat choice
        let chat_choice = ChatChoice {
            index: 0,
            message,
            finish_reason: Some(FinishReason::Stop),
            logprobs: None,
        };

        // Create the usage object with the missing fields
        let usage = CompletionUsage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            prompt_tokens_details: None,
            completion_tokens_details: None,
        };

        Ok(CreateChatCompletionResponse {
            id: "fake_id".to_string(),
            object: "chat.completion".to_string(),
            created: 0,
            model: model.clone(),
            system_fingerprint: Some("fake-fingerprint".to_string()),
            service_tier: None,
            choices: vec![chat_choice],
            usage: Some(usage),
        })
    }

    async fn list_models(&self) -> Result<Vec<Model>, anyhow::Error> {
        Ok(self.models.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::ChatCompletionRequestSystemMessageArgs;

    #[tokio::test]
    async fn test_fake_openai_client_responses() -> Result<(), anyhow::Error> {
        let client = FakeOpenAIClient::new()
            .with_response("First response")
            .with_response("Second response");

        // First call should return "First response"
        let system_msg = ChatCompletionRequestSystemMessageArgs::default()
            .content("You are helpful")
            .build()?;

        let response1 = client
            .chat_completion(
                "gpt-4".to_string(),
                vec![ChatCompletionRequestMessage::System(system_msg)],
            )
            .await
            .unwrap();
        assert_eq!(
            response1.choices[0].message.content,
            Some("First response".to_string())
        );

        // Second call should return "Second response"
        let response2 = client
            .chat_completion("gpt-4".to_string(), vec![])
            .await
            .unwrap();
        assert_eq!(
            response2.choices[0].message.content,
            Some("Second response".to_string())
        );

        // Third call should return the default response
        let response3 = client
            .chat_completion("gpt-4".to_string(), vec![])
            .await
            .unwrap();
        assert_eq!(
            response3.choices[0].message.content,
            Some("Fake default response".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fake_openai_client_models() {
        let models = vec![
            FakeOpenAIClient::create_model("test-model-1", "test"),
            FakeOpenAIClient::create_model("test-model-2", "test"),
        ];

        let client = FakeOpenAIClient::new().with_models(models);
        let response = client.list_models().await.unwrap();

        assert_eq!(response.len(), 2);
        assert_eq!(response[0].id, "test-model-1");
        assert_eq!(response[1].id, "test-model-2");
    }

    #[tokio::test]
    async fn test_json_validation() {
        // Test with valid JSON
        let client =
            FakeOpenAIClient::new().with_response(r#"{"test": "value"}"#);
        let result = client.chat_completion("json".to_string(), vec![]).await;
        assert!(result.is_ok());

        // Test with invalid JSON
        let client = FakeOpenAIClient::new().with_response("Invalid JSON");
        let result = client.chat_completion("json".to_string(), vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_request_tracking() {
        let client = FakeOpenAIClient::new().with_response("Test response");

        let _ = client
            .chat_completion("gpt-4".to_string(), vec![])
            .await
            .unwrap();

        let requests = client.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].model_name, "gpt-4");
    }

    #[tokio::test]
    async fn test_none_content_response() -> Result<(), anyhow::Error> {
        let client = FakeOpenAIClient::new().with_none_content_response();

        let system_msg = ChatCompletionRequestSystemMessageArgs::default()
            .content("Test prompt")
            .build()?;

        let response = client
            .chat_completion(
                "gpt-4".to_string(),
                vec![ChatCompletionRequestMessage::System(system_msg)],
            )
            .await
            .unwrap();

        // Verify that the content is None
        assert_eq!(response.choices[0].message.content, None);

        Ok(())
    }
}
