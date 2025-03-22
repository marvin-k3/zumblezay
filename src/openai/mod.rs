pub mod fake;
pub mod real;

use anyhow::Result;
use async_openai::types::{
    ChatCompletionRequestMessage, CreateChatCompletionResponse, Model,
};
use async_trait::async_trait;

/// Constants for message roles
pub const ROLE_SYSTEM: &str = "system";
pub const ROLE_USER: &str = "user";
pub const ROLE_ASSISTANT: &str = "assistant";

/// A struct to define what model was used for a request
#[derive(Debug, Clone)]
pub struct ModelRequest {
    pub model_name: String,
}

/// A trait that abstracts OpenAI client functionality for testing
///
/// This trait provides a common interface for both real and fake OpenAI clients,
/// making it easy to swap between them for testing purposes.
///
/// Implementation notes:
/// - Uses `async-trait` to enable async methods in traits
/// - Uses the actual OpenAI API types from the async_openai crate
#[async_trait]
pub trait OpenAIClientTrait: Send + Sync {
    /// Creates a chat completion by sending messages to the language model
    ///
    /// # Arguments
    /// * `model` - The model identifier (e.g., "gpt-4", "gpt-3.5-turbo")
    /// * `messages` - A sequence of messages using OpenAI types
    ///
    /// # Returns
    /// The complete ChatCompletionResponse from the model, or an error
    async fn chat_completion(
        &self,
        model: String,
        messages: Vec<ChatCompletionRequestMessage>,
    ) -> Result<CreateChatCompletionResponse, anyhow::Error>;

    /// Retrieves a list of available models
    ///
    /// # Returns
    /// A list of model information objects
    async fn list_models(&self) -> Result<Vec<Model>, anyhow::Error>;
}
