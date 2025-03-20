use crate::openai::OpenAIClientTrait;
use async_openai::config::OpenAIConfig;
use async_openai::types::{
    ChatCompletionRequestMessage, CreateChatCompletionRequestArgs,
    CreateChatCompletionResponse, Model,
};
use async_openai::Client;
use async_trait::async_trait;

// A real implementation of the OpenAI client
pub struct RealOpenAIClient {
    client: Client<OpenAIConfig>,
}

impl RealOpenAIClient {
    pub fn new(client: Client<OpenAIConfig>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl OpenAIClientTrait for RealOpenAIClient {
    async fn chat_completion(
        &self,
        model: String,
        messages: Vec<ChatCompletionRequestMessage>,
    ) -> Result<CreateChatCompletionResponse, anyhow::Error> {
        // Create the OpenAI request
        let request = CreateChatCompletionRequestArgs::default()
            .model(model)
            .messages(messages)
            .build()?;

        // Send the request to OpenAI
        let response = self.client.chat().create(request).await?;

        // Return the complete response
        Ok(response)
    }

    async fn list_models(&self) -> Result<Vec<Model>, anyhow::Error> {
        let response = self.client.models().list().await?;
        Ok(response.data)
    }
}
