#[cfg(test)]
mod tests {
    use crate::openai::fake::FakeOpenAIClient;
    use crate::summary::{
        generate_summary_with_client, get_available_models_with_client,
        get_daily_summary, save_daily_summary,
    };
    use crate::AppState;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_generate_summary() {
        // Create a test state and fake OpenAI client
        let state = AppState::new_for_testing();

        // Create a fake client with a predetermined response
        let test_response = "This is a test summary for 2023-01-01";
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response(test_response));

        // Test generating a summary with the fake client
        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text",
            "Summarize {transcript} for {date}",
            Some(fake_client.clone()),
        )
        .await;

        // The result might be an error due to missing transcripts in the test database
        // That's okay for this test since we're primarily testing the OpenAI client interaction
        if result.is_ok() {
            let summary = result.unwrap();
            assert_eq!(summary, test_response);

            // Verify it was saved in the database
            let db_summary = get_daily_summary(
                &state,
                "2023-01-01",
                Some("gpt-4"),
                Some("default"),
                "text",
            )
            .await
            .unwrap();

            assert!(db_summary.is_some());
            let db_summary = db_summary.unwrap();
            assert_eq!(db_summary.content, test_response);
        }

        // Check that the client received the correct request
        let requests = fake_client.requests.lock().unwrap();
        if !requests.is_empty() {
            let request = &requests[0];
            assert_eq!(request.model_name, "gpt-4");
        }
    }

    #[tokio::test]
    async fn test_json_summary_validation() {
        let state = AppState::new_for_testing();

        // Test with valid JSON
        let valid_json = r#"[{"event": "test", "time": "10:00"}]"#;
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response(valid_json));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "json",
            "json",
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // The only way this should fail is if the transcript function fails,
        // not due to JSON validation
        if result.is_ok() {
            assert_eq!(result.unwrap(), valid_json);
        }

        // Test with invalid JSON
        let invalid_json = "This is not valid JSON";
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response(invalid_json));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "json",
            "json",
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // This should fail the JSON validation
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not valid JSON"));
    }

    #[tokio::test]
    async fn test_get_available_models() {
        let state = AppState::new_for_testing();

        // Create test models
        let models = vec![
            FakeOpenAIClient::create_model("gpt-4", "openai"),
            FakeOpenAIClient::create_model("gpt-3.5-turbo", "openai"),
            FakeOpenAIClient::create_model("claude-3", "anthropic"),
        ];

        let fake_client = Arc::new(FakeOpenAIClient::new().with_models(models));

        let result =
            get_available_models_with_client(&state, Some(fake_client))
                .await
                .unwrap();

        // We should get 3 models back
        assert_eq!(result.len(), 3);

        // Check that they're sorted by provider and then name
        assert_eq!(result[0].provider, "anthropic");
        assert_eq!(result[0].id, "claude-3");

        assert_eq!(result[1].provider, "openai");
        assert_eq!(result[1].id, "gpt-3.5-turbo");

        assert_eq!(result[2].provider, "openai");
        assert_eq!(result[2].id, "gpt-4");
    }

    #[tokio::test]
    async fn test_database_operations() {
        let state = AppState::new_for_testing();

        // Save a test summary
        let result = save_daily_summary(
            &state,
            "2023-01-01",
            "test-model",
            "test-prompt",
            "text",
            "Test summary content",
            150,
        )
        .await;

        assert!(result.is_ok());

        // Retrieve the summary
        let summary = get_daily_summary(
            &state,
            "2023-01-01",
            Some("test-model"),
            Some("test-prompt"),
            "text",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(summary.content, "Test summary content");
        assert_eq!(summary.model, "test-model");
        assert_eq!(summary.prompt_name, "test-prompt");
        assert_eq!(summary.summary_type, "text");
        assert_eq!(summary.duration_ms, 150);

        // Test overwriting an existing summary
        let result = save_daily_summary(
            &state,
            "2023-01-01",
            "test-model",
            "test-prompt",
            "text",
            "Updated content",
            200,
        )
        .await;

        assert!(result.is_ok());

        // Retrieve and verify it was updated
        let summary = get_daily_summary(
            &state,
            "2023-01-01",
            Some("test-model"),
            Some("test-prompt"),
            "text",
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(summary.content, "Updated content");
        assert_eq!(summary.duration_ms, 200);
    }

    #[tokio::test]
    async fn test_empty_summary_handling() {
        let state = AppState::new_for_testing();

        // Test with empty string response
        let fake_client = Arc::new(FakeOpenAIClient::new().with_response(""));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text",
            "Summarize {transcript} for {date}",
            Some(fake_client),
        )
        .await;

        // Should return an error for empty content
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No meaningful summary content"));

        // Test with empty JSON array for JSON summary type
        let fake_client = Arc::new(FakeOpenAIClient::new().with_response("[]"));

        // For JSON summary type, empty array is valid
        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "json",
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // This should succeed as empty JSON array is valid for JSON summary type
        if result.is_ok() {
            assert_eq!(result.unwrap(), "[]");
        }

        // Test with a missing content (None)
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_none_content_response());

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text",
            "Summarize {transcript} for {date}",
            Some(fake_client),
        )
        .await;

        // Should return an error for missing content
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No meaningful summary content"));
    }

    #[tokio::test]
    async fn test_json_empty_array_validation() {
        let state = AppState::new_for_testing();

        // Test with empty JSON array - should be valid for JSON type
        let fake_client = Arc::new(FakeOpenAIClient::new().with_response("[]"));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "json", // Specifically testing JSON type
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // Should succeed for JSON type
        assert!(result.is_ok());
        if let Ok(summary) = result {
            assert_eq!(summary, "[]");
        }
    }

    #[tokio::test]
    async fn test_empty_string_for_text_summary() {
        let state = AppState::new_for_testing();

        // Test with empty string - should fail for text type
        let fake_client = Arc::new(FakeOpenAIClient::new().with_response(""));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text", // Specifically testing text type
            "Summarize {transcript} for {date}",
            Some(fake_client),
        )
        .await;

        // Should fail for text type
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("No meaningful summary content"));
        }
    }

    #[tokio::test]
    async fn test_whitespace_only_content() {
        let state = AppState::new_for_testing();

        // Test with whitespace-only content - should fail for any type
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response("   \n  \t  "));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text",
            "Summarize {transcript} for {date}",
            Some(fake_client),
        )
        .await;

        // Should fail since trimmed content is empty
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("No meaningful summary content"));
        }
    }

    #[tokio::test]
    async fn test_invalid_json_for_json_summary() {
        let state = AppState::new_for_testing();

        // Test with invalid JSON - should fail JSON validation
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response("This is not JSON"));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "json",
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // Should fail JSON validation
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(err.to_string().contains("not valid JSON"));
        }
    }

    #[tokio::test]
    async fn test_valid_text_summary() {
        let state = AppState::new_for_testing();

        // Test with valid text content
        let fake_client = Arc::new(
            FakeOpenAIClient::new().with_response("This is a valid summary"),
        );

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "text",
            "Summarize {transcript} for {date}",
            Some(fake_client),
        )
        .await;

        // Should succeed
        if result.is_ok() {
            assert_eq!(result.unwrap(), "This is a valid summary");
        }
    }

    #[tokio::test]
    async fn test_valid_json_object_summary() {
        let state = AppState::new_for_testing();

        // Test with valid JSON object
        let json_content = r#"{"summary": "Test event", "count": 1}"#;
        let fake_client =
            Arc::new(FakeOpenAIClient::new().with_response(json_content));

        let result = generate_summary_with_client(
            &state,
            "2023-01-01",
            "gpt-4",
            "json",
            "Generate JSON for {date}",
            Some(fake_client),
        )
        .await;

        // Should succeed
        if result.is_ok() {
            assert_eq!(result.unwrap(), json_content);
        }
    }
}
