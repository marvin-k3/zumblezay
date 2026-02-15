# Core Concepts

- **Single Axum service**: `zumblezay_server` owns HTTP routes, HTML templates, and background workers. `AppState` keeps shared pools, caches, and configuration.
- **Transcription pipeline**: `process_events` polls new events, extracts audio with FFmpeg, calls a Whisper-compatible endpoint, and writes transcripts into the application database.
- **Summary generation**: `summary::generate_summary` builds prompts and calls an OpenAI-compatible client. Results are cached per day and model.
- **Investigation flow**: `/api/investigate` and `/api/investigate/stream` use a Bedrock planner + synthesis pass over FTS transcript matches to answer ad-hoc questions with evidence moments.
- **Search index**: transcripts are indexed in the `transcript_search` FTS5 table so event filtering and investigations can do keyword retrieval efficiently.
- **Spend tracking**: Bedrock token usage and estimated cost are stored in `bedrock_spend_ledger`, backed by `bedrock_pricing` rates.
- **Caches**: camera names and storyboard artefacts are cached to avoid recomputing on every request. The cache database is safe to purge; it will repopulate.
- **CLI-first configuration**: every toggle has a CLI flag with an env var alias, making it easy to run locally or in containers.
