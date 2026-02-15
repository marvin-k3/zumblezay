# Operations & Configuration

All knobs are available as CLI flags with env var fallbacks (see `Args` in `src/app.rs`). Common settings:

- `--events-db` / `EVENTS_DB`: path to the read-only events database.
- `--zumblezay-db` / `ZUMBLEZAY_DB`: application database location.
- `--cache-db` / `CACHE_DB`: storyboard cache database.
- `--whisper-url` / `WHISPER_URL`: transcription backend endpoint.
- `--enable-transcription`: start the transcription worker.
- `--transcription-service`: transcription type key used when reading/writing transcript rows.
- `--enable-sync` / `--sync-interval`: keep local event metadata synchronized.
- `--openai-api-key` / `OPENAI_API_KEY`: key for summary generation provider.
- `--openai-api-base` / `OPENAI_API_BASE`: base URL for OpenAI-compatible summary provider.
- `--bedrock-region` / `AWS_REGION`: optional Bedrock region override.
- `--default-summary-model`: model id used when generating daily summaries.
- `--investigation-model` / `BEDROCK_INVESTIGATION_MODEL`: Bedrock model for `/api/investigate`.
- `--signing-secret` / `SIGNING_SECRET`: HMAC key for `/api/prompt_context/*` requests.
- `--video-path-original-prefix` / `--video-path-replacement-prefix`: map stored video paths to local readable paths.
- `--timezone`: timezone string used for exports.
- `--host` / `--port`: bind address for the HTTP server.

Logging uses `tracing`. Override verbosity with `RUST_LOG=zumblezay=debug` or set it via the CLI flag (see `--log-level` if present in your build).

Operational notes:

- A delayed Bedrock pricing refresh runs once shortly after startup (about 60 seconds) so spend ledger entries can use current rates.
- Storyboards and cached camera names are safe to clear; they will be recomputed on demand.
- The application and events databases should be backed up if you need historical transcripts, summary cache, and spend records.
