# Core Concepts

- **Single Axum service**: `zumblezay_server` owns HTTP routes, HTML templates, and background workers. `AppState` keeps shared pools, caches, and configuration.
- **Transcription pipeline**: `process_events` polls new events, extracts audio with FFmpeg, calls a Whisper-compatible endpoint, and writes transcripts into the application database.
- **Vision pipeline**: vision jobs live in SQLite and are claimed by external workers over HTTP. Results are stored per analysis type and feed downstream steps.
- **Summary generation**: `summary::generate_summary` builds prompts and calls an OpenAI-compatible client. Results are cached per day and model.
- **Caches**: camera names and storyboard artefacts are cached to avoid recomputing on every request. The cache database is safe to purge; it will repopulate.
- **CLI-first configuration**: every toggle has a CLI flag with an env var alias, making it easy to run locally or in containers.
