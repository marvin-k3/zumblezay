# Operations & Configuration

All knobs are available as CLI flags with env var fallbacks (see `Args` in `src/app.rs`). Common settings:

- `--events-db` / `EVENTS_DB`: path to the read-only events database.
- `--zumblezay-db` / `ZUMBLEZAY_DB`: application database location.
- `--cache-db` / `CACHE_DB`: storyboard cache database.
- `--whisper-url` / `WHISPER_URL`: transcription backend endpoint.
- `--enable-transcription`: start the transcription worker.
- `--enable-sync` / `--sync-interval`: keep local event metadata synchronized.
- `--default-summary-model`: model id used when generating daily summaries.
- `--timezone`: timezone string used for exports.
- `--host` / `--port`: bind address for the HTTP server.

Logging uses `tracing`. Override verbosity with `RUST_LOG=zumblezay=debug` or set it via the CLI flag (see `--log-level` if present in your build).

Storyboards and cached camera names are safe to clear; they will be recomputed on demand. The application and events databases should be backed up if you need historical transcripts.
