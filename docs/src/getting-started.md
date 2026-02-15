# Getting Started

## Prerequisites
- Rust toolchain (stable) and `cargo`.
- FFmpeg available on `PATH` for audio extraction.
- SQLite is bundled through `rusqlite`; only file paths are needed.
- Optional for summaries: an OpenAI-compatible API endpoint + key.
- Optional for investigations: AWS credentials with Bedrock access.
- Optional for UI tests: Node.js + npm (see [Testing & Development](testing.md)).

## Quick run
```bash
cargo run --bin zumblezay_server -- \
  --events-db /path/to/events.db \
  --zumblezay-db data/zumblezay.db \
  --cache-db data/cache.db
```

Helpful flags (full list via `--help`):
- `--whisper-url` / `WHISPER_URL`: upstream transcription endpoint (default `http://localhost:9000/asr`).
- `--enable-transcription`: start the background transcription worker.
- `--transcription-service`: transcription type key stored in the DB (default `whisper-local`).
- `--enable-sync` / `--sync-interval`: keep event metadata in sync.
- `--openai-api-key` + `--openai-api-base`: model provider for summary generation.
- `--default-summary-model`: model id used for daily summaries.
- `--bedrock-region` + `--investigation-model`: Bedrock settings used by `/api/investigate`.
- `--video-path-original-prefix` + `--video-path-replacement-prefix`: media path translation for `/video/{event_id}`.
- `--timezone`: override the timezone used in transcript exports.

After the server starts, open the dashboard at `http://127.0.0.1:3010` (or whichever port you configured with `--port`).

## Local run helper
For repeatable dev runs, use the `run.sh` helper and a gitignored `run.local.sh` file. See the Development Workflow chapter for details.
