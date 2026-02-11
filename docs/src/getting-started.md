# Getting Started

## Prerequisites
- Rust toolchain (stable) and `cargo`.
- FFmpeg available on `PATH` for audio extraction.
- SQLite is bundled through `rusqlite`; only file paths are needed.
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
- `--enable-sync` / `--sync-interval`: keep event metadata in sync.
- `--default-summary-model`: model id used for daily summaries.
- `--timezone`: override the timezone used in transcript exports.

After the server starts, open the dashboard at `http://127.0.0.1:3000` (or whichever port you configured).
