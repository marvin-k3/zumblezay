# zumblezay

Zumblezay is an Axum-based service that collects camera events, transcripts, and AI-generated summaries. It exposes a JSON/HTML API for reviewing activity, downloading captions, and kicking off summarization jobs with OpenAI-compatible models.

- See `docs/AI_AGENT_GUIDE.md` for an architecture walk-through tuned for LLM-based tooling.
- The primary binary is `zumblezay_server`; it wires databases, background workers, and the HTTP surface defined in `src/app.rs`.
- Three SQLite databases are in play: a read-only events source, the application database (`zumblezay.db`), and a cache database for storyboard artefacts.

## Documentation

- User/operator docs live in the mdBook under `docs/` (`book.toml` + `docs/src`). Serve locally with `mdbook serve docs --open` or build with `mdbook build docs` (install via `cargo install mdbook` if needed).
- The mdBook covers getting started, API usage, data model, operations, testing, and troubleshooting. The AI Agent Guide stays as the contributor map.

## Quick start

```bash
# Run the server with explicit database paths
cargo run --bin zumblezay_server -- \
  --events-db /path/to/events.db \
  --zumblezay-db data/zumblezay.db \
  --cache-db data/cache.db
```

Key CLI switches (defined in `src/app.rs`):

- `--whisper-url` / `WHISPER_URL`: upstream transcription endpoint (defaults to `http://localhost:9000/asr`).
- `--enable-transcription`: toggle the background transcription worker.
- `--enable-sync` / `--sync-interval`: control the event sync loop.
- `--default-summary-model`: model identifier passed to the summary generator.
- `--timezone`: overrides the timezone used in transcript exports.

`cargo run --bin zumblezay_server -- --help` prints the full list of flags and defaults.

## Running the integration tests

```bash
cargo test --test api_tests -- --nocapture
```

What to know:

- Tests spin up `AppState::new_for_testing()`, which wires three temporary SQLite files (events, zumblezay, cache) and applies the migrations from `init_events_db_for_testing`, `init_zumblezay_db`, and `init_cache_db`.
- The `/health` test suite includes a smoke test that binds to `127.0.0.1:0`. In sandboxed environments without networking permissions it skips itself automatically; you will still see the `[skipping...]` message in the output.
- Camera names are derived from event video paths (e.g. `/data/<camera>/<file>`). The helpers in `tests/api_tests.rs` insert rows and call `cache_camera_names` so the responses match production behavior.
- Transcript and summary assertions work against the real schemas: inserting into `events`, `transcriptions`, and `daily_summaries` is enough to exercise `/api/transcripts/*`, `/api/event/{id}`, `/api/captions/{id}`, and the summary cache endpoints.

If you need to reproduce one scenario in isolation, run it by name:

```bash
cargo test --test api_tests -- test_transcript_summary_endpoints_return_cached_content -- --nocapture
```

## Playwright UI tests

Browser-level smoke tests live under `playwright-tests/` and are powered by Playwright.

```bash
# Install JS dependencies (once)
npm install

# Download the headless browsers (once)
npm run playwright:install

# Run the UI suite
npm run test:ui
```

The Playwright config spins up `cargo run --bin playwright_server`, which seeds an in-memory test database, copies the sample `testdata/10s-bars.mp4`, and exposes the dashboard on `http://127.0.0.1:4173`. Override the host/port with `PLAYWRIGHT_TEST_HOST` / `PLAYWRIGHT_TEST_PORT` if you need to run multiple stacks side-by-side.

If a test fails you can rerun it in headed mode with `npm run test:ui:headed` and inspect the captured trace via `npx playwright show-trace <path-to-trace.zip>`.

## Manually seeding data

When you run the service locally you can seed the SQLite databases with the same shape the tests expect:

1. Create an `AppState` either through the CLI or `AppState::new_for_testing()` in a `cargo run` harness.
2. Insert camera events into the `events` table with:
   ```sql
   INSERT INTO events (
     event_id, created_at, event_start, event_end,
     event_type, camera_id, video_path
   ) VALUES (...)
   ```
   The integration helpers also assign `video_path` values like `/data/<camera>/<event>.mp4` so that `cache_camera_names` derives human-readable names.
3. Add transcripts by inserting JSON into `transcriptions.raw_response`. The service expects Whisper-like payloads containing a top-level `"text"` field and optional `"segments"` array.
4. Run the cache warmer `cache_camera_names(&state).await` so `/api/cameras` and transcript responses include friendly names.

Daily summaries are cached in the `daily_summaries` table. The tests demonstrate how to seed markdown and JSON variants for `/api/transcripts/summary/{date}`, `/api/transcripts/summary/{date}/json`, and `/api/transcripts/summary/{date}/type/{type}/model/{model}/prompt/{prompt}`.

## Useful endpoints

The integration tests cover the most important routes; refer to them as living examples of request/response behaviour:
- `GET /api/events` with filters for `date` (legacy), `date_start`, `date_end`, `camera_id`, `time_start`, `time_end`, plus pagination via `limit`, `cursor_start`, `cursor_event_id`. Returns `{ events, next_cursor }` and includes `snippet` when searching.
- `GET /api/cameras` after calling `cache_camera_names`.
- `GET /api/transcripts/json/{date}` and `GET /api/transcripts/csv/{date}` for transcript exports.
- `GET /api/event/{event_id}` for the combined event/transcription payload.
- `GET /api/captions/{event_id}` to stream WebVTT captions generated from the transcript JSON.
- `GET /api/transcripts/summary/{date}` and related summary endpoints that either reuse cached summaries or call out to the configured AI provider.

## Vision processing

Vision work is coordinated by the Rust service and stored in SQLite. External workers claim jobs over HTTP, fetch video from `/video/{event_id}`, and submit results back to `/api/vision/result`. See `docs/src/vision-processing.md` and the worker at `scripts/vision_worker.py`.

For additional examples open `tests/api_tests.rs`, where helper functions such as `insert_event_with_transcript` show the minimal inserts needed to mimic live data. Pair that with the module map in `docs/AI_AGENT_GUIDE.md` when asking tooling to extend the service.
