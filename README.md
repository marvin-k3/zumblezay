# zumblezay

Zumblezay is an Axum-based service that collects camera events, transcripts, and AI-generated summaries. It exposes a JSON/HTML API for reviewing activity, downloading captions, and kicking off summarization jobs with OpenAI-compatible models.

The project is set up so the application code and the integration test suite both talk to real SQLite databases. Temporary databases are created automatically for tests, which means you can exercise most of the stack without stubbing things out.

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

- `GET /api/events` with filters for `date`, `camera_id`, `time_start`, `time_end`.
- `GET /api/cameras` after calling `cache_camera_names`.
- `GET /api/transcripts/json/{date}` and `GET /api/transcripts/csv/{date}` for transcript exports.
- `GET /api/event/{event_id}` for the combined event/transcription payload.
- `GET /api/captions/{event_id}` to stream WebVTT captions generated from the transcript JSON.
- `GET /api/transcripts/summary/{date}` and related summary endpoints that either reuse cached summaries or call out to the configured AI provider.

For additional examples open `tests/api_tests.rs`, where helper functions such as `insert_event_with_transcript` show the minimal inserts needed to mimic live data.
