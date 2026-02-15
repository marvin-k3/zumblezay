# AI Agent Guide

This project ships a single Axum service that ingests camera timelines, runs Whisper-based transcription, and exposes an HTML/JSON interface for reviewing results. It also supports OpenAI-compatible daily summaries plus Bedrock-backed investigation queries with spend tracking. The code is organised so automated refactors can focus on a few well-defined surfaces. Use this guide as a quick orientation for large-language-model powered tooling.

## Repository Map
- `src/server_main.rs` – binary entrypoint; calls `app::serve`.
- `src/app.rs` – command-line parsing, dependency wiring, Axum router, HTML templates, and most HTTP handlers.
- `src/lib.rs` – shared types (`AppState`, `Event`, `ServiceStats`), SQLite bootstrapping, and the `create_app_state` factory.
- `src/process_events.rs` – background worker that fetches events, runs transcription, and writes transcripts.
- `src/transcription.rs` – Whisper/RunPod client plus FFmpeg audio extraction.
- `src/transcripts.rs` – helpers for persisting and formatting transcript data.
- `src/summary.rs` & `src/prompts.rs` – OpenAI-compatible summary generation and prompt templates.
- `src/bedrock.rs` – Bedrock client abstraction (sync + streaming completions) and usage extraction.
- `src/bedrock_spend.rs` – Bedrock pricing fetch/normalize logic and spend ledger writes.
- `src/investigation.rs` – investigation planner/retrieval/synthesis pipeline over transcript search.
- `src/storyboard.rs` – generates cached VTT/image storyboard artifacts.
- `src/prompt_context.rs` – in-memory signed cache for streaming prompt context.
- `src/schema_registry.rs` – lightweight schema artifact version tracking (used for transcript FTS rebuild state).
- `src/openai/` – trait + implementations for real vs fake OpenAI clients.
- `tests/api_tests.rs` – integration tests that exercise the HTTP API against temporary SQLite databases.
- `tests/bedrock_integration_tests.rs` – optional live Bedrock integration checks for investigation endpoints.

## Runtime Shape
1. `zumblezay_server` reads CLI arguments (`Args` in `src/app.rs`). File-system paths are resolved and `create_app_state` constructs an `Arc<AppState>`.
2. `AppState` holds pooled connections to three SQLite databases, background statistics, caches, OpenAI + Bedrock handles, and config such as Whisper URL, transcription type, model IDs, path mapping prefixes, and timezone.
3. `serve()` wires middleware (`TraceLayer`, compression) and installs routes. HTML pages stream from Tera templates in `src/templates/`.
4. Optional background tasks:
   - `process_events::process_events` polls the `events` database, extracts audio with FFmpeg, obtains transcripts, and stores them in `zumblezay_db`.
   - `sync_events` in `app.rs` (search for definition) updates the local cache of remote event metadata.

## Databases
`AppState` points at three SQLite files. Tests replace them with in-memory tempfiles.

| Pool | Purpose | Key Tables |
| --- | --- | --- |
| `events_db` | Read-only view of camera events seeded externally. | `events`, `backups` (testing schema). |
| `zumblezay_db` | Application state, cached transcriptions/summaries, and investigation metadata. | `events`, `transcriptions`, `daily_summaries`, `corrupted_files`, `transcript_search`, `metadata`, `bedrock_pricing`, `bedrock_spend_ledger`. |
| `cache_db` | Storyboard artefact cache. | `storyboard_cache`. |

Initial schema creation lives in `init_events_db_for_testing`, `init_zumblezay_db`, and `init_cache_db` (`src/lib.rs`). When adding columns, update both production and testing variants.

## HTTP & HTML Surfaces
The router (`routes` in `src/app.rs`) splits into pages and API endpoints:
- HTML dashboards: `/events/latest`, `/events/search` (with `/events` redirecting to Latest), plus `/status`, `/summary`, `/transcript`, `/investigate`.
- Health and status: `/health`, `/api/status`.
- Event data: `/api/events`, `/api/event/{event_id}`, `/api/cameras`, `/api/models`, `/video/{event_id}`, `/audio/{event_id}/wav`.
- Transcripts: `/api/transcripts/{csv|json}/{date}`, `/api/captions/{event_id}`, `/api/storyboard/{image|vtt}/{event_id}`.
- Summaries: `/api/transcripts/summary/...` including model/prompt selection and listing cached variants.
- Investigations: `/api/investigate` and `/api/investigate/stream` (SSE with planning/tool-use/delta events).
- Prompt context streaming: `/api/prompt_context/{key}/{offset}`; guarded by HMAC signatures (`prompt_context::sign`).

Review handler implementations in `src/app.rs` for error conventions and response shapes before extending routes.

## Transcription & Summary Pipeline
1. `process_events.rs` fetches candidate events via `fetch_new_events` (same file). Concurrency is throttled by `AppState::semaphore`.
2. Each event is passed to a `TranscriptionService`. `RealTranscriptionService` delegates to `transcription::get_transcript`, which abstracts over `whisper-local` and `runpod`.
3. Successful transcripts call `transcripts::save_transcript`, which inserts a `transcriptions` row, updates `transcript_search`, and updates stats.
4. Summaries live in `summary.rs`. `generate_summary` builds prompts from `prompts.rs`, hits the OpenAI client, and saves results to `daily_summaries`. Use `summary::get_daily_summary` to read cached data.
5. Investigations live in `investigation.rs`: Bedrock planner output creates transcript search queries/time windows, retrieval runs against `transcript_search`, and a Bedrock synthesis pass returns answer + evidence + usage. Spend logging writes to `bedrock_spend_ledger`, using `bedrock_pricing` (with AWS pricing fallback).

When modifying these flows, keep `ServiceStats`, `active_tasks`, and the corrupted file retry logic (`process_events.rs`) consistent.

## Prompt Context Store
`prompt_context::Store` keeps short-lived blobs keyed by HMAC-signed tokens. The Axum handler validates signatures via `prompt_context::sign::SignedRequestParams`. If you change signature or expiry semantics, update both the store and handler helpers together.

## Testing Strategy
- Integration tests (`tests/api_tests.rs`) use `AppState::new_for_testing()` to spin up isolated SQLite databases and apply migrations.
- `src/test_utils.rs` exposes helpers for seeding events, transcriptions, and summaries.
- Tests focus on `tower::ServiceExt` requests; they intentionally exercise the same code paths as production.
- When adding new tables or routes, extend the fixtures so tests mirror production defaults. Prefer using the existing helpers instead of inventing new SQL.

## Extending the Service
- **New HTTP endpoint** – add a handler in `src/app.rs`, wire it in `routes`, and create exports in `tests/api_tests.rs`.
- **New background job** – spawn alongside `process_events` inside `serve`; reuse the broadcast shutdown channel and `tokio::select!` pattern.
- **Schema changes** – update both production (`init_*` functions) and test helpers; ensure migrations run on boot.
- **Third-party API integrations** – guard configuration through `Args` (CLI flags + env overrides) and store credentials in `AppState`.
- **LLM prompt changes** – adjust `prompts.rs` and make sure corresponding summary parsing tests cover the new structure.

## Observability & Debugging
- `AppState::stats` tracks processed/error counts and cumulative durations; `/api/status` exposes human-readable metrics.
- Structured logs use `tracing` and are configured near the top of `serve()`. Adjust `RUST_LOG` or CLI flags to change verbosity.
- Camera names are cached in-memory; call `cache_camera_names` after seeding new events to reflect friendly names.

## Key Types
- `AppState` (`src/lib.rs`) – central dependency container shared across HTTP handlers and background tasks.
- `Event` (`src/lib.rs`) – canonical representation used throughout processing.
- `Transcript` / `TranscriptEntry` (`src/transcripts.rs`) – serialization helpers for exports.
- `StoryboardData` (`src/storyboard.rs`) – cached artefacts served through `/api/storyboard/*`.

Keep this mapping handy when asking an LLM to navigate or refactor code; it points directly to the modules that own each responsibility.
