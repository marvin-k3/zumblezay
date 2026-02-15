# Testing & Development

## Rust integration tests
```bash
cargo test --test api_tests -- --nocapture
```

The suite spins up `AppState::new_for_testing()` with three temporary SQLite files and populates schemas via `init_events_db_for_testing`, `init_zumblezay_db`, and `init_cache_db`. Routes are exercised through `tower::ServiceExt` so responses mirror production. If you need to run one scenario:

```bash
cargo test --test api_tests -- test_transcript_summary_endpoints_return_cached_content -- --nocapture
```

Optional live Bedrock integration tests:
```bash
RUN_BEDROCK_INTEGRATION=1 cargo test --test bedrock_integration_tests -- --nocapture
```
These tests are no-op unless `RUN_BEDROCK_INTEGRATION=1` is set, and they require valid AWS credentials plus Bedrock network access.

## Playwright UI tests
```bash
npm install
npm run playwright:install
npm run test:ui
```

`playwright_server` seeds an in-memory database, copies `testdata/10s-bars.mp4`, and serves the dashboard on `http://127.0.0.1:4173`. Use `PLAYWRIGHT_TEST_HOST` / `PLAYWRIGHT_TEST_PORT` to avoid port clashes, or rerun failures in headed mode:

```bash
npm run test:ui:headed
npx playwright show-trace <path-to-trace.zip>
```

The suite currently covers `events.spec.ts`, `transcript.spec.ts`, `summary.spec.ts`, and `investigate.spec.ts`.

## Local data seeding
For manual experiments, insert rows into `events` and `transcriptions` (see [Data & Storage](data-storage.md)). Start the server; it will cache camera names on boot so API and UI responses include friendly labels.
