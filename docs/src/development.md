# Development Workflow

## Local run helper (run.sh)
The repo ships `run.sh` to keep local-only flags and secrets out of git. The script expects a `run.local.sh` file that defines the binary and its args.

1) Copy the example and edit it for your machine:
```bash
cp run.local.sh.example run.local.sh
```

2) Update `run.local.sh` with your database paths, ports, and API keys. Example:
```bash
RUN_BIN="zumblezay_server"

RUN_ARGS=(
  --enable-sync=false
  --enable-transcription=false
  --port=3011
  --timezone=America/Los_Angeles
  --events-db=data/events-YYYYMMDD.db
  --zumblezay-db=data/zumblezay-YYYYMMDD.db
  --cache-db=data/cache.db
  --whisper-url=http://ai-host:9000/asr
  --openai-api-key=replace-with-local-key
  --openai-api-base=http://ai-host:4000/v1
)
```

3) Run the server:
```bash
./run.sh
```

Notes:
- `run.local.sh` is gitignored by design. Keep secrets and machine-specific paths there.
- `RUN_ARGS` must be a non-empty array. `run.sh` will error if it is missing or empty.
- Extra CLI flags passed to `./run.sh` are appended after `RUN_ARGS`.

## Alternative binaries
If you need to run another binary (such as the Playwright server), update `RUN_BIN` in `run.local.sh`:
```bash
RUN_BIN="playwright_server"
```

## Common dev commands
```bash
cargo test --test api_tests -- --nocapture
npm run test:ui
```
