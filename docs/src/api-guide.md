# API Guide

Zumblezay serves both HTML pages (`/events/latest`, `/events/search`, `/status`, `/summary`, `/transcript`, `/investigate`) and JSON endpoints. Highlights:

- `GET /api/events?date=YYYY-MM-DD` (legacy) or `date_start`/`date_end` plus `camera_id`, `time_start`, `time_end`, and search `q`. Supports pagination with `limit`, `cursor_start`, `cursor_event_id` and returns `{ events, next_cursor, latency_ms }`. Search results include `snippet` excerpts when `q` is present.
- `GET /api/event/{event_id}` — combined event + transcript payload.
- `GET /api/cameras` — camera list derived from cached names.
- `GET /api/models` — available summary models from the configured OpenAI-compatible provider.
- `GET /api/transcripts/json/{date}` and `/api/transcripts/csv/{date}` — export transcripts for a day.
- `GET /api/transcripts/summary/{date}` and `/api/transcripts/summary/{date}/json` — markdown/html or JSON daily summary (cached per model/prompt).
- `GET /api/transcripts/summary/{date}/list` and `/api/transcripts/summary/{date}/type/{type}/model/{model}/prompt/{prompt}` — summary index + direct retrieval.
- `POST /api/investigate` — Bedrock-powered investigation response with evidence and usage metadata.
- `POST /api/investigate/stream` — SSE investigation stream (`status`, `search_plan`, `tool_use`, `answer_delta`, `done`, `error` events).
- `GET /api/captions/{event_id}` — WebVTT stream for an event.
- `GET /api/storyboard/image/{event_id}` and `/api/storyboard/vtt/{event_id}` — storyboard artefacts.
- `GET /api/prompt_context/{key}/{offset}?hmac=...&expires=...` — signed prompt context fetch.
- `GET /api/status` / `GET /health` — lightweight health probes.

## Curl samples

Fetch events for a specific camera and date:
```bash
curl "http://127.0.0.1:3010/api/events?date=2024-03-20&camera_id=camera-1"
```

Search transcripts across a date range with pagination:
```bash
curl "http://127.0.0.1:3010/api/events?date_start=2024-03-01&date_end=2024-03-31&q=package&limit=200"
```

Download captions for playback:
```bash
curl -o captions.vtt "http://127.0.0.1:3010/api/captions/evt-1"
```

Pull a JSON summary:
```bash
curl "http://127.0.0.1:3010/api/transcripts/summary/2024-03-20/json"
```

Run an investigation question (non-streaming):
```bash
curl -X POST "http://127.0.0.1:3010/api/investigate" \
  -H "Content-Type: application/json" \
  -d '{"question":"when did the child have lunch","chat_history":[]}'
```

Open the streaming investigation endpoint:
```bash
curl -N -X POST "http://127.0.0.1:3010/api/investigate/stream" \
  -H "Content-Type: application/json" \
  -d '{"question":"was there a bottle before nap time?"}'
```
