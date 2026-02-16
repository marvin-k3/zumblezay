# API Guide

Zumblezay serves both HTML pages (`/events/latest`, `/events/search`, `/status`, `/summary`, `/transcript`) and JSON endpoints. Highlights:

- `GET /api/events?date=YYYY-MM-DD` (legacy) or `date_start`/`date_end` plus `camera_id`, `time_start`, `time_end`, and search `q`. Supports pagination with `limit`, `cursor_start`, `cursor_event_id` and returns `{ events, next_cursor }`. Search results include `snippet` excerpts when `q` is present.
  - Optional `search_mode` for transcript queries: `hybrid` (default), `bm25`, or `vector`.
  - Search responses include attribution fields: `match_sources`, `bm25_rank`, `vector_rank`, `bm25_rrf_score`, `vector_rrf_score`, and segment evidence metadata when available.
- `GET /api/event/{event_id}` — combined event + transcript payload.
- `GET /api/cameras` — camera list derived from cached names.
- `GET /api/transcripts/json/{date}` and `/api/transcripts/csv/{date}` — export transcripts for a day.
- `GET /api/captions/{event_id}` — WebVTT stream for an event.
- `GET /api/transcripts/summary/{date}` — daily summary (cached per model/prompt).
- `POST /api/investigate` and `POST /api/investigate/stream` — question answering over transcript evidence; request accepts optional `search_mode` (`hybrid`, `bm25`, `vector`).
- `GET /api/storyboard/{event_id}/image` and `/api/storyboard/{event_id}/vtt` — storyboard artefacts.
- `GET /api/status` / `GET /health` — lightweight health probes.

## Curl samples

Fetch events for a specific camera and date:
```bash
curl "http://127.0.0.1:3000/api/events?date=2024-03-20&camera_id=camera-1"
```

Search transcripts across a date range with pagination:
```bash
curl "http://127.0.0.1:3000/api/events?date_start=2024-03-01&date_end=2024-03-31&q=package&limit=200"
```

Download captions for playback:
```bash
curl -o captions.vtt "http://127.0.0.1:3000/api/captions/evt-1"
```

Pull a JSON summary:
```bash
curl "http://127.0.0.1:3000/api/transcripts/summary/2024-03-20"
```
