# API Guide

Zumblezay serves both HTML pages (`/`, `/events`, `/status`, `/summary`, `/transcript`) and JSON endpoints. Highlights:

- `GET /api/events?date=YYYY-MM-DD&camera_id=cam1&time_start=HH:MM&time_end=HH:MM` — list events with filters.
- `GET /api/event/{event_id}` — combined event + transcript payload.
- `GET /api/cameras` — camera list derived from cached names.
- `GET /api/transcripts/json/{date}` and `/api/transcripts/csv/{date}` — export transcripts for a day.
- `GET /api/captions/{event_id}` — WebVTT stream for an event.
- `GET /api/transcripts/summary/{date}` — daily summary (cached per model/prompt).
- `GET /api/storyboard/{event_id}/image` and `/api/storyboard/{event_id}/vtt` — storyboard artefacts.
- `GET /api/status` / `GET /health` — lightweight health probes.

## Curl samples

Fetch events for a specific camera and date:
```bash
curl "http://127.0.0.1:3000/api/events?date=2024-03-20&camera_id=camera-1"
```

Download captions for playback:
```bash
curl -o captions.vtt "http://127.0.0.1:3000/api/captions/evt-1"
```

Pull a JSON summary:
```bash
curl "http://127.0.0.1:3000/api/transcripts/summary/2024-03-20"
```
