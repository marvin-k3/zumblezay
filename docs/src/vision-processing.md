# Vision Processing

Zumblezay treats vision work as a queue backed by SQLite. The Rust service is the system of record: it owns the job tables, exposes a small HTTP API for workers, and stores results for downstream analysis. Workers can run anywhere and pull jobs over HTTP/JSON.

## Workflow

1. Rust inserts `vision_jobs` rows for events that need analysis.
2. Workers claim jobs with `POST /api/vision/claim`.
3. Workers fetch video from `/video/{event_id}` (out-of-band).
4. Workers submit results with `POST /api/vision/result` or errors with `POST /api/vision/fail`.

## Tables

`vision_jobs`
- One row per `(event_id, analysis_type)`.
- Lease-based locking with `locked_at` / `locked_by`.
- Retry tracking with `attempt_count`, `last_error`, and `next_retry_at`.

`vision_results`
- Stores the raw JSON output for each analysis step.
- Unique by `(event_id, analysis_type, model, version)` to keep runs idempotent.

## API Endpoints

`POST /api/vision/claim`
```json
{
  "worker_id": "worker-1",
  "max_jobs": 4,
  "lease_seconds": 300
}
```

`POST /api/vision/result`
```json
{
  "worker_id": "worker-1",
  "job_id": 42,
  "event_id": "evt-1",
  "analysis_type": "yolo_boxes",
  "duration_ms": 1200,
  "raw_response": { "frames": [] },
  "model": "yolov8n",
  "version": "1",
  "hash": "abc123",
  "metadata": { "crop": { "x": 0.1, "y": 0.2, "w": 0.5, "h": 0.6 } }
}
```

`POST /api/vision/fail`
```json
{
  "worker_id": "worker-1",
  "job_id": 42,
  "error": "model crashed"
}
```

`POST /api/vision/enqueue_test`
```json
{
  "date": "2024-04-15",
  "limit": 3
}
```

## Example Worker

See `scripts/vision_worker.py` for a full worker that claims jobs, downloads video, runs YOLO person detection, and submits results. Use `run_worker.sh` for a convenient wrapper.

The worker emits console logs by default. If `opentelemetry-sdk` is installed in the venv, it also prints OpenTelemetry spans to stdout via the console exporter. It streams frames from the `/video/{event_id}` URL via `ffmpeg`/`ffprobe`, so those binaries must be available on the worker host. Sampling defaults to 1fps, but the worker will process up to 2fps when motion exceeds the threshold.
