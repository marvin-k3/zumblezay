# Troubleshooting

- **Database path errors**: ensure directories for `--events-db`, `--zumblezay-db`, and `--cache-db` exist and are writable. The service will report a clear error if a path is empty or missing.
- **Camera names missing**: the cache builds on startup. If you seed data while running, restart the service to repopulate `cache_camera_names`.
- **FFmpeg not found**: transcripts require FFmpeg on `PATH`. Install it and restart.
- **Port already in use**: override `--port` or `--host` to avoid conflicts.
- **Whisper backend unreachable**: set `--whisper-url` to a reachable endpoint; look for network errors in logs.
- **Skipped health tests in CI**: the `/health` probe binds to `127.0.0.1:0`; in restricted sandboxes it is skipped by design.
