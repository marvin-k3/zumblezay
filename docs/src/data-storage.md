# Data & Storage

Three SQLite databases are used; tests replace them with temporary files.

- **Events DB** (`events_db`): read-only view of camera events seeded externally. Key table: `events`.
- **Application DB** (`zumblezay_db`): transcriptions, summaries, and app state. Key tables: `events`, `transcriptions`, `daily_summaries`, `corrupted_files`.
- **Cache DB** (`cache_db`): storyboard artefact cache. Key table: `storyboard_cache`.

Schemas are initialized in `init_events_db_for_testing`, `init_zumblezay_db`, and `init_cache_db` (see `src/lib.rs`). When adding a column, update both production and testing initializers.

## Seeding example
Insert an event and transcript to exercise API endpoints:

```sql
INSERT INTO events (
  event_id, created_at, event_start, event_end,
  event_type, camera_id, video_path
) VALUES ('evt-1', 1700000000, 1700000000, 1700000600, 'motion', 'camera-1', '/data/camera-1/evt-1.mp4');

INSERT INTO transcriptions (
  event_id, raw_response
) VALUES ('evt-1', json_object('text', 'Hello world from Zumblezay'));
```

When the server starts it calls `cache_camera_names` automatically. If you seed data while it is running, restart it or trigger the helper in a small harness so `/api/cameras` and transcript responses expose friendly names.
