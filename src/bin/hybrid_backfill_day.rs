use anyhow::Result;
use clap::Parser;
use rusqlite::params;
use serde_json::Value;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    db: String,
    #[arg(long)]
    date: String,
    #[arg(long, default_value = "America/Los_Angeles")]
    timezone: String,
    #[arg(long, default_value = "whisper-local")]
    transcription_type: String,
    #[arg(long, default_value_t = 256)]
    max_jobs_per_cycle: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let tz = zumblezay::time_util::get_local_timezone(Some(&args.timezone));

    let (start_ts, end_ts) =
        zumblezay::time_util::parse_local_date_to_utc_range_with_time(
            &args.date, &None, &None, tz,
        )?;

    let mut conn = rusqlite::Connection::open(&args.db)?;
    zumblezay::init_zumblezay_db(&mut conn)?;

    let mut stmt = conn.prepare(
        "SELECT e.event_id, e.event_start, t.raw_response
         FROM events e
         JOIN transcriptions t ON t.event_id = e.event_id
         WHERE e.event_start BETWEEN ? AND ?
           AND t.transcription_type = ?
         ORDER BY e.event_start ASC",
    )?;

    let rows = stmt.query_map(
        params![start_ts, end_ts, args.transcription_type.as_str()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )?;

    let mut total = 0usize;
    let mut parsed = 0usize;
    let mut parse_failures = 0usize;
    for row in rows {
        let (event_id, event_start, raw_response) = row?;
        total += 1;

        let parsed_json: Value = match serde_json::from_str(&raw_response) {
            Ok(value) => value,
            Err(_) => {
                parse_failures += 1;
                continue;
            }
        };
        parsed += 1;

        zumblezay::transcripts::update_transcript_search(
            &conn,
            &event_id,
            &parsed_json,
        )?;
        zumblezay::hybrid_search::index_transcript_units_for_event(
            &conn,
            &event_id,
            &parsed_json,
            event_start,
        )?;
    }
    drop(stmt);

    let mut processed_jobs = 0usize;
    loop {
        let n = zumblezay::hybrid_search::process_pending_embedding_jobs(
            &conn,
            args.max_jobs_per_cycle,
        )?;
        if n == 0 {
            break;
        }
        processed_jobs += n;
    }

    let transcript_units: i64 =
        conn.query_row("SELECT COUNT(*) FROM transcript_units", [], |row| {
            row.get(0)
        })?;
    let unit_embeddings: i64 =
        conn.query_row("SELECT COUNT(*) FROM unit_embeddings", [], |row| {
            row.get(0)
        })?;

    println!(
        "Backfill done for {} ({}): total_rows={}, parsed={}, parse_failures={}, processed_jobs={}, transcript_units={}, unit_embeddings={}",
        args.date,
        args.timezone,
        total,
        parsed,
        parse_failures,
        processed_jobs,
        transcript_units,
        unit_embeddings
    );

    Ok(())
}
