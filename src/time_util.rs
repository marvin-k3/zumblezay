use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use std::str::FromStr;

/// Get the local timezone as a chrono_tz::Tz
/// If configured_timezone is provided, it will be used
/// Otherwise, falls back to the system timezone from iana-time-zone
/// If both fail, defaults to Australia::Adelaide
pub fn get_local_timezone(configured_timezone: Option<&str>) -> Tz {
    // First try to use the configured timezone if provided
    if let Some(tz_str) = configured_timezone {
        if let Ok(tz) = Tz::from_str(tz_str) {
            return tz;
        }
    }

    // Otherwise try to get the system timezone
    match iana_time_zone::get_timezone() {
        Ok(tz_str) => {
            Tz::from_str(&tz_str).unwrap_or(chrono_tz::Australia::Adelaide)
        }
        Err(_) => chrono_tz::Australia::Adelaide,
    }
}

pub fn parse_local_date_to_utc_range(
    date: &str,
    timezone: Tz,
) -> Result<(f64, f64), anyhow::Error> {
    // Parse the date string in the specified timezone (expected format: YYYY-MM-DD)
    let naive_date = NaiveDateTime::parse_from_str(
        &format!("{} 00:00:00", date),
        "%Y-%m-%d %H:%M:%S",
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse date: {}", e))?;

    // Convert local midnight to UTC for database query
    let local_date = timezone
        .from_local_datetime(&naive_date)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to convert local date to UTC")
        })?;
    let utc_start = local_date.with_timezone(&Utc);
    let utc_end = utc_start + chrono::Duration::days(1);

    // Convert to Unix timestamp for the database query
    Ok((utc_start.timestamp() as f64, utc_end.timestamp() as f64))
}

pub fn parse_local_date_to_utc_range_with_time(
    date: &str,
    start_time: &Option<String>,
    end_time: &Option<String>,
    timezone: Tz,
) -> Result<(f64, f64), anyhow::Error> {
    let naive_start = NaiveDateTime::parse_from_str(
        &format!("{} {}", date, start_time.as_deref().unwrap_or("00:00:00")),
        "%Y-%m-%d %H:%M:%S",
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse start date: {}", e))?;

    let naive_end = NaiveDateTime::parse_from_str(
        &format!("{} {}", date, end_time.as_deref().unwrap_or("23:59:59")),
        "%Y-%m-%d %H:%M:%S",
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse end date: {}", e))?;

    let local_start = timezone
        .from_local_datetime(&naive_start)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to convert local date to UTC")
        })?;
    let utc_start = local_start.with_timezone(&Utc);

    let local_end = timezone
        .from_local_datetime(&naive_end)
        .single()
        .ok_or_else(|| {
            anyhow::anyhow!("Failed to convert local date to UTC")
        })?;
    let utc_end = local_end.with_timezone(&Utc);

    // Convert to Unix timestamp for the database query
    Ok((utc_start.timestamp() as f64, utc_end.timestamp() as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_local_date_to_utc_range_with_time() {
        // Use Adelaide timezone for tests to maintain compatibility with existing tests
        let la_tz = chrono_tz::Australia::Adelaide;

        // 2022-07-08 in Adelaide starts at 1657204200.0, last second is 1657290599.0
        let valid_date = "2022-07-08";
        // Unix timestamp: 1657229462.0
        let start_time = Some("07:01:02".to_string());
        // Unix timestamp: 1657258323.0
        let end_time = Some("15:02:03".to_string());

        let result = parse_local_date_to_utc_range_with_time(
            valid_date,
            &start_time,
            &end_time,
            la_tz,
        );
        assert!(result.is_ok());
        let (start, end) = result.unwrap();
        assert_eq!(start, 1657229462.0); // Expected Unix timestamp for start
        assert_eq!(end, 1657258323.0); // Expected Unix timestamp for end

        // Test with invalid date
        let invalid_date = "invalid-date";
        let result = parse_local_date_to_utc_range_with_time(
            invalid_date,
            &start_time,
            &end_time,
            la_tz,
        );
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(
            error.to_string().contains("Failed to parse start date"),
            "Error message: {}",
            error
        );

        // Test with missing start time
        let result = parse_local_date_to_utc_range_with_time(
            valid_date, &None, &end_time, la_tz,
        );
        assert!(result.is_ok());
        let (start, end) = result.unwrap();
        assert_eq!(start, 1657204200.0); // Expected Unix timestamp for start with default time
        assert_eq!(end, 1657258323.0); // Expected Unix timestamp for end

        // Test with missing end time
        let result = parse_local_date_to_utc_range_with_time(
            valid_date,
            &start_time,
            &None,
            la_tz,
        );
        assert!(result.is_ok());
        let (start, end) = result.unwrap();
        assert_eq!(start, 1657229462.0); // Expected Unix timestamp for start
        assert_eq!(end, 1657290599.0); // Expected Unix timestamp for end with default time
    }

    #[tokio::test]
    async fn test_get_local_timezone() {
        // Test with no configured timezone
        let tz1 = get_local_timezone(None);
        println!("Local timezone (from system): {:?}", tz1);

        // Test with valid configured timezone
        let tz2 = get_local_timezone(Some("Europe/London"));
        assert_eq!(tz2, chrono_tz::Europe::London);

        // Test with invalid configured timezone (should fall back to system timezone)
        let tz3 = get_local_timezone(Some("Invalid/Timezone"));
        println!("Local timezone (with invalid config): {:?}", tz3);

        // Test with explicit Adelaide timezone
        let tz4 = get_local_timezone(Some("Australia/Adelaide"));
        assert_eq!(tz4, chrono_tz::Australia::Adelaide);
    }

    #[tokio::test]
    async fn test_parse_local_date_to_utc_range() {
        // Test with Adelaide timezone
        let la_tz = chrono_tz::Australia::Adelaide;
        let result = parse_local_date_to_utc_range("2022-07-08", la_tz);
        assert!(result.is_ok());
        let (start, end) = result.unwrap();
        assert_eq!(start, 1657204200.0); // Expected Unix timestamp for start
        assert_eq!(end, 1657290600.0); // Expected Unix timestamp for end
    }
}
