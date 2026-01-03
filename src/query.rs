//! SQL query builder for OpenSky Trino database.
//!
//! Note: OpenSky stores timestamps as Unix epoch integers, not SQL TIMESTAMP types.

use crate::types::{QueryParams, FLIGHT_COLUMNS};
use chrono::{NaiveDateTime, Duration, Timelike};

/// The main table for state vector data.
const STATE_VECTORS_TABLE: &str = "minio.osky.state_vectors_data4";

/// The flights table for airport filtering.
const FLIGHTS_TABLE: &str = "minio.osky.flights_data4";

/// Build a SQL query for the history() method.
///
/// This generates a SELECT statement against state_vectors_data4,
/// optionally joining with flights_data4 for airport filtering.
pub fn build_history_query(params: &QueryParams) -> String {
    let columns = FLIGHT_COLUMNS.join(", ");

    let has_airport_filter = params.departure_airport.is_some()
        || params.arrival_airport.is_some()
        || params.airport.is_some();

    if has_airport_filter {
        build_airport_join_query(params, &columns)
    } else {
        build_simple_query(params, &columns)
    }
}

/// Build a simple query without airport join.
fn build_simple_query(params: &QueryParams, columns: &str) -> String {
    let mut sql = format!(
        "SELECT {columns}\nFROM {STATE_VECTORS_TABLE}\nWHERE 1=1"
    );

    // Time filters (required for partition pruning)
    // Note: OpenSky stores time/hour as Unix timestamps (integers)
    if let (Some(start), Some(stop)) = (&params.start, &params.stop) {
        let start_ts = datetime_to_unix(start);
        let stop_ts = datetime_to_unix(stop);
        let (start_hour_ts, stop_hour_ts) = compute_hour_bounds_unix(start, stop);

        sql.push_str(&format!("\n  AND time >= {start_ts}"));
        sql.push_str(&format!("\n  AND time <= {stop_ts}"));
        sql.push_str(&format!("\n  AND hour >= {start_hour_ts}"));
        sql.push_str(&format!("\n  AND hour < {stop_hour_ts}"));
    }

    // ICAO24 filter
    if let Some(icao24) = &params.icao24 {
        let icao24_lower = icao24.to_lowercase();
        if icao24_lower.contains('%') || icao24_lower.contains('_') {
            sql.push_str(&format!("\n  AND icao24 LIKE '{}'", escape_sql(&icao24_lower)));
        } else {
            sql.push_str(&format!("\n  AND icao24 = '{}'", escape_sql(&icao24_lower)));
        }
    }

    // Callsign filter
    if let Some(callsign) = &params.callsign {
        if callsign.contains('%') || callsign.contains('_') {
            sql.push_str(&format!("\n  AND callsign LIKE '{}'", escape_sql(callsign)));
        } else {
            sql.push_str(&format!("\n  AND callsign = '{}'", escape_sql(callsign)));
        }
    }

    // Geographic bounds
    if let Some(bounds) = &params.bounds {
        sql.push_str(&format!("\n  AND lon >= {}", bounds.west));
        sql.push_str(&format!("\n  AND lon <= {}", bounds.east));
        sql.push_str(&format!("\n  AND lat >= {}", bounds.south));
        sql.push_str(&format!("\n  AND lat <= {}", bounds.north));
    }

    // Order and limit
    sql.push_str("\nORDER BY time");

    if let Some(limit) = params.limit {
        sql.push_str(&format!("\nLIMIT {limit}"));
    }

    sql
}

/// Build a query with airport join.
fn build_airport_join_query(params: &QueryParams, columns: &str) -> String {
    let (start, stop) = match (&params.start, &params.stop) {
        (Some(s), Some(e)) => (s.as_str(), e.as_str()),
        _ => return build_simple_query(params, columns),
    };

    let start_ts = datetime_to_unix(start);
    let stop_ts = datetime_to_unix(stop);
    let (start_hour_ts, stop_hour_ts) = compute_hour_bounds_unix(start, stop);
    let (start_day_ts, stop_day_ts) = compute_day_bounds_unix(start, stop);

    // Build the flights subquery
    let mut flights_where = vec![
        format!("day >= {start_day_ts}"),
        format!("day <= {stop_day_ts}"),
    ];

    if let Some(icao24) = &params.icao24 {
        flights_where.push(format!("icao24 = '{}'", escape_sql(&icao24.to_lowercase())));
    }
    if let Some(callsign) = &params.callsign {
        flights_where.push(format!("callsign = '{}'", escape_sql(callsign)));
    }
    if let Some(dep) = &params.departure_airport {
        flights_where.push(format!("estdepartureairport = '{}'", escape_sql(dep)));
    }
    if let Some(arr) = &params.arrival_airport {
        flights_where.push(format!("estarrivalairport = '{}'", escape_sql(arr)));
    }
    if let Some(airport) = &params.airport {
        flights_where.push(format!(
            "(estdepartureairport = '{}' OR estarrivalairport = '{}')",
            escape_sql(airport), escape_sql(airport)
        ));
    }

    let flights_subquery = format!(
        r#"SELECT icao24, callsign, firstseen, lastseen
FROM {FLIGHTS_TABLE}
WHERE {}"#,
        flights_where.join("\n  AND ")
    );

    // Build the main query with join
    // Prefix all columns with sv. alias
    let prefixed_columns = columns.split(", ").map(|c| format!("sv.{c}")).collect::<Vec<_>>().join(", ");

    let mut sql = format!(
        r#"SELECT {prefixed_columns}
FROM {STATE_VECTORS_TABLE} sv
JOIN ({flights_subquery}) fl
  ON sv.icao24 = fl.icao24 AND sv.callsign = fl.callsign
WHERE sv.time >= fl.firstseen
  AND sv.time <= fl.lastseen
  AND sv.time >= {start_ts}
  AND sv.time <= {stop_ts}
  AND sv.hour >= {start_hour_ts}
  AND sv.hour < {stop_hour_ts}"#
    );

    // Geographic bounds
    if let Some(bounds) = &params.bounds {
        sql.push_str(&format!("\n  AND sv.lon >= {}", bounds.west));
        sql.push_str(&format!("\n  AND sv.lon <= {}", bounds.east));
        sql.push_str(&format!("\n  AND sv.lat >= {}", bounds.south));
        sql.push_str(&format!("\n  AND sv.lat <= {}", bounds.north));
    }

    sql.push_str("\nORDER BY sv.time");

    if let Some(limit) = params.limit {
        sql.push_str(&format!("\nLIMIT {limit}"));
    }

    sql
}

/// Convert datetime string to Unix timestamp.
fn datetime_to_unix(dt_str: &str) -> i64 {
    let dt = NaiveDateTime::parse_from_str(dt_str, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| {
            NaiveDateTime::parse_from_str(&format!("{} 00:00:00", dt_str), "%Y-%m-%d %H:%M:%S")
                .unwrap()
        });
    dt.and_utc().timestamp()
}

/// Compute hour bounds as Unix timestamps for partition pruning.
/// Returns (floor to hour, ceil to hour + 1).
fn compute_hour_bounds_unix(start: &str, stop: &str) -> (i64, i64) {
    let start_dt = NaiveDateTime::parse_from_str(start, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::parse_from_str(&format!("{} 00:00:00", start), "%Y-%m-%d %H:%M:%S").unwrap());
    let stop_dt = NaiveDateTime::parse_from_str(stop, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::parse_from_str(&format!("{} 23:59:59", stop), "%Y-%m-%d %H:%M:%S").unwrap());

    // Floor start to hour
    let start_hour = start_dt
        .with_minute(0).unwrap()
        .with_second(0).unwrap();

    // Ceil stop to next hour
    let stop_hour = stop_dt
        .with_minute(0).unwrap()
        .with_second(0).unwrap()
        + Duration::hours(1);

    (
        start_hour.and_utc().timestamp(),
        stop_hour.and_utc().timestamp(),
    )
}

/// Compute day bounds as Unix timestamps for flights table.
fn compute_day_bounds_unix(start: &str, stop: &str) -> (i64, i64) {
    let start_dt = NaiveDateTime::parse_from_str(start, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::parse_from_str(&format!("{} 00:00:00", start), "%Y-%m-%d %H:%M:%S").unwrap());
    let stop_dt = NaiveDateTime::parse_from_str(stop, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::parse_from_str(&format!("{} 23:59:59", stop), "%Y-%m-%d %H:%M:%S").unwrap());

    let start_day = start_dt.date().and_hms_opt(0, 0, 0).unwrap();
    let stop_day = (stop_dt.date() + Duration::days(1)).and_hms_opt(0, 0, 0).unwrap();

    (
        start_day.and_utc().timestamp(),
        stop_day.and_utc().timestamp(),
    )
}

/// Escape single quotes in SQL strings.
fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build a preview of the query (for display purposes).
pub fn build_query_preview(params: &QueryParams) -> String {
    let mut parts = vec!["trino.history(".to_string()];

    if let Some(start) = &params.start {
        parts.push(format!("    start=\"{start}\","));
    }
    if let Some(stop) = &params.stop {
        parts.push(format!("    stop=\"{stop}\","));
    }
    if let Some(icao24) = &params.icao24 {
        parts.push(format!("    icao24=\"{icao24}\","));
    }
    if let Some(callsign) = &params.callsign {
        parts.push(format!("    callsign=\"{callsign}\","));
    }
    if let Some(dep) = &params.departure_airport {
        parts.push(format!("    departure_airport=\"{dep}\","));
    }
    if let Some(arr) = &params.arrival_airport {
        parts.push(format!("    arrival_airport=\"{arr}\","));
    }
    if let Some(airport) = &params.airport {
        parts.push(format!("    airport=\"{airport}\","));
    }
    if let Some(bounds) = &params.bounds {
        parts.push(format!(
            "    bounds=({}, {}, {}, {}),",
            bounds.west, bounds.south, bounds.east, bounds.north
        ));
    }
    if let Some(limit) = params.limit {
        parts.push(format!("    limit={limit},"));
    }

    parts.push(")".to_string());
    parts.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_query() {
        let params = QueryParams::new()
            .icao24("485a32")
            .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

        let sql = build_history_query(&params);

        assert!(sql.contains("SELECT time, icao24"));
        assert!(sql.contains("FROM minio.osky.state_vectors_data4"));
        assert!(sql.contains("icao24 = '485a32'"));
        // Check for Unix timestamps (integers)
        assert!(sql.contains("time >= 1735725600"));  // 2025-01-01 10:00:00 UTC
        assert!(sql.contains("hour >= 1735725600"));
    }

    #[test]
    fn test_airport_query() {
        let params = QueryParams::new()
            .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59")
            .departure("EHAM")
            .arrival("EGLL");

        let sql = build_history_query(&params);

        assert!(sql.contains("JOIN"));
        assert!(sql.contains("flights_data4"));
        assert!(sql.contains("estdepartureairport = 'EHAM'"));
        assert!(sql.contains("estarrivalairport = 'EGLL'"));
    }

    #[test]
    fn test_wildcard_icao24() {
        let params = QueryParams::new()
            .icao24("485%")
            .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");

        let sql = build_history_query(&params);

        assert!(sql.contains("icao24 LIKE '485%'"));
    }

    #[test]
    fn test_hour_bounds_unix() {
        let (start, stop) = compute_hour_bounds_unix("2025-01-01 10:30:00", "2025-01-01 12:45:00");

        // 2025-01-01 10:00:00 UTC = 1735725600
        // 2025-01-01 13:00:00 UTC = 1735736400
        assert_eq!(start, 1735725600);
        assert_eq!(stop, 1735736400);
    }

    #[test]
    fn test_datetime_to_unix() {
        // 2024-11-08 10:00:00 UTC = 1731060000
        let ts = datetime_to_unix("2024-11-08 10:00:00");
        assert_eq!(ts, 1731060000);
    }

    #[test]
    fn test_query_preview() {
        let params = QueryParams::new()
            .icao24("485a32")
            .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00")
            .departure("EHAM");

        let preview = build_query_preview(&params);

        assert!(preview.contains("trino.history("));
        assert!(preview.contains("icao24=\"485a32\""));
        assert!(preview.contains("departure_airport=\"EHAM\""));
    }
}
