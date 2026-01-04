//! Core types for OpenSky queries and results.

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error types for OpenSky operations.
#[derive(Error, Debug)]
pub enum OpenSkyError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Query execution failed: {0}")]
    Query(String),

    #[error("Query was cancelled")]
    Cancelled,

    #[error("Invalid parameter: {0}")]
    InvalidParam(String),

    #[error("Data conversion error: {0}")]
    DataConversion(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parsing error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for OpenSky operations.
pub type Result<T> = std::result::Result<T, OpenSkyError>;

/// Geographic bounding box (west, south, east, north).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Bounds {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl Bounds {
    pub fn new(west: f64, south: f64, east: f64, north: f64) -> Self {
        Self { west, south, east, north }
    }
}

/// Parameters for querying flight history.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueryParams {
    /// Aircraft ICAO24 transponder code (hex string, e.g., "485a32")
    pub icao24: Option<String>,

    /// Query start time in UTC ("YYYY-MM-DD HH:MM:SS")
    pub start: Option<String>,

    /// Query end time in UTC ("YYYY-MM-DD HH:MM:SS")
    pub stop: Option<String>,

    /// Aircraft callsign
    pub callsign: Option<String>,

    /// Geographic bounding box
    pub bounds: Option<Bounds>,

    /// Departure airport ICAO code (e.g., "EHAM")
    pub departure_airport: Option<String>,

    /// Arrival airport ICAO code (e.g., "EGLL")
    pub arrival_airport: Option<String>,

    /// Airport (either departure or arrival)
    pub airport: Option<String>,

    /// Time buffer around flight (e.g., "1h", "30m")
    pub time_buffer: Option<String>,

    /// Maximum number of records to return
    pub limit: Option<u32>,
}

impl QueryParams {
    /// Create new empty query parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set ICAO24 filter.
    pub fn icao24(mut self, icao24: impl Into<String>) -> Self {
        self.icao24 = Some(icao24.into());
        self
    }

    /// Set time range.
    pub fn time_range(mut self, start: impl Into<String>, stop: impl Into<String>) -> Self {
        self.start = Some(start.into());
        self.stop = Some(stop.into());
        self
    }

    /// Set departure airport.
    pub fn departure(mut self, airport: impl Into<String>) -> Self {
        self.departure_airport = Some(airport.into());
        self
    }

    /// Set arrival airport.
    pub fn arrival(mut self, airport: impl Into<String>) -> Self {
        self.arrival_airport = Some(airport.into());
        self
    }

    /// Set result limit.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set geographic bounds.
    pub fn bounds(mut self, west: f64, south: f64, east: f64, north: f64) -> Self {
        self.bounds = Some(Bounds::new(west, south, east, north));
        self
    }

    /// Check if any query parameters are set.
    pub fn is_empty(&self) -> bool {
        self.icao24.is_none()
            && self.start.is_none()
            && self.stop.is_none()
            && self.callsign.is_none()
            && self.bounds.is_none()
            && self.departure_airport.is_none()
            && self.arrival_airport.is_none()
            && self.airport.is_none()
    }
}

/// Flight data columns returned by history queries (state vectors).
pub const FLIGHT_COLUMNS: &[&str] = &[
    "time",
    "icao24",
    "lat",
    "lon",
    "velocity",
    "heading",
    "vertrate",
    "callsign",
    "onground",
    "squawk",
    "baroaltitude",
    "geoaltitude",
    "hour",
];

/// Flight list columns returned by flightlist queries.
pub const FLIGHTLIST_COLUMNS: &[&str] = &[
    "icao24",
    "callsign",
    "firstseen",
    "lastseen",
    "estdepartureairport",
    "estarrivalairport",
    "day",
];

/// Default columns for raw data queries.
pub const RAWDATA_COLUMNS: &[&str] = &[
    "mintime",
    "rawmsg",
    "icao24",
];

/// Raw data table types available in OpenSky.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RawTable {
    /// Mode S rollcall replies (default)
    #[default]
    RollcallReplies,
    /// ACAS/TCAS data
    Acas,
    /// All-call replies
    AllcallReplies,
    /// Aircraft identification messages
    Identification,
    /// Operational status messages
    OperationalStatus,
    /// ADS-B position messages
    Position,
    /// ADS-B velocity messages
    Velocity,
}

impl RawTable {
    /// Get the SQL table name.
    pub fn table_name(&self) -> &'static str {
        match self {
            RawTable::RollcallReplies => "minio.osky.rollcall_replies_data4",
            RawTable::Acas => "minio.osky.acas_data4",
            RawTable::AllcallReplies => "minio.osky.allcall_replies_data4",
            RawTable::Identification => "minio.osky.identification_data4",
            RawTable::OperationalStatus => "minio.osky.operational_status_data4",
            RawTable::Position => "minio.osky.position_data4",
            RawTable::Velocity => "minio.osky.velocity_data4",
        }
    }
}

/// Wrapper around Polars DataFrame for flight data.
#[derive(Debug, Clone)]
pub struct FlightData {
    df: DataFrame,
}

impl FlightData {
    /// Create FlightData from a Polars DataFrame.
    pub fn new(df: DataFrame) -> Self {
        Self { df }
    }

    /// Get the underlying DataFrame.
    pub fn dataframe(&self) -> &DataFrame {
        &self.df
    }

    /// Get mutable reference to the underlying DataFrame.
    pub fn dataframe_mut(&mut self) -> &mut DataFrame {
        &mut self.df
    }

    /// Consume and return the underlying DataFrame.
    pub fn into_dataframe(self) -> DataFrame {
        self.df
    }

    /// Get the number of rows.
    pub fn len(&self) -> usize {
        self.df.height()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.df.height() == 0
    }

    /// Get column names.
    pub fn columns(&self) -> Vec<String> {
        self.df.get_column_names().iter().map(|s| s.to_string()).collect()
    }

    /// Export to CSV file.
    pub fn to_csv(&self, path: &str) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        CsvWriter::new(&mut file)
            .finish(&mut self.df.clone())
            .map_err(|e| OpenSkyError::DataConversion(e.to_string()))?;
        Ok(())
    }

    /// Export to Parquet file.
    pub fn to_parquet(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let mut file = std::fs::File::create(path)?;
        ParquetWriter::new(&mut file)
            .finish(&mut self.df.clone())
            .map_err(|e| OpenSkyError::DataConversion(e.to_string()))?;
        Ok(())
    }

    /// Load from Parquet file.
    pub fn from_parquet(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let file = std::fs::File::open(path)?;
        let df = ParquetReader::new(file)
            .finish()
            .map_err(|e| OpenSkyError::DataConversion(e.to_string()))?;
        Ok(Self { df })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_params_builder() {
        let params = QueryParams::new()
            .icao24("485a32")
            .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59")
            .departure("EHAM")
            .arrival("EGLL");

        assert_eq!(params.icao24, Some("485a32".to_string()));
        assert_eq!(params.departure_airport, Some("EHAM".to_string()));
        assert!(!params.is_empty());
    }

    #[test]
    fn test_query_params_empty() {
        let params = QueryParams::new();
        assert!(params.is_empty());
    }
}
