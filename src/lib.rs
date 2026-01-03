//! # opensky-rs
//!
//! A Rust client for the OpenSky Network Trino database.
//!
//! This crate provides access to historical flight data from the OpenSky Network,
//! allowing you to query ADS-B trajectory data for aircraft worldwide.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use opensky_rs::{Trino, QueryParams};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a Trino client (reads credentials from ~/.config/opensky/settings.conf)
//!     let mut trino = Trino::new().await?;
//!
//!     // Query flight history
//!     let params = QueryParams::new()
//!         .icao24("485a32")
//!         .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");
//!
//!     let data = trino.history(params).await?;
//!     println!("Got {} rows", data.len());
//!
//!     // Export to CSV
//!     data.to_csv("flight.csv")?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Configuration
//!
//! Credentials are read from `~/.config/opensky/settings.conf`:
//!
//! ```ini
//! [default]
//! username = your_username
//! password = your_password
//! ```
//!
//! Register for an account at <https://opensky-network.org/>.

pub mod cache;
pub mod config;
pub mod query;
pub mod trino;
pub mod types;

// Re-export main types for convenience
pub use cache::{cache_dir, cache_stats, clear_cache, purge_old_cache, CacheStats};
pub use config::Config;
pub use query::{build_history_query, build_query_preview};
pub use trino::{QueryStatus, Trino};
pub use types::{Bounds, FlightData, OpenSkyError, QueryParams, Result, FLIGHT_COLUMNS};
