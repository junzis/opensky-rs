# opensky

[![Crates.io](https://img.shields.io/crates/v/opensky.svg)](https://crates.io/crates/opensky)
[![Documentation](https://docs.rs/opensky/badge.svg)](https://docs.rs/opensky)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Rust client for the [OpenSky Network](https://opensky-network.org/) Trino database, providing access to historical ADS-B flight trajectory data.

## Features

- Query historical flight state vectors from the OpenSky Trino database
- Filter by ICAO24 transponder code, callsign, time range, and geographic bounds
- Filter by departure/arrival airports
- Automatic OAuth2 authentication with token refresh
- Query result caching (Parquet format) for faster repeated queries
- Progress callbacks for long-running queries
- Export results to CSV or Parquet
- Built on [Polars](https://pola.rs/) DataFrames for efficient data handling
- **Command-line interface** for quick queries without writing code

## Installation

### As a Library

Add this to your `Cargo.toml`:

```toml
[dependencies]
opensky = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Command-Line Interface

Install the CLI tool with cargo:

```bash
cargo install opensky --features cli
```

Or build from source:

```bash
git clone https://github.com/junzis/opensky-rs
cd opensky-rs
cargo build --release --features cli
# Binary at target/release/opensky
```

## Configuration

Before using the library, you need to configure your OpenSky Network credentials.

1. Register for an account at [opensky-network.org](https://opensky-network.org/)
2. Create the configuration file:

| Platform | Config Path |
|----------|-------------|
| **Linux** | `~/.config/opensky/settings.conf` |
| **macOS** | `~/Library/Application Support/opensky/settings.conf` |
| **Windows** | `%LOCALAPPDATA%\opensky\settings.conf` |

```ini
[default]
username = your_username
password = your_password

[cache]
purge = 90 days
```

You can also configure credentials using the CLI:

```bash
opensky config --username your_username --password your_password
```

## CLI Usage

The `opensky` CLI provides quick access to flight data from the command line.

### Query Historical Data

```bash
# Query by ICAO24 address
opensky history --start 2025-01-01 --icao24 485a32

# Query a specific time range
opensky history --start "2025-01-01 10:00:00" --stop "2025-01-01 12:00:00" --icao24 485a32

# Query with duration (30m, 2h, 1d, 1w max)
opensky history --start "2025-01-01 10:00:00" --duration 2h --icao24 485a32

# Query by callsign
opensky history --start 2025-01-01 --callsign KLM1234

# Query by airport
opensky history --start 2025-01-01 --departure EHAM
opensky history --start 2025-01-01 --arrival EGLL
opensky history --start 2025-01-01 --departure EHAM --arrival EGLL

# Limit results
opensky history --start 2025-01-01 --icao24 485a32 --limit 1000
```

### Export Results

```bash
# Export to CSV
opensky history --start 2025-01-01 --icao24 485a32 --output flight.csv

# Export to Parquet
opensky history --start 2025-01-01 --icao24 485a32 --output flight.parquet
```

### Show Generated Query

```bash
opensky history --start 2025-01-01 --icao24 485a32 --show-query
```

### Manage Configuration

```bash
# Set credentials
opensky config --username myuser --password mypass

# View current configuration
opensky config --show
```

## Library Quick Start

```rust
use opensky::{Trino, QueryParams};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a Trino client (reads credentials from config file)
    let mut trino = Trino::new().await?;

    // Query flight history by ICAO24 address
    let params = QueryParams::new()
        .icao24("485a32")
        .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

    let data = trino.history(params).await?;
    println!("Got {} rows", data.len());

    // Export to CSV
    data.to_csv("flight.csv")?;

    Ok(())
}
```

## Library Usage Examples

### Query by Aircraft

```rust
let params = QueryParams::new()
    .icao24("485a32")                    // ICAO24 transponder code
    .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

let data = trino.history(params).await?;
```

### Query by Callsign

```rust
let params = QueryParams::new()
    .callsign("KLM1234")
    .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");
```

### Query by Airport

```rust
// Flights departing from Amsterdam Schiphol
let params = QueryParams::new()
    .departure("EHAM")
    .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");

// Flights arriving at London Heathrow
let params = QueryParams::new()
    .arrival("EGLL")
    .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");

// Flights departing from EHAM and arriving at EGLL
let params = QueryParams::new()
    .departure("EHAM")
    .arrival("EGLL")
    .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");
```

### Flight List (flights_data4)

Query the flight list table for departure/arrival information:

```rust
// Get all flights from EHAM to EGLL on a given day
let params = QueryParams::new()
    .departure("EHAM")
    .arrival("EGLL")
    .time_range("2025-01-01 00:00:00", "2025-01-01 23:59:59");

let flights = trino.flightlist(params).await?;

// Returns: icao24, callsign, firstseen, lastseen,
//          estdepartureairport, estarrivalairport, day
```

### Query by Geographic Bounds

```rust
let params = QueryParams::new()
    .bounds(-10.0, 35.0, 30.0, 60.0)     // west, south, east, north
    .time_range("2025-01-01 10:00:00", "2025-01-01 10:30:00")
    .limit(10000);
```

### Wildcard Queries

```rust
// All aircraft with ICAO24 starting with "485"
let params = QueryParams::new()
    .icao24("485%")
    .time_range("2025-01-01 10:00:00", "2025-01-01 11:00:00");
```

### Progress Tracking

```rust
let data = trino
    .history_with_progress(params, |status| {
        println!(
            "State: {} | Progress: {:.1}% | Rows: {}",
            status.state, status.progress, status.row_count
        );
    })
    .await?;
```

### Cache Control

```rust
// Use cache (default behavior)
let data = trino.history(params.clone()).await?;

// Force fresh query, bypass cache
let data = trino.history_cached(params, false).await?;

// Get cache statistics
let stats = opensky::cache_stats()?;
println!("Cache: {} files, {}", stats.file_count, stats.size_human());

// Clear all cached data
opensky::clear_cache()?;
```

### Working with Results

```rust
let data = trino.history(params).await?;

// Access the underlying Polars DataFrame
let df = data.dataframe();
println!("{}", df.head(Some(10)));

// Export to different formats
data.to_csv("output.csv")?;
data.to_parquet("output.parquet")?;

// Load from Parquet
let data = FlightData::from_parquet("output.parquet")?;
```

## Data Columns

Queries return the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `time` | i64 | Unix timestamp (seconds) |
| `icao24` | String | ICAO24 transponder address |
| `lat` | f64 | Latitude (degrees) |
| `lon` | f64 | Longitude (degrees) |
| `velocity` | f64 | Ground speed (m/s) |
| `heading` | f64 | Track angle (degrees, clockwise from north) |
| `vertrate` | f64 | Vertical rate (m/s) |
| `callsign` | String | Aircraft callsign |
| `onground` | bool | Whether aircraft is on ground |
| `squawk` | String | Transponder squawk code |
| `baroaltitude` | f64 | Barometric altitude (meters) |
| `geoaltitude` | f64 | Geometric altitude (meters) |
| `hour` | i64 | Hour partition (Unix timestamp) |

## Running the Example

```bash
cargo run --example basic_query

# With custom parameters
cargo run --example basic_query -- 485a32 "2024-11-08 10:00:00" "2024-11-08 12:00:00"
```

## Related Projects

- [pyopensky](https://github.com/open-aviation/pyopensky) - Python client for OpenSky Network
- [traffic](https://github.com/xoolive/traffic) - Air traffic data analysis library (Python)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This library provides access to data from the [OpenSky Network](https://opensky-network.org/), a non-profit association based in Switzerland that provides open air traffic data for research and non-commercial purposes.
