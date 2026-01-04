//! OpenSky CLI - Command-line interface for querying OpenSky Network flight data.

use clap::{Parser, Subcommand};
use chrono::{NaiveDateTime, Duration};
use opensky::{QueryParams, Trino};
use std::path::PathBuf;

/// Parse a duration string like "30m", "2h", "1d", "1w" into chrono::Duration.
/// Maximum allowed is 1 week.
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Err("Empty duration".to_string());
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: i64 = num_str.parse().map_err(|_| format!("Invalid number: {}", num_str))?;

    if num <= 0 {
        return Err("Duration must be positive".to_string());
    }

    let duration = match unit {
        "m" => Duration::minutes(num),
        "h" => Duration::hours(num),
        "d" => Duration::days(num),
        "w" => Duration::weeks(num),
        _ => return Err(format!("Unknown unit '{}'. Use m, h, d, or w", unit)),
    };

    // Max 1 week
    if duration > Duration::weeks(1) {
        return Err("Duration cannot exceed 1 week".to_string());
    }

    Ok(duration)
}

#[derive(Parser)]
#[command(name = "opensky")]
#[command(author, version, about = "Query OpenSky Network flight data", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Query historical flight data
    History {
        /// Start time (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        #[arg(short, long)]
        start: String,

        /// Stop time (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
        #[arg(short = 'e', long, conflicts_with = "duration")]
        stop: Option<String>,

        /// Duration from start (e.g., 30m, 2h, 1d, 1w). Max 1 week.
        #[arg(short = 'D', long, conflicts_with = "stop")]
        duration: Option<String>,

        /// Aircraft ICAO24 address (hex, e.g., 485a32)
        #[arg(short, long)]
        icao24: Option<String>,

        /// Flight callsign (e.g., KLM1234)
        #[arg(short, long)]
        callsign: Option<String>,

        /// Departure airport (ICAO code, e.g., EHAM)
        #[arg(short, long)]
        departure: Option<String>,

        /// Arrival airport (ICAO code, e.g., EGLL)
        #[arg(short, long)]
        arrival: Option<String>,

        /// Airport (departure or arrival)
        #[arg(long)]
        airport: Option<String>,

        /// Maximum number of rows
        #[arg(short, long)]
        limit: Option<u32>,

        /// Output file (CSV or Parquet based on extension)
        #[arg(short, long)]
        output: Option<PathBuf>,

        /// Show generated SQL query
        #[arg(long)]
        show_query: bool,
    },

    /// Configure OpenSky credentials
    Config {
        /// OpenSky username
        #[arg(short, long)]
        username: Option<String>,

        /// OpenSky password
        #[arg(short, long)]
        password: Option<String>,

        /// Show current configuration
        #[arg(long)]
        show: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::History {
            start,
            stop,
            duration,
            icao24,
            callsign,
            departure,
            arrival,
            airport,
            limit,
            output,
            show_query,
        } => {
            // Build query parameters
            let mut params = QueryParams::new();

            // Parse start time
            let start_str = if start.contains(' ') {
                start.clone()
            } else {
                format!("{} 00:00:00", start)
            };

            // Parse stop time (from --stop, --duration, or default to end of start day)
            let stop_str = if let Some(dur_str) = duration {
                // Calculate stop from start + duration
                let dur = parse_duration(&dur_str)?;
                let start_dt = NaiveDateTime::parse_from_str(&start_str, "%Y-%m-%d %H:%M:%S")
                    .map_err(|e| format!("Invalid start time: {}", e))?;
                let stop_dt = start_dt + dur;
                stop_dt.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                match stop {
                    Some(s) if s.contains(' ') => s,
                    Some(s) => format!("{} 23:59:59", s),
                    None => {
                        let date_part = start.split(' ').next().unwrap_or(&start);
                        format!("{} 23:59:59", date_part)
                    }
                }
            };

            params.start = Some(start_str);
            params.stop = Some(stop_str);

            params.icao24 = icao24;
            params.callsign = callsign;
            params.departure_airport = departure;
            params.arrival_airport = arrival;
            params.airport = airport;
            params.limit = limit;

            // Show query if requested
            if show_query {
                let preview = opensky::build_query_preview(&params);
                println!("Query:\n{}\n", preview);
            }

            // Execute query
            println!("Connecting to OpenSky Trino...");
            let mut trino = Trino::new().await?;

            println!("Executing query...");
            let data = trino.history(params).await?;

            let row_count = data.len();
            println!("Retrieved {} rows", row_count);

            if row_count == 0 {
                println!("No data found for the specified criteria.");
                return Ok(());
            }

            // Output results
            match output {
                Some(path) => {
                    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("csv");
                    let path_str = path.to_string_lossy();

                    match extension {
                        "parquet" => {
                            data.to_parquet(&path)?;
                            println!("Saved to {}", path.display());
                        }
                        "csv" | _ => {
                            data.to_csv(&path_str)?;
                            println!("Saved to {}", path.display());
                        }
                    }
                }
                None => {
                    // Print first few rows to stdout
                    println!("\n{}", data.dataframe().head(Some(10)));
                    if row_count > 10 {
                        println!("... ({} more rows)", row_count - 10);
                    }
                }
            }
        }

        Commands::Config {
            username,
            password,
            show,
        } => {
            if show {
                match opensky::Config::load() {
                    Ok(config) => {
                        println!("OpenSky Configuration:");
                        println!("  Username: {}", config.username.unwrap_or_default());
                        println!(
                            "  Password: {}",
                            if config.password.is_some() {
                                "********"
                            } else {
                                "(not set)"
                            }
                        );
                    }
                    Err(_) => {
                        println!("No configuration found. Use --username and --password to set.");
                    }
                }
                return Ok(());
            }

            if username.is_none() && password.is_none() {
                println!("Use --username and --password to set credentials, or --show to view.");
                return Ok(());
            }

            // Load existing or create new config
            let mut config = opensky::Config::load().unwrap_or_default();

            if let Some(u) = username {
                config.username = Some(u);
            }
            if let Some(p) = password {
                config.password = Some(p);
            }

            config.save()?;
            println!("Configuration saved.");
        }
    }

    Ok(())
}
