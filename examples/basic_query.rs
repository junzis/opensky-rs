//! Basic query example for opensky.
//!
//! Run with: cargo run --example basic_query
//!
//! Make sure you have credentials configured in ~/.config/opensky/settings.conf

use opensky::{QueryParams, Trino};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get parameters from command line or use defaults
    let args: Vec<String> = env::args().collect();

    let icao24 = args.get(1).map(|s| s.as_str()).unwrap_or("485a32");
    let start = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or("2024-11-08 10:00:00");
    let stop = args
        .get(3)
        .map(|s| s.as_str())
        .unwrap_or("2024-11-08 12:00:00");

    println!("OpenSky-RS Basic Query Example");
    println!("================================");
    println!("ICAO24: {}", icao24);
    println!("Start:  {}", start);
    println!("Stop:   {}", stop);
    println!();

    // Create Trino client
    println!("Connecting to OpenSky Trino...");
    let mut trino = Trino::new().await?;

    // Build query parameters
    let params = QueryParams::new()
        .icao24(icao24)
        .time_range(start, stop)
        .limit(1000);

    // Show the generated SQL
    println!("\nGenerated query preview:");
    println!("{}", opensky::build_query_preview(&params));
    println!();

    // Execute query with progress
    println!("Executing query...");
    let data = trino
        .history_with_progress(params, |status| {
            println!(
                "  State: {} | Progress: {:.1}% | Rows: {}",
                status.state, status.progress, status.row_count
            );
        })
        .await?;

    println!("\nQuery complete!");
    println!("Rows returned: {}", data.len());
    println!("Columns: {:?}", data.columns());

    // Show first few rows
    if !data.is_empty() {
        println!("\nFirst 5 rows:");
        let df = data.dataframe();
        println!("{}", df.head(Some(5)));

        // Export to CSV
        let filename = format!("flight_{}.csv", icao24);
        data.to_csv(&filename)?;
        println!("\nExported to: {}", filename);
    }

    Ok(())
}
