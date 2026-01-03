# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-03

### Added

- Initial release
- Trino client for querying OpenSky Network historical flight data
- Query parameters: ICAO24, callsign, time range, geographic bounds
- Airport filtering (departure, arrival, or both)
- Wildcard support for ICAO24 and callsign queries
- OAuth2 authentication with automatic token refresh
- Query result caching in Parquet format
- Progress callbacks for long-running queries
- Export to CSV and Parquet formats
- Built on Polars DataFrames
