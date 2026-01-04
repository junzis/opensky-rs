//! Query result caching for OpenSky data.
//!
//! Caches query results as Parquet files in `~/.cache/opensky/`.
//! Cache keys are derived from query parameters using a hash.

use crate::types::{FlightData, QueryParams, OpenSkyError};
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

/// Default cache directory name.
const CACHE_DIR_NAME: &str = "opensky";

/// Get the cache directory path.
pub fn cache_dir() -> Option<PathBuf> {
    dirs::cache_dir().map(|d| d.join(CACHE_DIR_NAME))
}

/// Ensure the cache directory exists.
pub fn ensure_cache_dir() -> Result<PathBuf, OpenSkyError> {
    let dir = cache_dir().ok_or_else(|| {
        OpenSkyError::Config("Could not determine cache directory".to_string())
    })?;

    if !dir.exists() {
        fs::create_dir_all(&dir).map_err(|e| {
            OpenSkyError::Config(format!("Failed to create cache directory: {}", e))
        })?;
    }

    Ok(dir)
}

/// Generate a cache key (filename) from query parameters.
pub fn cache_key(params: &QueryParams) -> String {
    let mut hasher = DefaultHasher::new();

    // Hash all relevant parameters
    params.icao24.hash(&mut hasher);
    params.start.hash(&mut hasher);
    params.stop.hash(&mut hasher);
    params.callsign.hash(&mut hasher);
    params.departure_airport.hash(&mut hasher);
    params.arrival_airport.hash(&mut hasher);
    params.airport.hash(&mut hasher);
    params.limit.hash(&mut hasher);

    if let Some(bounds) = &params.bounds {
        // Hash bounds using their bit representation (f64 doesn't impl Hash)
        bounds.west.to_bits().hash(&mut hasher);
        bounds.south.to_bits().hash(&mut hasher);
        bounds.east.to_bits().hash(&mut hasher);
        bounds.north.to_bits().hash(&mut hasher);
    }

    let hash = hasher.finish();
    format!("{:016x}.parquet", hash)
}

/// Get the full cache file path for a query.
pub fn cache_path(params: &QueryParams) -> Option<PathBuf> {
    cache_dir().map(|d| d.join(cache_key(params)))
}

/// Check if a cached result exists and is not expired.
pub fn get_cached(params: &QueryParams, max_age: Option<Duration>) -> Option<FlightData> {
    let path = cache_path(params)?;

    if !path.exists() {
        return None;
    }

    // Check age if max_age specified
    if let Some(max_age) = max_age {
        if let Ok(metadata) = fs::metadata(&path) {
            if let Ok(modified) = metadata.modified() {
                if let Ok(age) = SystemTime::now().duration_since(modified) {
                    if age > max_age {
                        // Cache expired, remove it
                        let _ = fs::remove_file(&path);
                        return None;
                    }
                }
            }
        }
    }

    // Try to load the cached data
    FlightData::from_parquet(&path).ok()
}

/// Save query results to cache.
pub fn save_to_cache(params: &QueryParams, data: &FlightData) -> Result<PathBuf, OpenSkyError> {
    let dir = ensure_cache_dir()?;
    let path = dir.join(cache_key(params));

    data.to_parquet(&path)?;

    Ok(path)
}

/// Remove a specific cache entry.
pub fn remove_cached(params: &QueryParams) -> Result<(), OpenSkyError> {
    if let Some(path) = cache_path(params) {
        if path.exists() {
            fs::remove_file(&path).map_err(|e| {
                OpenSkyError::Config(format!("Failed to remove cache file: {}", e))
            })?;
        }
    }
    Ok(())
}

/// Clear all cached data.
pub fn clear_cache() -> Result<usize, OpenSkyError> {
    let dir = match cache_dir() {
        Some(d) if d.exists() => d,
        _ => return Ok(0),
    };

    let mut count = 0;
    for entry in fs::read_dir(&dir).map_err(|e| {
        OpenSkyError::Config(format!("Failed to read cache directory: {}", e))
    })? {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                if fs::remove_file(&path).is_ok() {
                    count += 1;
                }
            }
        }
    }

    Ok(count)
}

/// Purge cache entries older than the specified duration.
pub fn purge_old_cache(max_age: Duration) -> Result<usize, OpenSkyError> {
    let dir = match cache_dir() {
        Some(d) if d.exists() => d,
        _ => return Ok(0),
    };

    let mut count = 0;
    let now = SystemTime::now();

    for entry in fs::read_dir(&dir).map_err(|e| {
        OpenSkyError::Config(format!("Failed to read cache directory: {}", e))
    })? {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                if let Ok(metadata) = fs::metadata(&path) {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if fs::remove_file(&path).is_ok() {
                                    count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(count)
}

/// Get cache statistics.
pub fn cache_stats() -> Result<CacheStats, OpenSkyError> {
    let dir = match cache_dir() {
        Some(d) => d,
        None => return Ok(CacheStats::default()),
    };

    if !dir.exists() {
        return Ok(CacheStats::default());
    }

    let mut stats = CacheStats {
        directory: dir.clone(),
        ..Default::default()
    };

    for entry in fs::read_dir(&dir).map_err(|e| {
        OpenSkyError::Config(format!("Failed to read cache directory: {}", e))
    })? {
        if let Ok(entry) = entry {
            let path = entry.path();
            if path.extension().map_or(false, |e| e == "parquet") {
                stats.file_count += 1;
                if let Ok(metadata) = fs::metadata(&path) {
                    stats.total_size += metadata.len();
                }
            }
        }
    }

    Ok(stats)
}

/// Cache statistics.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub directory: PathBuf,
    pub file_count: usize,
    pub total_size: u64,
}

impl CacheStats {
    /// Get total size as a human-readable string.
    pub fn size_human(&self) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;

        if self.total_size >= GB {
            format!("{:.2} GB", self.total_size as f64 / GB as f64)
        } else if self.total_size >= MB {
            format!("{:.2} MB", self.total_size as f64 / MB as f64)
        } else if self.total_size >= KB {
            format!("{:.2} KB", self.total_size as f64 / KB as f64)
        } else {
            format!("{} B", self.total_size)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_deterministic() {
        let params = QueryParams::new()
            .icao24("485a32")
            .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

        let key1 = cache_key(&params);
        let key2 = cache_key(&params);

        assert_eq!(key1, key2);
        assert!(key1.ends_with(".parquet"));
    }

    #[test]
    fn test_cache_key_different_params() {
        let params1 = QueryParams::new()
            .icao24("485a32")
            .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

        let params2 = QueryParams::new()
            .icao24("485a33")
            .time_range("2025-01-01 10:00:00", "2025-01-01 12:00:00");

        let key1 = cache_key(&params1);
        let key2 = cache_key(&params2);

        assert_ne!(key1, key2);
    }
}
