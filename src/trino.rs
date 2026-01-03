//! Trino HTTP client for OpenSky database.

use crate::cache;
use crate::config::Config;
use crate::query::build_history_query;
use crate::types::{FlightData, OpenSkyError, QueryParams, Result, FLIGHT_COLUMNS};

use polars::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// OpenSky authentication endpoint.
const AUTH_URL: &str = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token";

/// Trino query endpoint.
const TRINO_URL: &str = "https://trino.opensky-network.org/v1/statement";

/// Trino client for OpenSky database queries.
pub struct Trino {
    client: Client,
    config: Config,
    token: Option<TokenInfo>,
}

#[derive(Debug, Clone)]
struct TokenInfo {
    access_token: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

/// OAuth token response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Trino query response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoResponse {
    id: Option<String>,
    info_uri: Option<String>,
    next_uri: Option<String>,
    columns: Option<Vec<TrinoColumn>>,
    data: Option<Vec<Vec<serde_json::Value>>>,
    stats: Option<TrinoStats>,
    error: Option<TrinoError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoColumn {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoStats {
    state: String,
    progress_percentage: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrinoError {
    message: String,
    error_name: Option<String>,
}

/// Query execution status.
#[derive(Debug, Clone, Serialize)]
pub struct QueryStatus {
    pub query_id: Option<String>,
    pub state: String,
    pub progress: f64,
    pub row_count: usize,
}

impl Trino {
    /// Create a new Trino client, loading config from the default location.
    pub async fn new() -> Result<Self> {
        let config = Config::load()?;
        Self::with_config(config).await
    }

    /// Create a new Trino client with the given config.
    pub async fn with_config(config: Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent("opensky/0.1.0")
            .build()?;

        Ok(Self {
            client,
            config,
            token: None,
        })
    }

    /// Get or refresh the authentication token.
    async fn get_token(&mut self) -> Result<String> {
        // Check if we have a valid token
        if let Some(ref token) = self.token {
            let now = chrono::Utc::now();
            // Use token if it's still valid (with 1 minute margin)
            if token.expires_at > now + chrono::Duration::minutes(1) {
                return Ok(token.access_token.clone());
            }
        }

        // Request new token with retry
        let username = self.config.require_username()?;
        let password = self.config.require_password()?;

        let mut last_error = None;
        for attempt in 1..=3 {
            // Small delay between retries
            if attempt > 1 {
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }

            let result = self
                .client
                .post(AUTH_URL)
                .form(&[
                    ("client_id", "trino-client"),
                    ("grant_type", "password"),
                    ("username", username),
                    ("password", password),
                ])
                .send()
                .await;

            match result {
                Ok(response) => {
                    if response.status() == 401 || response.status() == 400 {
                        return Err(OpenSkyError::Auth(
                            "Authentication failed. Check your username and password.".into(),
                        ));
                    }

                    response.error_for_status_ref()?;

                    let token_response: TokenResponse = response.json().await?;
                    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(token_response.expires_in as i64);

                    self.token = Some(TokenInfo {
                        access_token: token_response.access_token.clone(),
                        expires_at,
                    });

                    return Ok(token_response.access_token);
                }
                Err(e) => {
                    last_error = Some(e);
                    // Continue to retry
                }
            }
        }

        // All retries failed
        Err(last_error.unwrap().into())
    }

    /// Execute the history query and return flight data.
    pub async fn history(&mut self, params: QueryParams) -> Result<FlightData> {
        self.history_cached(params, true).await
    }

    /// Execute history query with caching control.
    ///
    /// - `cached=true`: Use cache if available, otherwise query and cache result
    /// - `cached=false`: Force fresh query, bypass and clear existing cache
    pub async fn history_cached(&mut self, params: QueryParams, cached: bool) -> Result<FlightData> {
        // Check cache first
        if cached {
            if let Some(data) = cache::get_cached(&params, None) {
                return Ok(data);
            }
        } else {
            // Clear existing cache for this query
            let _ = cache::remove_cached(&params);
        }

        // Execute query
        let sql = build_history_query(&params);
        let data = self.execute_query(&sql).await?;

        // Cache the result if we got data
        if !data.is_empty() {
            let _ = cache::save_to_cache(&params, &data);
        }

        Ok(data)
    }

    /// Execute a raw SQL query.
    pub async fn execute_query(&mut self, sql: &str) -> Result<FlightData> {
        let token = self.get_token().await?;
        let username = self.config.username.as_deref().unwrap_or("opensky");

        // Initial query submission
        let response = self
            .client
            .post(TRINO_URL)
            .header("Authorization", format!("Bearer {}", token))
            .header("X-Trino-User", username)
            .header("X-Trino-Source", "opensky")
            .header("X-Trino-Catalog", "minio")
            .header("X-Trino-Schema", "osky")
            .body(sql.to_string())
            .send()
            .await?;

        response.error_for_status_ref()?;

        let mut trino_response: TrinoResponse = response.json().await?;

        // Check for immediate errors
        if let Some(error) = &trino_response.error {
            return Err(OpenSkyError::Query(error.message.clone()));
        }

        // Collect all data by polling nextUri
        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut columns: Option<Vec<TrinoColumn>> = trino_response.columns;

        // Collect data from first response
        if let Some(data) = trino_response.data {
            all_rows.extend(data);
        }

        // Poll for more results
        while let Some(next_uri) = trino_response.next_uri {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let response = self
                .client
                .get(&next_uri)
                .header("Authorization", format!("Bearer {}", token))
                .header("X-Trino-User", username)
                .send()
                .await?;

            response.error_for_status_ref()?;
            trino_response = response.json().await?;

            if let Some(error) = &trino_response.error {
                return Err(OpenSkyError::Query(error.message.clone()));
            }

            // Update columns if we get them
            if columns.is_none() {
                columns = trino_response.columns;
            }

            if let Some(data) = trino_response.data {
                all_rows.extend(data);
            }
        }

        // Convert to DataFrame
        let df = self.rows_to_dataframe(&columns.unwrap_or_default(), all_rows)?;
        Ok(FlightData::new(df))
    }

    /// Execute query with progress callback.
    pub async fn history_with_progress<F>(
        &mut self,
        params: QueryParams,
        progress_callback: F,
    ) -> Result<FlightData>
    where
        F: FnMut(QueryStatus),
    {
        self.history_with_progress_cached(params, true, progress_callback).await
    }

    /// Execute query with progress callback and caching control.
    pub async fn history_with_progress_cached<F>(
        &mut self,
        params: QueryParams,
        cached: bool,
        mut progress_callback: F,
    ) -> Result<FlightData>
    where
        F: FnMut(QueryStatus),
    {
        // Check cache first
        if cached {
            if let Some(data) = cache::get_cached(&params, None) {
                // Report cached status
                progress_callback(QueryStatus {
                    query_id: None,
                    state: "CACHED".to_string(),
                    progress: 100.0,
                    row_count: data.len(),
                });
                return Ok(data);
            }
        } else {
            // Clear existing cache for this query
            let _ = cache::remove_cached(&params);
        }

        let sql = build_history_query(&params);
        let token = self.get_token().await?;
        let username = self.config.username.as_deref().unwrap_or("opensky");

        // Initial query submission
        let response = self
            .client
            .post(TRINO_URL)
            .header("Authorization", format!("Bearer {}", token))
            .header("X-Trino-User", username)
            .header("X-Trino-Source", "opensky")
            .header("X-Trino-Catalog", "minio")
            .header("X-Trino-Schema", "osky")
            .body(sql.to_string())
            .send()
            .await?;

        response.error_for_status_ref()?;

        let mut trino_response: TrinoResponse = response.json().await?;
        let query_id = trino_response.id.clone();

        if let Some(error) = &trino_response.error {
            return Err(OpenSkyError::Query(error.message.clone()));
        }

        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut columns: Option<Vec<TrinoColumn>> = trino_response.columns;

        if let Some(data) = trino_response.data {
            all_rows.extend(data);
        }

        // Report initial status
        let status = QueryStatus {
            query_id: query_id.clone(),
            state: trino_response
                .stats
                .as_ref()
                .map(|s| s.state.clone())
                .unwrap_or_else(|| "RUNNING".to_string()),
            progress: trino_response
                .stats
                .as_ref()
                .and_then(|s| s.progress_percentage)
                .unwrap_or(0.0),
            row_count: all_rows.len(),
        };
        progress_callback(status);

        while let Some(next_uri) = trino_response.next_uri {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let response = self
                .client
                .get(&next_uri)
                .header("Authorization", format!("Bearer {}", token))
                .header("X-Trino-User", username)
                .send()
                .await?;

            response.error_for_status_ref()?;
            trino_response = response.json().await?;

            if let Some(error) = &trino_response.error {
                return Err(OpenSkyError::Query(error.message.clone()));
            }

            if columns.is_none() {
                columns = trino_response.columns;
            }

            if let Some(data) = trino_response.data {
                all_rows.extend(data);
            }

            // Report progress
            let status = QueryStatus {
                query_id: query_id.clone(),
                state: trino_response
                    .stats
                    .as_ref()
                    .map(|s| s.state.clone())
                    .unwrap_or_else(|| "RUNNING".to_string()),
                progress: trino_response
                    .stats
                    .as_ref()
                    .and_then(|s| s.progress_percentage)
                    .unwrap_or(0.0),
                row_count: all_rows.len(),
            };
            progress_callback(status);
        }

        let df = self.rows_to_dataframe(&columns.unwrap_or_default(), all_rows)?;
        let data = FlightData::new(df);

        // Cache the result if we got data
        if !data.is_empty() {
            let _ = cache::save_to_cache(&params, &data);
        }

        Ok(data)
    }

    /// Cancel a running query.
    pub async fn cancel(&mut self, query_id: &str) -> Result<()> {
        let token = self.get_token().await?;
        let username = self.config.username.as_deref().unwrap_or("opensky");

        let url = format!("https://trino.opensky-network.org/v1/query/{}", query_id);

        let response = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("X-Trino-User", username)
            .send()
            .await?;

        if response.status().is_success() || response.status() == 204 {
            Ok(())
        } else {
            Err(OpenSkyError::Query(format!(
                "Failed to cancel query: {}",
                response.status()
            )))
        }
    }

    /// Convert Trino rows to a Polars DataFrame.
    fn rows_to_dataframe(
        &self,
        columns: &[TrinoColumn],
        rows: Vec<Vec<serde_json::Value>>,
    ) -> Result<DataFrame> {
        if rows.is_empty() {
            // Return empty DataFrame with correct columns
            let series: Vec<Column> = FLIGHT_COLUMNS
                .iter()
                .map(|name| Column::new((*name).into(), Vec::<String>::new()))
                .collect();
            return DataFrame::new(series)
                .map_err(|e| OpenSkyError::DataConversion(e.to_string()));
        }

        // Build series for each column
        let mut series_vec: Vec<Column> = Vec::new();

        for (col_idx, col) in columns.iter().enumerate() {
            let values: Vec<Option<&serde_json::Value>> = rows
                .iter()
                .map(|row| row.get(col_idx))
                .collect();

            let series = match col.col_type.as_str() {
                "double" | "real" => {
                    let data: Vec<Option<f64>> = values
                        .iter()
                        .map(|v| v.and_then(|x| x.as_f64()))
                        .collect();
                    Column::new(col.name.clone().into(), data)
                }
                "bigint" | "integer" => {
                    let data: Vec<Option<i64>> = values
                        .iter()
                        .map(|v| v.and_then(|x| x.as_i64()))
                        .collect();
                    Column::new(col.name.clone().into(), data)
                }
                "boolean" => {
                    let data: Vec<Option<bool>> = values
                        .iter()
                        .map(|v| v.and_then(|x| x.as_bool()))
                        .collect();
                    Column::new(col.name.clone().into(), data)
                }
                _ => {
                    // Default to string for varchar, timestamp, etc.
                    let data: Vec<Option<String>> = values
                        .iter()
                        .map(|v| {
                            v.and_then(|x| {
                                if x.is_string() {
                                    x.as_str().map(|s| s.to_string())
                                } else if x.is_null() {
                                    None
                                } else {
                                    Some(x.to_string())
                                }
                            })
                        })
                        .collect();
                    Column::new(col.name.clone().into(), data)
                }
            };

            series_vec.push(series);
        }

        DataFrame::new(series_vec).map_err(|e| OpenSkyError::DataConversion(e.to_string()))
    }

    /// Get the current query ID (if a query is running).
    pub fn current_query_id(&self) -> Option<&str> {
        // This would need state tracking for async queries
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_info() {
        let token = TokenInfo {
            access_token: "test".to_string(),
            expires_at: chrono::Utc::now() + chrono::Duration::hours(1),
        };
        assert!(!token.access_token.is_empty());
    }
}
