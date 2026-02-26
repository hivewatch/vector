use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use http::Request;
use hyper::Body;
use tokio::time::{Duration, Instant};
use tracing::warn;

use crate::{
    event::{Event, Value},
    http::HttpClient,
    transforms::{SyncTransform, TransformOutputsBuf},
};

pub struct CacheEntry {
    pub thing_name: String,
    pub expires_at: Instant,
}

/// Resolves the AWS IoT thing name for a PACS door ID via GraphQL and routes the
/// event to the appropriate named output.
#[derive(Clone)]
pub struct GraphqlPacsRouter {
    pub http_client: HttpClient,
    pub endpoint: String,
    pub query: String,
    /// RFC 6901 JSON Pointer to the thing name in the GraphQL response.
    pub thing_name_pointer: String,
    /// Event field name that holds the door ID.
    pub door_id_field: String,
    /// IoT thing name of the local device (from `AWS_IOT_THING_NAME`).
    pub local_thing_name: String,
    pub timeout: Duration,
    pub cache_ttl: Duration,
    pub cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

impl GraphqlPacsRouter {
    async fn lookup_thing_name(&self, door_id: &str) -> crate::Result<String> {
        // Return cached result if still valid.
        {
            let cache = self.cache.lock().expect("cache lock poisoned");
            if let Some(entry) = cache.get(door_id) {
                if entry.expires_at > Instant::now() {
                    return Ok(entry.thing_name.clone());
                }
            }
        }

        // Build GraphQL POST request.
        let body = serde_json::json!({
            "query": self.query,
            "variables": { "doorId": door_id }
        });
        let body_bytes = Bytes::from(serde_json::to_vec(&body)?);
        let request = Request::post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(Body::from(body_bytes))?;

        // Send with timeout.
        let response = tokio::time::timeout(self.timeout, self.http_client.send(request))
            .await
            .map_err(|_| "GraphQL request timed out")?
            .map_err(crate::Error::from)?;

        if !response.status().is_success() {
            return Err(format!(
                "GraphQL API returned non-success status {}",
                response.status()
            )
            .into());
        }

        // Read and parse response body.
        let body_bytes = http_body::Body::collect(response.into_body())
            .await?
            .to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes)?;

        let thing_name = json
            .pointer(&self.thing_name_pointer)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!(
                    "thing name not found at JSON pointer '{}' in GraphQL response",
                    self.thing_name_pointer
                )
            })?
            .to_string();

        // Populate cache.
        {
            let mut cache = self.cache.lock().expect("cache lock poisoned");
            cache.insert(
                door_id.to_string(),
                CacheEntry {
                    thing_name: thing_name.clone(),
                    expires_at: Instant::now() + self.cache_ttl,
                },
            );
        }

        Ok(thing_name)
    }
}

impl SyncTransform for GraphqlPacsRouter {
    fn transform(&mut self, mut event: Event, output: &mut TransformOutputsBuf) {
        // Extract door_id in a scoped block so the immutable borrow of `event` ends
        // before we need to mutably borrow it to insert the routing destination field.
        let door_id = {
            let Event::Log(ref log) = event else {
                warn!(message = "graphql_pacs_router only handles log events, dropping.");
                return;
            };

            match log.get(self.door_id_field.as_str()) {
                Some(Value::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
                Some(other) => match other.as_str() {
                    Some(s) => s.into_owned(),
                    None => {
                        warn!(
                            message = "door_id field is not a string value, dropping event.",
                            field = %self.door_id_field
                        );
                        return;
                    }
                },
                None => {
                    warn!(
                        message = "Event missing door_id field, dropping.",
                        field = %self.door_id_field
                    );
                    return;
                }
            }
        };

        // Run the async GraphQL lookup synchronously within the running Tokio runtime.
        // `block_in_place` moves the current task off the async scheduler thread, allowing
        // `block_on` to drive async work without blocking the scheduler.
        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.lookup_thing_name(&door_id))
        });

        match result {
            Ok(thing_name) => {
                let port = if thing_name == self.local_thing_name {
                    "local"
                } else {
                    "remote"
                };

                // Annotate the event with the routing decision before forwarding.
                if let Event::Log(ref mut log) = event {
                    log.insert("routingDestination", port);
                }

                output.push(Some(port), event);
            }
            Err(err) => {
                warn!(
                    message = "GraphQL lookup failed, dropping event.",
                    door_id = %door_id,
                    error = %err
                );
            }
        }
    }
}
