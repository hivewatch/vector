# PACS Event Pipeline

This document describes the custom Vector pipeline for routing Physical Access Control System (PACS) door events to InfluxDB or MQTT based on AWS IoT thing assignment.

## Overview

```
PACS JSON source
      │
      ▼
graphql_pacs_router  ──── (GraphQL lookup: door ID → IoT thing name) ────┐
      │                                                                    │
      ├─ .local  (thing name == AWS_IOT_THING_NAME)                       │
      │       └──► InfluxDB sink                                          │
      │                                                                    │
      └─ .remote (thing name != AWS_IOT_THING_NAME)                      │
              └──► MQTT sink                                              │
                                                                          │
              [GraphQL errors → event dropped]  ◄────────────────────────┘
```

Each PACS event carries a `door_id`. The transform queries a GraphQL API to find which AWS IoT thing is responsible for that door. If the returned thing name matches the current device (`AWS_IOT_THING_NAME`), the event is stored locally in InfluxDB; otherwise it is forwarded over MQTT to the correct device.

---

## Custom Transform: `graphql_pacs_router`

**Source files:**
- [`src/transforms/graphql_pacs_router/mod.rs`](src/transforms/graphql_pacs_router/mod.rs)
- [`src/transforms/graphql_pacs_router/config.rs`](src/transforms/graphql_pacs_router/config.rs)
- [`src/transforms/graphql_pacs_router/transform.rs`](src/transforms/graphql_pacs_router/transform.rs)

**Feature flag:** `transforms-graphql_pacs_router` (included in `transforms-logs`)

### What it does per event

1. Reads the `door_id` field (configurable) from the incoming log event.
2. Checks an in-process cache (keyed by `door_id`, TTL-based) for a previously resolved thing name.
3. On cache miss: POSTs a GraphQL query to the configured endpoint with `$doorId` as a variable.
4. Extracts the IoT thing name from the response using a JSON Pointer (RFC 6901).
5. Compares the thing name to `AWS_IOT_THING_NAME` (read from the environment at startup).
6. Inserts a `routingDestination` field (`"local"` or `"remote"`) into the event.
7. Routes the event to the matching named output port.
8. Drops the event (with a warning log) if the GraphQL lookup fails for any reason.

### Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | **required** | GraphQL API URL |
| `query` | string | **required** | GraphQL query accepting `$doorId: String!` |
| `thing_name_pointer` | string | `/data/door/thingName` | RFC 6901 JSON Pointer into the GraphQL response |
| `door_id_field` | string | `door_id` | Event field holding the door ID |
| `timeout_secs` | integer | `5` | Per-request HTTP timeout |
| `cache_ttl_secs` | integer | `60` | How long to cache door ID → thing name mappings |
| `proxy` | object | (global proxy) | Optional HTTP proxy override |

**Required environment variable:** `AWS_IOT_THING_NAME` — the IoT thing name of the device running this Vector instance. Vector will refuse to start if this is not set.

### Named outputs

| Output port | Condition | Intended sink |
|-------------|-----------|---------------|
| `.local` | thing name == `AWS_IOT_THING_NAME` | InfluxDB |
| `.remote` | thing name != `AWS_IOT_THING_NAME` | MQTT |

### Added event field

The transform inserts `routingDestination` (`"local"` or `"remote"`) into every event before forwarding. This is available to downstream transforms or for observability.

---

## Example Vector Configuration

```toml
# ── Source ────────────────────────────────────────────────────────────────────
[sources.pacs_events]
type = "kafka"                          # or any log source
# ...

# ── Transform ─────────────────────────────────────────────────────────────────
[transforms.pacs_router]
type    = "graphql_pacs_router"
inputs  = ["pacs_events"]

endpoint = "https://api.example.com/graphql"

query = """
  query GetDoor($doorId: String!) {
    door(id: $doorId) {
      thingName
    }
  }
"""

# JSON Pointer into the GraphQL response
thing_name_pointer = "/data/door/thingName"

# Field in the PACS event that holds the door identifier
door_id_field = "door_id"

# HTTP + cache tuning
timeout_secs    = 5
cache_ttl_secs  = 60

# ── Sinks ─────────────────────────────────────────────────────────────────────
[sinks.influxdb]
type   = "influxdb_logs"
inputs = ["pacs_router.local"]
# ... InfluxDB connection config

[sinks.mqtt]
type   = "mqtt"
inputs = ["pacs_router.remote"]
# ... MQTT broker config
```

### Expected PACS event shape (minimal)

```json
{
  "door_id": "door-abc-123",
  "timestamp": "2026-02-26T10:00:00Z",
  "event_type": "access_granted",
  "badge_id": "badge-xyz"
}
```

After routing, the event will have an additional field:

```json
{
  "door_id": "door-abc-123",
  "timestamp": "2026-02-26T10:00:00Z",
  "event_type": "access_granted",
  "badge_id": "badge-xyz",
  "routingDestination": "local"
}
```

### GraphQL response shape (minimal)

```json
{
  "data": {
    "door": {
      "thingName": "my-iot-gateway-001"
    }
  }
}
```

---

## Implementation Notes

### Async HTTP inside a synchronous transform

The transform uses `SyncTransform` (required for multi-output routing). To make HTTP calls from a synchronous context within Vector's Tokio runtime:

```rust
tokio::task::block_in_place(|| {
    tokio::runtime::Handle::current().block_on(self.lookup_thing_name(&door_id))
});
```

`block_in_place` moves the current task off the async worker thread so that `block_on` can drive async work without blocking the scheduler. This is safe in Vector's multi-threaded Tokio runtime but means one worker thread is occupied per in-flight GraphQL request. The TTL cache is the primary mitigation — most events for well-known doors will be served from cache.

### Cache design

- **Structure:** `Arc<Mutex<HashMap<String, CacheEntry>>>` shared across cloned transform instances.
- **Invalidation:** time-based TTL (`cache_ttl_secs`), checked on every read.
- **No background eviction:** stale entries are only evicted on the next access for the same door ID. For deployments with very large numbers of distinct door IDs, memory growth is bounded by the number of unique doors seen within the TTL window.

### Error handling

All GraphQL errors (network failures, timeouts, non-2xx responses, missing JSON pointer) cause the event to be **dropped** with a `warn!` log line. No retry is attempted. The in-memory cache is unaffected by failed lookups.