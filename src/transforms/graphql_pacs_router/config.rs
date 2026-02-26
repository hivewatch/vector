use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::time::Duration;
use vector_lib::{
    config::{LogNamespace, clone_input_definitions},
    configurable::configurable_component,
};

use crate::{
    config::{
        DataType, GenerateConfig, Input, OutputId, ProxyConfig, TransformConfig, TransformContext,
        TransformOutput,
    },
    http::HttpClient,
    schema,
    transforms::Transform,
};

use super::transform::GraphqlPacsRouter;

fn default_thing_name_pointer() -> String {
    "/data/door/thingName".to_string()
}

fn default_door_id_field() -> String {
    "door_id".to_string()
}

const fn default_timeout_secs() -> u64 {
    5
}

const fn default_cache_ttl_secs() -> u64 {
    60
}

/// Configuration for the `graphql_pacs_router` transform.
///
/// Routes PACS door access events to either a local (InfluxDB) or remote (MQTT) output
/// based on the AWS IoT thing name returned by a GraphQL API for the event's door ID.
///
/// The current device's IoT thing name is read from the `AWS_IOT_THING_NAME` environment
/// variable at startup. Events whose door resolves to a matching thing name are routed to
/// the `local` output; all others are routed to the `remote` output. Events are dropped
/// when the GraphQL lookup fails.
#[configurable_component(transform(
    "graphql_pacs_router",
    "Route PACS door events via GraphQL lookup to local (InfluxDB) or remote (MQTT) output."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct GraphqlPacsRouterConfig {
    /// The GraphQL API endpoint URL.
    #[configurable(metadata(docs::examples = "https://api.example.com/graphql"))]
    pub endpoint: String,

    /// The GraphQL query to execute per event.
    ///
    /// Must accept a `$doorId` variable (String) and return the AWS IoT thing name.
    ///
    /// Example:
    /// ```graphql
    /// query GetDoor($doorId: String!) {
    ///   door(id: $doorId) { thingName }
    /// }
    /// ```
    pub query: String,

    /// RFC 6901 JSON Pointer path into the GraphQL response to extract the IoT thing name.
    ///
    /// For example, if the response is `{"data": {"door": {"thingName": "device-001"}}}`,
    /// the pointer would be `/data/door/thingName`.
    #[serde(default = "default_thing_name_pointer")]
    #[configurable(metadata(docs::examples = "/data/door/thingName"))]
    pub thing_name_pointer: String,

    /// The event field containing the door ID.
    ///
    /// This field is read from each incoming log event and used as the `$doorId`
    /// variable in the GraphQL query.
    #[serde(default = "default_door_id_field")]
    #[configurable(metadata(docs::examples = "door_id"))]
    pub door_id_field: String,

    /// HTTP request timeout for GraphQL queries, in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Cache TTL for door ID to IoT thing name mappings, in seconds.
    ///
    /// Repeated events for the same door ID within this window reuse the cached
    /// thing name and do not issue additional GraphQL requests.
    #[serde(default = "default_cache_ttl_secs")]
    pub cache_ttl_secs: u64,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "crate::serde::is_default")]
    pub proxy: ProxyConfig,
}

impl GenerateConfig for GraphqlPacsRouterConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"endpoint = "https://api.example.com/graphql"
query = "query GetDoor($doorId: String!) { door(id: $doorId) { thingName } }"
"#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "graphql_pacs_router")]
impl TransformConfig for GraphqlPacsRouterConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        let local_thing_name = std::env::var("AWS_IOT_THING_NAME").map_err(|_| {
            "AWS_IOT_THING_NAME environment variable is not set; \
             graphql_pacs_router requires this variable to identify the local IoT device"
        })?;

        let proxy = ProxyConfig::merge_with_env(&context.globals.proxy, &self.proxy);
        let http_client = HttpClient::new(None, &proxy)?;

        Ok(Transform::synchronous(GraphqlPacsRouter {
            http_client,
            endpoint: self.endpoint.clone(),
            query: self.query.clone(),
            thing_name_pointer: self.thing_name_pointer.clone(),
            door_id_field: self.door_id_field.clone(),
            local_thing_name,
            timeout: Duration::from_secs(self.timeout_secs),
            cache_ttl: Duration::from_secs(self.cache_ttl_secs),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _enrichment_tables: vector_lib::enrichment::TableRegistry,
        input_definitions: &[(OutputId, schema::Definition)],
        _global_log_namespace: LogNamespace,
    ) -> Vec<TransformOutput> {
        vec![
            TransformOutput::new(DataType::Log, clone_input_definitions(input_definitions))
                .with_port("local"),
            TransformOutput::new(DataType::Log, clone_input_definitions(input_definitions))
                .with_port("remote"),
        ]
    }
}
