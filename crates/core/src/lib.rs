//! Luau execution engine: loads a tenant script, sandboxes it, and dispatches
//! an invocation to the handler matching the trigger.

mod config;
mod dynamo_db;
mod engine;
pub mod ingress;
mod modules;
mod placement;
mod s3_data;
pub mod s3_events;
mod sqs_queue;
pub mod telemetry;

pub use engine::{Placement, TenantConfig, TenantLimits};
pub use placement::{PlaceOptions, Placed};

pub use config::{GlobalConfig, IngressConfig};
pub use dynamo_db::DynamoBackend;
pub use engine::{Engine, Limits};
pub use integraal_wrappers::{fail_inflight_http, Vault, VaultEntry};
pub use s3_data::S3Backend;
pub use s3_events::S3Events;
pub use sqs_queue::SqsBackend;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trigger {
    /// Direct Lambda invoke (or any synchronous RPC).
    Invoke,
    /// HTTP request via ALB -> Fargate.
    Http,
    /// Queue message (SQS -> Fargate).
    Queue,
    /// Internal: the tenant's router script choosing a script for a message.
    Route,
    /// Internal: the tenant's placement script choosing a worker class (intake Lambda).
    Place,
}

impl Trigger {
    /// Handler names tried in order. The generic `handle` is the fallback, so a
    /// script can serve every trigger with a single function.
    fn handlers(self) -> [&'static str; 2] {
        match self {
            Trigger::Invoke => ["on_invoke", "handle"],
            Trigger::Http => ["on_http", "handle"],
            Trigger::Queue => ["on_queue", "handle"],
            Trigger::Route => ["on_route", "route"],
            Trigger::Place => ["on_place", "place"],
        }
    }
}

/// Wire envelope shared by Lambda payloads, HTTP bodies and queue messages.
///
/// `script` is optional: without it the tenant's routes / router script choose one
/// (see `TenantConfig`). `trigger` is set by the entrypoint, not the sender.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    pub tenant: String,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default = "default_trigger")]
    pub trigger: Trigger,
    #[serde(default)]
    pub event: serde_json::Value,
    /// Which delivery of the message this is (SQS `ApproximateReceiveCount`); 0 when unknown. Set by
    /// the entrypoint, never read from the wire, and exposed to scripts as `ctx.attempt`.
    #[serde(skip)]
    pub attempt: u32,
}

fn default_trigger() -> Trigger {
    Trigger::Invoke
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Load(#[from] integraal_loader::LoadError),
    #[error(transparent)]
    Wrappers(#[from] integraal_wrappers::WrapperError),
    #[error("invalid tenant config.json: {0}")]
    Config(String),
    #[error("script must return a table of handlers")]
    BadModule,
    #[error("script exports no handler for {0:?} trigger (expected one of {1:?})")]
    NoHandler(Trigger, [&'static str; 2]),
    #[error("no route for this message: no matching route and no router script")]
    NoRoute,
    #[error("script timed out")]
    Timeout,
    #[error("script error: {0}")]
    Script(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// An AWS SDK error with its whole cause chain. `to_string()` alone gives just "service error".
pub(crate) fn sdk_err<E: std::error::Error>(e: &E) -> String {
    aws_sdk_s3::error::DisplayErrorContext(e).to_string()
}
