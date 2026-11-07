//! Tenant-authored web apps use ordinary tenant-scoped HTTP invocations.
use crate::{Engine, Error, Invocation, Trigger};
use serde_json::Value;

impl Engine {
    /// None means that this tenant has not configured a web app.
    pub async fn invoke_web(&self, tenant: &str, request: Value) -> Result<Option<Value>, Error> {
        integraal_loader::ScriptKey::config(tenant)?;
        let Some(app) = self.tenant_config(tenant).await?.web else {
            return Ok(None);
        };
        self.invoke(Invocation {
            capabilities: None,
            tenant: tenant.into(),
            script: Some(app.script),
            version: app.version,
            trigger: Trigger::Http,
            event: request,
            attempt: 0,
            continues_trace: false,
        })
        .await
        .map(Some)
    }
}
