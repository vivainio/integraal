//! List deployed script directories for a tenant.
use crate::Error;
use integraal_loader::ScriptKey;

#[derive(Clone)]
pub struct ScriptCatalog {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}
impl ScriptCatalog {
    pub async fn from_config(config: &crate::GlobalConfig) -> Self {
        let sdk = aws_config::load_from_env().await;
        Self {
            client: aws_sdk_s3::Client::new(&sdk),
            bucket: config.scripts.bucket.clone(),
            prefix: config.scripts.prefix.clone(),
        }
    }
    /// Returns script directories only; the caller must authorize the tenant.
    pub async fn list(&self, tenant: &str) -> Result<Vec<String>, Error> {
        ScriptKey::config(tenant)?;
        let prefix = format!("{}{tenant}/", self.prefix);
        let mut token = None;
        let mut names = Vec::new();
        loop {
            let response = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .delimiter("/")
                .set_continuation_token(token)
                .send()
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
            for entry in response.common_prefixes() {
                if let Some(name) = entry
                    .prefix()
                    .and_then(|s| s.strip_prefix(&prefix))
                    .and_then(|s| s.strip_suffix('/'))
                {
                    if name != "lib" && ScriptKey::new(tenant, name, None).is_ok() {
                        names.push(name.to_owned());
                    }
                }
            }
            token = response.next_continuation_token().map(str::to_owned);
            if token.is_none() {
                break;
            }
            if names.len() > 10000 {
                return Err(Error::Internal("too many scripts".into()));
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }
}
