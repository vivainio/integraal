use async_trait::async_trait;
use aws_sdk_s3::{error::SdkError, operation::get_object::GetObjectError, Client};

use crate::{Fetched, LoadError, Script, ScriptKey, ScriptStore};

pub struct S3Store {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Store {
    pub fn new(client: Client, bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    pub async fn from_env(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        // Honors AWS_ENDPOINT_URL, so `AWS_ENDPOINT_URL=http://localhost:5070` targets roto.
        // Path-style addressing is required by local simulators.
        let cfg = aws_config::load_from_env().await;
        let conf = aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(std::env::var("AWS_ENDPOINT_URL").is_ok())
            .build();
        Self::new(Client::from_conf(conf), bucket, prefix)
    }
}

#[async_trait]
impl ScriptStore for S3Store {
    async fn fetch(&self, key: &ScriptKey, etag: Option<&str>) -> Result<Fetched, LoadError> {
        let object_key = format!("{}{}", self.prefix, key.object_key());
        let mut req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&object_key);
        if let Some(etag) = etag {
            req = req.if_none_match(etag);
        }
        match req.send().await {
            Ok(out) => {
                let etag = out.e_tag().map(str::to_string);
                let bytes = out
                    .body
                    .collect()
                    .await
                    .map_err(|e| LoadError::Storage(e.to_string()))?;
                let source = String::from_utf8(bytes.to_vec()).map_err(|_| LoadError::NotUtf8)?;
                Ok(Fetched::Fresh(Script { source, etag }))
            }
            Err(SdkError::ServiceError(e)) if matches!(e.err(), GetObjectError::NoSuchKey(_)) => {
                Err(LoadError::NotFound(object_key))
            }
            Err(SdkError::ServiceError(e)) if e.raw().status().as_u16() == 304 => {
                Ok(Fetched::NotModified)
            }
            Err(e) => Err(LoadError::Storage(e.to_string())),
        }
    }
}
