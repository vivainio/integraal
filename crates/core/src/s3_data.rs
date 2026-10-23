use aws_sdk_s3::{error::SdkError, operation::get_object::GetObjectError, Client};
use integraal_wrappers::{ObjectBackend, PresignMethod};
use std::io::Read;

use aws_sdk_s3::{presigning::PresigningConfig, primitives::ByteStream};
use tokio::runtime::Handle;

/// S3-backed `ObjectBackend`. Blocks on the captured runtime handle, so it must
/// only be called from blocking threads (the engine runs scripts on one).
pub struct S3Backend {
    client: Client,
    bucket: String,
    handle: Handle,
}

impl S3Backend {
    /// Must be called from within a Tokio runtime.
    pub async fn from_env(bucket: impl Into<String>) -> Self {
        let cfg = aws_config::load_from_env().await;
        let conf = aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(std::env::var("AWS_ENDPOINT_URL").is_ok())
            .build();
        Self {
            client: Client::from_conf(conf),
            bucket: bucket.into(),
            handle: Handle::current(),
        }
    }
}

/// Pulls an S3 body chunk by chunk, so a large object is never held in memory.
struct S3Reader {
    body: ByteStream,
    handle: Handle,
    chunk: Vec<u8>,
    pos: usize,
}

impl Read for S3Reader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.pos >= self.chunk.len() {
            match self
                .handle
                .block_on(self.body.try_next())
                .map_err(std::io::Error::other)?
            {
                Some(b) => {
                    self.chunk = b.to_vec();
                    self.pos = 0;
                }
                None => return Ok(0),
            }
        }
        let n = out.len().min(self.chunk.len() - self.pos);
        out[..n].copy_from_slice(&self.chunk[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

impl ObjectBackend for S3Backend {
    fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        self.handle.block_on(async {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(out) => Ok(Some(Box::new(S3Reader {
                    body: out.body,
                    handle: self.handle.clone(),
                    chunk: Vec::new(),
                    pos: 0,
                }) as Box<dyn Read + Send>)),
                Err(SdkError::ServiceError(e))
                    if matches!(e.err(), GetObjectError::NoSuchKey(_)) =>
                {
                    Ok(None)
                }
                Err(e) => Err(e.to_string()),
            }
        })
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.handle.block_on(async {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(out) => Ok(Some(
                    out.body
                        .collect()
                        .await
                        .map_err(|e| e.to_string())?
                        .to_vec(),
                )),
                Err(SdkError::ServiceError(e))
                    if matches!(e.err(), GetObjectError::NoSuchKey(_)) =>
                {
                    Ok(None)
                }
                Err(e) => Err(e.to_string()),
            }
        })
    }

    fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body.into())
                .send()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn presign(
        &self,
        method: PresignMethod,
        key: &str,
        ttl: std::time::Duration,
    ) -> Result<String, String> {
        self.handle.block_on(async {
            let cfg = PresigningConfig::expires_in(ttl).map_err(|e| e.to_string())?;
            match method {
                PresignMethod::Get => self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .presigned(cfg)
                    .await
                    .map(|r| r.uri().to_string())
                    .map_err(|e| e.to_string()),
                PresignMethod::Put => self
                    .client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .presigned(cfg)
                    .await
                    .map(|r| r.uri().to_string())
                    .map_err(|e| e.to_string()),
            }
        })
    }

    fn list(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String> {
        self.handle.block_on(async {
            let out = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(limit.min(1000) as i32)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            Ok(out
                .contents()
                .iter()
                .filter_map(|o| o.key().map(str::to_string))
                .collect())
        })
    }
}
