use aws_sdk_s3::{error::SdkError, operation::get_object::GetObjectError, Client};
use integraal_wrappers::{ObjectBackend, ObjectWriter, PresignMethod};
use std::io::Read;

use aws_sdk_s3::{
    presigning::PresigningConfig,
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
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

/// S3 rejects non-final multipart parts under 5 MiB.
const PART_BYTES: usize = 8 * 1024 * 1024;

/// Streams an upload as a multipart upload, holding at most one part in memory. Objects smaller
/// than a part are sent with a plain PUT, so no multipart upload exists for them. Dropping the
/// writer without `commit` aborts the upload, so no orphaned parts keep accruing storage.
struct S3Writer {
    client: Client,
    bucket: String,
    key: String,
    handle: Handle,
    buf: Vec<u8>,
    upload_id: Option<String>,
    parts: Vec<CompletedPart>,
}

impl S3Writer {
    #[tracing::instrument(name = "s3.upload_part", skip_all, fields(bucket = %self.bucket))]
    fn upload_part(&mut self, body: Vec<u8>) -> Result<(), String> {
        let (client, bucket, key) = (self.client.clone(), self.bucket.clone(), self.key.clone());
        let existing = self.upload_id.clone();
        let number = self.parts.len() as i32 + 1;
        let (id, part) = self.handle.block_on(async move {
            let id = match existing {
                Some(id) => id,
                None => client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .upload_id()
                    .ok_or("S3 returned no upload id")?
                    .to_string(),
            };
            let out = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&id)
                .part_number(number)
                .body(body.into())
                .send()
                .await;
            match out {
                Ok(o) => Ok((
                    id,
                    CompletedPart::builder()
                        .part_number(number)
                        .set_e_tag(o.e_tag().map(str::to_string))
                        .build(),
                )),
                Err(e) => {
                    // Ours alone to clean up: the caller never learns this id.
                    let _ = client
                        .abort_multipart_upload()
                        .bucket(&bucket)
                        .key(&key)
                        .upload_id(&id)
                        .send()
                        .await;
                    Err(e.to_string())
                }
            }
        })?;
        self.upload_id = Some(id);
        self.parts.push(part);
        Ok(())
    }
}

impl ObjectWriter for S3Writer {
    fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.buf.extend_from_slice(data);
        while self.buf.len() >= PART_BYTES {
            let rest = self.buf.split_off(PART_BYTES);
            let part = std::mem::replace(&mut self.buf, rest);
            self.upload_part(part)?;
        }
        Ok(())
    }

    #[tracing::instrument(name = "s3.commit", skip_all, fields(bucket = %self.bucket))]
    fn commit(mut self: Box<Self>) -> Result<(), String> {
        let tail = std::mem::take(&mut self.buf);
        if self.upload_id.is_none() {
            let (client, bucket, key) =
                (self.client.clone(), self.bucket.clone(), self.key.clone());
            return self.handle.block_on(async move {
                client
                    .put_object()
                    .bucket(bucket)
                    .key(key)
                    .body(tail.into())
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            });
        }
        if !tail.is_empty() {
            self.upload_part(tail)?;
        }
        let id = self.upload_id.take().unwrap();
        let (client, bucket, key) = (self.client.clone(), self.bucket.clone(), self.key.clone());
        let parts = std::mem::take(&mut self.parts);
        let done = self.handle.block_on(async {
            client
                .complete_multipart_upload()
                .bucket(&bucket)
                .key(&key)
                .upload_id(&id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        });
        if done.is_err() {
            self.upload_id = Some(id); // Drop aborts it
        }
        done
    }

    fn abort(self: Box<Self>) {}
}

impl Drop for S3Writer {
    fn drop(&mut self) {
        // Spawned rather than blocked on: a drop may happen on a runtime thread.
        if let Some(id) = self.upload_id.take() {
            let (client, bucket, key) =
                (self.client.clone(), self.bucket.clone(), self.key.clone());
            self.handle.spawn(async move {
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(id)
                    .send()
                    .await;
            });
        }
    }
}

impl ObjectBackend for S3Backend {
    fn create(&self, key: &str) -> Result<Box<dyn ObjectWriter>, String> {
        Ok(Box::new(S3Writer {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: key.to_string(),
            handle: self.handle.clone(),
            buf: Vec::new(),
            upload_id: None,
            parts: Vec::new(),
        }))
    }

    #[tracing::instrument(name = "s3.open", skip_all, fields(bucket = %self.bucket))]
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

    #[tracing::instrument(name = "s3.get", skip_all, fields(bucket = %self.bucket))]
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

    #[tracing::instrument(name = "s3.put", skip_all, fields(bucket = %self.bucket))]
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

    #[tracing::instrument(name = "s3.delete", skip_all, fields(bucket = %self.bucket))]
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

    #[tracing::instrument(name = "s3.list", skip_all, fields(bucket = %self.bucket))]
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
