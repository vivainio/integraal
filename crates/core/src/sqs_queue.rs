use aws_sdk_sqs::{types::SendMessageBatchRequestEntry, Client};
use integraal_wrappers::{OutMessage, QueueBackend};
use tokio::runtime::Handle;

/// SQS-backed `QueueBackend`. Blocks on the captured runtime handle, so it must only be called from
/// blocking threads (the engine runs scripts on one).
pub struct SqsBackend {
    client: Client,
    queue_url: String,
    handle: Handle,
}

/// Largest total size of one `SendMessageBatch`.
const MAX_BATCH_BYTES: usize = 250 * 1024;

impl SqsBackend {
    /// Must be called from within a Tokio runtime.
    pub async fn from_env(queue_url: impl Into<String>) -> Self {
        let cfg = aws_config::load_from_env().await;
        Self {
            client: Client::new(&cfg),
            queue_url: queue_url.into(),
            handle: Handle::current(),
        }
    }
}

/// Splits into runs of at most 10 messages and `MAX_BATCH_BYTES` bytes.
fn batches(messages: &[OutMessage]) -> Vec<&[OutMessage]> {
    let (mut out, mut start, mut bytes) = (Vec::new(), 0, 0);
    for (i, m) in messages.iter().enumerate() {
        if i > start && (i - start == 10 || bytes + m.body.len() > MAX_BATCH_BYTES) {
            out.push(&messages[start..i]);
            (start, bytes) = (i, 0);
        }
        bytes += m.body.len();
    }
    if start < messages.len() {
        out.push(&messages[start..]);
    }
    out
}

impl QueueBackend for SqsBackend {
    #[tracing::instrument(name = "sqs.send", skip_all, fields(messages = messages.len()))]
    fn send(&self, messages: &[OutMessage]) -> Result<(), String> {
        for batch in batches(messages) {
            let entries = batch
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    SendMessageBatchRequestEntry::builder()
                        .id(i.to_string())
                        .message_body(&m.body)
                        .delay_seconds(m.delay_secs as i32)
                        .build()
                        .map_err(|e| e.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let out = self.handle.block_on(
                self.client
                    .send_message_batch()
                    .queue_url(&self.queue_url)
                    .set_entries(Some(entries))
                    .send(),
            );
            let out = out.map_err(|e| e.to_string())?;
            if !out.failed().is_empty() {
                return Err(format!(
                    "{} of {} messages were rejected by the queue",
                    out.failed().len(),
                    batch.len()
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msgs(sizes: &[usize]) -> Vec<OutMessage> {
        sizes
            .iter()
            .map(|n| OutMessage {
                body: "x".repeat(*n),
                delay_secs: 0,
            })
            .collect()
    }

    #[test]
    fn batches_respect_sqs_limits() {
        let m = msgs(&[1; 25]);
        let b = batches(&m);
        assert_eq!(b.iter().map(|b| b.len()).collect::<Vec<_>>(), [10, 10, 5]);
        let big = msgs(&[200 * 1024, 100 * 1024, 100 * 1024]);
        let b = batches(&big);
        assert_eq!(b.iter().map(|b| b.len()).collect::<Vec<_>>(), [1, 2]);
        assert!(batches(&[]).is_empty());
    }
}
