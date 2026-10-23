use aws_sdk_dynamodb::{
    error::SdkError,
    types::{AttributeValue, ReturnValue},
    Client,
};
use integraal_wrappers::{Item, ItemBackend};
use tokio::runtime::Handle;

/// DynamoDB-backed `ItemBackend`. Blocks on the captured runtime handle, so it must
/// only be called from blocking threads (the engine runs scripts on one).
///
/// Table schema: string partition key `pk`, string sort key `sk`, and TTL enabled on
/// the number attribute `ttl`. The value is stored as a string in `v`.
pub struct DynamoBackend {
    client: Client,
    table: String,
    handle: Handle,
}

impl DynamoBackend {
    /// Must be called from within a Tokio runtime.
    pub async fn from_env(table: impl Into<String>) -> Self {
        let cfg = aws_config::load_from_env().await;
        Self {
            client: Client::new(&cfg),
            table: table.into(),
            handle: Handle::current(),
        }
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn parse(attrs: &std::collections::HashMap<String, AttributeValue>) -> Option<Item> {
    Some(Item {
        sk: attrs.get("sk")?.as_s().ok()?.clone(),
        value: attrs.get("v")?.as_s().ok()?.clone(),
        ttl: attrs.get("ttl")?.as_n().ok()?.parse().ok()?,
    })
}

impl ItemBackend for DynamoBackend {
    fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.handle.block_on(async {
            let out = self
                .client
                .get_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", s(sk))
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            Ok(out.item().and_then(parse))
        })
    }

    fn put(&self, pk: &str, item: Item) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .put_item()
                .table_name(&self.table)
                .item("pk", s(pk))
                .item("sk", s(&item.sk))
                .item("v", s(&item.value))
                .item("ttl", AttributeValue::N(item.ttl.to_string()))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", s(sk))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }

    fn put_if_absent(&self, pk: &str, item: Item) -> Result<bool, String> {
        self.handle.block_on(async {
            let res = self
                .client
                .put_item()
                .table_name(&self.table)
                .item("pk", s(pk))
                .item("sk", s(&item.sk))
                .item("v", s(&item.value))
                .item("ttl", AttributeValue::N(item.ttl.to_string()))
                .condition_expression("attribute_not_exists(pk)")
                .send()
                .await;
            match res {
                Ok(_) => Ok(true),
                Err(SdkError::ServiceError(e))
                    if e.err().is_conditional_check_failed_exception() =>
                {
                    Ok(false)
                }
                Err(e) => Err(e.to_string()),
            }
        })
    }

    fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.handle.block_on(async {
            let out = self
                .client
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", s(sk))
                .return_values(ReturnValue::AllOld)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            Ok(out.attributes().and_then(parse))
        })
    }

    fn query(&self, pk: &str, sk_prefix: &str, limit: usize) -> Result<Vec<Item>, String> {
        self.handle.block_on(async {
            let mut q = self
                .client
                .query()
                .table_name(&self.table)
                .consistent_read(true)
                .limit(limit as i32);
            q = if sk_prefix.is_empty() {
                q.key_condition_expression("pk = :pk")
                    .expression_attribute_values(":pk", s(pk))
            } else {
                q.key_condition_expression("pk = :pk AND begins_with(sk, :p)")
                    .expression_attribute_values(":pk", s(pk))
                    .expression_attribute_values(":p", s(sk_prefix))
            };
            let out = q.send().await.map_err(|e| e.to_string())?;
            Ok(out.items().iter().filter_map(parse).collect())
        })
    }
}
