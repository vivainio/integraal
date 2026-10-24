use crate::sdk_err;
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

/// DynamoDB rejects an empty string as a key attribute, but "no sort key" (locks, callbacks, the
/// vault key) is an empty `sk` here. Control characters cannot appear in script-supplied keys, so a
/// stand-in made of one never collides with them.
const EMPTY_SK: &str = "\u{2}";

fn sk_attr(sk: &str) -> AttributeValue {
    s(if sk.is_empty() { EMPTY_SK } else { sk })
}

fn sk_value(stored: &str) -> String {
    if stored == EMPTY_SK {
        String::new()
    } else {
        stored.to_string()
    }
}

fn parse(attrs: &std::collections::HashMap<String, AttributeValue>) -> Option<Item> {
    // A counter (`add`) keeps its number in `n`; that is its value.
    let value = match attrs.get("n") {
        Some(n) => n.as_n().ok()?.clone(),
        None => attrs.get("v")?.as_s().ok()?.clone(),
    };
    Some(Item {
        sk: sk_value(attrs.get("sk")?.as_s().ok()?),
        value,
        ttl: attrs.get("ttl")?.as_n().ok()?.parse().ok()?,
    })
}

impl ItemBackend for DynamoBackend {
    #[tracing::instrument(name = "dynamodb.get", skip_all, fields(table = %self.table))]
    fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.handle.block_on(async {
            let out = self
                .client
                .get_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", sk_attr(sk))
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| sdk_err(&e))?;
            Ok(out.item().and_then(parse))
        })
    }

    #[tracing::instrument(name = "dynamodb.put", skip_all, fields(table = %self.table))]
    fn put(&self, pk: &str, item: Item) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .put_item()
                .table_name(&self.table)
                .item("pk", s(pk))
                .item("sk", sk_attr(&item.sk))
                .item("v", s(&item.value))
                .item("ttl", AttributeValue::N(item.ttl.to_string()))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| sdk_err(&e))
        })
    }

    #[tracing::instrument(name = "dynamodb.delete", skip_all, fields(table = %self.table))]
    fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        self.handle.block_on(async {
            self.client
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", sk_attr(sk))
                .send()
                .await
                .map(|_| ())
                .map_err(|e| sdk_err(&e))
        })
    }

    #[tracing::instrument(name = "dynamodb.put_if_absent", skip_all, fields(table = %self.table))]
    fn put_if_absent(&self, pk: &str, item: Item) -> Result<bool, String> {
        self.handle.block_on(async {
            let res = self
                .client
                .put_item()
                .table_name(&self.table)
                .item("pk", s(pk))
                .item("sk", sk_attr(&item.sk))
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
                Err(e) => Err(sdk_err(&e)),
            }
        })
    }

    #[tracing::instrument(name = "dynamodb.put_if_free", skip_all, fields(table = %self.table))]
    fn put_if_free(&self, pk: &str, item: Item, now: u64) -> Result<bool, String> {
        self.handle.block_on(async {
            let res = self
                .client
                .put_item()
                .table_name(&self.table)
                .item("pk", s(pk))
                .item("sk", sk_attr(&item.sk))
                .item("v", s(&item.value))
                .item("ttl", AttributeValue::N(item.ttl.to_string()))
                .condition_expression("attribute_not_exists(pk) OR #t <= :now")
                .expression_attribute_names("#t", "ttl")
                .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
                .send()
                .await;
            match res {
                Ok(_) => Ok(true),
                Err(SdkError::ServiceError(e))
                    if e.err().is_conditional_check_failed_exception() =>
                {
                    Ok(false)
                }
                Err(e) => Err(sdk_err(&e)),
            }
        })
    }

    #[tracing::instrument(name = "dynamodb.delete_if_value", skip_all, fields(table = %self.table))]
    fn delete_if_value(&self, pk: &str, sk: &str, value: &str) -> Result<bool, String> {
        self.handle.block_on(async {
            let res = self
                .client
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", sk_attr(sk))
                .condition_expression("v = :v")
                .expression_attribute_values(":v", s(value))
                .send()
                .await;
            match res {
                Ok(_) => Ok(true),
                Err(SdkError::ServiceError(e))
                    if e.err().is_conditional_check_failed_exception() =>
                {
                    Ok(false)
                }
                Err(e) => Err(sdk_err(&e)),
            }
        })
    }

    #[tracing::instrument(name = "dynamodb.take", skip_all, fields(table = %self.table))]
    fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.handle.block_on(async {
            let out = self
                .client
                .delete_item()
                .table_name(&self.table)
                .key("pk", s(pk))
                .key("sk", sk_attr(sk))
                .return_values(ReturnValue::AllOld)
                .send()
                .await
                .map_err(|e| sdk_err(&e))?;
            Ok(out.attributes().and_then(parse))
        })
    }

    #[tracing::instrument(name = "dynamodb.add", skip_all, fields(table = %self.table))]
    fn add(&self, pk: &str, sk: &str, delta: i64, ttl: u64, now: u64) -> Result<i64, String> {
        self.handle.block_on(async {
            // A row can be replaced between the two steps, so retry a few times before giving up.
            for _ in 0..3 {
                let res = self
                    .client
                    .update_item()
                    .table_name(&self.table)
                    .key("pk", s(pk))
                    .key("sk", sk_attr(sk))
                    .update_expression("SET v = if_not_exists(v, :z), #t = :ttl ADD n :d")
                    .condition_expression(
                        "attribute_not_exists(pk) OR (attribute_exists(n) AND #t > :now)",
                    )
                    .expression_attribute_names("#t", "ttl")
                    .expression_attribute_values(":z", s("0"))
                    .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
                    .expression_attribute_values(":d", AttributeValue::N(delta.to_string()))
                    .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
                    .return_values(ReturnValue::UpdatedNew)
                    .send()
                    .await;
                match res {
                    Ok(out) => {
                        return out
                            .attributes()
                            .and_then(|a| a.get("n"))
                            .and_then(|n| n.as_n().ok())
                            .and_then(|n| n.parse().ok())
                            .ok_or_else(|| "counter update returned no value".to_string());
                    }
                    Err(SdkError::ServiceError(e))
                        if e.err().is_conditional_check_failed_exception() => {}
                    Err(e) => return Err(sdk_err(&e)),
                }
                // The row is expired, or not a counter: start a fresh counter over an expired row.
                let res = self
                    .client
                    .put_item()
                    .table_name(&self.table)
                    .item("pk", s(pk))
                    .item("sk", sk_attr(sk))
                    .item("v", s(&delta.to_string()))
                    .item("n", AttributeValue::N(delta.to_string()))
                    .item("ttl", AttributeValue::N(ttl.to_string()))
                    .condition_expression("attribute_not_exists(pk) OR #t <= :now")
                    .expression_attribute_names("#t", "ttl")
                    .expression_attribute_values(":now", AttributeValue::N(now.to_string()))
                    .send()
                    .await;
                match res {
                    Ok(_) => return Ok(delta),
                    Err(SdkError::ServiceError(e))
                        if e.err().is_conditional_check_failed_exception() => {}
                    Err(e) => return Err(sdk_err(&e)),
                }
            }
            Err("key holds a value that is not a counter".to_string())
        })
    }

    #[tracing::instrument(name = "dynamodb.query", skip_all, fields(table = %self.table))]
    fn query(
        &self,
        pk: &str,
        sk_prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Item>, String> {
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
            if !after.is_empty() {
                q = q
                    .exclusive_start_key("pk", s(pk))
                    .exclusive_start_key("sk", s(after));
            }
            let out = q.send().await.map_err(|e| sdk_err(&e))?;
            Ok(out.items().iter().filter_map(parse).collect())
        })
    }
}
