use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Every item expires this long after its last write.
pub const DB_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const MAX_ITEM_BYTES: usize = 64 * 1024;
pub const MAX_QUERY_ITEMS: usize = 100;
const MAX_KEY_LEN: usize = 256;

/// One stored item. `ttl` is the Unix expiry in seconds (the DynamoDB TTL attribute).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub sk: String,
    pub value: String,
    pub ttl: u64,
}

/// A single-table store keyed by partition key + sort key (DynamoDB in production).
/// Implementations may block; scripts run on blocking threads. They must not
/// interpret keys: confinement and TTL are applied by `DbScope`.
pub trait ItemBackend: Send + Sync {
    fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String>;
    fn put(&self, pk: &str, item: Item) -> Result<(), String>;
    fn delete(&self, pk: &str, sk: &str) -> Result<(), String>;
    /// Items under `pk` whose sort key starts with `sk_prefix`, ascending by sort key, at most `limit`.
    fn query(&self, pk: &str, sk_prefix: &str, limit: usize) -> Result<Vec<Item>, String>;
}

/// A backend confined to partition keys starting with `TEN#<tenant>`. Scripts supply
/// a partition name and an optional sort key; the full partition key is always
/// `TEN#<tenant>#<pk>`, so a script cannot address another tenant's rows.
#[derive(Clone)]
pub struct DbScope {
    backend: Arc<dyn ItemBackend>,
    tenant: String,
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn check(what: &str, v: &str, allow_empty: bool) -> Result<(), String> {
    let bad = (v.is_empty() && !allow_empty) || v.len() > MAX_KEY_LEN || v.chars().any(char::is_control);
    if bad {
        return Err(format!("invalid {what} {v:?}"));
    }
    Ok(())
}

impl DbScope {
    pub fn new(backend: Arc<dyn ItemBackend>, tenant: impl Into<String>) -> Self {
        Self { backend, tenant: tenant.into() }
    }

    fn full_pk(&self, pk: &str) -> Result<String, String> {
        check("pk", pk, false)?;
        Ok(format!("TEN#{}#{}", self.tenant, pk))
    }

    /// DynamoDB deletes expired items lazily (up to 48h late), so reads drop anything already expired.
    fn live(item: Item) -> Option<Item> {
        (item.ttl > now_secs()).then_some(item)
    }

    pub fn get(&self, pk: &str, sk: &str) -> Result<Option<String>, String> {
        check("sk", sk, true)?;
        Ok(self.backend.get(&self.full_pk(pk)?, sk)?.and_then(Self::live).map(|i| i.value))
    }

    pub fn put(&self, pk: &str, sk: &str, value: String) -> Result<(), String> {
        check("sk", sk, true)?;
        if value.len() > MAX_ITEM_BYTES {
            return Err(format!("item exceeds {MAX_ITEM_BYTES} bytes"));
        }
        let item = Item { sk: sk.to_string(), value, ttl: now_secs() + DB_TTL.as_secs() };
        self.backend.put(&self.full_pk(pk)?, item)
    }

    pub fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        check("sk", sk, true)?;
        self.backend.delete(&self.full_pk(pk)?, sk)
    }

    /// Live items in the partition, as `(sk, value)` pairs.
    pub fn query(&self, pk: &str, sk_prefix: &str, limit: Option<usize>) -> Result<Vec<(String, String)>, String> {
        check("sk_prefix", sk_prefix, true)?;
        let limit = limit.unwrap_or(MAX_QUERY_ITEMS).clamp(1, MAX_QUERY_ITEMS);
        let items = self.backend.query(&self.full_pk(pk)?, sk_prefix, limit)?;
        Ok(items.into_iter().filter_map(Self::live).map(|i| (i.sk, i.value)).collect())
    }
}
