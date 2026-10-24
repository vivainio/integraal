use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Every item expires this long after its last write.
pub const DB_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
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
    /// Atomically deletes the item and returns it, so concurrent callers cannot both receive it.
    fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String>;
    /// Writes the item only if no item exists at `(pk, item.sk)`; returns whether it wrote.
    fn put_if_absent(&self, pk: &str, item: Item) -> Result<bool, String>;
    /// Writes the item if none exists at `(pk, item.sk)` or the existing one's `ttl` is at or before
    /// `now` (expired but not yet swept by DynamoDB); returns whether it wrote.
    fn put_if_free(&self, pk: &str, item: Item, now: u64) -> Result<bool, String>;
    /// Deletes the item only if its value is `value`; returns whether it deleted.
    fn delete_if_value(&self, pk: &str, sk: &str, value: &str) -> Result<bool, String>;
    /// Atomically adds `delta` to the counter at `(pk, sk)` and returns the new value, creating it at
    /// `delta` if it is absent or expired (`ttl <= now`). Every call sets the item's expiry to `ttl`.
    /// The item then reads back (`get`, `query`) with the number as its value.
    fn add(&self, pk: &str, sk: &str, delta: i64, ttl: u64, now: u64) -> Result<i64, String>;
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
    signer: Arc<CallbackSigner>,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn check(what: &str, v: &str, allow_empty: bool) -> Result<(), String> {
    let bad =
        (v.is_empty() && !allow_empty) || v.len() > MAX_KEY_LEN || v.chars().any(char::is_control);
    if bad {
        return Err(format!("invalid {what} {v:?}"));
    }
    Ok(())
}

impl DbScope {
    pub fn new(backend: Arc<dyn ItemBackend>, tenant: impl Into<String>) -> Self {
        let signer = Arc::new(CallbackSigner::new(backend.clone()));
        Self {
            backend,
            tenant: tenant.into(),
            signer,
        }
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
        Ok(self
            .backend
            .get(&self.full_pk(pk)?, sk)?
            .and_then(Self::live)
            .map(|i| i.value))
    }

    pub fn put(&self, pk: &str, sk: &str, value: String) -> Result<(), String> {
        self.put_for(pk, sk, value, DB_TTL)
    }

    /// Like `put`, with an expiry of `ttl` (never longer than `DB_TTL`).
    fn put_for(&self, pk: &str, sk: &str, value: String, ttl: Duration) -> Result<(), String> {
        check("sk", sk, true)?;
        if value.len() > MAX_ITEM_BYTES {
            return Err(format!("item exceeds {MAX_ITEM_BYTES} bytes"));
        }
        let item = Item {
            sk: sk.to_string(),
            value,
            ttl: now_secs() + ttl.min(DB_TTL).as_secs(),
        };
        self.backend.put(&self.full_pk(pk)?, item)
    }

    /// Writes the item only if the key holds no live item; returns whether it wrote. An item that has
    /// expired but not yet been swept by DynamoDB counts as absent.
    pub fn put_if_absent(&self, pk: &str, sk: &str, value: String) -> Result<bool, String> {
        check("sk", sk, true)?;
        if value.len() > MAX_ITEM_BYTES {
            return Err(format!("item exceeds {MAX_ITEM_BYTES} bytes"));
        }
        let now = now_secs();
        let item = Item {
            sk: sk.to_string(),
            value,
            ttl: now + DB_TTL.as_secs(),
        };
        self.backend.put_if_free(&self.full_pk(pk)?, item, now)
    }

    /// Atomically adds `delta` to the counter at `(pk, sk)` and returns the new value. A missing or
    /// expired counter starts from 0. Use keys that only `add` writes; `get` reads a counter back as a number.
    pub fn add(&self, pk: &str, sk: &str, delta: i64) -> Result<i64, String> {
        check("sk", sk, true)?;
        let now = now_secs();
        self.backend
            .add(&self.full_pk(pk)?, sk, delta, now + DB_TTL.as_secs(), now)
    }

    pub fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
        check("sk", sk, true)?;
        self.backend.delete(&self.full_pk(pk)?, sk)
    }

    /// Live items in the partition, as `(sk, value)` pairs.
    pub fn query(
        &self,
        pk: &str,
        sk_prefix: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, String)>, String> {
        check("sk_prefix", sk_prefix, true)?;
        let limit = limit.unwrap_or(MAX_QUERY_ITEMS).clamp(1, MAX_QUERY_ITEMS);
        let items = self.backend.query(&self.full_pk(pk)?, sk_prefix, limit)?;
        Ok(items
            .into_iter()
            .filter_map(Self::live)
            .map(|i| (i.sk, i.value))
            .collect())
    }
}

/// Longest lease `lock_acquire` grants.
pub const MAX_LOCK_LEASE: Duration = Duration::from_secs(15 * 60);

impl DbScope {
    /// The partition holding a lock. It contains a control character, which `check` rejects in
    /// script-supplied keys, so scripts cannot read or forge locks through `native.db`.
    fn lock_pk(&self, name: &str) -> Result<String, String> {
        check("lock name", name, false)?;
        Ok(format!("TEN#{}#\u{1}lock#{name}", self.tenant))
    }

    /// Tries once to take the named lock for `lease`. Returns the owner id to pass to `lock_release`,
    /// or `None` if someone else holds it. A lock whose lease has run out is up for grabs even if
    /// DynamoDB has not swept its row yet.
    ///
    /// The stored expiry is rounded up to whole seconds and padded by one more, so a holder that
    /// stops itself `lease` after it *started asking* is gone before anyone else can take over.
    pub fn lock_acquire(&self, name: &str, lease: Duration) -> Result<Option<String>, String> {
        let pk = self.lock_pk(name)?;
        let lease = lease.min(MAX_LOCK_LEASE);
        let owner = hex(&random::<16>()?);
        let now = now_secs();
        let item = Item {
            sk: String::new(),
            value: owner.clone(),
            ttl: now + lease.as_secs() + u64::from(lease.subsec_nanos() > 0) + 1,
        };
        Ok(self.backend.put_if_free(&pk, item, now)?.then_some(owner))
    }

    /// Frees the lock if `owner` still holds it; a lock that expired and moved on is left alone.
    pub fn lock_release(&self, name: &str, owner: &str) -> Result<(), String> {
        self.backend
            .delete_if_value(&self.lock_pk(name)?, "", owner)
            .map(|_| ())
    }
}

/// What a callback resolves to: the script to run and the state saved when it was created.
#[derive(Debug, Clone, PartialEq)]
pub struct Callback {
    pub script: Option<String>,
    pub state: serde_json::Value,
}

/// A consumed callback. `item` and `id` let a caller that fails to deliver put it back.
#[derive(Debug, Clone)]
pub struct Taken {
    pub callback: Callback,
    pub id: String,
    pub item: Item,
}

/// Result of presenting a callback token.
#[derive(Debug)]
pub enum Take {
    /// Malformed, wrong tenant, or bad signature. Decided without touching the table.
    Invalid,
    /// Correctly signed but past its expiry. Decided without touching the table.
    Expired,
    /// Never existed, or already used.
    Gone,
    Found(Taken),
}

const KEY_PK: &str = "SYS#callback-key";
/// Far-future expiry: DynamoDB TTL only deletes items whose `ttl` has passed.
pub(crate) const KEY_TTL: u64 = 253_402_300_799;

/// HMAC key for callback tokens. Nobody configures it: the first process to need it
/// generates a random key and stores it in the table outside any `TEN#` partition
/// (tenants cannot address it); everyone else reads it. To rotate, delete the row.
/// Callbacks live at most `DB_TTL`, so outstanding ones simply stop working.
pub struct CallbackSigner {
    backend: Arc<dyn ItemBackend>,
    key: OnceLock<[u8; 32]>,
}

pub(crate) fn random<const N: usize>() -> Result<[u8; N], String> {
    let mut b = [0u8; N];
    getrandom::fill(&mut b).map_err(|e| format!("no randomness available: {e}"))?;
    Ok(b)
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.is_ascii() || !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

impl CallbackSigner {
    pub fn new(backend: Arc<dyn ItemBackend>) -> Self {
        Self {
            backend,
            key: OnceLock::new(),
        }
    }

    fn key(&self) -> Result<[u8; 32], String> {
        if let Some(k) = self.key.get() {
            return Ok(*k);
        }
        let read = || -> Result<Option<[u8; 32]>, String> {
            match self.backend.get(KEY_PK, "")? {
                None => Ok(None),
                Some(item) => unhex(&item.value)
                    .and_then(|v| <[u8; 32]>::try_from(v).ok())
                    .map(Some)
                    .ok_or_else(|| "corrupt callback signing key".to_string()),
            }
        };
        let key = match read()? {
            Some(k) => k,
            None => {
                let fresh = Item {
                    sk: String::new(),
                    value: hex(&random::<32>()?),
                    ttl: KEY_TTL,
                };
                self.backend.put_if_absent(KEY_PK, fresh)?; // losing a race is fine: we re-read the winner's key
                read()?.ok_or("callback signing key missing after create")?
            }
        };
        Ok(*self.key.get_or_init(|| key))
    }

    fn tag(&self, tenant: &str, id: &str, exp: u64) -> Result<Hmac<Sha256>, String> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key()?).map_err(|e| e.to_string())?;
        mac.update(format!("{tenant}\n{id}\n{exp}").as_bytes());
        Ok(mac)
    }

    /// `<id:32 hex><expiry:16 hex><tag:32 hex>`: 80 characters.
    fn sign(&self, tenant: &str, id: &str, exp: u64) -> Result<String, String> {
        let tag = self.tag(tenant, id, exp)?.finalize().into_bytes();
        Ok(format!("{id}{exp:016x}{}", hex(&tag[..16])))
    }

    /// `Some((id, exp))` when the token is well formed and correctly signed for `tenant`.
    fn verify(&self, tenant: &str, token: &str) -> Result<Option<(String, u64)>, String> {
        if token.len() != 80 || !token.is_ascii() {
            return Ok(None);
        }
        let (id, rest) = token.split_at(32);
        let (exp, tag) = rest.split_at(16);
        let (Ok(exp), Some(tag)) = (u64::from_str_radix(exp, 16), unhex(tag)) else {
            return Ok(None);
        };
        if unhex(id).is_none() {
            return Ok(None);
        }
        // constant-time comparison of the truncated tag
        Ok(self
            .tag(tenant, id, exp)?
            .verify_truncated_left(&tag)
            .is_ok()
            .then(|| (id.to_string(), exp)))
    }
}

fn callback_pk(id: &str) -> String {
    format!("cb#{id}")
}

impl DbScope {
    /// Replaces the signer (the engine shares one across tenants so the key is read once per process).
    pub fn with_signer(mut self, signer: Arc<CallbackSigner>) -> Self {
        self.signer = signer;
        self
    }

    /// Stores a single-use callback and returns its signed token. The row lives in the
    /// tenant's own partition namespace as `cb#<id>`, with a random 128-bit id.
    pub fn create_callback(
        &self,
        script: Option<String>,
        state: serde_json::Value,
        ttl: Duration,
    ) -> Result<String, String> {
        let id = hex(&random::<16>()?);
        let value = serde_json::json!({ "script": script, "state": state }).to_string();
        self.put_for(&callback_pk(&id), "", value, ttl)?;
        let exp = now_secs() + ttl.min(DB_TTL).as_secs();
        self.signer.sign(&self.tenant, &id, exp)
    }

    /// Consumes the callback: the first presentation of a valid token gets it, every later one gets `Gone`.
    /// Forged, mistenanted and expired tokens are rejected before the table is read.
    pub fn take_callback(&self, token: &str) -> Result<Take, String> {
        let Some((id, exp)) = self.signer.verify(&self.tenant, token)? else {
            return Ok(Take::Invalid);
        };
        if exp <= now_secs() {
            return Ok(Take::Expired);
        }
        let Some(item) = self
            .backend
            .take(&self.full_pk(&callback_pk(&id))?, "")?
            .and_then(Self::live)
        else {
            return Ok(Take::Gone);
        };
        let v: serde_json::Value =
            serde_json::from_str(&item.value).map_err(|e| format!("corrupt callback: {e}"))?;
        let script = v.get("script").and_then(|s| s.as_str()).map(str::to_string);
        let callback = Callback {
            script,
            state: v.get("state").cloned().unwrap_or(serde_json::Value::Null),
        };
        Ok(Take::Found(Taken { callback, id, item }))
    }

    /// Puts a taken callback back (delivery failed), keeping its original expiry.
    pub fn restore_callback(&self, taken: Taken) -> Result<(), String> {
        self.backend
            .put(&self.full_pk(&callback_pk(&taken.id))?, taken.item)
    }
}
