//! Credential vault: secrets that scripts can send but never read.
//!
//! Operators store a secret together with the URL prefixes it may be sent to. A script puts
//! `{{vault:NAME}}` in the headers or body of a `native.http` request; the host substitutes the
//! value after the tenant's allowlist check, and only when the request URL also matches the
//! secret's own `allow` list. No native function returns a secret.
//!
//! Entries live in the item table under `VAULT#<tenant>`, which `native.db` cannot address, with no
//! expiry. Values are sealed with a key derived from the master keys, bound to tenant and name.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    crypto::{check_name, MasterKeys},
    db::KEY_TTL,
    http::{matches_rule, pattern_is_valid},
    Item, ItemBackend,
};

pub const MAX_SECRET_BYTES: usize = 16 * 1024;
/// Shorter secrets are refused so that redacting them from responses cannot mangle ordinary text.
pub const MIN_SECRET_BYTES: usize = 8;
const MAX_ALLOW: usize = 16;

/// What an operator may see of an entry: never the value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VaultEntry {
    pub name: String,
    pub allow: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Stored {
    allow: Vec<String>,
    sealed: String,
}

#[derive(Clone)]
pub struct Vault {
    backend: Arc<dyn ItemBackend>,
    keys: MasterKeys,
}

fn check_tenant(tenant: &str) -> Result<(), String> {
    let ok = !tenant.is_empty()
        && tenant.len() <= 128
        && tenant
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        && tenant != "."
        && tenant != "..";
    ok.then_some(())
        .ok_or_else(|| format!("invalid tenant {tenant:?}"))
}

/// One row per tenant that has ever stored a secret (sort key = tenant), so rotation can find them all.
const INDEX_PK: &str = "SYS#vault-index";
/// Most rows one query reads: the cap on tenants (and on entries per tenant) that rotation can cover.
const MAX_ROWS: usize = 1000;

fn pk(tenant: &str) -> String {
    format!("VAULT#{tenant}")
}

impl Vault {
    pub fn new(backend: Arc<dyn ItemBackend>, keys: MasterKeys) -> Self {
        Self { backend, keys }
    }

    /// Stores or replaces a secret. `allow` lists the URL prefixes it may be sent to, in the same
    /// syntax as `http.allow` (`https://api.example.com/v1/`, `https://*.example.com`); it may not be empty.
    pub fn put(
        &self,
        tenant: &str,
        name: &str,
        value: &str,
        allow: &[String],
    ) -> Result<(), String> {
        check_tenant(tenant)?;
        check_name(name)?;
        if !(MIN_SECRET_BYTES..=MAX_SECRET_BYTES).contains(&value.len()) {
            return Err(format!(
                "secret must be {MIN_SECRET_BYTES}-{MAX_SECRET_BYTES} bytes"
            ));
        }
        if allow.is_empty() || allow.len() > MAX_ALLOW {
            return Err(format!("allow needs 1-{MAX_ALLOW} url prefixes"));
        }
        if let Some(bad) = allow.iter().find(|a| !pattern_is_valid(a)) {
            return Err(format!("invalid allow url {bad:?}"));
        }
        let sealed = self.keys.seal_secret(tenant, name, value.as_bytes())?;
        // Index first: an index row without a secret is harmless, a secret missing from the index is not.
        self.backend.put(
            INDEX_PK,
            Item {
                sk: tenant.to_string(),
                value: String::new(),
                ttl: KEY_TTL,
            },
        )?;
        self.store(tenant, name, allow.to_vec(), &sealed)
    }

    fn store(
        &self,
        tenant: &str,
        name: &str,
        allow: Vec<String>,
        sealed: &[u8],
    ) -> Result<(), String> {
        let value = serde_json::to_string(&Stored {
            allow,
            sealed: STANDARD.encode(sealed),
        })
        .map_err(|e| e.to_string())?;
        self.backend.put(
            &pk(tenant),
            Item {
                sk: name.to_string(),
                value,
                ttl: KEY_TTL,
            },
        )
    }

    pub fn delete(&self, tenant: &str, name: &str) -> Result<(), String> {
        check_tenant(tenant)?;
        check_name(name)?;
        self.backend.delete(&pk(tenant), name)
    }

    /// Names and allowed URLs of a tenant's entries (not the values).
    pub fn list(&self, tenant: &str) -> Result<Vec<VaultEntry>, String> {
        check_tenant(tenant)?;
        self.backend
            .query(&pk(tenant), "", 1000)?
            .into_iter()
            .map(|i| {
                let s: Stored = serde_json::from_str(&i.value).map_err(|e| e.to_string())?;
                Ok(VaultEntry {
                    name: i.sk,
                    allow: s.allow,
                })
            })
            .collect()
    }

    /// Re-seals every entry of the tenant that is not under the active master key; returns how many.
    pub fn rekey(&self, tenant: &str) -> Result<usize, String> {
        check_tenant(tenant)?;
        let items = self.backend.query(&pk(tenant), "", MAX_ROWS)?;
        if items.len() >= MAX_ROWS {
            return Err(format!("tenant {tenant:?} has {MAX_ROWS} or more vault entries; rotation cannot cover them"));
        }
        let mut n = 0;
        for item in items {
            let stored: Stored = serde_json::from_str(&item.value).map_err(|e| e.to_string())?;
            let blob = STANDARD.decode(&stored.sealed).map_err(|e| e.to_string())?;
            if self.keys.is_active(&blob) {
                continue;
            }
            let plain = self.keys.open_secret(tenant, &item.sk, &blob)?;
            let sealed = self.keys.seal_secret(tenant, &item.sk, &plain)?;
            self.store(tenant, &item.sk, stored.allow, &sealed)?;
            n += 1;
        }
        Ok(n)
    }

    /// Tenants that have stored secrets (they may since have deleted them all).
    pub fn tenants(&self) -> Result<Vec<String>, String> {
        let rows = self.backend.query(INDEX_PK, "", MAX_ROWS)?;
        if rows.len() >= MAX_ROWS {
            return Err(format!(
                "{MAX_ROWS} or more tenants use the vault; rotation cannot cover them"
            ));
        }
        Ok(rows.into_iter().map(|i| i.sk).collect())
    }

    /// `rekey` for every tenant in the index: `(tenants visited, entries re-sealed)`. After a master
    /// key rotation, run this and then retire the old key. Safe to repeat or resume after a failure.
    pub fn rekey_all(&self) -> Result<(usize, usize), String> {
        let tenants = self.tenants()?;
        let mut entries = 0;
        for t in &tenants {
            entries += self.rekey(t).map_err(|e| format!("tenant {t:?}: {e}"))?;
        }
        Ok((tenants.len(), entries))
    }

    /// The secret, provided `url` is one of the entry's allowed targets. Error text never includes the value.
    pub(crate) fn resolve(&self, tenant: &str, name: &str, url: &Url) -> Result<Vec<u8>, String> {
        check_name(name)?;
        let item = self
            .backend
            .get(&pk(tenant), name)?
            .ok_or_else(|| format!("vault secret {name:?} does not exist"))?;
        let stored: Stored = serde_json::from_str(&item.value).map_err(|e| e.to_string())?;
        if !stored.allow.iter().any(|p| matches_rule(p, url)) {
            return Err(format!(
                "vault secret {name:?} may not be sent to {}",
                url.host_str().unwrap_or("this url")
            ));
        }
        let blob = STANDARD.decode(&stored.sealed).map_err(|e| e.to_string())?;
        self.keys.open_secret(tenant, name, &blob)
    }
}

const OPEN: &[u8] = b"{{vault:";

/// Replaces each `{{vault:NAME}}` in `input` with `resolve(NAME)`.
pub(crate) fn substitute(
    input: &[u8],
    mut resolve: impl FnMut(&str) -> Result<Vec<u8>, String>,
) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(input.len());
    let mut rest = input;
    while let Some(i) = find(rest, OPEN) {
        out.extend_from_slice(&rest[..i]);
        let after = &rest[i + OPEN.len()..];
        let end = find(after, b"}}").ok_or("unterminated {{vault: placeholder")?;
        let name = std::str::from_utf8(&after[..end]).map_err(|_| "invalid vault secret name")?;
        out.extend_from_slice(&resolve(name)?);
        rest = &after[end + 2..];
    }
    out.extend_from_slice(rest);
    Ok(out)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Replaces every occurrence of any secret in `data`, so an API that echoes its input back does not hand it to the script.
pub(crate) fn redact(data: &[u8], secrets: &[Vec<u8>]) -> Vec<u8> {
    let mut out = data.to_vec();
    for s in secrets.iter().filter(|s| !s.is_empty()) {
        let mut next = Vec::with_capacity(out.len());
        let mut rest = out.as_slice();
        while let Some(i) = find(rest, s) {
            next.extend_from_slice(&rest[..i]);
            next.extend_from_slice(b"[redacted]");
            rest = &rest[i + s.len()..];
        }
        next.extend_from_slice(rest);
        out = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MasterKey;
    use std::{collections::BTreeMap, sync::Mutex};

    #[derive(Default)]
    struct Mem(Mutex<BTreeMap<(String, String), Item>>);

    impl ItemBackend for Mem {
        fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            Ok(self.0.lock().unwrap().get(&(pk.into(), sk.into())).cloned())
        }
        fn put(&self, pk: &str, item: Item) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .insert((pk.into(), item.sk.clone()), item);
            Ok(())
        }
        fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(&(pk.into(), sk.into()));
            Ok(())
        }
        fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            Ok(self.0.lock().unwrap().remove(&(pk.into(), sk.into())))
        }
        fn put_if_absent(&self, _: &str, _: Item) -> Result<bool, String> {
            unimplemented!()
        }
        fn query(&self, pk: &str, prefix: &str, limit: usize) -> Result<Vec<Item>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|((p, s), _)| p == pk && s.starts_with(prefix))
                .map(|(_, i)| i.clone())
                .take(limit)
                .collect())
        }
    }

    fn keys(ids: &[u8], active: u8) -> MasterKeys {
        MasterKeys::new(
            ids.iter().map(|i| (*i, MasterKey::from_bytes([*i; 32]))),
            active,
        )
        .unwrap()
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn secret_is_sealed_scoped_to_urls_and_tenant() {
        let mem = Arc::new(Mem::default());
        let v = Vault::new(mem.clone(), keys(&[1], 1));
        v.put(
            "acme",
            "stripe",
            "sk_live_123456",
            &["https://api.stripe.com/v1/".into()],
        )
        .unwrap();

        let raw = mem.0.lock().unwrap().values().next().unwrap().value.clone();
        assert!(!raw.contains("sk_live_123456"), "stored in plaintext");
        assert_eq!(
            v.resolve("acme", "stripe", &url("https://api.stripe.com/v1/charges"))
                .unwrap(),
            b"sk_live_123456"
        );
        let denied = v
            .resolve("acme", "stripe", &url("https://evil.example.com/v1/"))
            .unwrap_err();
        assert!(!denied.contains("sk_live") && denied.contains("may not be sent"));
        assert!(v
            .resolve("acme", "stripe", &url("https://api.stripe.com/v2/"))
            .is_err());
        assert!(v
            .resolve("globex", "stripe", &url("https://api.stripe.com/v1/"))
            .is_err());
        assert_eq!(v.list("acme").unwrap()[0].name, "stripe");

        // Moving the row to another tenant's partition must not decrypt it.
        let item = mem.0.lock().unwrap().values().next().unwrap().clone();
        mem.put("VAULT#globex", item).unwrap();
        assert!(v
            .resolve("globex", "stripe", &url("https://api.stripe.com/v1/"))
            .is_err());

        v.delete("acme", "stripe").unwrap();
        assert!(v
            .resolve("acme", "stripe", &url("https://api.stripe.com/v1/"))
            .is_err());
    }

    #[test]
    fn put_validates() {
        let v = Vault::new(Arc::new(Mem::default()), keys(&[1], 1));
        let ok = ["https://a.example.com/".to_string()];
        assert!(v.put("t", "n", "short", &ok).is_err());
        assert!(v.put("t", "n", "long enough value", &[]).is_err());
        assert!(v
            .put("t", "n", "long enough value", &["not a url".into()])
            .is_err());
        assert!(v.put("t", "bad name", "long enough value", &ok).is_err());
        assert!(v.put("a/b", "n", "long enough value", &ok).is_err());
        assert!(v.put("t", "n", "long enough value", &ok).is_ok());
    }

    #[test]
    fn rekey_moves_entries_to_the_active_key() {
        let mem = Arc::new(Mem::default());
        let allow = ["https://api.example.com/".to_string()];
        Vault::new(mem.clone(), keys(&[1], 1))
            .put("t", "k", "secret-value-1", &allow)
            .unwrap();

        let rotated = Vault::new(mem.clone(), keys(&[1, 2], 2));
        assert_eq!(
            rotated
                .resolve("t", "k", &url("https://api.example.com/x"))
                .unwrap(),
            b"secret-value-1"
        );
        assert_eq!(rotated.rekey("t").unwrap(), 1);
        assert_eq!(rotated.rekey("t").unwrap(), 0);

        // Key 1 can now be retired.
        let only2 = Vault::new(mem, keys(&[2], 2));
        assert_eq!(
            only2
                .resolve("t", "k", &url("https://api.example.com/x"))
                .unwrap(),
            b"secret-value-1"
        );
    }

    #[test]
    fn rekey_all_finds_every_tenant_through_the_index() {
        let mem = Arc::new(Mem::default());
        let allow = ["https://api.example.com/".to_string()];
        let old = Vault::new(mem.clone(), keys(&[1], 1));
        old.put("a", "k1", "secret-value-1", &allow).unwrap();
        old.put("a", "k2", "secret-value-2", &allow).unwrap();
        old.put("b", "k1", "secret-value-3", &allow).unwrap();
        old.put("gone", "k1", "secret-value-4", &allow).unwrap();
        old.delete("gone", "k1").unwrap();

        let new = Vault::new(mem.clone(), keys(&[1, 2], 2));
        assert_eq!(new.tenants().unwrap().len(), 3);
        assert_eq!(new.rekey_all().unwrap(), (3, 3));
        assert_eq!(new.rekey_all().unwrap(), (3, 0), "idempotent");

        let only2 = Vault::new(mem, keys(&[2], 2));
        for (t, n, v) in [("a", "k2", "secret-value-2"), ("b", "k1", "secret-value-3")] {
            let got = only2.resolve(t, n, &url("https://api.example.com/x"));
            assert_eq!(got.unwrap(), v.as_bytes());
        }
    }

    #[test]
    fn placeholders_are_substituted_and_echoes_redacted() {
        let r = |n: &str| match n {
            "a" => Ok(b"AAAAAAAA".to_vec()),
            _ => Err(format!("no {n}")),
        };
        assert_eq!(
            substitute(b"Bearer {{vault:a}}!", r).unwrap(),
            b"Bearer AAAAAAAA!"
        );
        assert_eq!(
            substitute(b"{{vault:a}}{{vault:a}}", r).unwrap(),
            b"AAAAAAAAAAAAAAAA"
        );
        assert_eq!(
            substitute(b"no placeholders", r).unwrap(),
            b"no placeholders"
        );
        assert!(substitute(b"{{vault:b}}", r).is_err());
        assert!(substitute(b"{{vault:a", r).is_err());
        assert_eq!(
            redact(b"you sent AAAAAAAA ok", &[b"AAAAAAAA".to_vec()]),
            b"you sent [redacted] ok"
        );
    }
}
