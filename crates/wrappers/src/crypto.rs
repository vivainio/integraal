//! `ig.crypto`: authenticated encryption and hashing for tenant scripts.
//!
//! Scripts never hold key material. They name a key (`"invoices"`) and the host derives
//! 32 bytes for it from the operator's master key with HKDF-SHA256, bound to the tenant,
//! so two tenants naming the same key get unrelated keys. Rotating is choosing a new name.
//!
//! The operator holds numbered master keys and marks one active. New ciphertext is sealed with
//! the active key and records its number, so older ciphertext stays readable while the previous
//! keys remain configured; retire a key once nothing sealed with it is left.
//!
//! Ciphertext layout: `0x01 || key id (1 byte) || nonce (24 bytes, random) || XChaCha20-Poly1305(ciphertext || tag)`.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use chacha20poly1305::{
    aead::{Aead, Payload},
    KeyInit, XChaCha20Poly1305, XNonce,
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use mlua::{Lua, LuaString, Table};
use sha2::{Digest, Sha256};

use crate::db::{hex, random, unhex};

const FORMAT: u8 = 1;
const NONCE: usize = 24;
const HEADER: usize = 2;
const TAG: usize = 16;
/// Most `ig.crypto.random` returns in one call.
const MAX_RANDOM: usize = 1024;

/// The operator's root secret. Never exposed to scripts.
#[derive(Clone)]
pub struct MasterKey([u8; 32]);

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(..)")
    }
}

impl MasterKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 32 bytes written as 64 hex characters or as base64.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        let bytes = unhex(s)
            .filter(|b| b.len() == 32)
            .or_else(|| STANDARD.decode(s).ok().filter(|b| b.len() == 32))
            .ok_or("master key must be 32 bytes, as 64 hex characters or base64")?;
        Ok(Self(bytes.try_into().expect("length checked")))
    }

    fn derive(&self, tenant: &str, name: &str) -> [u8; 32] {
        self.expand("integraal/v1", tenant, name)
    }

    /// The age (X25519) identity behind `public_key`/`open`; unrelated to the symmetric key of the same name.
    fn age_identity(&self, tenant: &str, name: &str) -> age::x25519::Identity {
        use bech32::ToBase32;
        let seed = self.expand("integraal/v1/age", tenant, name);
        bech32::encode("age-secret-key-", seed.to_base32(), bech32::Variant::Bech32)
            .expect("fixed hrp")
            .to_uppercase()
            .parse()
            .expect("well-formed age identity")
    }

    fn expand(&self, label: &str, tenant: &str, name: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        // Length-prefixed so ("a", "bc") and ("ab", "c") cannot collide.
        let info = format!("{label}/{}:{tenant}/{}:{name}", tenant.len(), name.len());
        Hkdf::<Sha256>::new(None, &self.0)
            .expand(info.as_bytes(), &mut out)
            .expect("32 bytes is a valid HKDF length");
        out
    }
}

/// Numbered master keys plus the one used for new data.
#[derive(Clone, Debug)]
pub struct MasterKeys {
    keys: std::collections::BTreeMap<u8, MasterKey>,
    active: u8,
}

impl MasterKeys {
    /// `active` must be one of `keys`.
    pub fn new(
        keys: impl IntoIterator<Item = (u8, MasterKey)>,
        active: u8,
    ) -> Result<Self, String> {
        let keys: std::collections::BTreeMap<_, _> = keys.into_iter().collect();
        if !keys.contains_key(&active) {
            return Err(format!(
                "active key {active} is not among the configured keys"
            ));
        }
        Ok(Self { keys, active })
    }

    /// A single key, numbered 1.
    pub fn single(key: MasterKey) -> Self {
        Self::new([(1, key)], 1).expect("1 is present")
    }

    /// Seals `plain` for one vault entry with the active key; bound to tenant and name.
    pub(crate) fn seal_secret(
        &self,
        tenant: &str,
        name: &str,
        plain: &[u8],
    ) -> Result<Vec<u8>, String> {
        let key = self
            .get(self.active)?
            .expand("integraal/v1/vault", tenant, name);
        encrypt(
            self.active,
            &key,
            plain,
            format!("vault/{tenant}/{name}").as_bytes(),
        )
    }

    /// Opens a blob from `seal_secret` with whichever configured key sealed it.
    pub(crate) fn open_secret(
        &self,
        tenant: &str,
        name: &str,
        blob: &[u8],
    ) -> Result<Vec<u8>, String> {
        let id = blob_key_id(blob)?;
        let key = self.get(id)?.expand("integraal/v1/vault", tenant, name);
        decrypt(&key, blob, format!("vault/{tenant}/{name}").as_bytes())
    }

    pub(crate) fn is_active(&self, blob: &[u8]) -> bool {
        blob_key_id(blob).is_ok_and(|id| id == self.active)
    }

    fn ids(&self) -> impl Iterator<Item = u8> + '_ {
        self.keys.keys().copied()
    }

    fn get(&self, id: u8) -> Result<&MasterKey, String> {
        self.keys
            .get(&id)
            .ok_or_else(|| format!("key {id} is not configured (retired?)"))
    }
}

pub(crate) fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid key name {name:?} (1-64 characters: letters, digits, - _ .)"
        ))
    }
}

fn encrypt(id: u8, key: &[u8; 32], plain: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    let nonce = random::<NONCE>()?;
    let ct = XChaCha20Poly1305::new(key.into())
        .encrypt(XNonce::from_slice(&nonce), Payload { msg: plain, aad })
        .map_err(|_| "encryption failed".to_string())?;
    let mut out = Vec::with_capacity(HEADER + NONCE + ct.len());
    out.push(FORMAT);
    out.push(id);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

/// The key number a blob was sealed with.
fn blob_key_id(blob: &[u8]) -> Result<u8, String> {
    if blob.len() < HEADER + NONCE + TAG || blob[0] != FORMAT {
        return Err("not a value produced by ig.crypto.encrypt".into());
    }
    Ok(blob[1])
}

fn decrypt(key: &[u8; 32], blob: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
    blob_key_id(blob)?;
    let (nonce, ct) = blob[HEADER..].split_at(NONCE);
    XChaCha20Poly1305::new(key.into())
        .decrypt(XNonce::from_slice(nonce), Payload { msg: ct, aad })
        .map_err(|_| "decryption failed: wrong key or aad, or the data was modified".to_string())
}

pub(crate) fn api(
    lua: &Lua,
    master: Option<MasterKeys>,
    tenant: &str,
    max_bytes: usize,
) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    fn err(m: impl std::fmt::Display) -> mlua::Error {
        mlua::Error::runtime(m.to_string())
    }
    fn no_keys() -> mlua::Error {
        err("ig.crypto keys are not configured for this runtime (crypto.keys)")
    }

    let master_active = master.as_ref().map(|m| m.active);
    let age_master = master.clone();
    let age_tenant = tenant.to_string();
    let key_for = {
        let tenant = tenant.to_string();
        move |name: &str, id: Option<u8>| -> mlua::Result<(u8, [u8; 32])> {
            let master = master.as_ref().ok_or_else(no_keys)?;
            check_name(name).map_err(err)?;
            let id = id.unwrap_or(master.active);
            Ok((id, master.get(id).map_err(err)?.derive(&tenant, name)))
        }
    };
    let sized = move |b: &[u8]| -> mlua::Result<()> {
        if b.len() > max_bytes {
            return Err(err(format!("input exceeds {max_bytes} bytes")));
        }
        Ok(())
    };

    let (k, s) = (key_for.clone(), sized);
    t.set(
        "encrypt",
        lua.create_function(
            move |lua, (name, plain, aad): (String, LuaString, Option<LuaString>)| {
                s(&plain.as_bytes())?;
                let aad = aad.as_ref().map(|a| a.as_bytes());
                let (id, key) = k(&name, None)?;
                let out = encrypt(id, &key, &plain.as_bytes(), aad.as_deref().unwrap_or(&[]))
                    .map_err(err)?;
                lua.create_string(out)
            },
        )?,
    )?;

    let k = key_for.clone();
    t.set(
        "decrypt",
        lua.create_function(
            move |lua, (name, blob, aad): (String, LuaString, Option<LuaString>)| {
                let aad = aad.as_ref().map(|a| a.as_bytes());
                let id = blob_key_id(&blob.as_bytes()).map_err(err)?;
                let out = decrypt(
                    &k(&name, Some(id))?.1,
                    &blob.as_bytes(),
                    aad.as_deref().unwrap_or(&[]),
                )
                .map_err(err)?;
                lua.create_string(out)
            },
        )?,
    )?;

    let active = master_active;
    t.set(
        "is_current",
        lua.create_function(move |_, blob: LuaString| {
            let id = blob_key_id(&blob.as_bytes()).map_err(err)?;
            Ok(Some(id) == active)
        })?,
    )?;

    let k = key_for;
    t.set(
        "hmac_sha256",
        lua.create_function(
            move |lua, (name, data, key_id): (String, LuaString, Option<u8>)| {
                let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&k(&name, key_id)?.1)
                    .map_err(mlua::Error::external)?;
                mac.update(&data.as_bytes());
                lua.create_string(mac.finalize().into_bytes().as_slice())
            },
        )?,
    )?;

    // Public-key encryption in the age format (https://age-encryption.org), so anyone holding the
    // public key can encrypt with the stock `age` tool and only this tenant's scripts can open it.
    let m = age_master.clone();
    let tn = age_tenant.clone();
    t.set(
        "public_key",
        lua.create_function(move |_, (name, key_id): (String, Option<u8>)| {
            check_name(&name).map_err(err)?;
            let m = m.as_ref().ok_or_else(no_keys)?;
            let id = key_id.unwrap_or(m.active);
            Ok(m.get(id)
                .map_err(err)?
                .age_identity(&tn, &name)
                .to_public()
                .to_string())
        })?,
    )?;

    let s = sized;
    t.set(
        "seal",
        lua.create_function(move |lua, (recipient, plain): (String, LuaString)| {
            s(&plain.as_bytes())?;
            let r: age::x25519::Recipient = recipient
                .parse()
                .map_err(|e| err(format!("invalid age public key: {e}")))?;
            lua.create_string(
                age::encrypt(&r, &plain.as_bytes())
                    .map_err(|e| err(format!("seal failed: {e}")))?,
            )
        })?,
    )?;

    let (m, tn) = (age_master, age_tenant);
    t.set(
        "open",
        lua.create_function(move |lua, (name, blob): (String, LuaString)| {
            check_name(&name).map_err(err)?;
            let m = m.as_ref().ok_or_else(no_keys)?;
            let blob = blob.as_bytes();
            if blob.starts_with(b"-----BEGIN AGE") {
                return Err(err("armored input is not supported; encrypt without `-a`"));
            }
            // age ciphertext does not say which key sealed it, so try each configured master key.
            for id in m.ids() {
                let identity = m.get(id).map_err(err)?.age_identity(&tn, &name);
                if let Ok(plain) = age::decrypt(&identity, &blob) {
                    return lua.create_string(plain);
                }
            }
            Err(err(
                "open failed: not sealed to this tenant's key, or the data was modified",
            ))
        })?,
    )?;

    t.set(
        "sha256",
        lua.create_function(|lua, data: LuaString| {
            lua.create_string(Sha256::digest(&*data.as_bytes()).as_slice())
        })?,
    )?;

    t.set(
        "random",
        lua.create_function(|lua, n: usize| {
            if n == 0 || n > MAX_RANDOM {
                return Err(err(format!("random: n must be 1..={MAX_RANDOM}")));
            }
            let mut b = vec![0u8; n];
            getrandom::fill(&mut b).map_err(|e| err(format!("no randomness available: {e}")))?;
            lua.create_string(b)
        })?,
    )?;

    t.set(
        "hex_encode",
        lua.create_function(|_, data: LuaString| Ok(hex(&data.as_bytes())))?,
    )?;
    t.set(
        "hex_decode",
        lua.create_function(|lua, s: String| match unhex(&s) {
            Some(b) => lua.create_string(b),
            None => Err(err("invalid hex")),
        })?,
    )?;
    t.set(
        "base64_encode",
        lua.create_function(|_, data: LuaString| Ok(STANDARD.encode(&*data.as_bytes())))?,
    )?;
    t.set(
        "base64_decode",
        lua.create_function(|lua, s: String| match STANDARD.decode(s.trim()) {
            Ok(b) => lua.create_string(b),
            Err(_) => Err(err("invalid base64")),
        })?,
    )?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lua_with(master: Option<MasterKeys>, tenant: &str) -> Lua {
        let lua = Lua::new();
        lua.globals()
            .set("crypto", api(&lua, master, tenant, 1024).unwrap())
            .unwrap();
        lua
    }

    fn master(b: u8) -> Option<MasterKeys> {
        Some(MasterKeys::single(MasterKey::from_bytes([b; 32])))
    }

    #[test]
    fn round_trip_with_aad_and_tamper_detection() {
        let lua = lua_with(master(1), "acme");
        let ok: bool = lua
            .load(
                r#"
                local c = crypto.encrypt("k", "secret", "ctx")
                assert(c ~= "secret" and #c == 2 + 24 + 6 + 16)
                assert(crypto.decrypt("k", c, "ctx") == "secret")
                assert(not pcall(crypto.decrypt, "k", c, "other"))
                assert(not pcall(crypto.decrypt, "other-key", c, "ctx"))
                local bad = c:sub(1, -2) .. string.char((c:byte(-1) + 1) % 256)
                assert(not pcall(crypto.decrypt, "k", bad, "ctx"))
                assert(crypto.encrypt("k", "x") ~= crypto.encrypt("k", "x"), "nonce reuse")
                return true"#,
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn tenants_and_masters_are_isolated() {
        let sealed = |m, tenant| -> Vec<u8> {
            lua_with(m, tenant)
                .load(r#"return crypto.encrypt("k", "hi")"#)
                .eval::<LuaString>()
                .unwrap()
                .as_bytes()
                .to_vec()
        };
        let blob = sealed(master(1), "acme");
        for (m, tenant) in [(master(1), "globex"), (master(2), "acme")] {
            let lua = lua_with(m, tenant);
            lua.globals()
                .set("blob", lua.create_string(&blob).unwrap())
                .unwrap();
            assert!(lua.load(r#"crypto.decrypt("k", blob)"#).exec().is_err());
        }
    }

    #[test]
    fn unconfigured_and_bad_names_fail_but_hashing_works() {
        let lua = lua_with(None, "acme");
        assert!(lua.load(r#"crypto.encrypt("k", "x")"#).exec().is_err());
        let hex: String = lua
            .load(r#"return crypto.hex_encode(crypto.sha256("abc"))"#)
            .eval()
            .unwrap();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let lua = lua_with(master(1), "acme");
        assert!(lua.load(r#"crypto.encrypt("", "x")"#).exec().is_err());
        assert!(lua.load(r#"crypto.encrypt("a/b", "x")"#).exec().is_err());
        assert!(lua
            .load(r#"crypto.encrypt("k", string.rep("x", 2000))"#)
            .exec()
            .is_err());
    }

    #[test]
    fn rotation_keeps_old_data_readable() {
        let key = |b| MasterKey::from_bytes([b; 32]);
        let only1 = Some(MasterKeys::new([(1, key(1))], 1).unwrap());
        let both = Some(MasterKeys::new([(1, key(1)), (2, key(2))], 2).unwrap());
        let only2 = Some(MasterKeys::new([(2, key(2))], 2).unwrap());
        assert!(MasterKeys::new([(1, key(1))], 2).is_err());

        let old: Vec<u8> = lua_with(only1, "acme")
            .load(r#"return crypto.encrypt("k", "hi")"#)
            .eval::<LuaString>()
            .unwrap()
            .as_bytes()
            .to_vec();
        for (keys, readable) in [(both, true), (only2, false)] {
            let lua = lua_with(keys, "acme");
            lua.globals()
                .set("old", lua.create_string(&old).unwrap())
                .unwrap();
            let r = lua
                .load(r#"return crypto.decrypt("k", old)"#)
                .eval::<LuaString>();
            assert_eq!(r.is_ok(), readable);
        }
        let lua = lua_with(
            Some(MasterKeys::new([(1, key(1)), (2, key(2))], 2).unwrap()),
            "acme",
        );
        lua.globals()
            .set("old", lua.create_string(&old).unwrap())
            .unwrap();
        let ok: bool = lua
            .load(
                r#"
                assert(not crypto.is_current(old))
                local fresh = crypto.encrypt("k", crypto.decrypt("k", old))
                assert(crypto.is_current(fresh) and fresh:byte(2) == 2)
                assert(crypto.hmac_sha256("k", "m") ~= crypto.hmac_sha256("k", "m", 1))
                return crypto.hmac_sha256("k", "m", 2) == crypto.hmac_sha256("k", "m")"#,
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn public_key_sealing_round_trips_and_survives_rotation() {
        let key = |b| MasterKey::from_bytes([b; 32]);
        let v1 = Some(MasterKeys::new([(1, key(1))], 1).unwrap());
        let v12 = Some(MasterKeys::new([(1, key(1)), (2, key(2))], 2).unwrap());

        let lua = lua_with(v1, "acme");
        let (pk, blob): (String, LuaString) = lua
            .load(
                r#"
                local pk = crypto.public_key("inbox")
                assert(pk:sub(1, 4) == "age1" and pk == crypto.public_key("inbox"))
                assert(pk ~= crypto.public_key("other"))
                local blob = crypto.seal(pk, "from outside")
                assert(crypto.open("inbox", blob) == "from outside")
                assert(not pcall(crypto.open, "other", blob))
                assert(not pcall(crypto.open, "inbox", blob:sub(1, -2)))
                assert(not pcall(crypto.seal, "nonsense", "x"))
                return pk, blob"#,
            )
            .eval()
            .unwrap();
        let blob = blob.as_bytes().to_vec();

        // After rotation old uploads still open, and the public key changes with the active key.
        let lua = lua_with(v12, "acme");
        lua.globals()
            .set("blob", lua.create_string(&blob).unwrap())
            .unwrap();
        lua.globals().set("old_pk", pk).unwrap();
        lua.load(
            r#"
            assert(crypto.open("inbox", blob) == "from outside")
            assert(crypto.public_key("inbox") ~= old_pk)
            assert(crypto.public_key("inbox", 1) == old_pk)"#,
        )
        .exec()
        .unwrap();

        // Another tenant cannot open it.
        let lua = lua_with(Some(MasterKeys::new([(1, key(1))], 1).unwrap()), "globex");
        lua.globals()
            .set("blob", lua.create_string(&blob).unwrap())
            .unwrap();
        assert!(lua.load(r#"crypto.open("inbox", blob)"#).exec().is_err());
    }

    #[test]
    fn master_key_parsing() {
        let h = "00".repeat(32);
        assert!(MasterKey::parse(&h).is_ok());
        assert!(MasterKey::parse(&STANDARD.encode([7u8; 32])).is_ok());
        assert!(MasterKey::parse("abcd").is_err());
        assert!(MasterKey::parse(&"zz".repeat(32)).is_err());
    }
}
