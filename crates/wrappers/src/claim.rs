//! Recoverable consumer leases with durable completion results. All transitions use one row.
use crate::{
    db::{hex, now_secs, random},
    Budget, DbScope, HostContext, Item, NO_EXPIRY,
};
use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use std::sync::Arc;

const DEFAULT_LEASE: u64 = 60;
const MAX_LEASE: u64 = 900;
const MAX_RESULT: usize = 8 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
enum State {
    Running { token: String },
    Complete { token: String, result: Json },
}
fn lease(value: u64) -> Result<u64, String> {
    if !(1..=MAX_LEASE).contains(&value) {
        return Err("lease_s must be between 1 and 900".into());
    }
    Ok(value)
}
fn item(state: &State, ttl: u64) -> Result<Item, String> {
    Ok(Item {
        sk: String::new(),
        value: serde_json::to_string(state).map_err(|e| e.to_string())?,
        ttl,
    })
}
fn state(row: &Item) -> Result<State, String> {
    serde_json::from_str(&row.value).map_err(|e| format!("corrupt consumer claim: {e}"))
}
fn acquire(db: &DbScope, id: &str, lease_s: u64, now: u64) -> Result<Json, String> {
    let pk = db.claim_pk(id)?;
    let expires_s = now + lease(lease_s)?;
    let token = hex(&random::<16>()?);
    if db.raw().put_if_free(
        &pk,
        item(
            &State::Running {
                token: token.clone(),
            },
            expires_s,
        )?,
        now,
    )? {
        return Ok(json!({ "state": "acquired", "token": token, "expires_s": expires_s }));
    }
    let row = db
        .raw()
        .get(&pk, "")?
        .ok_or("claim changed during acquisition; retry")?;
    match state(&row)? {
        State::Complete { result, .. } => Ok(json!({ "state": "complete", "result": result })),
        State::Running { .. } => {
            if row.ttl <= now {
                return Err("claim expired during acquisition; retry".into());
            }
            Ok(json!({ "state": "busy", "expires_s": row.ttl }))
        }
    }
}
fn complete(db: &DbScope, id: &str, token: &str, result: Json, now: u64) -> Result<bool, String> {
    let pk = db.claim_pk(id)?;
    if serde_json::to_vec(&result)
        .map_err(|e| e.to_string())?
        .len()
        > MAX_RESULT
    {
        return Err(
            "claim result exceeds 8 KB; store large results in a store and record their key".into(),
        );
    }
    let Some(row) = db.raw().get(&pk, "")? else {
        return Ok(false);
    };
    match state(&row)? {
        State::Complete {
            token: owner,
            result: saved,
        } => Ok(owner == token && saved == result),
        State::Running { token: owner } if owner == token => {
            let done = State::Complete {
                token: owner,
                result,
            };
            db.raw()
                .replace_if_current(&pk, &row, item(&done, NO_EXPIRY)?, now)
        }
        _ => Ok(false),
    }
}
fn renew(
    db: &DbScope,
    id: &str,
    token: &str,
    lease_s: u64,
    now: u64,
) -> Result<Option<u64>, String> {
    let pk = db.claim_pk(id)?;
    let expires_s = now + lease(lease_s)?;
    let Some(row) = db.raw().get(&pk, "")? else {
        return Ok(None);
    };
    match state(&row)? {
        State::Running { token: owner } if owner == token => {
            // Preserve the current expiry when an early renewal requests a shorter duration.
            let expires_s = expires_s.max(row.ttl);
            let mut replacement = row.clone();
            replacement.ttl = expires_s;
            if db.raw().replace_if_current(&pk, &row, replacement, now)? {
                Ok(Some(expires_s))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

pub(crate) fn api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let api = lua.create_table()?;
    let make = {
        let db = host.db.clone();
        move || -> mlua::Result<DbScope> {
            let db = db.clone().ok_or_else(|| {
                mlua::Error::runtime("native.claim needs native.db (the deployment's db section)")
            })?;
            // Reserve both operations before changing state.
            for _ in 0..2 {
                budget.take_data().map_err(mlua::Error::runtime)?;
            }
            Ok(db)
        }
    };
    let m = make.clone();
    api.set(
        "acquire",
        lua.create_function(move |lua, (id, opts): (String, Option<Table>)| {
            let lease_s = opts
                .map(|t| t.get::<Option<u64>>("lease_s"))
                .transpose()?
                .flatten()
                .unwrap_or(DEFAULT_LEASE);
            let value = acquire(&m()?, &id, lease_s, now_secs()).map_err(mlua::Error::runtime)?;
            lua.to_value(&value)
        })?,
    )?;
    let m = make.clone();
    api.set(
        "complete",
        lua.create_function(move |lua, (id, token, value): (String, String, Value)| {
            let result = match value {
                Value::Nil => Json::Null,
                v => lua.from_value(v)?,
            };
            complete(&m()?, &id, &token, result, now_secs()).map_err(mlua::Error::runtime)
        })?,
    )?;
    api.set(
        "renew",
        lua.create_function(
            move |_, (id, token, opts): (String, String, Option<Table>)| {
                let lease_s = opts
                    .map(|t| t.get::<Option<u64>>("lease_s"))
                    .transpose()?
                    .flatten()
                    .unwrap_or(DEFAULT_LEASE);
                renew(&make()?, &id, &token, lease_s, now_secs()).map_err(mlua::Error::runtime)
            },
        )?,
    )?;
    Ok(api)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::tests::Mem, ItemBackend};
    fn setup() -> (DbScope, Arc<Mem>) {
        let mem = Arc::new(Mem::default());
        (DbScope::new(mem.clone(), "acme"), mem)
    }
    fn token(value: &Json) -> &str {
        value["token"].as_str().unwrap()
    }
    #[test]
    fn abandoned_work_is_reclaimed_and_the_expired_owner_cannot_complete() {
        let (db, _) = setup();
        let first = acquire(&db, "job", 10, 100).unwrap();
        assert_eq!(first["state"], "acquired");
        assert_eq!(
            acquire(&db, "job", 10, 109).unwrap(),
            json!({"state":"busy","expires_s":110})
        );
        assert!(!complete(&db, "job", token(&first), json!(1), 110).unwrap());
        assert_eq!(renew(&db, "job", token(&first), 10, 110).unwrap(), None);
        let second = acquire(&db, "job", 10, 110).unwrap();
        assert_ne!(token(&first), token(&second));
        assert!(!complete(&db, "job", token(&first), json!(1), 111).unwrap());
        assert_eq!(renew(&db, "job", token(&first), 10, 111).unwrap(), None);
        assert!(complete(&db, "job", token(&second), json!({"key":"out/job"}), 111).unwrap());
        assert_eq!(
            acquire(&db, "job", 10, 10_000_000).unwrap(),
            json!({"state":"complete","result":{"key":"out/job"}})
        );
    }
    #[test]
    fn renew_extends_live_ownership_and_stale_conditional_writes_fail() {
        let (db, mem) = setup();
        let claim = acquire(&db, "job", 10, 100).unwrap();
        let pk = db.claim_pk("job").unwrap();
        let old = mem.get(&pk, "").unwrap().unwrap();
        assert_eq!(
            renew(&db, "job", token(&claim), 20, 105).unwrap(),
            Some(125)
        );
        let mut stale_replacement = old.clone();
        stale_replacement.ttl = NO_EXPIRY;
        assert!(!mem
            .replace_if_current(&pk, &old, stale_replacement, 106)
            .unwrap());
        assert_eq!(renew(&db, "job", token(&claim), 1, 106).unwrap(), Some(125));
        assert_eq!(acquire(&db, "job", 10, 111).unwrap()["state"], "busy");
        assert!(complete(&db, "job", token(&claim), json!(true), 120).unwrap());
        assert_eq!(renew(&db, "job", token(&claim), 10, 121).unwrap(), None);
    }
    #[test]
    fn completion_is_durable_and_safe_to_retry_after_a_lost_response() {
        let (db, mem) = setup();
        let claim = acquire(&db, "job", 10, 100).unwrap();
        assert!(!complete(&db, "job", "wrong", json!(1), 101).unwrap());
        assert!(complete(&db, "job", token(&claim), json!(1), 101).unwrap());
        assert!(complete(&db, "job", token(&claim), json!(1), 10_000_000).unwrap());
        assert!(!complete(&db, "job", token(&claim), json!(2), 102).unwrap());
        assert!(!complete(&db, "job", "wrong", json!(1), 102).unwrap());
        let pk = db.claim_pk("job").unwrap();
        assert_eq!(mem.get(&pk, "").unwrap().unwrap().ttl, NO_EXPIRY);
        assert_eq!(
            acquire(&db, "job", 10, 10_000_000).unwrap(),
            json!({"state":"complete","result":1})
        );
    }
    #[test]
    fn concurrent_workers_get_one_owner_and_one_completion_result() {
        let (db, _) = setup();
        let claims = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| acquire(&db, "job", 60, 100).unwrap()))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            claims.iter().filter(|r| r["state"] == "acquired").count(),
            1
        );
        let owned = claims.iter().find(|r| r["state"] == "acquired").unwrap();
        let count = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|n| {
                    scope.spawn({
                        let db = &db;
                        move || complete(db, "job", token(owned), json!(n), 101).unwrap()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| usize::from(h.join().unwrap()))
                .sum::<usize>()
        });
        assert_eq!(count, 1);
    }
    #[test]
    fn tenant_isolation_validation_and_physical_expiry() {
        let (db, mem) = setup();
        let other = DbScope::new(mem.clone(), "other");
        let c = acquire(&db, "job", 10, 100).unwrap();
        assert_eq!(
            acquire(&other, "job", 10, 100).unwrap()["state"],
            "acquired"
        );
        assert!(db.get("\u{1}claim#job", "").is_err());
        assert!(acquire(&db, "bad\n", 10, 100).is_err());
        assert!(acquire(&db, &"x".repeat(201), 10, 100).is_err());
        for seconds in [0, 901] {
            assert!(acquire(&db, "job", seconds, 100).is_err());
        }
        assert!(complete(&db, "job", token(&c), json!("x".repeat(MAX_RESULT)), 101).is_err());
        assert!(!complete(&db, "missing", token(&c), Json::Null, 101).unwrap());
        assert_eq!(renew(&db, "missing", token(&c), 10, 101).unwrap(), None);
        mem.delete(&db.claim_pk("job").unwrap(), "").unwrap();
        assert_eq!(acquire(&db, "job", 10, 120).unwrap()["state"], "acquired");
    }
}
