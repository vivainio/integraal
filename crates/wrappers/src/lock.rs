//! `igp.lock.with(name, opts, fn)`: mutual exclusion inside the process (`scope = "local"`) or
//! across every process sharing the table (`scope = "db"`).
//!
//! The lock is held only while `fn` runs, and the lease is enforced by the deadline stack: when it
//! runs out, `fn` is interrupted before anyone else can take the lock.

use std::{
    collections::HashSet,
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};

use mlua::{Function, Lua, MultiValue, Table, Value};

use crate::{deadline::run_block, Budget, DbScope, Deadline, MAX_LOCK_LEASE};

/// Lease a `db` lock gets when the script names none.
const DEFAULT_DB_LEASE: Duration = Duration::from_secs(30);
/// Between polls of a held `db` lock: starts here, grows by half each time, plus jitter.
const POLL_START: Duration = Duration::from_millis(50);
const POLL_MAX: Duration = Duration::from_millis(500);

/// The locks held in this process. One table is shared by every invocation, so scripts running
/// at the same time on a Fargate task exclude each other. Keys include the tenant.
#[derive(Default)]
pub struct LocalLocks {
    held: Mutex<HashSet<String>>,
    freed: Condvar,
}

/// Frees the lock when dropped, on every exit path.
pub struct LocalGuard {
    locks: &'static LocalLocks,
    key: String,
}

impl Drop for LocalGuard {
    fn drop(&mut self) {
        self.locks.held.lock().unwrap().remove(&self.key);
        self.locks.freed.notify_all();
    }
}

impl LocalLocks {
    pub fn global() -> &'static LocalLocks {
        static LOCKS: OnceLock<LocalLocks> = OnceLock::new();
        LOCKS.get_or_init(LocalLocks::default)
    }

    /// Takes the lock, waiting until `until` for its holder to finish.
    pub fn acquire(&'static self, key: String, until: Instant) -> Option<LocalGuard> {
        let mut held = self.held.lock().unwrap();
        loop {
            if !held.contains(&key) {
                held.insert(key.clone());
                return Some(LocalGuard { locks: self, key });
            }
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            held = self.freed.wait_timeout(held, left).unwrap().0;
        }
    }
}

enum Scope {
    Local,
    Db,
}

/// Something held while `fn` runs.
enum Held {
    Local(#[allow(dead_code)] LocalGuard),
    Db {
        scope: DbScope,
        name: String,
        owner: String,
    },
}

fn jitter(max: Duration) -> Duration {
    let mut b = [0u8; 2];
    let _ = getrandom::fill(&mut b);
    max.mul_f64(f64::from(u16::from_le_bytes(b)) / f64::from(u16::MAX))
}

fn opt_ms(opts: &Option<Table>, key: &str) -> mlua::Result<Option<u64>> {
    let Some(t) = opts else { return Ok(None) };
    match t.get::<Value>(key)? {
        Value::Nil => Ok(None),
        Value::Integer(n) if n >= 0 => Ok(Some(n as u64)),
        Value::Number(n) if n >= 0.0 && n.is_finite() => Ok(Some(n as u64)),
        _ => Err(mlua::Error::runtime(format!(
            "lock option {key} must be a non-negative number of milliseconds"
        ))),
    }
}

pub fn api(
    lua: &Lua,
    tenant: &str,
    db: Option<DbScope>,
    budget: Arc<Budget>,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let tenant = tenant.to_string();
    let t = lua.create_table()?;
    t.set(
        "with",
        lua.create_function(
            move |_, (name, opts, f): (String, Option<Table>, Function)| {
                let scope = match opts
                    .as_ref()
                    .map(|o| o.get::<Option<String>>("scope"))
                    .transpose()?
                    .flatten()
                    .as_deref()
                {
                    None | Some("local") => Scope::Local,
                    Some("db") => Scope::Db,
                    Some(other) => {
                        return Err(mlua::Error::runtime(format!(
                            "lock scope must be \"local\" or \"db\", not {other:?}"
                        )))
                    }
                };
                if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
                    return Err(mlua::Error::runtime(format!("invalid lock name {name:?}")));
                }
                let lease = opt_ms(&opts, "lease_ms")?.map(Duration::from_millis);
                if lease.is_some_and(|l| l.is_zero() || l > MAX_LOCK_LEASE) {
                    return Err(mlua::Error::runtime(format!(
                        "lease_ms must be between 1 and {}",
                        MAX_LOCK_LEASE.as_millis()
                    )));
                }
                let wait = Duration::from_millis(opt_ms(&opts, "wait_ms")?.unwrap_or(0));

                // The lease runs from before the first request, so it ends no later than the stored one.
                let started = Instant::now();
                let give_up = (started + wait).min(deadline.effective());
                let (held, lease) = match scope {
                    Scope::Local => {
                        let key = format!("{tenant}\u{1}{name}");
                        (
                            LocalLocks::global().acquire(key, give_up).map(Held::Local),
                            lease,
                        )
                    }
                    Scope::Db => {
                        let scope = db.clone().ok_or_else(|| {
                            mlua::Error::runtime("igp.lock needs db.table for scope \"db\"")
                        })?;
                        let lease = lease.unwrap_or(DEFAULT_DB_LEASE);
                        let mut pause = POLL_START;
                        let owner = loop {
                            budget.take_data().map_err(mlua::Error::runtime)?;
                            if let Some(o) = scope
                                .lock_acquire(&name, lease)
                                .map_err(mlua::Error::runtime)?
                            {
                                break Some(o);
                            }
                            let sleep = (pause + jitter(pause))
                                .min(give_up.saturating_duration_since(Instant::now()));
                            if sleep.is_zero() {
                                break None;
                            }
                            std::thread::sleep(sleep);
                            pause = (pause + pause / 2).min(POLL_MAX);
                        };
                        (
                            owner.map(|owner| Held::Db {
                                scope,
                                name: name.clone(),
                                owner,
                            }),
                            Some(lease),
                        )
                    }
                };
                let Some(held) = held else {
                    // Out of time because the invocation (or an enclosing block) is, not because it is busy.
                    deadline.check().map_err(mlua::Error::runtime)?;
                    return Ok(MultiValue::from_iter([Value::Boolean(false)]));
                };

                let res = match lease {
                    Some(l) => run_block(&deadline, started + l, &f),
                    None => f.call::<MultiValue>(()),
                };
                if let Held::Db { scope, name, owner } = &held {
                    // Best effort: if this fails the lease simply runs out.
                    if let Err(e) = scope.lock_release(name, owner) {
                        tracing::warn!(%name, "releasing lock failed: {e}");
                    }
                }
                let mut out = MultiValue::from_iter([Value::Boolean(true)]);
                for v in res? {
                    out.push_back(v);
                }
                Ok(out)
            },
        )?,
    )?;
    Ok(t)
}
