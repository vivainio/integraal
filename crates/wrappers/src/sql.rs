//! `ig.sql`: scratch SQLite databases for working data and bookkeeping within a run.
//!
//! Databases live in memory and vanish with the invocation. They can be written to a store
//! (`db:save(sink)`) and opened again later (`ig.sql.load(stream)`), which is how work is
//! handed from one invocation to the next.

use std::{
    io::{self, Read},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use crate::Deadline;
use mlua::{AnyUserData, Lua, LuaSerdeExt, MetaMethod, Table, UserData, UserDataMethods, Value};
use rusqlite::{
    config::DbConfig,
    hooks::{AuthAction, AuthContext, Authorization},
    limits::Limit,
    types::ValueRef,
    Connection, ErrorCode,
};

use crate::{sink::Sink, xml::take_stream};

/// `query` materializes its result; past this many rows the script must narrow it (LIMIT, paging).
const MAX_ROWS: usize = 100_000;
const MAGIC: &[u8] = b"SQLite format 3\0";
const SAVE_CHUNK: usize = 1024 * 1024;

#[derive(Clone)]
struct Caps {
    max_sql_bytes: u64,
    max_object_bytes: usize,
    deadline: Deadline,
}

struct Db {
    conn: Mutex<Option<Connection>>,
    /// SQLite runs `PRAGMA page_count` internally to serialize; the authorizer must let it through.
    open_gate: Arc<AtomicBool>,
    caps: Caps,
}

fn sql_err(e: rusqlite::Error) -> mlua::Error {
    if let rusqlite::Error::SqliteFailure(f, _) = &e {
        if f.code == ErrorCode::OperationInterrupted {
            return mlua::Error::runtime("__timeout__");
        }
    }
    mlua::Error::runtime(format!("sql: {e}"))
}

/// Locks a connection that has no pragmas, attached files or extensions to reach outside memory.
fn lock_down(conn: &Connection, caps: Caps, gate: Arc<AtomicBool>) -> rusqlite::Result<()> {
    // Pragmas first: the authorizer below denies all of them to scripts.
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    conn.execute_batch("PRAGMA temp_store = MEMORY; PRAGMA foreign_keys = ON;")?;
    let pages = (caps.max_sql_bytes as i64 / page_size.max(512)).max(1);
    conn.query_row(&format!("PRAGMA max_page_count = {pages}"), [], |_| Ok(()))?;
    // A loaded file is untrusted input: no unsafe schema functions, no writes to shadow tables.
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false)?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    let strlen = caps.max_object_bytes.min(i32::MAX as usize) as i32;
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, strlen)?;
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, strlen)?;
    conn.set_limit(Limit::SQLITE_LIMIT_ATTACHED, 0)?;
    let deadline = caps.deadline.clone();
    conn.progress_handler(1000, Some(move || deadline.check().is_err()))?;
    conn.authorizer(Some(move |ctx: AuthContext<'_>| match ctx.action {
        _ if gate.load(Ordering::Relaxed) => Authorization::Allow,
        AuthAction::Attach { .. } | AuthAction::Detach { .. } | AuthAction::Pragma { .. } => {
            Authorization::Deny
        }
        AuthAction::Unknown { .. } => Authorization::Deny,
        _ => Authorization::Allow,
    }))?;
    Ok(())
}

fn to_sql(v: Value) -> mlua::Result<rusqlite::types::Value> {
    use rusqlite::types::Value as S;
    Ok(match v {
        Value::Nil => S::Null,
        Value::LightUserData(p) if p.0.is_null() => S::Null,
        Value::Boolean(b) => S::Integer(b as i64),
        Value::Integer(i) => S::Integer(i),
        Value::Number(f) if f.fract() == 0.0 && f.abs() < 9e15 => S::Integer(f as i64),
        Value::Number(f) => S::Real(f),
        Value::String(s) => match std::str::from_utf8(&s.as_bytes()) {
            Ok(t) => S::Text(t.to_string()),
            Err(_) => S::Blob(s.as_bytes().to_vec()),
        },
        Value::UserData(u) if mlua_decimal::as_decimal(&u).is_some() => {
            S::Text(mlua_decimal::as_decimal(&u).unwrap().to_string())
        }
        other => {
            return Err(mlua::Error::runtime(format!(
                "cannot bind a {} to an SQL parameter",
                other.type_name()
            )))
        }
    })
}

fn params(t: Option<Table>) -> mlua::Result<Vec<rusqlite::types::Value>> {
    let Some(t) = t else { return Ok(Vec::new()) };
    (1..=t.raw_len())
        .map(|i| to_sql(t.raw_get::<Value>(i)?))
        .collect()
}

fn row_table(lua: &Lua, stmt_cols: &[String], row: &rusqlite::Row<'_>) -> mlua::Result<Table> {
    let t = lua.create_table_with_capacity(0, stmt_cols.len())?;
    for (i, name) in stmt_cols.iter().enumerate() {
        match row.get_ref(i).map_err(sql_err)? {
            ValueRef::Null => {}
            ValueRef::Integer(n) => t.set(name.as_str(), n)?,
            ValueRef::Real(f) => t.set(name.as_str(), f)?,
            ValueRef::Text(b) | ValueRef::Blob(b) => t.set(name.as_str(), lua.create_string(b)?)?,
        }
    }
    Ok(t)
}

impl Db {
    fn with<T>(&self, f: impl FnOnce(&Connection) -> mlua::Result<T>) -> mlua::Result<T> {
        self.caps.deadline.check().map_err(mlua::Error::runtime)?;
        let g = self.conn.lock().unwrap();
        f(g.as_ref()
            .ok_or_else(|| mlua::Error::runtime("sql database is closed"))?)
    }

    /// Serializes the database with the authorizer stood down; no script code runs meanwhile.
    fn serialized<T>(
        &self,
        c: &Connection,
        f: impl FnOnce(&[u8]) -> mlua::Result<T>,
    ) -> mlua::Result<T> {
        self.open_gate.store(true, Ordering::Relaxed);
        let data = c.serialize("main");
        self.open_gate.store(false, Ordering::Relaxed);
        f(&data.map_err(sql_err)?)
    }

    fn rows(
        &self,
        lua: &Lua,
        sql: &str,
        p: Vec<rusqlite::types::Value>,
        max: usize,
    ) -> mlua::Result<Table> {
        self.with(|c| {
            let mut stmt = c.prepare(sql).map_err(sql_err)?;
            let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
            let mut rows = stmt
                .query(rusqlite::params_from_iter(p))
                .map_err(sql_err)?;
            let out = lua.create_table()?;
            let mut n = 0;
            while let Some(row) = rows.next().map_err(sql_err)? {
                if n >= max {
                    return Err(mlua::Error::runtime(format!(
                        "query returned more than {max} rows; add a LIMIT or page through the result"
                    )));
                }
                n += 1;
                out.raw_set(n, row_table(lua, &cols, row)?)?;
            }
            Ok(out)
        })
    }
}

impl UserData for Db {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // Runs statements. Without parameters `sql` may hold several; with them it must be one.
        // Returns the number of rows the last statement changed.
        methods.add_method("exec", |_, this, (sql, p): (String, Option<Table>)| {
            let p = params(p)?;
            this.with(|c| {
                if p.is_empty() {
                    c.execute_batch(&sql).map_err(sql_err)?;
                } else {
                    c.execute(&sql, rusqlite::params_from_iter(p))
                        .map_err(sql_err)?;
                }
                Ok(c.changes())
            })
        });
        // All result rows as an array of `{column = value}` tables (NULL columns are absent).
        methods.add_method("query", |lua, this, (sql, p): (String, Option<Table>)| {
            this.rows(lua, &sql, params(p)?, MAX_ROWS)
        });
        // The first result row, or nil.
        methods.add_method("get", |lua, this, (sql, p): (String, Option<Table>)| {
            let rows = this.rows(lua, &sql, params(p)?, MAX_ROWS)?;
            rows.raw_get::<Value>(1)
        });
        methods.add_method("last_insert_id", |_, this, ()| {
            this.with(|c| Ok(c.last_insert_rowid()))
        });
        // The database file as a string, capped at the object size limit.
        methods.add_method("dump", |lua, this, ()| {
            this.with(|c| {
                this.serialized(c, |data| {
                    if data.len() > this.caps.max_object_bytes {
                        return Err(mlua::Error::runtime(
                            "database exceeds the object size limit; use :save(sink) to stream it",
                        ));
                    }
                    lua.create_string(data)
                })
            })
        });
        // Streams the database file into a sink from `store.create`; close the sink to commit.
        methods.add_method("save", |_, this, sink: AnyUserData| {
            let sink = Sink::from_userdata(&sink)
                .ok_or_else(|| mlua::Error::runtime("db:save expects a sink"))?;
            this.with(|c| {
                this.serialized(c, |data| {
                    for chunk in data.chunks(SAVE_CHUNK) {
                        sink.write_bytes(chunk).map_err(mlua::Error::runtime)?;
                    }
                    Ok(data.len())
                })
            })
        });
        // Frees the database early.
        methods.add_method("close", |_, this, ()| {
            this.conn.lock().unwrap().take();
            Ok(())
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("SqlDatabase"));
    }
}

fn open(caps: Caps, load: Option<Vec<u8>>) -> mlua::Result<Db> {
    let mut conn = Connection::open_in_memory().map_err(sql_err)?;
    if let Some(bytes) = load {
        let n = bytes.len();
        conn.deserialize_read_exact("main", io::Cursor::new(bytes), n, false)
            .map_err(sql_err)?;
        // Surface a corrupt or non-database file now rather than at first use.
        conn.query_row("PRAGMA schema_version", [], |_| Ok(()))
            .map_err(|e| mlua::Error::runtime(format!("sql: not a valid database file: {e}")))?;
    }
    let gate = Arc::new(AtomicBool::new(false));
    lock_down(&conn, caps.clone(), gate.clone()).map_err(sql_err)?;
    Ok(Db {
        conn: Mutex::new(Some(conn)),
        open_gate: gate,
        caps,
    })
}

pub fn api(
    lua: &Lua,
    max_sql_bytes: u64,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let caps = Caps {
        max_sql_bytes,
        max_object_bytes,
        deadline,
    };
    let sql = lua.create_table()?;
    sql.set("null", lua.null())?;
    sql.set(
        "open",
        lua.create_function({
            let caps = caps.clone();
            move |_, ()| open(caps.clone(), None)
        })?,
    )?;
    // Opens a database file saved earlier, from a string or a stream (e.g. `store.open(key)`).
    sql.set(
        "load",
        lua.create_function(move |_, source: Value| {
            let mut r: Box<dyn Read + Send> = match source {
                Value::String(s) => Box::new(io::Cursor::new(s.as_bytes().to_vec())),
                Value::UserData(ud) => take_stream(&ud)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "ig.sql.load expects a string or a stream",
                    ))
                }
            };
            let mut bytes = Vec::new();
            (&mut r)
                .take(caps.max_sql_bytes + 1)
                .read_to_end(&mut bytes)
                .map_err(mlua::Error::external)?;
            if bytes.len() as u64 > caps.max_sql_bytes {
                return Err(mlua::Error::runtime(
                    "database file exceeds the sql size limit (max_sql_mb)",
                ));
            }
            if !bytes.starts_with(MAGIC) {
                return Err(mlua::Error::runtime("sql: not a SQLite database file"));
            }
            open(caps.clone(), Some(bytes))
        })?,
    )?;
    Ok(sql)
}
