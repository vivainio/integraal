//! `native.xml.extract(source, rules)`: declarative record extraction from a string or stream,
//! one Lua table per matched element. Rules use the `xmlstream` extractor DSL.

use std::{
    io::Read,
    sync::{Arc, Mutex},
};

use crate::{
    xml::{source_reader, Limited},
    Deadline,
};
use mlua::{Lua, Table, Value};
use xmlstream::{Extract, Options, Reader, Record, Records};

type Src = Limited<Box<dyn Read + Send>>;

/// Records borrow the compiled rules; `ex` keeps them alive for as long as `records` runs.
struct Run {
    // Field order matters: `records` must drop before `ex`.
    records: Records<'static, Src>,
    #[allow(dead_code)]
    ex: Arc<Extract>,
}

pub(crate) fn api(
    lua: &Lua,
    xml: &Table,
    max_stream_bytes: u64,
    deadline: Deadline,
) -> mlua::Result<()> {
    xml.set(
        "extract",
        lua.create_function(move |lua, (source, rules): (Value, String)| {
            let ex = Arc::new(
                Extract::parse(&rules).map_err(|e| mlua::Error::runtime(e.to_string()))?,
            );
            let src = Limited {
                inner: source_reader(source, "native.xml.extract")?,
                left: u64::MAX,
                deadline: deadline.clone(),
            };
            let reader = Reader::with_options(
                src,
                Options {
                    max_bytes: max_stream_bytes,
                    ..Options::default()
                },
            );
            let records = ex.run(reader);
            // SAFETY: `records` only borrows the `Extract` inside the `Arc` held next to it
            // in `Run`, which drops it first; the heap allocation never moves.
            let records: Records<'static, Src> = unsafe { std::mem::transmute(records) };
            let run = Mutex::new(Some(Run { records, ex }));
            lua.create_function(move |lua, ()| {
                let mut guard = run.lock().unwrap();
                let Some(r) = guard.as_mut() else {
                    return Ok(Value::Nil);
                };
                match r.records.next() {
                    Some(Ok(rec)) => Ok(Value::Table(record_table(lua, &rec, true)?)),
                    Some(Err(e)) => {
                        *guard = None;
                        Err(mlua::Error::runtime(e.to_string()))
                    }
                    None => {
                        *guard = None;
                        Ok(Value::Nil)
                    }
                }
            })
        })?,
    )
}

fn record_table(lua: &Lua, rec: &Record, tag: bool) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    if let (true, Some(name)) = (tag, rec.rule_name()) {
        t.set("_rule", name)?;
    }
    for (k, v) in rec.iter() {
        t.set(k, value(lua, v)?)?;
    }
    Ok(t)
}

fn value(lua: &Lua, v: &xmlstream::Value) -> mlua::Result<Value> {
    use xmlstream::Value as V;
    Ok(match v {
        V::Null => Value::Nil,
        V::Text(s) => Value::String(lua.create_string(s)?),
        V::Int(i) => Value::Integer(*i),
        V::Number(n) => Value::Number(*n),
        V::Bool(b) => Value::Boolean(*b),
        V::List(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                t.set(i + 1, value(lua, item)?)?;
            }
            Value::Table(t)
        }
        V::Record(r) => Value::Table(record_table(lua, r, false)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn lua() -> Lua {
        let lua = Lua::new();
        let xml = lua.create_table().unwrap();
        let d = Deadline::new(Instant::now() + Duration::from_secs(30));
        api(&lua, &xml, 1 << 20, d).unwrap();
        lua.globals().set("xml", xml).unwrap();
        lua
    }

    const ORDERS: &str = r#"<orders>
        <order id="1" status="open"><total>9.5</total><sku>a</sku><sku>b</sku></order>
        <order id="2" status="closed"><total>3</total></order>
        <refund id="7"><amount>2</amount></refund>
    </orders>"#;

    #[test]
    fn records_come_back_as_typed_tables() {
        let out: String = lua()
            .load(format!(
                r#"
                local out = {{}}
                for r in xml.extract([==[{ORDERS}]==], [[
                    open = //order[@status='open'] {{
                        id = @id
                        total = total/text() as number
                        skus = sku/text() *
                    }}
                    refund = //refund {{ id = @id; note = note/text() ? }}
                ]]) do
                    table.insert(out, `{{r._rule}}:{{r.id}}:{{r.total}}:{{r.skus and #r.skus}}:{{r.note}}`)
                end
                return table.concat(out, ",")
                "#
            ))
            .eval()
            .unwrap();
        assert_eq!(out, "open:1:9.5:2:nil,refund:7:nil:nil:nil");
    }

    #[test]
    fn bad_rules_and_bad_xml_raise() {
        let lua = lua();
        assert!(lua
            .load(r#"xml.extract("<a/>", "not a rule")"#)
            .exec()
            .is_err());
        assert!(lua
            .load(r#"for r in xml.extract("<a><b></a>", "//b { x = @x }") do end"#)
            .exec()
            .is_err());
    }
}
