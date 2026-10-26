//! `native.xml.events(source, opts)`: a pull parser over a string or a byte stream.
//! Memory use is bounded by a single event, not by the document.

use std::{
    io::{self, Read},
    sync::Mutex,
};

use crate::Deadline;
use mlua::{AnyUserData, Lua, MetaMethod, Table, UserData, Value};
use xmlstream::EventRef;

/// Default total input cap for one parse; guards against unbounded/endless streams.
pub const MAX_STREAM_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_DEPTH: usize = 256;

/// A readable byte source handed to scripts (`native.store.open`, streaming HTTP).
/// Consumed by the first parser that takes it.
pub struct ByteStream(Mutex<Option<Box<dyn Read + Send>>>);

impl ByteStream {
    pub fn new(r: Box<dyn Read + Send>) -> Self {
        Self(Mutex::new(Some(r)))
    }
}

impl UserData for ByteStream {
    fn add_methods<M: mlua::UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("close", |_, this, ()| {
            this.0.lock().unwrap().take();
            Ok(())
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("ByteStream"));
    }
}

/// Caps total bytes read and enforces the invocation deadline on every read.
pub(crate) struct Limited<R> {
    pub inner: R,
    pub left: u64,
    pub deadline: Deadline,
}

impl<R: Read> Read for Limited<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.deadline.check().map_err(io::Error::other)?;
        if self.left == 0 {
            return Err(io::Error::other("input exceeds stream limit"));
        }
        let max = buf.len().min(self.left.min(usize::MAX as u64) as usize);
        let n = self.inner.read(&mut buf[..max])?;
        self.left -= n as u64;
        Ok(n)
    }
}

pub(crate) struct State {
    reader: xmlstream::Reader<Limited<Box<dyn Read + Send>>>,
    depth: usize,
}

impl State {
    pub(crate) fn new(
        reader: Box<dyn Read + Send>,
        max_bytes: u64,
        deadline: Deadline,
        trim: bool,
    ) -> Self {
        let opts = xmlstream::Options {
            trim,
            max_bytes,
            max_depth: MAX_DEPTH,
        };
        let reader = xmlstream::Reader::with_options(
            Limited {
                inner: reader,
                left: u64::MAX,
                deadline,
            },
            opts,
        );
        State { reader, depth: 0 }
    }
}

/// A string or a stream argument as a reader.
pub(crate) fn source_reader(source: Value, who: &str) -> mlua::Result<Box<dyn Read + Send>> {
    match source {
        Value::String(s) => Ok(Box::new(io::Cursor::new(s.as_bytes().to_vec()))),
        Value::UserData(ud) => take_stream(&ud),
        _ => Err(mlua::Error::runtime(format!(
            "{who} expects a string or a stream"
        ))),
    }
}

pub fn api(
    lua: &Lua,
    max_stream_bytes: u64,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let xml = lua.create_table()?;
    let events_deadline = deadline.clone();
    xml.set(
        "events",
        lua.create_function(move |lua, (source, opts): (Value, Option<Table>)| {
            let reader = source_reader(source, "native.xml.events")?;
            let trim = opts
                .map(|o| o.get::<Option<bool>>("trim"))
                .transpose()?
                .flatten()
                .unwrap_or(false);
            let state = Mutex::new(State::new(
                reader,
                max_stream_bytes,
                events_deadline.clone(),
                trim,
            ));
            lua.create_function(move |lua, ()| next_event(lua, &mut state.lock().unwrap()))
        })?,
    )?;
    crate::xml_extract::api(lua, &xml, max_stream_bytes, deadline.clone())?;
    crate::xml_dom::api(lua, &xml, max_object_bytes, deadline.clone())?;
    crate::xml_writer::api(lua, &xml, max_object_bytes, deadline)?;
    Ok(xml)
}

pub(crate) fn take_stream(ud: &AnyUserData) -> mlua::Result<Box<dyn Read + Send>> {
    let stream = ud
        .borrow::<ByteStream>()
        .map_err(|_| mlua::Error::runtime("not a stream"))?;
    let taken = stream.0.lock().unwrap().take();
    taken.ok_or_else(|| mlua::Error::runtime("stream already consumed or closed"))
}

fn xml_err(e: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(e.to_string())
}

fn next_event(lua: &Lua, st: &mut State) -> mlua::Result<Value> {
    let Some(ev) = st.reader.next_ref().map_err(xml_err)? else {
        return Ok(Value::Nil);
    };
    match ev {
        EventRef::Start { name, attrs } => {
            let t = lua.create_table()?;
            let at = lua.create_table()?;
            for (k, v) in attrs {
                at.set(k.as_str(), v.as_str())?;
            }
            t.set("type", "start")?;
            t.set("tag", name)?;
            t.set("attrs", at)?;
            t.set("depth", st.depth)?;
            st.depth += 1;
            Ok(Value::Table(t))
        }
        EventRef::End { name } => {
            st.depth = st.depth.saturating_sub(1);
            end_table(lua, name, st.depth)
        }
        EventRef::Text(t) => text_table(lua, "text", t, st.depth),
        EventRef::CData(t) => text_table(lua, "cdata", t, st.depth),
        EventRef::Comment(t) => text_table(lua, "comment", t, st.depth),
    }
}

fn end_table(lua: &Lua, tag: &str, depth: usize) -> mlua::Result<Value> {
    let t = lua.create_table()?;
    t.set("type", "end")?;
    t.set("tag", tag)?;
    t.set("depth", depth)?;
    Ok(Value::Table(t))
}

fn text_table(lua: &Lua, kind: &str, text: &str, depth: usize) -> mlua::Result<Value> {
    let t = lua.create_table()?;
    t.set("type", kind)?;
    t.set("text", text)?;
    t.set("depth", depth)?;
    Ok(Value::Table(t))
}
