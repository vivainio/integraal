//! `native.xml.events(source, opts)`: a pull parser over a string or a byte stream.
//! Memory use is bounded by a single event, not by the document.

use std::{
    io::{self, BufReader, Read},
    sync::Mutex,
    time::Instant,
};

use mlua::{AnyUserData, Lua, MetaMethod, Table, UserData, Value};
use quick_xml::{events::Event, Reader};

/// Default total input cap for one parse; guards against unbounded/endless streams.
pub const MAX_STREAM_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_DEPTH: usize = 256;

/// A readable byte source handed to scripts (`native.bucket.open`, streaming HTTP).
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
    pub deadline: Instant,
}

impl<R: Read> Read for Limited<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if Instant::now() >= self.deadline {
            return Err(io::Error::other("__timeout__"));
        }
        if self.left == 0 {
            return Err(io::Error::other("input exceeds stream limit"));
        }
        let max = buf.len().min(self.left.min(usize::MAX as u64) as usize);
        let n = self.inner.read(&mut buf[..max])?;
        self.left -= n as u64;
        Ok(n)
    }
}

struct State {
    reader: Reader<BufReader<Limited<Box<dyn Read + Send>>>>,
    buf: Vec<u8>,
    depth: usize,
    trim: bool,
    /// `<a/>` yields start then end; the end is queued here.
    pending_end: Option<String>,
    queued: Option<Ev>,
    done: bool,
}

pub fn api(lua: &Lua, max_stream_bytes: u64, deadline: Instant) -> mlua::Result<Table> {
    let xml = lua.create_table()?;
    xml.set(
        "events",
        lua.create_function(move |lua, (source, opts): (Value, Option<Table>)| {
            let reader: Box<dyn Read + Send> = match source {
                Value::String(s) => Box::new(io::Cursor::new(s.as_bytes().to_vec())),
                Value::UserData(ud) => take_stream(&ud)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "native.xml.events expects a string or a stream",
                    ))
                }
            };
            let trim = opts
                .map(|o| o.get::<Option<bool>>("trim"))
                .transpose()?
                .flatten()
                .unwrap_or(false);
            let mut r = Reader::from_reader(BufReader::new(Limited {
                inner: reader,
                left: max_stream_bytes,
                deadline,
            }));
            r.config_mut().check_end_names = true;
            let state = Mutex::new(State {
                reader: r,
                buf: Vec::new(),
                depth: 0,
                trim,
                pending_end: None,
                queued: None,
                done: false,
            });
            lua.create_function(move |lua, ()| next_event(lua, &mut state.lock().unwrap()))
        })?,
    )?;
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
    mlua::Error::runtime(format!("xml: {e}"))
}

/// Owned, Lua-free form of a parser event so text runs can be coalesced.
enum Ev {
    Start {
        tag: String,
        attrs: Vec<(String, String)>,
        empty: bool,
    },
    End(String),
    Text(String),
    CData(String),
    Comment(String),
    Eof,
}

/// Reads one raw event. Entity references arrive as separate events from
/// quick-xml; predefined and numeric ones are resolved, anything else is rejected.
fn read_raw(st: &mut State) -> mlua::Result<Ev> {
    loop {
        st.buf.clear();
        let decoder = st.reader.decoder();
        let ev = st.reader.read_event_into(&mut st.buf).map_err(xml_err)?;
        return Ok(match ev {
            Event::Start(ref e) | Event::Empty(ref e) => {
                let empty = matches!(ev, Event::Empty(_));
                let mut attrs = Vec::new();
                for a in e.attributes() {
                    let a = a.map_err(xml_err)?;
                    let key = String::from_utf8_lossy(a.key.as_ref()).into_owned();
                    attrs.push((
                        key,
                        a.decode_and_unescape_value(decoder)
                            .map_err(xml_err)?
                            .into_owned(),
                    ));
                }
                Ev::Start {
                    tag: String::from_utf8_lossy(e.name().as_ref()).into_owned(),
                    attrs,
                    empty,
                }
            }
            Event::End(e) => Ev::End(String::from_utf8_lossy(e.name().as_ref()).into_owned()),
            Event::Text(t) => {
                let raw = t.decode().map_err(xml_err)?;
                Ev::Text(raw.into_owned())
            }
            Event::GeneralRef(r) => {
                let resolved = match r.resolve_char_ref().map_err(xml_err)? {
                    Some(c) => c.to_string(),
                    None => {
                        let name = r.decode().map_err(xml_err)?;
                        quick_xml::escape::resolve_predefined_entity(&name)
                            .ok_or_else(|| xml_err(format!("unknown entity &{name};")))?
                            .to_string()
                    }
                };
                Ev::Text(resolved)
            }
            Event::CData(c) => Ev::CData(String::from_utf8_lossy(&c.into_inner()).into_owned()),
            Event::Comment(c) => Ev::Comment(String::from_utf8_lossy(&c.into_inner()).into_owned()),
            Event::DocType(_) => return Err(xml_err("DOCTYPE/DTD is not allowed")),
            Event::Eof => Ev::Eof,
            Event::Decl(_) | Event::PI(_) => continue,
        });
    }
}

/// Next logical event: adjacent text and entity references are merged into one text run.
fn read_logical(st: &mut State) -> mlua::Result<Ev> {
    if let Some(ev) = st.queued.take() {
        return Ok(ev);
    }
    let mut text = String::new();
    loop {
        match read_raw(st)? {
            Ev::Text(t) => text.push_str(&t),
            other => {
                let text = if st.trim {
                    text.trim().to_string()
                } else {
                    text
                };
                if text.is_empty() {
                    return Ok(other);
                }
                st.queued = Some(other);
                return Ok(Ev::Text(text));
            }
        }
    }
}

fn next_event(lua: &Lua, st: &mut State) -> mlua::Result<Value> {
    if let Some(tag) = st.pending_end.take() {
        st.depth -= 1;
        return end_table(lua, &tag, st.depth);
    }
    if st.done {
        return Ok(Value::Nil);
    }
    match read_logical(st)? {
        Ev::Start { tag, attrs, empty } => {
            let t = lua.create_table()?;
            let at = lua.create_table()?;
            for (k, v) in attrs {
                at.set(k, v)?;
            }
            t.set("type", "start")?;
            t.set("tag", tag.as_str())?;
            t.set("attrs", at)?;
            t.set("depth", st.depth)?;
            st.depth += 1;
            if st.depth > MAX_DEPTH {
                return Err(xml_err("nesting too deep"));
            }
            if empty {
                st.pending_end = Some(tag);
            }
            Ok(Value::Table(t))
        }
        Ev::End(tag) => {
            st.depth = st.depth.saturating_sub(1);
            end_table(lua, &tag, st.depth)
        }
        Ev::Text(t) => text_table(lua, "text", &t, st.depth),
        Ev::CData(t) => text_table(lua, "cdata", &t, st.depth),
        Ev::Comment(t) => text_table(lua, "comment", &t, st.depth),
        Ev::Eof => {
            st.done = true;
            if st.depth != 0 {
                return Err(xml_err("unexpected end of document"));
            }
            Ok(Value::Nil)
        }
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
