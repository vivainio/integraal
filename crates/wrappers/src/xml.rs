//! `ig.xml.events(source, opts)`: a pull parser over a string or a byte stream.
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

/// A readable byte source handed to scripts (`ig.store.open`, streaming HTTP).
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
        // Iterator over the stream in blocks of `size` bytes (default 64 KB, at most 1 MB).
        // Consumes the stream, like a parser; total bytes and time are limited as for parsers.
        methods.add_method("chunks", |lua, this, size: Option<i64>| {
            let size = chunk_size(size)?;
            let limits = stream_limits(lua)?;
            let reader = take_reader(this)?;
            let state = Mutex::new(Some(Limited {
                inner: reader,
                left: limits.max_bytes,
                deadline: limits.deadline,
            }));
            lua.create_function(move |lua, ()| {
                let mut guard = state.lock().unwrap();
                let Some(r) = guard.as_mut() else {
                    return Ok(mlua::Value::Nil);
                };
                let mut buf = vec![0u8; size];
                let n = fill(r, &mut buf).map_err(mlua::Error::external)?;
                if n == 0 {
                    guard.take();
                    return Ok(mlua::Value::Nil);
                }
                Ok(mlua::Value::String(lua.create_string(&buf[..n])?))
            })
        });
        // Copies the whole stream into a sink natively and returns the byte count. Any failure
        // aborts the sink, so no partial object appears. The sink is left open for the caller.
        methods.add_method("pipe", |lua, this, sink: AnyUserData| {
            let sink = crate::sink::Sink::from_userdata(&sink)
                .ok_or_else(|| mlua::Error::runtime("pipe expects a sink"))?;
            let limits = stream_limits(lua)?;
            let mut r = Limited {
                inner: take_reader(this)?,
                left: limits.max_bytes,
                deadline: limits.deadline,
            };
            let mut buf = vec![0u8; DEFAULT_CHUNK];
            let mut total = 0u64;
            loop {
                let n = match r.read(&mut buf) {
                    Ok(n) => n,
                    Err(e) => {
                        sink.abort();
                        return Err(mlua::Error::external(e));
                    }
                };
                if n == 0 {
                    return Ok(total);
                }
                if let Err(e) = sink.write_bytes(&buf[..n]) {
                    sink.abort();
                    return Err(mlua::Error::runtime(e));
                }
                total += n as u64;
            }
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("ByteStream"));
    }
}

const DEFAULT_CHUNK: usize = 64 * 1024;
const MAX_CHUNK: usize = 1024 * 1024;

/// Limits that apply to reads made by `ByteStream` methods; set once per Lua state by `api`.
#[derive(Clone)]
struct StreamLimits {
    max_bytes: u64,
    deadline: Deadline,
}

fn stream_limits(lua: &Lua) -> mlua::Result<StreamLimits> {
    lua.app_data_ref::<StreamLimits>()
        .map(|l| l.clone())
        .ok_or_else(|| mlua::Error::runtime("stream limits are not configured"))
}

fn chunk_size(size: Option<i64>) -> mlua::Result<usize> {
    match size {
        None => Ok(DEFAULT_CHUNK),
        Some(n) if (1..=MAX_CHUNK as i64).contains(&n) => Ok(n as usize),
        Some(_) => Err(mlua::Error::runtime(format!(
            "chunk size must be between 1 and {MAX_CHUNK}"
        ))),
    }
}

/// Reads until `buf` is full or the source ends.
fn fill(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

fn take_reader(stream: &ByteStream) -> mlua::Result<Box<dyn Read + Send>> {
    let taken = stream.0.lock().unwrap().take();
    taken.ok_or_else(|| mlua::Error::runtime("stream already consumed or closed"))
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
    lua.set_app_data(StreamLimits {
        max_bytes: max_stream_bytes,
        deadline: deadline.clone(),
    });
    let xml = lua.create_table()?;
    let events_deadline = deadline.clone();
    xml.set(
        "events",
        lua.create_function(move |lua, (source, opts): (Value, Option<Table>)| {
            let reader = source_reader(source, "ig.xml.events")?;
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

#[cfg(test)]
mod stream_tests {
    use super::*;
    use crate::{sink::Sink, ObjectWriter};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    #[derive(Default)]
    struct Out {
        data: Arc<Mutex<Vec<u8>>>,
        state: Arc<Mutex<&'static str>>,
    }

    struct W(Vec<u8>, Arc<Mutex<Vec<u8>>>, Arc<Mutex<&'static str>>);

    impl ObjectWriter for W {
        fn write(&mut self, data: &[u8]) -> Result<(), String> {
            self.0.extend_from_slice(data);
            Ok(())
        }
        fn commit(self: Box<Self>) -> Result<(), String> {
            *self.1.lock().unwrap() = self.0;
            *self.2.lock().unwrap() = "committed";
            Ok(())
        }
        fn abort(self: Box<Self>) {
            *self.2.lock().unwrap() = "aborted";
        }
    }

    fn setup(max: u64, src: Vec<u8>) -> (Lua, Out) {
        let lua = Lua::new();
        let deadline = Deadline::new(Instant::now() + Duration::from_secs(30));
        let out = Out::default();
        let xml = api(&lua, max, 1 << 20, deadline.clone()).unwrap();
        lua.globals().set("xml", xml).unwrap();
        let w = W(Vec::new(), out.data.clone(), out.state.clone());
        let sink = Sink::new(Box::new(w), u64::MAX, deadline);
        lua.globals().set("sink", sink).unwrap();
        let stream = ByteStream::new(Box::new(io::Cursor::new(src)));
        lua.globals().set("stream", stream).unwrap();
        (lua, out)
    }

    #[test]
    fn chunks_yield_blocks_then_end() {
        let (lua, _) = setup(1 << 20, (0..10u8).collect());
        let n: Vec<usize> = lua
            .load("local t = {} for c in stream:chunks(4) do t[#t+1] = #c end return t")
            .eval::<Vec<usize>>()
            .unwrap();
        assert_eq!(n, vec![4, 4, 2]);
        // consumed
        assert!(lua.load("stream:chunks()").exec().is_err());
    }

    #[test]
    fn chunk_size_is_validated() {
        let (lua, _) = setup(1 << 20, vec![1, 2, 3]);
        assert!(lua.load("stream:chunks(0)").exec().is_err());
        assert!(lua.load("stream:chunks(1048577)").exec().is_err());
    }

    #[test]
    fn chunks_respect_stream_limit() {
        let (lua, _) = setup(8, vec![0; 100]);
        let r = lua.load("for c in stream:chunks(4) do end").exec();
        assert!(r.unwrap_err().to_string().contains("stream limit"));
    }

    #[test]
    fn pipe_copies_and_leaves_sink_open() {
        let src: Vec<u8> = (0..200_000u32).map(|i| i as u8).collect();
        let (lua, out) = setup(1 << 30, src.clone());
        let n: u64 = lua.load("return stream:pipe(sink)").eval().unwrap();
        assert_eq!(n, src.len() as u64);
        assert_eq!(*out.state.lock().unwrap(), "");
        assert_eq!(lua.load("return sink:close()").eval::<u64>().unwrap(), n);
        assert_eq!(*out.data.lock().unwrap(), src);
    }

    #[test]
    fn pipe_aborts_sink_on_limit() {
        let (lua, out) = setup(1000, vec![0; 100_000]);
        assert!(lua.load("stream:pipe(sink)").exec().is_err());
        assert_eq!(*out.state.lock().unwrap(), "aborted");
        assert!(out.data.lock().unwrap().is_empty());
    }

    #[test]
    fn pipe_rejects_non_sink() {
        let (lua, _) = setup(1 << 20, vec![1]);
        assert!(lua.load("stream:pipe(stream)").exec().is_err());
    }
}
