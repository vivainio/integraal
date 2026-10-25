//! `native.gzip`: streaming gzip over the same byte streams and sinks as the rest of the API.
//!
//! `open` decompresses a stream on the fly, `writer` compresses into a store sink (or memory),
//! and `compress`/`decompress` handle small strings.

use std::{
    io::{self, BufReader, Read, Write},
    sync::Mutex,
};

use crate::Deadline;
use flate2::{bufread::MultiGzDecoder, write::GzEncoder, Compression};
use mlua::{AnyUserData, Lua, LuaString, MetaMethod, Table, UserData, UserDataMethods, Value};

use crate::{
    sink::Sink,
    xml::{take_stream, ByteStream, Limited},
};

/// Fails once more than `left` bytes have been produced (decompression bomb guard).
pub(crate) struct Capped<R> {
    pub inner: R,
    pub left: u64,
}

impl<R: Read> Read for Capped<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n as u64 > self.left {
            return Err(io::Error::other(
                "decompressed data exceeds the stream size limit",
            ));
        }
        self.left -= n as u64;
        Ok(n)
    }
}

fn source(v: Value, what: &str) -> mlua::Result<Box<dyn Read + Send>> {
    match v {
        Value::String(s) => Ok(Box::new(io::Cursor::new(s.as_bytes().to_vec()))),
        Value::UserData(ud) => take_stream(&ud),
        _ => Err(mlua::Error::runtime(format!(
            "{what} expects a string or a stream"
        ))),
    }
}

enum Target {
    Sink(Sink),
    Mem { buf: Vec<u8>, cap: usize },
}

impl Write for Target {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match self {
            Target::Sink(s) => s.write(data),
            Target::Mem { buf, cap } => {
                if buf.len() + data.len() > *cap {
                    return Err(io::Error::other(
                        "gzip output exceeds the object size limit; write into a store sink to stream it",
                    ));
                }
                buf.extend_from_slice(data);
                Ok(data.len())
            }
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct GzipWriter(Mutex<Option<GzEncoder<Target>>>);

impl GzipWriter {
    fn with<T>(&self, f: impl FnOnce(&mut GzEncoder<Target>) -> io::Result<T>) -> mlua::Result<T> {
        let mut g = self.0.lock().unwrap();
        let enc = g
            .as_mut()
            .ok_or_else(|| mlua::Error::runtime("gzip writer is already finished"))?;
        f(enc).map_err(mlua::Error::external)
    }
}

impl UserData for GzipWriter {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("write", |_, this, data: LuaString| {
            this.with(|e| e.write_all(&data.as_bytes()))
        });
        // Writes the gzip trailer. Returns the data for in-memory writers; with a sink, close the
        // sink afterwards to commit the upload.
        methods.add_method("finish", |lua, this, ()| {
            let enc = this
                .0
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| mlua::Error::runtime("gzip writer is already finished"))?;
            match enc.finish().map_err(mlua::Error::external)? {
                Target::Mem { buf, .. } => Ok(Value::String(lua.create_string(&buf)?)),
                Target::Sink(_) => Ok(Value::Nil),
            }
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("GzipWriter"));
    }
}

pub fn api(
    lua: &Lua,
    max_stream_bytes: u64,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let gz = lua.create_table()?;

    // Stream of the decompressed bytes of a gzip stream or string (multi-member files included).
    gz.set(
        "open",
        lua.create_function(move |lua, src: Value| {
            let r = source(src, "native.gzip.open")?;
            let input = BufReader::new(Limited {
                inner: r,
                left: max_stream_bytes,
                deadline: deadline.clone(),
            });
            let dec = Capped {
                inner: MultiGzDecoder::new(input),
                left: max_stream_bytes,
            };
            lua.create_userdata(ByteStream::new(Box::new(dec)))
        })?,
    )?;

    gz.set(
        "writer",
        lua.create_function(move |_, sink: Option<AnyUserData>| {
            let target = match sink {
                Some(ud) => Target::Sink(
                    Sink::from_userdata(&ud)
                        .ok_or_else(|| mlua::Error::runtime("native.gzip.writer expects a sink"))?,
                ),
                None => Target::Mem {
                    buf: Vec::new(),
                    cap: max_object_bytes,
                },
            };
            Ok(GzipWriter(Mutex::new(Some(GzEncoder::new(
                target,
                Compression::default(),
            )))))
        })?,
    )?;

    gz.set(
        "compress",
        lua.create_function(move |lua, data: LuaString| {
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(&data.as_bytes())
                .map_err(mlua::Error::external)?;
            let out = enc.finish().map_err(mlua::Error::external)?;
            if out.len() > max_object_bytes {
                return Err(mlua::Error::runtime(
                    "gzip output exceeds the object size limit",
                ));
            }
            lua.create_string(&out)
        })?,
    )?;

    gz.set(
        "decompress",
        lua.create_function(move |lua, data: LuaString| {
            let mut out = Vec::new();
            MultiGzDecoder::new(&data.as_bytes()[..])
                .take(max_object_bytes as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| mlua::Error::runtime(format!("gzip: {e}")))?;
            if out.len() > max_object_bytes {
                return Err(mlua::Error::runtime(
                    "decompressed data exceeds the object size limit; use native.gzip.open to stream it",
                ));
            }
            lua.create_string(&out)
        })?,
    )?;
    Ok(gz)
}
