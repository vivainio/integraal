//! `ig.zip.entries(source)`: iterates the entries of a zip archive in a single forward pass.
//! Nothing is buffered beyond one entry's read chunk, so archives larger than memory work.
//!
//! The archive is read from its local headers, never its central directory (a stream cannot
//! seek). Stored and deflated entries are supported, including deflated entries whose sizes follow
//! the data (data descriptor); encrypted entries and other methods are rejected.

use std::{
    io::{self, BufRead, BufReader, Read},
    sync::{Arc, Mutex},
};

use crate::Deadline;
use flate2::{Decompress, FlushDecompress, Status};
use mlua::{Lua, MetaMethod, Table, UserData, UserDataFields, UserDataMethods, Value};

use crate::xml::{take_stream, ByteStream, Limited};

const SIG_LOCAL: u32 = 0x0403_4b50;
const SIG_CENTRAL: u32 = 0x0201_4b50;
const SIG_EOCD: u32 = 0x0605_4b50;
const SIG_EOCD64: u32 = 0x0606_4b50;
const SIG_DESCRIPTOR: u32 = 0x0807_4b50;
const MAX_NAME: usize = 4096;

type Src = BufReader<Limited<Box<dyn Read + Send>>>;

struct Body {
    deflated: bool,
    dec: Option<Decompress>,
    /// Compressed bytes left; `None` when the size follows the data.
    comp_left: Option<u64>,
    comp_read: u64,
    out: u64,
    crc: crc32fast::Hasher,
    /// Expected `(crc, uncompressed size)` from the header, when it carried them.
    expect: Option<(u32, u64)>,
    zip64: bool,
    finished: bool,
}

struct Inner {
    src: Src,
    /// Uncompressed bytes the whole archive may still produce (zip bomb guard).
    out_left: u64,
    cur: Option<Body>,
    /// Bumped per entry; a handle from an earlier entry no longer reads.
    gen: u64,
    done: bool,
}

type Shared = Arc<Mutex<Inner>>;

fn bad(msg: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("zip: {msg}"))
}

fn u16le(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}
fn u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
fn u64le(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

fn read_n(src: &mut Src, n: usize) -> io::Result<Vec<u8>> {
    let mut v = vec![0; n];
    src.read_exact(&mut v).map_err(|e| match e.kind() {
        io::ErrorKind::UnexpectedEof => bad("truncated archive"),
        _ => e,
    })?;
    Ok(v)
}

struct Header {
    name: String,
    size: Option<u64>,
    body: Body,
}

/// Skips whatever is left of the current entry, then reads the next local header.
/// `None` once the central directory is reached.
fn advance(inner: &mut Inner) -> io::Result<Option<Header>> {
    if inner.done {
        return Ok(None);
    }
    if let Some(mut b) = inner.cur.take() {
        skip(inner, &mut b)?;
    }
    inner.gen += 1;
    let sig = u32le(&read_n(&mut inner.src, 4)?);
    match sig {
        SIG_LOCAL => {}
        SIG_CENTRAL | SIG_EOCD | SIG_EOCD64 => {
            inner.done = true;
            return Ok(None);
        }
        _ => return Err(bad("not a zip archive (bad entry signature)")),
    }
    let h = read_n(&mut inner.src, 26)?;
    let flags = u16le(&h[2..]);
    let method = u16le(&h[4..]);
    let (crc, mut comp, mut size) = (
        u32le(&h[10..]),
        u32le(&h[14..]) as u64,
        u32le(&h[18..]) as u64,
    );
    let (nlen, elen) = (u16le(&h[22..]) as usize, u16le(&h[24..]) as usize);
    if nlen > MAX_NAME {
        return Err(bad("entry name too long"));
    }
    if flags & 1 != 0 {
        return Err(bad("encrypted entries are not supported"));
    }
    let deflated = match method {
        0 => false,
        8 => true,
        m => return Err(bad(format!("unsupported compression method {m}"))),
    };
    let name = String::from_utf8_lossy(&read_n(&mut inner.src, nlen)?).into_owned();
    let extra = read_n(&mut inner.src, elen)?;
    let descriptor = flags & 8 != 0;

    let mut zip64 = false;
    let mut e = &extra[..];
    while e.len() >= 4 {
        let (id, len) = (u16le(e), u16le(&e[2..]) as usize);
        let Some(data) = e.get(4..4 + len) else { break };
        if id == 1 {
            zip64 = true;
            let mut d = data;
            for field in [&mut size, &mut comp] {
                if *field == 0xFFFF_FFFF && d.len() >= 8 {
                    *field = u64le(d);
                    d = &d[8..];
                }
            }
        }
        e = &e[4 + len..];
    }

    let sized = !descriptor;
    if !sized && !deflated {
        return Err(bad(
            "stored entries with a trailing data descriptor are not supported",
        ));
    }
    Ok(Some(Header {
        name,
        size: sized.then_some(size),
        body: Body {
            deflated,
            dec: deflated.then(|| Decompress::new(false)),
            comp_left: sized.then_some(comp),
            comp_read: 0,
            out: 0,
            crc: crc32fast::Hasher::new(),
            expect: sized.then_some((crc, size)),
            zip64,
            finished: false,
        },
    }))
}

/// Discards the unread part of an entry: raw skip when its compressed size is known, otherwise
/// decompress to nowhere to find where it ends.
fn skip(inner: &mut Inner, b: &mut Body) -> io::Result<()> {
    if b.finished {
        return Ok(());
    }
    if let Some(left) = b.comp_left {
        let n = io::copy(&mut (&mut inner.src).take(left), &mut io::sink())?;
        if n != left {
            return Err(bad("truncated archive"));
        }
        b.finished = true;
        return Ok(());
    }
    let mut buf = [0u8; 8192];
    while read_body(inner, b, &mut buf)? > 0 {}
    Ok(())
}

fn read_body(inner: &mut Inner, b: &mut Body, buf: &mut [u8]) -> io::Result<usize> {
    if b.finished || buf.is_empty() {
        return Ok(0);
    }
    let n = if b.deflated {
        loop {
            let avail = inner.src.fill_buf()?;
            if avail.is_empty() {
                return Err(bad("truncated archive"));
            }
            let avail = match b.comp_left {
                Some(left) => &avail[..avail.len().min(left.min(usize::MAX as u64) as usize)],
                None => avail,
            };
            if avail.is_empty() {
                return Err(bad("deflate stream longer than its entry"));
            }
            let dec = b.dec.as_mut().unwrap();
            let (in0, out0) = (dec.total_in(), dec.total_out());
            let status = dec
                .decompress(avail, buf, FlushDecompress::None)
                .map_err(|e| bad(format!("corrupt deflate data: {e}")))?;
            let used = (dec.total_in() - in0) as usize;
            let produced = (dec.total_out() - out0) as usize;
            inner.src.consume(used);
            b.comp_read += used as u64;
            if let Some(left) = &mut b.comp_left {
                *left -= used as u64;
            }
            if status == Status::StreamEnd {
                b.finished = true;
            }
            if produced > 0 || b.finished {
                break produced;
            }
            if used == 0 {
                return Err(bad("corrupt deflate data"));
            }
        }
    } else {
        let left = b.comp_left.unwrap_or(0);
        let max = buf.len().min(left.min(usize::MAX as u64) as usize);
        let n = inner.src.read(&mut buf[..max])?;
        if n == 0 && max > 0 {
            return Err(bad("truncated archive"));
        }
        b.comp_read += n as u64;
        b.comp_left = Some(left - n as u64);
        if left == n as u64 {
            b.finished = true;
        }
        n
    };
    b.out += n as u64;
    b.crc.update(&buf[..n]);
    if (n as u64) > inner.out_left {
        return Err(io::Error::other(
            "zip: uncompressed data exceeds stream limit",
        ));
    }
    inner.out_left -= n as u64;
    if b.finished {
        finish(inner, b)?;
    }
    Ok(n)
}

/// Checks the entry's integrity once its data has been fully consumed.
fn finish(inner: &mut Inner, b: &mut Body) -> io::Result<()> {
    if matches!(b.comp_left, Some(left) if left != 0) {
        return Err(bad("deflate stream shorter than its entry"));
    }
    let (crc, size) = match b.expect {
        Some(e) => e,
        None => {
            // Data descriptor: optional signature, crc, then two sizes (8 bytes each under zip64).
            let mut w = u32le(&read_n(&mut inner.src, 4)?);
            if w == SIG_DESCRIPTOR {
                w = u32le(&read_n(&mut inner.src, 4)?);
            }
            let sizes = read_n(&mut inner.src, if b.zip64 { 16 } else { 8 })?;
            let (comp, size) = if b.zip64 {
                (u64le(&sizes), u64le(&sizes[8..]))
            } else {
                (u32le(&sizes) as u64, u32le(&sizes[4..]) as u64)
            };
            if comp != b.comp_read {
                return Err(bad("entry compressed size mismatch"));
            }
            (w, size)
        }
    };
    if size != b.out {
        return Err(bad("entry size mismatch"));
    }
    if crc != b.crc.clone().finalize() {
        return Err(bad("entry checksum mismatch"));
    }
    Ok(())
}

/// Reads the entry of generation `gen`; fails once the iterator moved on.
struct EntryReader {
    shared: Shared,
    gen: u64,
}

impl Read for EntryReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut inner = self.shared.lock().unwrap();
        if inner.gen != self.gen {
            return Err(io::Error::other(
                "zip entry is no longer available (the iterator moved on)",
            ));
        }
        let Some(mut b) = inner.cur.take() else {
            return Ok(0);
        };
        let r = read_body(&mut inner, &mut b, buf);
        inner.cur = Some(b);
        r
    }
}

struct Entry {
    name: String,
    size: Option<u64>,
    reader: Mutex<Option<EntryReader>>,
    max_bytes: usize,
}

impl Entry {
    fn take(&self) -> mlua::Result<EntryReader> {
        self.reader
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| mlua::Error::runtime("zip entry already opened or read"))
    }
}

impl UserData for Entry {
    fn add_fields<F: UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("name", |_, t| Ok(t.name.clone()));
        fields.add_field_method_get("size", |_, t| Ok(t.size));
        fields.add_field_method_get("is_dir", |_, t| Ok(t.name.ends_with('/')));
    }

    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // A stream of the entry's uncompressed bytes, e.g. for `ig.xml.events`.
        methods.add_method("open", |lua, this, ()| {
            let r = this.take()?;
            lua.create_userdata(ByteStream::new(Box::new(r)))
        });
        // The whole entry as a string, capped at the invocation's object size limit.
        methods.add_method("read", |lua, this, ()| {
            let r = this.take()?;
            let mut out = Vec::new();
            r.take(this.max_bytes as u64 + 1)
                .read_to_end(&mut out)
                .map_err(mlua::Error::external)?;
            if out.len() > this.max_bytes {
                return Err(mlua::Error::runtime(
                    "zip entry exceeds the object size limit; use :open() to stream it",
                ));
            }
            lua.create_string(&out)
        });
        methods.add_meta_method(MetaMethod::ToString, |_, t, ()| {
            Ok(format!("ZipEntry({})", t.name))
        });
    }
}

pub fn api(
    lua: &Lua,
    max_stream_bytes: u64,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let zip = lua.create_table()?;
    let entries_deadline = deadline.clone();
    zip.set(
        "entries",
        lua.create_function(move |lua, source: Value| {
            let deadline = entries_deadline.clone();
            let reader: Box<dyn Read + Send> = match source {
                Value::String(s) => Box::new(io::Cursor::new(s.as_bytes().to_vec())),
                Value::UserData(ud) => take_stream(&ud)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "ig.zip.entries expects a string or a stream",
                    ))
                }
            };
            let shared: Shared = Arc::new(Mutex::new(Inner {
                src: BufReader::new(Limited {
                    inner: reader,
                    left: max_stream_bytes,
                    deadline: deadline.clone(),
                }),
                out_left: max_stream_bytes,
                cur: None,
                gen: 0,
                done: false,
            }));
            lua.create_function(move |lua, ()| {
                let mut inner = shared.lock().unwrap();
                let Some(h) = advance(&mut inner).map_err(mlua::Error::external)? else {
                    return Ok(Value::Nil);
                };
                inner.cur = Some(h.body);
                let entry = Entry {
                    name: h.name,
                    size: h.size,
                    reader: Mutex::new(Some(EntryReader {
                        shared: shared.clone(),
                        gen: inner.gen,
                    })),
                    max_bytes: max_object_bytes,
                };
                Ok(Value::UserData(lua.create_userdata(entry)?))
            })
        })?,
    )?;
    crate::zip_writer::api(lua, &zip, max_object_bytes, deadline)?;
    Ok(zip)
}
