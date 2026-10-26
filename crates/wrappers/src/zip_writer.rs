//! `ig.zip.writer(sink?)`: writes a zip archive in a single forward pass, so the archive can
//! be larger than memory and go straight into a store sink.
//!
//! Entries are deflated and carry their sizes in trailing data descriptors (zip64 sized), so
//! nothing has to be known up front and no seeking is needed. Without a sink the archive is
//! built in memory (capped at the object size limit) and `finish()` returns it as a string.

use std::{
    io::Read,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::Deadline;
use flate2::{Compress, Compression, FlushCompress, Status};
use mlua::{Lua, LuaString, MetaMethod, Table, UserData, UserDataMethods, Value};

use crate::{sink::Sink, xml::take_stream};

const SIG_LOCAL: u32 = 0x0403_4b50;
const SIG_CENTRAL: u32 = 0x0201_4b50;
const SIG_EOCD: u32 = 0x0605_4b50;
const SIG_EOCD64: u32 = 0x0606_4b50;
const SIG_LOCATOR64: u32 = 0x0706_4b50;
const SIG_DESCRIPTOR: u32 = 0x0807_4b50;
/// Data descriptor follows the data; names are UTF-8.
const FLAGS: u16 = 0x0808;
const MAX_NAME: usize = 4096;
const MAX_ENTRIES: usize = 100_000;
const CHUNK: usize = 64 * 1024;

enum Target {
    Sink(Sink),
    Mem { buf: Vec<u8>, cap: usize },
}

struct Done {
    name: Vec<u8>,
    crc: u32,
    comp: u64,
    size: u64,
    offset: u64,
    time: u16,
    date: u16,
}

struct Open {
    name: Vec<u8>,
    offset: u64,
    time: u16,
    date: u16,
    enc: Compress,
    crc: crc32fast::Hasher,
    size: u64,
    comp: u64,
}

struct Writer {
    target: Target,
    pos: u64,
    done: Vec<Done>,
    cur: Option<Open>,
    finished: bool,
}

impl Writer {
    fn put(&mut self, data: &[u8]) -> Result<(), String> {
        match &mut self.target {
            Target::Sink(s) => s.write_bytes(data)?,
            Target::Mem { buf, cap } => {
                if buf.len() + data.len() > *cap {
                    return Err(
                        "zip exceeds the object size limit; write into a store sink to stream it"
                            .into(),
                    );
                }
                buf.extend_from_slice(data);
            }
        }
        self.pos += data.len() as u64;
        Ok(())
    }

    fn begin(&mut self, name: &str) -> Result<(), String> {
        self.live()?;
        self.end_entry()?;
        if name.is_empty() || name.len() > MAX_NAME || name.contains('\0') {
            return Err(format!("invalid zip entry name {name:?}"));
        }
        if self.done.len() >= MAX_ENTRIES {
            return Err(format!("zip has too many entries (max {MAX_ENTRIES})"));
        }
        let (time, date) = dos_now();
        let offset = self.pos;
        let mut h = Vec::with_capacity(30 + name.len() + 20);
        h.extend(SIG_LOCAL.to_le_bytes());
        h.extend(45u16.to_le_bytes());
        h.extend(FLAGS.to_le_bytes());
        h.extend(8u16.to_le_bytes());
        h.extend(time.to_le_bytes());
        h.extend(date.to_le_bytes());
        h.extend([0u8; 12]); // crc and sizes follow the data
        h.extend((name.len() as u16).to_le_bytes());
        h.extend(20u16.to_le_bytes());
        h.extend(name.as_bytes());
        // Empty zip64 extra: marks the descriptor below as using 8-byte sizes.
        h.extend(1u16.to_le_bytes());
        h.extend(16u16.to_le_bytes());
        h.extend([0u8; 16]);
        self.put(&h)?;
        self.cur = Some(Open {
            name: name.as_bytes().to_vec(),
            offset,
            time,
            date,
            enc: Compress::new(Compression::default(), false),
            crc: crc32fast::Hasher::new(),
            size: 0,
            comp: 0,
        });
        Ok(())
    }

    fn live(&self) -> Result<(), String> {
        if self.finished {
            Err("zip writer is already finished".into())
        } else {
            Ok(())
        }
    }

    fn write(&mut self, mut data: &[u8]) -> Result<(), String> {
        self.live()?;
        let mut cur = self
            .cur
            .take()
            .ok_or("no open zip entry; call add(name) first")?;
        cur.crc.update(data);
        cur.size += data.len() as u64;
        let mut out = vec![0u8; CHUNK];
        let r = (|| {
            while !data.is_empty() {
                let (in0, out0) = (cur.enc.total_in(), cur.enc.total_out());
                cur.enc
                    .compress(data, &mut out, FlushCompress::None)
                    .map_err(|e| e.to_string())?;
                data = &data[(cur.enc.total_in() - in0) as usize..];
                let n = (cur.enc.total_out() - out0) as usize;
                cur.comp += n as u64;
                self.put(&out[..n])?;
            }
            Ok(())
        })();
        self.cur = Some(cur);
        r
    }

    fn end_entry(&mut self) -> Result<(), String> {
        let Some(mut cur) = self.cur.take() else {
            return Ok(());
        };
        let mut out = vec![0u8; CHUNK];
        loop {
            let out0 = cur.enc.total_out();
            let st = cur
                .enc
                .compress(&[], &mut out, FlushCompress::Finish)
                .map_err(|e| e.to_string())?;
            let n = (cur.enc.total_out() - out0) as usize;
            cur.comp += n as u64;
            self.put(&out[..n])?;
            if st == Status::StreamEnd {
                break;
            }
        }
        let crc = cur.crc.finalize();
        let mut d = Vec::with_capacity(24);
        d.extend(SIG_DESCRIPTOR.to_le_bytes());
        d.extend(crc.to_le_bytes());
        d.extend(cur.comp.to_le_bytes());
        d.extend(cur.size.to_le_bytes());
        self.put(&d)?;
        self.done.push(Done {
            name: cur.name,
            crc,
            comp: cur.comp,
            size: cur.size,
            offset: cur.offset,
            time: cur.time,
            date: cur.date,
        });
        Ok(())
    }

    /// Writes the central directory and returns the in-memory archive, if that is the target.
    fn finish(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.live()?;
        self.end_entry()?;
        let cd_start = self.pos;
        let done = std::mem::take(&mut self.done);
        for e in &done {
            let mut zip64 = Vec::new();
            let mut field = |v: u64| -> u32 {
                if v >= 0xFFFF_FFFF {
                    zip64.extend(v.to_le_bytes());
                    0xFFFF_FFFF
                } else {
                    v as u32
                }
            };
            let (size, comp, offset) = (field(e.size), field(e.comp), field(e.offset));
            let extra_len = if zip64.is_empty() { 0 } else { 4 + zip64.len() };
            let mut c = Vec::with_capacity(46 + e.name.len() + extra_len);
            c.extend(SIG_CENTRAL.to_le_bytes());
            c.extend(45u16.to_le_bytes());
            c.extend(45u16.to_le_bytes());
            c.extend(FLAGS.to_le_bytes());
            c.extend(8u16.to_le_bytes());
            c.extend(e.time.to_le_bytes());
            c.extend(e.date.to_le_bytes());
            c.extend(e.crc.to_le_bytes());
            c.extend(comp.to_le_bytes());
            c.extend(size.to_le_bytes());
            c.extend((e.name.len() as u16).to_le_bytes());
            c.extend((extra_len as u16).to_le_bytes());
            c.extend([0u8; 6]); // comment length, disk, internal attrs
            let ext_attr: u32 = if e.name.ends_with(b"/") { 0x10 } else { 0 };
            c.extend(ext_attr.to_le_bytes());
            c.extend(offset.to_le_bytes());
            c.extend(&e.name);
            if !zip64.is_empty() {
                c.extend(1u16.to_le_bytes());
                c.extend((zip64.len() as u16).to_le_bytes());
                c.extend(&zip64);
            }
            self.put(&c)?;
        }
        let cd_size = self.pos - cd_start;
        let n = done.len() as u64;
        let big = n >= 0xFFFF || cd_start >= 0xFFFF_FFFF || cd_size >= 0xFFFF_FFFF;
        if big {
            let eocd64 = self.pos;
            let mut z = Vec::with_capacity(76);
            z.extend(SIG_EOCD64.to_le_bytes());
            z.extend(44u64.to_le_bytes());
            z.extend(45u16.to_le_bytes());
            z.extend(45u16.to_le_bytes());
            z.extend([0u8; 8]); // disk numbers
            z.extend(n.to_le_bytes());
            z.extend(n.to_le_bytes());
            z.extend(cd_size.to_le_bytes());
            z.extend(cd_start.to_le_bytes());
            z.extend(SIG_LOCATOR64.to_le_bytes());
            z.extend(0u32.to_le_bytes());
            z.extend(eocd64.to_le_bytes());
            z.extend(1u32.to_le_bytes());
            self.put(&z)?;
        }
        let mut e = Vec::with_capacity(22);
        e.extend(SIG_EOCD.to_le_bytes());
        e.extend([0u8; 4]); // disk numbers
        let count = if big { 0xFFFF } else { n as u16 };
        e.extend(count.to_le_bytes());
        e.extend(count.to_le_bytes());
        e.extend((if big { 0xFFFF_FFFF } else { cd_size as u32 }).to_le_bytes());
        e.extend((if big { 0xFFFF_FFFF } else { cd_start as u32 }).to_le_bytes());
        e.extend(0u16.to_le_bytes());
        self.put(&e)?;
        self.finished = true;
        Ok(match &mut self.target {
            Target::Mem { buf, .. } => Some(std::mem::take(buf)),
            Target::Sink(_) => None,
        })
    }
}

/// Current time as MS-DOS `(time, date)`; the format cannot express anything before 1980.
fn dos_now() -> (u16, u16) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (days, rem) = ((secs / 86400) as i64, (secs % 86400) as u16);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u16;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u16;
    let y = (yoe + era * 400 + i64::from(m <= 2)).max(1980) as u16;
    let time = ((rem / 3600) << 11) | (((rem % 3600) / 60) << 5) | ((rem % 60) / 2);
    (time, ((y - 1980) << 9) | (m << 5) | d)
}

struct ZipWriter {
    w: Mutex<Writer>,
    deadline: Deadline,
}

impl ZipWriter {
    fn with<T>(&self, f: impl FnOnce(&mut Writer) -> Result<T, String>) -> mlua::Result<T> {
        f(&mut self.w.lock().unwrap()).map_err(mlua::Error::runtime)
    }
}

impl UserData for ZipWriter {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // `add(name)` opens an entry for `write`; `add(name, data)` writes a whole entry from a
        // string or a stream and closes it.
        methods.add_method(
            "add",
            |_, this, (name, content): (String, Option<Value>)| {
                this.with(|w| w.begin(&name))?;
                let whole = !matches!(content, None | Some(Value::Nil));
                match content {
                    None | Some(Value::Nil) => {}
                    Some(Value::String(s)) => this.with(|w| w.write(&s.as_bytes()))?,
                    Some(Value::UserData(ud)) => {
                        let mut r: Box<dyn Read + Send> = take_stream(&ud)?;
                        let mut buf = vec![0u8; CHUNK];
                        loop {
                            this.deadline.check().map_err(mlua::Error::runtime)?;
                            let n = r.read(&mut buf).map_err(mlua::Error::external)?;
                            if n == 0 {
                                break;
                            }
                            this.with(|w| w.write(&buf[..n]))?;
                        }
                    }
                    Some(_) => {
                        return Err(mlua::Error::runtime(
                            "zip entry content must be a string or a stream",
                        ))
                    }
                }
                if whole {
                    this.with(|w| w.end_entry())?;
                }
                Ok(())
            },
        );
        methods.add_method("write", |_, this, data: LuaString| {
            this.with(|w| w.write(&data.as_bytes()))
        });
        // Writes the central directory. Returns the archive for in-memory writers; with a sink,
        // close the sink afterwards to commit the upload.
        methods.add_method("finish", |lua, this, ()| {
            match this.with(|w| w.finish())? {
                Some(bytes) => Ok(Value::String(lua.create_string(&bytes)?)),
                None => Ok(Value::Nil),
            }
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("ZipWriter"));
    }
}

pub fn api(
    lua: &Lua,
    zip: &Table,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<()> {
    zip.set(
        "writer",
        lua.create_function(move |_, sink: Option<mlua::AnyUserData>| {
            let target = match sink {
                Some(ud) => Target::Sink(
                    Sink::from_userdata(&ud)
                        .ok_or_else(|| mlua::Error::runtime("ig.zip.writer expects a sink"))?,
                ),
                None => Target::Mem {
                    buf: Vec::new(),
                    cap: max_object_bytes,
                },
            };
            Ok(ZipWriter {
                w: Mutex::new(Writer {
                    target,
                    pos: 0,
                    done: Vec::new(),
                    cur: None,
                    finished: false,
                }),
                deadline: deadline.clone(),
            })
        })?,
    )
}
