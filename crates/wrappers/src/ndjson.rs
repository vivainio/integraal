//! `native.ndjson.rows(source)`: one JSON value per line, from a string or a byte stream.
//! Memory use is bounded by a single line, not by the input.

use std::io::{self, BufRead, BufReader, Read};

use crate::{
    xml::{take_stream, Limited},
    Deadline,
};
use mlua::{Lua, LuaSerdeExt, Table, Value};
use std::sync::Mutex;

/// Reads newline-delimited JSON. Blank lines are skipped.
pub(crate) struct Rows<R> {
    reader: R,
    buf: Vec<u8>,
    line: u64,
    max_line: usize,
}

impl<R: BufRead> Rows<R> {
    pub fn new(reader: R, max_line: usize) -> Self {
        Self {
            reader,
            buf: Vec::new(),
            line: 0,
            max_line,
        }
    }

    pub fn next_value(&mut self) -> Result<Option<serde_json::Value>, String> {
        loop {
            self.buf.clear();
            let n = (&mut self.reader)
                .take(self.max_line as u64 + 1)
                .read_until(b'\n', &mut self.buf)
                .map_err(|e| format!("ndjson: {e}"))?;
            if n == 0 {
                return Ok(None);
            }
            self.line += 1;
            if self.buf.len() > self.max_line && self.buf.last() != Some(&b'\n') {
                return Err(format!(
                    "ndjson: line {} exceeds {} bytes",
                    self.line, self.max_line
                ));
            }
            let line = self.buf.trim_ascii();
            if line.is_empty() {
                continue;
            }
            return serde_json::from_slice(line)
                .map(Some)
                .map_err(|e| format!("ndjson: line {}: {e}", self.line));
        }
    }
}

pub fn api(
    lua: &Lua,
    max_stream_bytes: u64,
    max_line_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<Table> {
    let nd = lua.create_table()?;
    nd.set(
        "rows",
        lua.create_function(move |lua, source: Value| {
            let reader: Box<dyn Read + Send> = match source {
                Value::String(s) => Box::new(io::Cursor::new(s.as_bytes().to_vec())),
                Value::UserData(ud) => take_stream(&ud)?,
                _ => {
                    return Err(mlua::Error::runtime(
                        "native.ndjson.rows expects a string or a stream",
                    ))
                }
            };
            let rows = Mutex::new(Rows::new(
                BufReader::new(Limited {
                    inner: reader,
                    left: max_stream_bytes,
                    deadline: deadline.clone(),
                }),
                max_line_bytes,
            ));
            lua.create_function(move |lua, ()| {
                match rows
                    .lock()
                    .unwrap()
                    .next_value()
                    .map_err(mlua::Error::runtime)?
                {
                    Some(v) => lua.to_value(&v),
                    None => Ok(Value::Nil),
                }
            })
        })?,
    )?;
    Ok(nd)
}
