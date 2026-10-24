//! `native.xml.writer(sink?, opts?)`: a streaming XML generator. Text and attribute values are
//! escaped, names are validated and well-formedness (balanced tags, a single root) is enforced,
//! so a script cannot emit a broken document by accident. Output goes straight into a bucket
//! sink, or is built in memory (capped at the object size limit) and returned by `finish()`.

use std::sync::Mutex;

use crate::{sink::Sink, Deadline};
use mlua::{Lua, LuaString, MetaMethod, Table, UserData, UserDataMethods, Value, Variadic};

/// Pending output is handed to a sink in pieces of about this size.
const FLUSH_AT: usize = 64 * 1024;
const MAX_DEPTH: usize = 256;

enum Target {
    Sink(Sink),
    Mem { cap: usize },
}

struct Frame {
    tag: String,
    has_child: bool,
    has_text: bool,
}

struct Writer {
    target: Target,
    buf: Vec<u8>,
    /// Bytes already handed to the sink.
    flushed: u64,
    stack: Vec<Frame>,
    /// The last start tag is still open (`<a` + attributes, no `>` yet).
    pending: bool,
    root_done: bool,
    started: bool,
    finished: bool,
    declaration: bool,
    indent: Option<usize>,
}

fn is_name_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == ':'
}

fn is_name_char(c: char) -> bool {
    is_name_start(c) || c.is_numeric() || matches!(c, '-' | '.' | '\u{b7}')
}

fn check_name(kind: &str, name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if is_name_start(c) && chars.all(is_name_char) => Ok(()),
        _ => Err(format!("invalid {kind} name {name:?}")),
    }
}

fn is_xml_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r')
        || ('\u{20}'..='\u{d7ff}').contains(&c)
        || ('\u{e000}'..='\u{fffd}').contains(&c)
        || c >= '\u{10000}'
}

fn check_chars(s: &str) -> Result<(), String> {
    match s.chars().find(|c| !is_xml_char(*c)) {
        None => Ok(()),
        Some(c) => Err(format!(
            "character U+{:04X} is not allowed in XML",
            c as u32
        )),
    }
}

pub(crate) fn escape(out: &mut Vec<u8>, s: &str, attr: bool) {
    for c in s.chars() {
        match c {
            '&' => out.extend_from_slice(b"&amp;"),
            '<' => out.extend_from_slice(b"&lt;"),
            '>' => out.extend_from_slice(b"&gt;"),
            '\r' => out.extend_from_slice(b"&#13;"),
            '"' if attr => out.extend_from_slice(b"&quot;"),
            '\t' if attr => out.extend_from_slice(b"&#9;"),
            '\n' if attr => out.extend_from_slice(b"&#10;"),
            c => {
                let mut b = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
            }
        }
    }
}

impl Writer {
    fn len(&self) -> u64 {
        self.flushed + self.buf.len() as u64
    }

    fn live(&self) -> Result<(), String> {
        if self.finished {
            Err("writer is already finished".into())
        } else {
            Ok(())
        }
    }

    /// Writes the XML declaration before the first thing, if asked for.
    fn begin(&mut self) {
        if !self.started {
            self.started = true;
            if self.declaration {
                self.buf
                    .extend_from_slice(br#"<?xml version="1.0" encoding="UTF-8"?>"#);
            }
        }
    }

    fn close_tag(&mut self) {
        if self.pending {
            self.buf.push(b'>');
            self.pending = false;
        }
    }

    fn newline(&mut self, depth: usize) {
        if let Some(n) = self.indent {
            if self.len() > 0 {
                self.buf.push(b'\n');
            }
            self.buf.resize(self.buf.len() + n * depth, b' ');
        }
    }

    /// Common start of every node that is not text: closes the open tag and indents.
    fn begin_node(&mut self) {
        self.begin();
        self.close_tag();
        let depth = self.stack.len();
        match self.stack.last_mut() {
            Some(f) => {
                let inline = f.has_text;
                f.has_child = true;
                if !inline {
                    self.newline(depth);
                }
            }
            None => self.newline(0),
        }
    }

    fn start(&mut self, tag: &str, attrs: &[(String, String)]) -> Result<(), String> {
        self.live()?;
        check_name("element", tag)?;
        if self.stack.is_empty() && self.root_done {
            return Err("document already has a root element".into());
        }
        if self.stack.len() >= MAX_DEPTH {
            return Err("nesting too deep".into());
        }
        for (k, v) in attrs {
            check_name("attribute", k)?;
            check_chars(v)?;
        }
        self.begin_node();
        self.buf.push(b'<');
        self.buf.extend_from_slice(tag.as_bytes());
        for (k, v) in attrs {
            self.buf.push(b' ');
            self.buf.extend_from_slice(k.as_bytes());
            self.buf.extend_from_slice(b"=\"");
            escape(&mut self.buf, v, true);
            self.buf.push(b'"');
        }
        self.pending = true;
        self.stack.push(Frame {
            tag: tag.to_string(),
            has_child: false,
            has_text: false,
        });
        Ok(())
    }

    fn end(&mut self) -> Result<(), String> {
        self.live()?;
        let f = self.stack.pop().ok_or("end() without a matching start()")?;
        if self.pending {
            self.buf.extend_from_slice(b"/>");
            self.pending = false;
        } else {
            if f.has_child && !f.has_text {
                let depth = self.stack.len();
                self.newline(depth);
            }
            self.buf.extend_from_slice(b"</");
            self.buf.extend_from_slice(f.tag.as_bytes());
            self.buf.push(b'>');
        }
        if self.stack.is_empty() {
            self.root_done = true;
        }
        Ok(())
    }

    fn text(&mut self, s: &str) -> Result<(), String> {
        self.live()?;
        check_chars(s)?;
        if self.stack.is_empty() {
            return Err("text outside the root element".into());
        }
        self.close_tag();
        self.stack.last_mut().unwrap().has_text = true;
        escape(&mut self.buf, s, false);
        Ok(())
    }

    fn cdata(&mut self, s: &str) -> Result<(), String> {
        self.live()?;
        check_chars(s)?;
        if self.stack.is_empty() {
            return Err("cdata outside the root element".into());
        }
        self.close_tag();
        self.stack.last_mut().unwrap().has_text = true;
        self.buf.extend_from_slice(b"<![CDATA[");
        // "]]>" cannot appear inside a section; split it across two.
        self.buf
            .extend_from_slice(s.replace("]]>", "]]]]><![CDATA[>").as_bytes());
        self.buf.extend_from_slice(b"]]>");
        Ok(())
    }

    fn comment(&mut self, s: &str) -> Result<(), String> {
        self.live()?;
        check_chars(s)?;
        if s.contains("--") || s.ends_with('-') {
            return Err("a comment cannot contain \"--\" or end with \"-\"".into());
        }
        self.begin_node();
        self.buf.extend_from_slice(b"<!--");
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.extend_from_slice(b"-->");
        Ok(())
    }

    /// Hands buffered output on: to the sink, or checks the in-memory cap.
    fn flush(&mut self, force: bool) -> Result<(), String> {
        match &self.target {
            Target::Sink(s) => {
                if force || self.buf.len() >= FLUSH_AT {
                    s.write_bytes(&self.buf)?;
                    self.flushed += self.buf.len() as u64;
                    self.buf.clear();
                }
            }
            Target::Mem { cap } => {
                if self.buf.len() > *cap {
                    return Err(
                        "document exceeds the object size limit; write into a bucket sink to stream it"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.live()?;
        if let Some(f) = self.stack.last() {
            return Err(format!("element <{}> is not closed", f.tag));
        }
        if !self.root_done {
            return Err("document has no root element".into());
        }
        if self.indent.is_some() {
            self.buf.push(b'\n');
        }
        self.finished = true;
        self.flush(true)?;
        Ok(match self.target {
            Target::Mem { .. } => Some(std::mem::take(&mut self.buf)),
            Target::Sink(_) => None,
        })
    }
}

struct XmlWriter {
    w: Mutex<Writer>,
    deadline: Deadline,
}

fn str_arg<'a>(v: &'a LuaString, what: &str) -> mlua::Result<std::borrow::Cow<'a, str>> {
    v.to_str()
        .map(|s| std::borrow::Cow::Owned(s.to_string()))
        .map_err(|_| mlua::Error::runtime(format!("xml writer: {what} is not valid UTF-8")))
}

/// Attribute table to sorted pairs, so output does not depend on table iteration order.
fn attrs_arg(t: Option<Table>) -> mlua::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    if let Some(t) = t {
        for pair in t.pairs::<String, Value>() {
            let (k, v) = pair?;
            let v = match v {
                Value::String(s) => str_arg(&s, "attribute value")?.into_owned(),
                Value::Integer(i) => i.to_string(),
                Value::Number(n) => n.to_string(),
                Value::Boolean(b) => b.to_string(),
                _ => {
                    return Err(mlua::Error::runtime(format!(
                        "xml writer: attribute {k:?} must be a string, number or boolean"
                    )))
                }
            };
            out.push((k, v));
        }
        out.sort();
    }
    Ok(out)
}

impl XmlWriter {
    fn run<T>(&self, f: impl FnOnce(&mut Writer) -> Result<T, String>) -> mlua::Result<T> {
        self.deadline.check().map_err(mlua::Error::runtime)?;
        let mut w = self.w.lock().unwrap();
        let r = f(&mut w).and_then(|v| w.flush(false).map(|_| v));
        r.map_err(|e| mlua::Error::runtime(format!("xml writer: {e}")))
    }
}

impl UserData for XmlWriter {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method(
            "start",
            |_, this, (tag, attrs): (LuaString, Option<Table>)| {
                let tag = str_arg(&tag, "tag")?;
                let attrs = attrs_arg(attrs)?;
                this.run(|w| w.start(&tag, &attrs))
            },
        );
        methods.add_method("close", |_, this, ()| this.run(|w| w.end()));
        methods.add_method("text", |_, this, s: LuaString| {
            let s = str_arg(&s, "text")?;
            this.run(|w| w.text(&s))
        });
        methods.add_method("cdata", |_, this, s: LuaString| {
            let s = str_arg(&s, "cdata")?;
            this.run(|w| w.cdata(&s))
        });
        methods.add_method("comment", |_, this, s: LuaString| {
            let s = str_arg(&s, "comment")?;
            this.run(|w| w.comment(&s))
        });
        // element(tag, attrs?, text?): a complete element with optional text content.
        methods.add_method(
            "element",
            |_, this, (tag, rest): (LuaString, Variadic<Value>)| {
                let tag = str_arg(&tag, "tag")?;
                let mut attrs = None;
                let mut text = None;
                for v in rest {
                    match v {
                        Value::Table(t) => attrs = Some(t),
                        Value::String(s) => text = Some(str_arg(&s, "text")?.into_owned()),
                        Value::Integer(i) => text = Some(i.to_string()),
                        Value::Number(n) => text = Some(n.to_string()),
                        Value::Nil => {}
                        _ => {
                            return Err(mlua::Error::runtime(
                                "xml writer: element expects (tag, attrs?, text?)",
                            ))
                        }
                    }
                }
                let attrs = attrs_arg(attrs)?;
                this.run(|w| {
                    w.start(&tag, &attrs)?;
                    if let Some(t) = text.filter(|t| !t.is_empty()) {
                        w.text(&t)?;
                    }
                    w.end()
                })
            },
        );
        methods.add_method("finish", |lua, this, ()| {
            match this.run(|w| w.finish())? {
                Some(b) => Ok(Value::String(lua.create_string(b)?)),
                None => Ok(Value::Nil),
            }
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("XmlWriter"));
    }
}

pub fn api(
    lua: &Lua,
    xml: &Table,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<()> {
    xml.set(
        "writer",
        lua.create_function(move |_, (a, b): (Option<Value>, Option<Value>)| {
            // writer(sink?, opts?) or writer(opts)
            let (sink, opts) = match (a, b) {
                (Some(Value::Table(t)), None) => (None, Some(t)),
                (Some(Value::UserData(ud)), o) => (
                    Some(ud),
                    o.and_then(|v| match v {
                        Value::Table(t) => Some(t),
                        _ => None,
                    }),
                ),
                (None | Some(Value::Nil), o) => (
                    None,
                    o.and_then(|v| match v {
                        Value::Table(t) => Some(t),
                        _ => None,
                    }),
                ),
                _ => {
                    return Err(mlua::Error::runtime(
                        "native.xml.writer expects (sink?, opts?)",
                    ))
                }
            };
            let target = match sink {
                Some(ud) => Target::Sink(
                    Sink::from_userdata(&ud)
                        .ok_or_else(|| mlua::Error::runtime("native.xml.writer expects a sink"))?,
                ),
                None => Target::Mem {
                    cap: max_object_bytes,
                },
            };
            let (declaration, indent) = match opts {
                Some(o) => (
                    o.get::<Option<bool>>("declaration")?.unwrap_or(true),
                    o.get::<Option<usize>>("indent")?.map(|n| n.min(16)),
                ),
                None => (true, None),
            };
            Ok(XmlWriter {
                w: Mutex::new(Writer {
                    target,
                    buf: Vec::new(),
                    flushed: 0,
                    stack: Vec::new(),
                    pending: false,
                    root_done: false,
                    started: false,
                    finished: false,
                    declaration,
                    indent,
                }),
                deadline: deadline.clone(),
            })
        })?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn run(code: &str) -> String {
        let lua = Lua::new();
        let xml = lua.create_table().unwrap();
        let d = Deadline::new(Instant::now() + Duration::from_secs(30));
        api(&lua, &xml, 1 << 20, d).unwrap();
        lua.globals().set("xml", xml).unwrap();
        lua.load(code).eval().unwrap()
    }

    #[test]
    fn escapes_and_sorts_attributes() {
        let out = run(r#"
            local w = xml.writer({ declaration = false })
            w:start("a", { z = "1", b = 'q"<&\n' })
            w:text("x < y & z > \r")
            w:start("e"); w:close()
            w:cdata("a]]>b")
            w:comment(" hi ")
            w:close()
            return w:finish()"#);
        assert_eq!(
            out,
            "<a b=\"q&quot;&lt;&amp;&#10;\" z=\"1\">x &lt; y &amp; z &gt; &#13;<e/><![CDATA[a]]]]><![CDATA[>b]]><!-- hi --></a>"
        );
    }

    #[test]
    fn declaration_and_indent() {
        let out = run(r#"
            local w = xml.writer({ indent = 2 })
            w:start("r")
            w:element("a", { id = 1 }, "t")
            w:start("b"); w:element("c"); w:close()
            w:close()
            return w:finish()"#);
        assert_eq!(
            out,
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<r>\n  <a id=\"1\">t</a>\n  <b>\n    <c/>\n  </b>\n</r>\n"
        );
    }

    #[test]
    fn rejects_malformed_use() {
        let lua = Lua::new();
        let xml = lua.create_table().unwrap();
        let d = Deadline::new(Instant::now() + Duration::from_secs(30));
        api(&lua, &xml, 1 << 20, d).unwrap();
        lua.globals().set("xml", xml).unwrap();
        for code in [
            r#"xml.writer():start("1bad")"#,
            r#"xml.writer():start("a", { ["x y"] = "1" })"#,
            r#"xml.writer():close()"#,
            r#"xml.writer():text("x")"#,
            r#"local w = xml.writer(); w:start("a"); w:finish()"#,
            r#"xml.writer():finish()"#,
            r#"local w = xml.writer(); w:element("a"); w:start("b")"#,
            r#"local w = xml.writer(); w:start("a"); w:text("\0")"#,
            r#"local w = xml.writer(); w:start("a"); w:comment("a--b")"#,
            r#"local w = xml.writer(); w:element("a"); w:finish(); w:start("b")"#,
        ] {
            assert!(lua.load(code).exec().is_err(), "should fail: {code}");
        }
    }
}
