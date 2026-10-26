//! Structured script failures: what went wrong in a handler and where, captured while the Luau
//! stack still exists. The engine keeps `Error` as it was (callers match on it); the details travel
//! beside it to the flow log footer.
//!
//! Nothing here records script data: a table thrown with `error({...})` is described by its keys
//! only, and frames carry chunk names, lines and function names. Deeper capture (values, locals,
//! the input event) belongs to watched scripts and goes through the debug sink instead.

use std::sync::{Arc, Mutex};

use mlua::{Lua, Value};
use serde::Serialize;

/// Most frames kept from one stack.
const MAX_FRAMES: usize = 24;
/// Longest message kept, in bytes.
const MAX_MESSAGE_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Frame {
    /// The chunk the function was defined in: `tenant/script`, or `[C]` for a native function.
    pub chunk: String,
    pub line: Option<usize>,
    pub function: Option<String>,
    pub native: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// The script did not parse, or failed while its module body ran.
    Syntax,
    /// `error(...)` or a runtime fault in the handler.
    Runtime,
    /// A native function refused or failed (the stack ends in a `[C]` frame).
    Native,
    Timeout,
    Memory,
    /// The handler returned, but its result was not usable.
    BadResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct Failure {
    pub kind: Kind,
    pub message: String,
    /// Keys of a table thrown as the error, never its values.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub error_keys: Vec<String>,
    pub frames: Vec<Frame>,
}

impl Failure {
    /// The frame to name when summarizing: the innermost one in the script's own code.
    pub fn top(&self) -> Option<&Frame> {
        self.frames
            .iter()
            .find(|f| !f.native)
            .or_else(|| self.frames.first())
    }

    /// `acme/feed:142 in parse_row`, for one-line summaries.
    pub fn location(&self) -> Option<String> {
        let f = self.top()?;
        let mut s = f.chunk.clone();
        if let Some(l) = f.line {
            s.push_str(&format!(":{l}"));
        }
        if let Some(n) = &f.function {
            s.push_str(&format!(" in {n}"));
        }
        Some(s)
    }

    /// The Luau-style traceback text of the frames.
    pub fn traceback(&self) -> String {
        let mut out = String::from("stack traceback:");
        for f in &self.frames {
            out.push_str("\n\t");
            out.push_str(&f.chunk);
            if let Some(l) = f.line {
                out.push_str(&format!(":{l}"));
            }
            if let Some(n) = &f.function {
                out.push_str(&format!(" function {n}"));
            }
        }
        out
    }
}

fn cut(mut s: String) -> String {
    if s.len() > MAX_MESSAGE_BYTES {
        let mut end = MAX_MESSAGE_BYTES;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

/// The Luau stack as it is right now, innermost first, starting `skip` levels up.
pub(crate) fn frames(lua: &Lua, skip: usize) -> Vec<Frame> {
    let mut out = Vec::new();
    for level in skip..skip + MAX_FRAMES {
        let frame = lua.inspect_stack(level, |d| {
            let src = d.source();
            let native = src.what == "C";
            Frame {
                chunk: if native {
                    "[C]".into()
                } else {
                    src.short_src.as_deref().unwrap_or("?").to_string()
                },
                line: if native { None } else { d.current_line() },
                function: d.names().name.map(|n| n.into_owned()),
                native,
            }
        });
        match frame {
            Some(f) => out.push(f),
            None => break,
        }
    }
    out
}

/// What the error handler and the interrupt saw, shared with the VM that owns them.
#[derive(Default)]
pub(crate) struct Capture {
    /// Set by the `xpcall` handler, which runs at the point of the error with the stack intact.
    handled: Option<Handled>,
    /// Set by the interrupt when the invocation's own deadline passes, in case the handler does
    /// not run for an interrupt.
    timeout_frames: Option<Vec<Frame>>,
}

struct Handled {
    message: String,
    error_keys: Vec<String>,
    frames: Vec<Frame>,
}

#[derive(Clone, Default)]
pub(crate) struct CaptureSlot(Arc<Mutex<Capture>>);

impl CaptureSlot {
    /// Called from the interrupt when the hard deadline has passed.
    pub(crate) fn timed_out(&self, lua: &Lua) {
        let mut c = self.0.lock().unwrap();
        if c.timeout_frames.is_none() {
            c.timeout_frames = Some(frames(lua, 0));
        }
    }

    /// The message handler for `xpcall`: records the stack and returns the message to throw on.
    pub(crate) fn handler(&self, lua: &Lua, err: Value) -> mlua::Result<String> {
        let (message, error_keys) = describe(&err);
        // Level 0 is this handler.
        let frames = frames(lua, 1);
        let mut c = self.0.lock().unwrap();
        c.handled = Some(Handled {
            message: cut(message.clone()),
            error_keys,
            frames,
        });
        Ok(message)
    }

    /// The failure for `kind`, from what was captured. `fallback` is the message to use when the
    /// error never reached the handler (a memory error, a syntax error).
    pub(crate) fn finish(&self, kind: Kind, fallback: &str) -> Failure {
        let mut c = self.0.lock().unwrap();
        let handled = c.handled.take();
        let timeout_frames = c.timeout_frames.take();
        match handled {
            Some(h) => Failure {
                // A runtime error whose innermost frame is a native function other than `error`
                // came from the host API (a refused `ig.db` call, say), not from script logic.
                kind: match (kind, h.frames.first()) {
                    (Kind::Runtime, Some(f))
                        if f.native && f.function.as_deref() != Some("error") =>
                    {
                        Kind::Native
                    }
                    _ => kind,
                },
                message: h.message,
                error_keys: h.error_keys,
                frames: if kind == Kind::Timeout && h.frames.is_empty() {
                    timeout_frames.unwrap_or_default()
                } else {
                    h.frames
                },
            },
            None => Failure {
                kind,
                message: cut(fallback.to_string()),
                error_keys: Vec::new(),
                frames: timeout_frames.unwrap_or_default(),
            },
        }
    }
}

/// The message of an error value, and the keys when it is a table.
fn describe(err: &Value) -> (String, Vec<String>) {
    match err {
        Value::String(s) => (s.to_string_lossy().to_string(), Vec::new()),
        Value::Table(t) => {
            let mut keys: Vec<String> = t
                .pairs::<Value, Value>()
                .filter_map(Result::ok)
                .map(|(k, _)| match k {
                    Value::String(s) => s.to_string_lossy().to_string(),
                    other => format!("[{}]", other.type_name()),
                })
                .take(16)
                .collect();
            keys.sort();
            (
                format!("error object (table with keys: {})", keys.join(", ")),
                keys,
            )
        }
        other => (
            other
                .to_string()
                .unwrap_or_else(|_| format!("<{}>", other.type_name())),
            Vec::new(),
        ),
    }
}
