//! Write side of streams: `native.bucket.<name>.create(key)` returns a sink that uploads as it is
//! written, so output larger than memory works. `native.zip.writer` writes into one.

use std::{
    io,
    sync::{Arc, Mutex},
    time::Instant,
};

use mlua::{AnyUserData, LuaString, MetaMethod, UserData, UserDataMethods};

use crate::ObjectWriter;

struct State {
    w: Option<Box<dyn ObjectWriter>>,
    written: u64,
    left: u64,
    deadline: Instant,
}

/// Shared handle to an upload; the script's userdata and a zip writer can both hold one.
#[derive(Clone)]
pub struct Sink(Arc<Mutex<State>>);

impl Sink {
    pub fn new(w: Box<dyn ObjectWriter>, max_bytes: u64, deadline: Instant) -> Self {
        Self(Arc::new(Mutex::new(State {
            w: Some(w),
            written: 0,
            left: max_bytes,
            deadline,
        })))
    }

    pub fn from_userdata(ud: &AnyUserData) -> Option<Sink> {
        ud.borrow::<Sink>().ok().map(|s| s.clone())
    }

    pub fn write_bytes(&self, data: &[u8]) -> Result<(), String> {
        let mut st = self.0.lock().unwrap();
        if Instant::now() >= st.deadline {
            return Err("__timeout__".into());
        }
        if data.len() as u64 > st.left {
            return Err("sink exceeds the stream size limit".into());
        }
        let w = st.w.as_mut().ok_or("sink is already closed")?;
        w.write(data)?;
        st.left -= data.len() as u64;
        st.written += data.len() as u64;
        Ok(())
    }

    fn close(&self) -> Result<u64, String> {
        let mut st = self.0.lock().unwrap();
        let w = st.w.take().ok_or("sink is already closed")?;
        w.commit()?;
        Ok(st.written)
    }

    fn abort(&self) {
        if let Some(w) = self.0.lock().unwrap().w.take() {
            w.abort();
        }
    }
}

impl io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_bytes(buf).map_err(io::Error::other)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl UserData for Sink {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("write", |_, this, data: LuaString| {
            this.write_bytes(&data.as_bytes())
                .map_err(mlua::Error::runtime)
        });
        // Commits the upload and returns the number of bytes written.
        methods.add_method("close", |_, this, ()| {
            this.close().map_err(mlua::Error::runtime)
        });
        // Discards the upload.
        methods.add_method("abort", |_, this, ()| {
            this.abort();
            Ok(())
        });
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("Sink"));
    }
}
