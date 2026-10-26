use mlua::{Function, Lua, MultiValue, Table};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// Raised when the invocation's own deadline passes. Not catchable by the script in any useful
/// way: the interrupt keeps firing.
pub const TIMEOUT: &str = "__timeout__";
/// Raised inside a `ig.deadline.with` block whose time is up (or an enclosing block's).
pub const BLOCK_EXPIRED: &str = "__deadline__";

struct Block {
    id: u64,
    at: Instant,
    /// Set when the VM was interrupted (or a native call refused) on account of this block.
    fired: bool,
}

struct Inner {
    hard: Instant,
    base: Instant,
    /// The earliest of `hard` and every open block, in nanoseconds after `base`. Kept in an atomic
    /// so the per-instruction interrupt check stays cheap.
    effective: AtomicU64,
    blocks: Mutex<Vec<Block>>,
    next_id: AtomicU64,
}

/// The invocation deadline plus a stack of tighter, nested ones opened by `ig.deadline.with`.
/// Shared by every native function and the VM interrupt; a block can only shorten the budget.
#[derive(Clone)]
pub struct Deadline(Arc<Inner>);

impl std::fmt::Debug for Deadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Deadline({:?})", self.hard())
    }
}

impl Deadline {
    pub fn new(hard: Instant) -> Self {
        let base = Instant::now().min(hard);
        Self(Arc::new(Inner {
            hard,
            base,
            effective: AtomicU64::new(nanos(hard, base)),
            blocks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
        }))
    }

    /// The invocation's own deadline.
    pub fn hard(&self) -> Instant {
        self.0.hard
    }

    /// The earliest deadline in force right now.
    pub fn effective(&self) -> Instant {
        self.0.base + Duration::from_nanos(self.0.effective.load(Ordering::Relaxed))
    }

    pub fn remaining(&self) -> Duration {
        self.effective().saturating_duration_since(Instant::now())
    }

    fn expired(&self) -> bool {
        nanos(Instant::now(), self.0.base) >= self.0.effective.load(Ordering::Relaxed)
    }

    /// `Err` with the marker native functions and the interrupt raise once time is up.
    /// Records which block ran out, so `ig.deadline.with` knows the error is its own.
    pub fn check(&self) -> Result<(), &'static str> {
        if !self.expired() {
            return Ok(());
        }
        if Instant::now() >= self.0.hard {
            return Err(TIMEOUT);
        }
        let now = Instant::now();
        let mut blocks = self.0.blocks.lock().unwrap();
        // The outermost expired block is the one to unwind to.
        if let Some(b) = blocks.iter_mut().find(|b| now >= b.at) {
            b.fired = true;
        }
        Err(BLOCK_EXPIRED)
    }

    fn push(&self, at: Instant) -> u64 {
        let id = self.0.next_id.fetch_add(1, Ordering::Relaxed);
        let mut blocks = self.0.blocks.lock().unwrap();
        blocks.push(Block {
            id,
            at,
            fired: false,
        });
        self.refresh(&blocks);
        id
    }

    /// Closes the block; returns whether it interrupted anything, whether its time has passed, and
    /// whether an enclosing block (or the invocation) has run out too.
    fn pop(&self, id: u64) -> (bool, bool, bool) {
        let now = Instant::now();
        let mut blocks = self.0.blocks.lock().unwrap();
        let pos = blocks.iter().rposition(|b| b.id == id);
        let own = pos.map(|p| blocks.remove(p));
        self.refresh(&blocks);
        let outer = Instant::now() >= self.0.hard || blocks.iter().any(|b| now >= b.at);
        let (fired, past) = own.map_or((false, false), |b| (b.fired, now >= b.at));
        (fired, past, outer)
    }

    fn refresh(&self, blocks: &[Block]) {
        let at = blocks
            .iter()
            .map(|b| b.at)
            .min()
            .map_or(self.0.hard, |m| m.min(self.0.hard));
        self.0
            .effective
            .store(nanos(at, self.0.base), Ordering::Relaxed);
    }
}

fn nanos(t: Instant, base: Instant) -> u64 {
    t.saturating_duration_since(base)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

/// Runs `f` with a block ending at `at` pushed on the deadline stack, and translates the block
/// running out into "deadline exceeded" (see `api`).
pub(crate) fn run_block(
    deadline: &Deadline,
    at: Instant,
    f: &Function,
) -> mlua::Result<MultiValue> {
    let id = deadline.push(at);
    let res = f.call::<MultiValue>(());
    let (fired, past, outer_expired) = deadline.pop(id);
    if outer_expired {
        // Unwinding to an enclosing block or the invocation limit: not ours to translate.
        return Err(mlua::Error::runtime(match deadline.check() {
            Err(m) => m,
            Ok(()) => BLOCK_EXPIRED,
        }));
    }
    match res {
        // Finished normally after the time was up, or swallowed the interrupt with a pcall.
        Ok(_) if fired => Err(mlua::Error::runtime("deadline exceeded")),
        // Failed because of the clock (a native call timed out, say), not on its own.
        Err(_) if fired || past => Err(mlua::Error::runtime("deadline exceeded")),
        other => other,
    }
}

/// `ig.deadline.with(ms, fn)`: runs `fn()` and returns its results. If `fn` is still running
/// after `ms` (or the invocation's own time is shorter), it is interrupted and `with` raises
/// "deadline exceeded", which the caller can `pcall`. Blocks nest; the earliest deadline wins.
pub fn api(lua: &Lua, deadline: Deadline) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    t.set(
        "with",
        lua.create_function(move |_, (ms, f): (u64, Function)| {
            if ms == 0 {
                return Err(mlua::Error::runtime("deadline must be at least 1 ms"));
            }
            run_block(&deadline, Instant::now() + Duration::from_millis(ms), &f)
        })?,
    )?;
    Ok(t)
}
