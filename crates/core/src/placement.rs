//! Placement: which worker size class a message should run on.
//!
//! A tenant's `config.json` can name a static `placement.class`, and optionally a placement script
//! whose `on_place`/`place` handler picks a class per message. Scripts run here under a stripped
//! sandbox (no buckets, table, vault or HTTP) with a tiny time and heap budget, and whatever they
//! return is checked against the tenant's allowed classes. Placement never fails a message: on any
//! problem the tenant's static class (or none, meaning the deployment default) is used instead.
//!
//! A batch is grouped by tenant. Each tenant gets one Luau VM for the whole batch, so the script
//! is compiled once. Every message still runs in a fresh environment (a new module evaluation
//! with its own globals), and the VM is thrown away after an error, timeout or heavy heap use.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use integraal_loader::ScriptKey;
use integraal_wrappers::parse_runtime_version;
use mlua::{
    chunk::{ChunkMode, Compiler},
    Function, LuaSerdeExt, Table, VmState,
};

use crate::{
    engine::{internal, new_vm, script_err, Limits, Services, TenantConfig, Vm},
    Engine, Error, Invocation, Trigger,
};

/// Budgets for placement scripts.
#[derive(Debug, Clone)]
pub struct PlaceOptions {
    /// One message's script run.
    pub timeout: Duration,
    /// Heap limit of a tenant's placement VM.
    pub memory_bytes: usize,
    /// The whole batch. Messages reached after this are placed without running their script.
    pub budget: Duration,
}

impl Default for PlaceOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(500),
            memory_bytes: 16 * 1024 * 1024,
            budget: Duration::from_secs(30),
        }
    }
}

/// Where one message goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    /// The class, or `None` for the deployment's default.
    pub class: Option<String>,
    /// Why the script's answer was not used, if it was not.
    pub error: Option<String>,
}

impl Placed {
    fn fallback(class: Option<String>, error: impl Into<String>) -> Self {
        Self {
            class,
            error: Some(error.into()),
        }
    }
}

/// Tenants placed concurrently.
const CONCURRENT_TENANTS: usize = 16;
/// A VM is rebuilt after this many messages, whatever its heap looks like.
const RECYCLE_AFTER: usize = 1000;

impl Engine {
    /// Places each invocation; the result is index-aligned with `items`. The invocation's `event`
    /// is what the placement script sees.
    pub async fn place_batch(&self, items: Vec<Invocation>, opts: &PlaceOptions) -> Vec<Placed> {
        let cutoff = Instant::now() + opts.budget;
        let mut out = vec![Placed::fallback(None, "not placed"); items.len()];
        let mut by_tenant: HashMap<String, Vec<(usize, serde_json::Value)>> = HashMap::new();
        for (i, inv) in items.into_iter().enumerate() {
            by_tenant
                .entry(inv.tenant)
                .or_default()
                .push((i, inv.event));
        }
        let mut tenants: Vec<_> = by_tenant.into_iter().collect();
        tenants.sort_by(|a, b| a.0.cmp(&b.0));

        while !tenants.is_empty() {
            let window: Vec<_> = tenants
                .drain(..CONCURRENT_TENANTS.min(tenants.len()))
                .collect();
            let mut set = tokio::task::JoinSet::new();
            for (tenant, events) in window {
                let (engine, opts) = (self.clone(), opts.clone());
                set.spawn(async move {
                    let (indices, events): (Vec<usize>, Vec<_>) = events.into_iter().unzip();
                    let placed = engine.place_tenant(&tenant, events, &opts, cutoff).await;
                    (indices, placed)
                });
            }
            while let Some(done) = set.join_next().await {
                match done {
                    Ok((indices, placed)) => {
                        for (i, p) in indices.into_iter().zip(placed) {
                            out[i] = p;
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "placement task failed"),
                }
            }
        }
        out
    }

    async fn place_tenant(
        &self,
        tenant: &str,
        events: Vec<serde_json::Value>,
        opts: &PlaceOptions,
        cutoff: Instant,
    ) -> Vec<Placed> {
        let n = events.len();
        let config = match self.tenant_config(tenant).await {
            Ok(c) => c,
            Err(e) => return vec![Placed::fallback(None, e.to_string()); n],
        };
        let default = config.placement.class.clone();
        let Some(script) = config.placement.script.clone() else {
            return vec![
                Placed {
                    class: default,
                    error: None,
                };
                n
            ];
        };
        let source = match ScriptKey::new(tenant, &script, None) {
            Ok(key) => match self.loader().load(&key).await {
                Ok(s) => s.source.clone(),
                Err(e) => return vec![Placed::fallback(default, e.to_string()); n],
            },
            Err(e) => return vec![Placed::fallback(default, e.to_string()); n],
        };

        let mut allowed = config.placement.classes.clone();
        allowed.extend(default.clone());
        let (tenant, opts) = (tenant.to_string(), opts.clone());
        let answers = tokio::task::spawn_blocking(move || {
            run_batch(&tenant, &script, &source, &config, &events, &opts, cutoff)
        })
        .await;
        match answers {
            Ok(answers) => answers
                .into_iter()
                .map(|a| decide(&default, &allowed, a))
                .collect(),
            Err(e) => vec![Placed::fallback(default, format!("placement panicked: {e}")); n],
        }
    }
}

/// Checks a script's answer against what the tenant may use.
fn decide(
    default: &Option<String>,
    allowed: &[String],
    answer: Result<Option<String>, String>,
) -> Placed {
    match answer {
        Ok(None) => Placed {
            class: default.clone(),
            error: None,
        },
        Ok(Some(c)) if allowed.contains(&c) => Placed {
            class: Some(c),
            error: None,
        },
        Ok(Some(c)) => Placed::fallback(
            default.clone(),
            format!("class {c:?} is not allowed for this tenant"),
        ),
        Err(e) => Placed::fallback(default.clone(), e),
    }
}

/// A tenant's placement VM, with the knob that sets when the current message must be done.
struct Placer {
    vm: Vm,
    base: Instant,
    until: Arc<AtomicU64>,
    served: usize,
}

fn build_placer(
    version: u32,
    tenant: &str,
    script: &str,
    config: &TenantConfig,
    opts: &PlaceOptions,
    cutoff: Instant,
) -> Result<Placer, Error> {
    let limits = Limits {
        timeout: cutoff
            .saturating_duration_since(Instant::now())
            .max(Duration::from_millis(1)),
        memory_bytes: opts.memory_bytes,
        max_http_requests: 0,
        max_data_ops: 0,
        ..Limits::default()
    };
    // Only `vars` carries over: no http policy and no buckets, so the native API has nothing to reach.
    let stripped = TenantConfig {
        vars: config.vars.clone(),
        ..TenantConfig::default()
    };
    let vm = new_vm(
        version,
        tenant,
        script,
        &limits,
        &Services::bare(),
        &stripped,
    )?;

    // The per-message clock replaces the whole-VM interrupt installed by `new_vm`.
    let base = Instant::now();
    let until = Arc::new(AtomicU64::new(0));
    let (watch, b) = (until.clone(), base);
    vm.lua.set_interrupt(move |_| {
        if b.elapsed().as_nanos() as u64 >= watch.load(Ordering::Relaxed) {
            Err(mlua::Error::runtime("__timeout__")) // the marker `script_err` maps to a timeout
        } else {
            Ok(VmState::Continue)
        }
    });
    Ok(Placer {
        vm,
        base,
        until,
        served: 0,
    })
}

/// Runs the placement script for each event of one tenant. Never panics the batch: every event
/// gets an answer, and any failure retires the VM.
fn run_batch(
    tenant: &str,
    script: &str,
    source: &str,
    config: &TenantConfig,
    events: &[serde_json::Value],
    opts: &PlaceOptions,
    cutoff: Instant,
) -> Vec<Result<Option<String>, String>> {
    let prepared = parse_runtime_version(source)
        .map_err(|e| e.to_string())
        .and_then(|v| {
            Compiler::new()
                .compile(source)
                .map(|b| (v, b))
                .map_err(|e| e.to_string())
        });
    let (version, bytecode) = match prepared {
        Ok(p) => p,
        Err(e) => return vec![Err(e); events.len()],
    };

    let mut placer: Option<Placer> = None;
    let mut answers = Vec::with_capacity(events.len());
    for event in events {
        if Instant::now() >= cutoff {
            answers.push(Err("placement time budget used up".to_string()));
            continue;
        }
        if placer.is_none() {
            match build_placer(version, tenant, script, config, opts, cutoff) {
                Ok(p) => placer = Some(p),
                Err(e) => {
                    answers.push(Err(e.to_string()));
                    continue;
                }
            }
        }
        let p = placer.as_mut().expect("built above");
        let by = (Instant::now() + opts.timeout).min(cutoff);
        p.until.store(
            by.saturating_duration_since(p.base).as_nanos() as u64,
            Ordering::Relaxed,
        );
        p.served += 1;
        let answer = place_one(&p.vm, tenant, script, &bytecode, event, cutoff);
        let heavy = p.vm.lua.used_memory() > opts.memory_bytes / 2 || p.served >= RECYCLE_AFTER;
        if answer.is_err() || heavy {
            placer = None;
        }
        answers.push(answer.map_err(|e| e.to_string()));
    }
    answers
}

/// One message: evaluates the script in a fresh environment (so globals and module-level state
/// from earlier messages are gone) and calls its handler.
fn place_one(
    vm: &Vm,
    tenant: &str,
    script: &str,
    bytecode: &[u8],
    event: &serde_json::Value,
    cutoff: Instant,
) -> Result<Option<String>, Error> {
    let lua = &vm.lua;
    let env = lua.create_table().map_err(internal)?;
    let meta = lua.create_table().map_err(internal)?;
    meta.set("__index", lua.globals()).map_err(internal)?;
    env.set_metatable(Some(meta)).map_err(internal)?;

    let module: Table = lua
        .load(bytecode)
        .set_name(format!("={tenant}/{script}"))
        .set_mode(ChunkMode::Binary)
        .set_environment(env)
        .into_function()
        .and_then(|f| f.call(()))
        .map_err(|e| script_err(e, cutoff))?;

    let handlers = Trigger::Place.handlers();
    let handler: Function = handlers
        .iter()
        .find_map(|name| module.get::<Option<Function>>(*name).ok().flatten())
        .ok_or(Error::NoHandler(Trigger::Place, handlers))?;

    let ctx = lua.create_table().map_err(internal)?;
    ctx.set("tenant", tenant).map_err(internal)?;
    ctx.set("script", script).map_err(internal)?;
    ctx.set("trigger", "place").map_err(internal)?;
    ctx.set("runtime", vm.version).map_err(internal)?;
    let event = lua.to_value(event).map_err(internal)?;
    let out: mlua::Value = handler
        .call((ctx, event))
        .map_err(|e| script_err(e, cutoff))?;
    let result: serde_json::Value = lua
        .from_value(out)
        .map_err(|e| Error::Script(format!("placement result is not JSON-serializable: {e}")))?;
    match result {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s)),
        serde_json::Value::Object(o) => match o.get("class") {
            Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(_) => Err(Error::Script("placement `class` must be a string".into())),
        },
        _ => Err(Error::Script(
            "placement must return a class name or { class }".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use integraal_loader::{CachedLoader, Fetched, LoadError, MemoryStore, ScriptStore};
    use serde_json::json;

    struct Shared(Arc<MemoryStore>);
    #[async_trait::async_trait]
    impl ScriptStore for Shared {
        async fn fetch(&self, k: &ScriptKey, e: Option<&str>) -> Result<Fetched, LoadError> {
            self.0.fetch(k, e).await
        }
    }

    /// Tenant `acme` with the given placement script and `config.json` placement block.
    fn engine(script: &str, placement: &str) -> Engine {
        let store = Arc::new(MemoryStore::default());
        store.put(&ScriptKey::new("acme", "p", None).unwrap(), script);
        store.put(
            &ScriptKey::config("acme").unwrap(),
            &format!(r#"{{"placement": {placement}}}"#),
        );
        Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))),
            Limits::default(),
        )
    }

    fn inv(event: serde_json::Value) -> Invocation {
        Invocation {
            tenant: "acme".into(),
            script: None,
            version: None,
            trigger: Trigger::Queue,
            event,
            attempt: 0,
        }
    }

    fn class(p: &Placed) -> Option<&str> {
        p.class.as_deref()
    }

    const PICK: &str = r#"{"class":"small","classes":["large"],"script":"p"}"#;

    #[tokio::test]
    async fn static_class_needs_no_script() {
        let e = engine("", r#"{"class":"large"}"#);
        let p = e
            .place_batch(vec![inv(json!({}))], &PlaceOptions::default())
            .await;
        assert_eq!(
            p,
            [Placed {
                class: Some("large".into()),
                error: None
            }]
        );
    }

    #[tokio::test]
    async fn script_picks_a_class_per_message_and_answers_are_checked() {
        let e = engine(
            r#"--!runtime 1
            return { on_place = function(ctx, ev)
              if ev.pick == "table" then return { class = "large" } end
              if ev.pick == "none" then return nil end
              return ev.pick
            end }"#,
            PICK,
        );
        let events = ["large", "table", "none", "elsewhere", "small"]
            .map(|p| inv(json!({ "pick": p })))
            .to_vec();
        let p = e.place_batch(events, &PlaceOptions::default()).await;
        let got: Vec<_> = p.iter().map(class).collect();
        assert_eq!(
            got,
            [
                Some("large"),
                Some("large"),
                Some("small"),
                Some("small"),
                Some("small")
            ]
        );
        assert!(p[3].error.as_deref().unwrap().contains("not allowed"));
        assert!(p
            .iter()
            .enumerate()
            .all(|(i, x)| (i == 3) == x.error.is_some()));
    }

    #[tokio::test]
    async fn messages_do_not_see_each_others_state() {
        // Both a global and a module-level local would leak if the module were evaluated once.
        let e = engine(
            r#"--!runtime 1
            local seen = 0
            return { on_place = function(ctx, ev)
              seen += 1
              leaked = (leaked or 0) + 1
              if seen > 1 or leaked > 1 then return "large" end
              return "small"
            end }"#,
            PICK,
        );
        let p = e
            .place_batch(vec![inv(json!({})); 4], &PlaceOptions::default())
            .await;
        assert!(p
            .iter()
            .all(|x| class(x) == Some("small") && x.error.is_none()));
    }

    #[tokio::test]
    async fn a_failing_or_runaway_message_does_not_affect_the_rest() {
        let e = engine(
            r#"--!runtime 1
            return { on_place = function(ctx, ev)
              if ev.boom then error("boom") end
              if ev.spin then while true do end end
              return "large"
            end }"#,
            PICK,
        );
        let events = vec![
            inv(json!({})),
            inv(json!({"boom": true})),
            inv(json!({})),
            inv(json!({"spin": true})),
            inv(json!({})),
        ];
        let opts = PlaceOptions {
            timeout: Duration::from_millis(100),
            ..PlaceOptions::default()
        };
        let p = e.place_batch(events, &opts).await;
        let got: Vec<_> = p.iter().map(|x| (class(x), x.error.is_some())).collect();
        assert_eq!(
            got,
            [
                (Some("large"), false),
                (Some("small"), true),
                (Some("large"), false),
                (Some("small"), true),
                (Some("large"), false),
            ]
        );
    }

    #[tokio::test]
    async fn script_cannot_reach_data_or_network() {
        let e = engine(
            r#"--!runtime 1
            return { on_place = function(ctx, ev)
              local ok = pcall(function() return native.http.get("https://example.com/") end)
              if ok then return "large" end
              return "small"
            end }"#,
            PICK,
        );
        let p = e
            .place_batch(vec![inv(json!({}))], &PlaceOptions::default())
            .await;
        assert_eq!(class(&p[0]), Some("small"));
    }

    #[tokio::test]
    async fn spent_budget_places_the_rest_by_the_static_class() {
        let e = engine(
            r#"--!runtime 1
            return { on_place = function() return "large" end }"#,
            PICK,
        );
        let opts = PlaceOptions {
            budget: Duration::ZERO,
            ..PlaceOptions::default()
        };
        let p = e.place_batch(vec![inv(json!({}))], &opts).await;
        assert_eq!(class(&p[0]), Some("small"));
        assert!(p[0].error.is_some());
    }

    #[tokio::test]
    async fn bad_config_or_missing_script_falls_back() {
        let e = engine("", r#"{"class":"small","script":"nope"}"#);
        let p = e
            .place_batch(vec![inv(json!({}))], &PlaceOptions::default())
            .await;
        assert_eq!((class(&p[0]), p[0].error.is_some()), (Some("small"), true));
    }
}
