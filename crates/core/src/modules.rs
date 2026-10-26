//! `require`: modules from the tenant's `lib/` folder or from the global, versioned shared library.
//!
//! ```lua
//! local dates = require("@lib/util/dates")          -- <tenant>/lib/util/dates.luau
//! local csv   = require("@shared/csv@2.1.0")        -- @shared/csv/2.1.0.luau
//! local igp   = require("@native/patterns@1")       -- built-in patterns, version 1
//! ```
//!
//! A module is a chunk that returns a value (usually a table). It runs once per invocation, in the
//! same sandboxed VM as the script, and later `require`s of it return the same value.

use std::sync::Arc;

use integraal_loader::{CachedLoader, ScriptKey};
use mlua::{Lua, Table, Value};

/// Most distinct modules one invocation may load.
const MAX_MODULES: usize = 64;

/// What `require` needs to fetch module source from the VM thread.
#[derive(Clone)]
pub(crate) struct ModuleHost {
    pub(crate) loader: Arc<CachedLoader>,
    pub(crate) handle: tokio::runtime::Handle,
    pub(crate) debug: Option<crate::debug::DebugRun>,
}

/// Maps a `require` argument to the object it names.
pub(crate) fn resolve(tenant: &str, spec: &str) -> Result<ScriptKey, String> {
    let key = if let Some(path) = spec.strip_prefix("@lib/") {
        ScriptKey::lib(tenant, path)
    } else if let Some(rest) = spec.strip_prefix("@shared/") {
        let (name, version) = rest.rsplit_once('@').ok_or_else(|| {
            format!(
                "require {spec:?}: shared modules need a version, e.g. \"@shared/{rest}@1.0.0\""
            )
        })?;
        ScriptKey::shared(name, version)
    } else {
        return Err(format!(
            "require {spec:?}: expected \"@lib/<path>\" or \"@shared/<name>@<version>\""
        ));
    };
    key.map_err(|e| format!("require {spec:?}: {e}"))
}

pub(crate) fn install(lua: &Lua, host: ModuleHost, tenant: &str) -> mlua::Result<()> {
    let tenant = tenant.to_string();
    let loaded: Table = lua.create_table()?;
    let loading: Table = lua.create_table()?;
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let require = lua.create_function(move |lua, spec: String| {
        if spec.starts_with("@native/") {
            return integraal_wrappers::require_native(lua, &spec).map(Value::Table);
        }
        let key = resolve(&tenant, &spec).map_err(mlua::Error::runtime)?;
        let id = key.object_key();
        let cached: Value = loaded.get(id.as_str())?;
        if !cached.is_nil() {
            return Ok(cached);
        }
        if loading.get::<bool>(id.as_str())? {
            return Err(mlua::Error::runtime(format!(
                "require {spec:?}: circular dependency"
            )));
        }
        if count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= MAX_MODULES {
            return Err(mlua::Error::runtime(format!(
                "require {spec:?}: more than {MAX_MODULES} modules loaded"
            )));
        }
        let module = host
            .handle
            .block_on(host.loader.load(&key))
            .map_err(|e| mlua::Error::runtime(format!("require {spec:?}: {e}")))?;
        loading.set(id.as_str(), true)?;
        let result = lua
            .load(&module.source)
            .set_name(format!("={id}"))
            .into_function()
            .and_then(|f| {
                if let Some(d) = &host.debug {
                    d.register(&id, &f);
                }
                f.call::<Value>(())
            });
        loading.set(id.as_str(), Value::Nil)?;
        let value = match result? {
            Value::Nil => Value::Boolean(true), // a module that returns nothing still counts as loaded
            v => v,
        };
        loaded.set(id.as_str(), value.clone())?;
        Ok(value)
    })?;
    lua.globals().set("require", require)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_specs() {
        assert_eq!(
            resolve("acme", "@lib/util/dates").unwrap().object_key(),
            "acme/lib/util/dates.luau"
        );
        assert_eq!(
            resolve("acme", "@shared/csv@2.1.0").unwrap().object_key(),
            "@shared/csv/2.1.0.luau"
        );
        for bad in [
            "@shared/csv",
            "@shared/csv@latest",
            "@lib/../acme2/x",
            "./util",
            "@lib/",
            "@other/x",
        ] {
            assert!(resolve("acme", bad).is_err(), "{bad}");
        }
    }
}
