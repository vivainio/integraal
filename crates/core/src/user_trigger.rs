//! Metadata and argument validation for explicitly user-invokable scripts.
use crate::Error;
use mlua::{Lua, LuaSerdeExt, Table, VmState};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub arguments: Vec<Argument>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Argument {
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub kind: Kind,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    String,
    Boolean,
    Number,
}
fn matches_kind(kind: &Kind, value: &Value) -> bool {
    match kind {
        Kind::String => value.as_str().is_some_and(|s| s.len() <= 4096),
        Kind::Boolean => value.is_boolean(),
        Kind::Number => value.is_number(),
    }
}
impl Metadata {
    pub fn parse(value: Value) -> Result<Self, Error> {
        let meta: Self = serde_json::from_value(value)
            .map_err(|e| Error::Script(format!("invalid user_trigger metadata: {e}")))?;
        if meta.title.trim().is_empty()
            || meta.title.len() > 160
            || meta.description.len() > 1024
            || meta.arguments.len() > 32
        {
            return Err(Error::Script(
                "invalid user_trigger title, description or argument count".into(),
            ));
        }
        let mut names = HashSet::new();
        for arg in &meta.arguments {
            if arg.name.is_empty()
                || arg.name.len() > 64
                || !arg
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || !names.insert(&arg.name)
                || arg.label.trim().is_empty()
                || arg.label.len() > 160
                || arg
                    .default
                    .as_ref()
                    .is_some_and(|v| !matches_kind(&arg.kind, v))
            {
                return Err(Error::Script(format!(
                    "invalid user_trigger argument {:?}",
                    arg.name
                )));
            }
        }
        Ok(meta)
    }
    pub fn arguments(&self, value: Value) -> Result<Value, Error> {
        let mut input = value
            .as_object()
            .cloned()
            .ok_or_else(|| Error::Script("arguments must be a JSON object".into()))?;
        if input
            .keys()
            .any(|name| !self.arguments.iter().any(|a| &a.name == name))
        {
            return Err(Error::Script("unknown argument".into()));
        }
        let mut output = Map::new();
        for arg in &self.arguments {
            let val = input.remove(&arg.name).or_else(|| arg.default.clone());
            match val {
                Some(v) if matches_kind(&arg.kind, &v) => {
                    output.insert(arg.name.clone(), v);
                }
                Some(_) => return Err(Error::Script(format!("invalid value for {}", arg.name))),
                None if arg.required => {
                    return Err(Error::Script(format!(
                        "missing required argument {}",
                        arg.name
                    )))
                }
                None => {}
            }
        }
        Ok(Value::Object(output))
    }
}

/// Evaluate module metadata with no native APIs or module execution. Native imports are inert
/// tables; module initialization must not call them. This VM has independent memory/time limits.
pub fn describe(source: &str) -> Result<Metadata, Error> {
    integraal_wrappers::parse_runtime_version(source).map_err(Error::from)?;
    let lua = Lua::new();
    lua.set_memory_limit(8 * 1024 * 1024)
        .map_err(|e| Error::Script(e.to_string()))?;
    let require = lua
        .create_function(|lua, _: String| lua.create_table())
        .map_err(|e| Error::Script(e.to_string()))?;
    lua.globals()
        .set("require", require)
        .map_err(|e| Error::Script(e.to_string()))?;
    lua.sandbox(true)
        .map_err(|e| Error::Script(e.to_string()))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    lua.set_interrupt(move |_| {
        if std::time::Instant::now() >= deadline {
            Err(mlua::Error::runtime("metadata evaluation timed out"))
        } else {
            Ok(VmState::Continue)
        }
    });
    let module: Table = lua
        .load(source)
        .eval()
        .map_err(|e| Error::Script(format!("metadata evaluation failed: {e}")))?;
    let _: mlua::Function = module
        .get("on_user_trigger")
        .map_err(|_| Error::Script("script has no on_user_trigger handler".into()))?;
    let value: mlua::Value = module
        .get("user_trigger")
        .map_err(|e| Error::Script(e.to_string()))?;
    Metadata::parse(
        lua.from_value(value)
            .map_err(|e| Error::Script(e.to_string()))?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SOURCE: &str = r#"--!runtime 1
        local p = require("@native/patterns@1")
        return { user_trigger = { title = "Hello", arguments = {
            { name = "name", label = "Name", type = "string", required = true },
            { name = "loud", label = "Loud", type = "boolean", default = false },
        } }, on_user_trigger = function(_, args) return args end }
    "#;
    #[test]
    fn restricted_metadata_and_argument_validation() {
        let meta = describe(SOURCE).unwrap();
        assert_eq!(
            meta.arguments(json!({"name":"Aino"})).unwrap(),
            json!({"name":"Aino","loud":false})
        );
        assert!(meta.arguments(json!({})).is_err());
        assert!(meta.arguments(json!({"name": 1})).is_err());
        assert!(meta
            .arguments(json!({"name":"Aino", "extra":true}))
            .is_err());
        assert!(describe("--!runtime 1\nig.store.Out.put('x','x'); return {} ").is_err());
        assert!(
            describe("--!runtime 1\nreturn { user_trigger = {title='x', arguments={}} }").is_err()
        );
    }
}
