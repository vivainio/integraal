use crate::LoadError;

/// Address of a tenant script: `<tenant>/<script>/<version>.luau`.
/// `version` defaults to `latest`, which is mutable and revalidated;
/// any other version is treated as immutable and cached without revalidation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScriptKey {
    tenant: String,
    script: String,
    version: String,
    kind: Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Script,
    /// Per-tenant `<tenant>/config.json`; mutable, always revalidated.
    Config,
    /// Tenant library module `<tenant>/lib/<path>.luau` (`script` holds the path); mutable.
    Lib,
    /// Shared module `@shared/<name>/<version>.luau`; versions are always immutable.
    Shared,
}

/// Directory under a tenant that holds its `require("@lib/...")` modules. A script cannot use this name.
pub const LIB_DIR: &str = "lib";
/// Key prefix of the global shared modules; `@` is not a valid tenant name, so it cannot collide.
const SHARED_DIR: &str = "@shared";
const MAX_MODULE_DEPTH: usize = 8;

fn valid(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

impl ScriptKey {
    pub fn new(tenant: &str, script: &str, version: Option<&str>) -> Result<Self, LoadError> {
        let version = version.unwrap_or("latest");
        for (what, v) in [("tenant", tenant), ("script", script), ("version", version)] {
            if !valid(v) {
                return Err(LoadError::InvalidName(what, v.to_string()));
            }
        }
        if script == LIB_DIR {
            return Err(LoadError::InvalidName("script", script.to_string()));
        }
        Ok(Self {
            tenant: tenant.into(),
            script: script.into(),
            version: version.into(),
            kind: Kind::Script,
        })
    }

    /// Key of the tenant's access-policy file in the script bucket.
    pub fn config(tenant: &str) -> Result<Self, LoadError> {
        if !valid(tenant) {
            return Err(LoadError::InvalidName("tenant", tenant.to_string()));
        }
        Ok(Self {
            tenant: tenant.into(),
            script: String::new(),
            version: String::new(),
            kind: Kind::Config,
        })
    }

    /// A module in the tenant's `lib/` folder; `path` is `/`-separated without extension, e.g. `util/dates`.
    pub fn lib(tenant: &str, path: &str) -> Result<Self, LoadError> {
        if !valid(tenant) {
            return Err(LoadError::InvalidName("tenant", tenant.to_string()));
        }
        let segments: Vec<&str> = path.split('/').collect();
        if segments.len() > MAX_MODULE_DEPTH || !segments.iter().all(|s| valid(s)) {
            return Err(LoadError::InvalidName("module path", path.to_string()));
        }
        Ok(Self {
            tenant: tenant.into(),
            script: path.into(),
            version: "latest".into(),
            kind: Kind::Lib,
        })
    }

    /// A pinned version of a global shared module. `latest` is refused: shared code is only ever versioned.
    pub fn shared(name: &str, version: &str) -> Result<Self, LoadError> {
        for (what, v) in [("module", name), ("module version", version)] {
            if !valid(v) {
                return Err(LoadError::InvalidName(what, v.to_string()));
            }
        }
        if version == "latest" {
            return Err(LoadError::InvalidName(
                "module version",
                version.to_string(),
            ));
        }
        Ok(Self {
            tenant: String::new(),
            script: name.into(),
            version: version.into(),
            kind: Kind::Shared,
        })
    }

    pub fn object_key(&self) -> String {
        match self.kind {
            Kind::Script => format!("{}/{}/{}.luau", self.tenant, self.script, self.version),
            Kind::Config => format!("{}/config.json", self.tenant),
            Kind::Lib => format!("{}/{LIB_DIR}/{}.luau", self.tenant, self.script),
            Kind::Shared => format!("{SHARED_DIR}/{}/{}.luau", self.script, self.version),
        }
    }

    pub fn is_immutable(&self) -> bool {
        match self.kind {
            Kind::Script => self.version != "latest",
            Kind::Shared => true,
            Kind::Config | Kind::Lib => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal() {
        assert!(ScriptKey::new("..", "a", None).is_err());
        assert!(ScriptKey::new("t", "a/b", None).is_err());
        assert!(ScriptKey::new("t", "a", Some("")).is_err());
    }

    #[test]
    fn module_keys() {
        let l = ScriptKey::lib("acme", "util/dates").unwrap();
        assert_eq!(l.object_key(), "acme/lib/util/dates.luau");
        assert!(!l.is_immutable());
        let s = ScriptKey::shared("strings", "2.1.0").unwrap();
        assert_eq!(s.object_key(), "@shared/strings/2.1.0.luau");
        assert!(s.is_immutable());
        assert!(ScriptKey::shared("strings", "latest").is_err());
        assert!(ScriptKey::lib("acme", "../x").is_err());
        assert!(ScriptKey::lib("acme", "a//b").is_err());
        assert!(
            ScriptKey::new("acme", "lib", None).is_err(),
            "lib/ is reserved"
        );
    }

    #[test]
    fn builds_key() {
        let k = ScriptKey::new("acme", "billing", Some("v3")).unwrap();
        assert_eq!(k.object_key(), "acme/billing/v3.luau");
        assert!(k.is_immutable());
    }
}
