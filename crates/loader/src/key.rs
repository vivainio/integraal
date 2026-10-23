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
}

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

    pub fn object_key(&self) -> String {
        match self.kind {
            Kind::Script => format!("{}/{}/{}.luau", self.tenant, self.script, self.version),
            Kind::Config => format!("{}/config.json", self.tenant),
        }
    }

    pub fn is_immutable(&self) -> bool {
        self.kind == Kind::Script && self.version != "latest"
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
    fn builds_key() {
        let k = ScriptKey::new("acme", "billing", Some("v3")).unwrap();
        assert_eq!(k.object_key(), "acme/billing/v3.luau");
        assert!(k.is_immutable());
    }
}
