use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use crate::{Fetched, LoadError, Script, ScriptKey, ScriptStore};

struct Entry {
    script: Arc<Script>,
    checked_at: Instant,
}

/// In-memory cache over a `ScriptStore`.
/// Immutable (pinned-version) scripts are never revalidated; `latest` is
/// revalidated by ETag once `revalidate_after` has elapsed.
pub struct CachedLoader {
    store: Box<dyn ScriptStore>,
    revalidate_after: Duration,
    entries: RwLock<HashMap<ScriptKey, Entry>>,
}

impl CachedLoader {
    pub fn new(store: impl ScriptStore + 'static, revalidate_after: Duration) -> Self {
        Self { store: Box::new(store), revalidate_after, entries: RwLock::default() }
    }

    pub async fn load(&self, key: &ScriptKey) -> Result<Arc<Script>, LoadError> {
        let stale_etag = {
            let entries = self.entries.read().unwrap();
            match entries.get(key) {
                Some(e) if key.is_immutable() || e.checked_at.elapsed() < self.revalidate_after => {
                    return Ok(e.script.clone());
                }
                Some(e) => Some((e.script.clone(), e.script.etag.clone())),
                None => None,
            }
        };

        let etag = stale_etag.as_ref().and_then(|(_, t)| t.as_deref());
        match self.store.fetch(key, etag).await? {
            Fetched::NotModified => {
                let (script, _) = stale_etag.ok_or_else(|| LoadError::Storage("304 without cached copy".into()))?;
                self.put(key, script.clone());
                Ok(script)
            }
            Fetched::Fresh(script) => {
                tracing::debug!(key = %key.object_key(), "script loaded");
                let script = Arc::new(script);
                self.put(key, script.clone());
                Ok(script)
            }
        }
    }

    fn put(&self, key: &ScriptKey, script: Arc<Script>) {
        self.entries.write().unwrap().insert(key.clone(), Entry { script, checked_at: Instant::now() });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    #[tokio::test]
    async fn revalidates_latest_and_pins_versions() {
        let store = Arc::new(MemoryStore::default());
        struct Shared(Arc<MemoryStore>);
        #[async_trait::async_trait]
        impl ScriptStore for Shared {
            async fn fetch(&self, k: &ScriptKey, e: Option<&str>) -> Result<Fetched, LoadError> {
                self.0.fetch(k, e).await
            }
        }
        let latest = ScriptKey::new("t", "s", None).unwrap();
        let pinned = ScriptKey::new("t", "s", Some("v1")).unwrap();
        store.put(&latest, "return 1");
        store.put(&pinned, "return 1");

        let loader = CachedLoader::new(Shared(store.clone()), Duration::ZERO);
        assert_eq!(loader.load(&latest).await.unwrap().source, "return 1");
        assert_eq!(loader.load(&pinned).await.unwrap().source, "return 1");

        store.put(&latest, "return 22");
        store.put(&pinned, "return 22");
        assert_eq!(loader.load(&latest).await.unwrap().source, "return 22");
        assert_eq!(loader.load(&pinned).await.unwrap().source, "return 1");
    }
}
