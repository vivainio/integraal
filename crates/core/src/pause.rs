//! Shared queue-admission control. Scripts cannot address the system partition through ig.db.
use std::collections::BTreeSet;

use anyhow::{anyhow, ensure, Result};
use integraal_loader::ScriptKey;
use integraal_wrappers::{Item, ItemBackend, MAX_ITEM_BYTES, NO_EXPIRY};
use serde::{Deserialize, Serialize};

const PK: &str = "SYS#OPS";
const SK: &str = "queue-pause";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub fleet: bool,
    pub tenants: BTreeSet<String>,
    pub revision: u64,
    pub changed_at_ms: u64,
    pub request_id: String,
    pub reason: String,
}

impl State {
    pub fn tenant_paused(&self, tenant: &str) -> bool {
        self.fleet || self.tenants.contains(tenant)
    }
}

pub(crate) fn read(backend: &dyn ItemBackend) -> Result<State> {
    backend
        .get(PK, SK)
        .map_err(anyhow::Error::msg)?
        .map(|item| serde_json::from_str(&item.value).map_err(anyhow::Error::from))
        .unwrap_or_else(|| Ok(State::default()))
}

pub(crate) fn change(
    backend: &dyn ItemBackend,
    tenant: Option<&str>,
    paused: bool,
    reason: &str,
    request_id: &str,
) -> Result<State> {
    ensure!(
        !reason.trim().is_empty() && reason.len() <= 1024,
        "pause/resume needs a reason of 1–1024 bytes"
    );
    if let Some(tenant) = tenant {
        ScriptKey::config(tenant)?;
    }
    for _ in 0..8 {
        let current = backend.get(PK, SK).map_err(anyhow::Error::msg)?;
        let mut state: State = current
            .as_ref()
            .map(|item| serde_json::from_str(&item.value))
            .transpose()?
            .unwrap_or_default();
        if let Some(tenant) = tenant {
            if paused {
                state.tenants.insert(tenant.into());
            } else {
                state.tenants.remove(tenant);
            }
        } else {
            state.fleet = paused;
        }
        state.revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow!("pause revision overflow"))?;
        state.reason = reason.into();
        state.request_id = request_id.into();
        state.changed_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;
        let value = serde_json::to_string(&state)?;
        ensure!(
            value.len() <= MAX_ITEM_BYTES - 1024,
            "too many paused tenants in the fleet control slot"
        );
        let replacement = Item {
            sk: SK.into(),
            value,
            ttl: NO_EXPIRY,
        };
        let written = match current {
            Some(expected) => {
                backend.replace_if_current(PK, &expected, replacement, state.changed_at_ms / 1000)
            }
            None => backend.put_if_absent(PK, replacement),
        }
        .map_err(anyhow::Error::msg)?;
        if written {
            return Ok(state);
        }
    }
    anyhow::bail!("pause control changed concurrently; retry the command")
}

impl crate::Engine {
    pub async fn queue_pause_state(&self) -> Result<State> {
        if self.operations.is_none() {
            return Ok(State::default());
        }
        let backend = self
            .item_backend()
            .ok_or_else(|| anyhow!("pause control requires db"))?;
        tokio::task::spawn_blocking(move || read(backend.as_ref())).await?
    }
}
