use anyhow::{Result, bail};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A daemon-owned admission lifetime shared by all connections/reconnect attempts.
/// Expiry is terminal, including after a wall-clock rollback. Renewal is explicit
/// and can only extend an unexpired, unrevoked grant.
#[derive(Clone, Debug)]
pub struct Lease {
    id: String,
    state: Arc<Mutex<State>>,
}

#[derive(Debug)]
struct State {
    deadline: Instant,
    expires_at_ms: u64,
    ended: Option<&'static str>,
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

impl Lease {
    pub fn new(id: impl Into<String>, expires_at_ms: u64) -> Result<Self> {
        let id = id.into();
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            bail!("invalid lease identity");
        }
        let remaining = expires_at_ms.saturating_sub(now_ms());
        if remaining == 0 {
            bail!("lease has already expired");
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(remaining))
            .ok_or_else(|| anyhow::anyhow!("lease deadline is out of range"))?;
        Ok(Self {
            id,
            state: Arc::new(Mutex::new(State {
                deadline,
                expires_at_ms,
                ended: None,
            })),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.state.lock().expect("lease state").expires_at_ms
    }

    pub fn revoke(&self) {
        self.state.lock().expect("lease state").ended = Some("revoked");
    }

    pub fn ended(&self) -> Option<&'static str> {
        let mut state = self.state.lock().expect("lease state");
        if state.ended.is_none()
            && (Instant::now() >= state.deadline || now_ms() >= state.expires_at_ms)
        {
            state.ended = Some("expired");
        }
        state.ended
    }

    pub fn renew(&self, expires_at_ms: u64) -> Result<()> {
        let mut state = self.state.lock().expect("lease state");
        if state.ended.is_some()
            || Instant::now() >= state.deadline
            || now_ms() >= state.expires_at_ms
        {
            state.ended.get_or_insert("expired");
            bail!("an ended lease cannot be renewed");
        }
        if expires_at_ms < state.expires_at_ms {
            bail!("renewal cannot shorten a lease; revoke it explicitly");
        }
        // Extend the original monotonic grant by the authorized delta. Repeating
        // a renewal must not gain lifetime when the wall clock moves backward.
        let deadline = state
            .deadline
            .checked_add(Duration::from_millis(expires_at_ms - state.expires_at_ms))
            .ok_or_else(|| anyhow::anyhow!("lease deadline is out of range"))?;
        state.expires_at_ms = expires_at_ms;
        state.deadline = deadline;
        Ok(())
    }
}
