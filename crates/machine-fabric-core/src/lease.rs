use crate::now_ms;
use machine_fabric_schema::{Lease, LeaseKind};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("resource is leased by {owner} until {expires_at}")]
    Active { owner: String, expires_at: u64 },
    #[error("lease not found")]
    NotFound,
    #[error("lease is owned by another writer")]
    OwnedByOther,
    #[error("handoff target does not match")]
    InvalidHandoff,
}

#[derive(Debug, Default)]
pub struct LeaseTable {
    leases: BTreeMap<String, Lease>,
    fences: BTreeMap<String, u64>,
}

impl LeaseTable {
    pub fn from_leases(leases: impl IntoIterator<Item = Lease>) -> Self {
        Self::from_snapshot(leases, BTreeMap::new())
    }

    pub fn from_snapshot(
        leases: impl IntoIterator<Item = Lease>,
        mut fences: BTreeMap<String, u64>,
    ) -> Self {
        let mut table = Self::default();
        for lease in leases {
            let fence = fences.entry(lease.resource.clone()).or_default();
            *fence = (*fence).max(lease.fence);
            table.leases.insert(lease.resource.clone(), lease);
        }
        table.fences = fences;
        table
    }

    pub fn acquire(
        &mut self,
        kind: LeaseKind,
        resource: impl Into<String>,
        owner: impl Into<String>,
        ttl_ms: u64,
    ) -> Result<Lease, LeaseError> {
        let now = now_ms();
        let resource = resource.into();
        let owner = owner.into();
        if let Some(existing) = self.leases.get(&resource)
            && existing.expires_at > now
        {
            return Err(LeaseError::Active {
                owner: existing.owner.clone(),
                expires_at: existing.expires_at,
            });
        }
        let fence = self.fences.get(&resource).copied().unwrap_or(0) + 1;
        self.fences.insert(resource.clone(), fence);
        let lease = Lease {
            id: format!("lease_{}", Uuid::new_v4().simple()),
            kind,
            resource: resource.clone(),
            owner,
            token: format!("token_{}", Uuid::new_v4().simple()),
            fence,
            acquired_at: now,
            updated_at: now,
            expires_at: now.saturating_add(ttl_ms),
            handoff_to: None,
        };
        self.leases.insert(resource, lease.clone());
        Ok(lease)
    }

    pub fn get(&self, resource: &str) -> Option<&Lease> {
        self.leases.get(resource)
    }

    pub fn validate(&self, resource: &str, owner: &str, token: &str) -> Result<&Lease, LeaseError> {
        let lease = self.leases.get(resource).ok_or(LeaseError::NotFound)?;
        if lease.expires_at <= now_ms() {
            return Err(LeaseError::NotFound);
        }
        if lease.owner != owner || lease.token != token {
            return Err(LeaseError::OwnedByOther);
        }
        Ok(lease)
    }

    pub fn renew(
        &mut self,
        resource: &str,
        owner: &str,
        token: &str,
        ttl_ms: u64,
    ) -> Result<Lease, LeaseError> {
        self.validate(resource, owner, token)?;
        let now = now_ms();
        let lease = self.leases.get_mut(resource).expect("validated lease");
        lease.updated_at = now;
        lease.expires_at = now.saturating_add(ttl_ms);
        Ok(lease.clone())
    }

    pub fn handoff(
        &mut self,
        resource: &str,
        owner: &str,
        token: &str,
        target: impl Into<String>,
    ) -> Result<Lease, LeaseError> {
        self.validate(resource, owner, token)?;
        let lease = self.leases.get_mut(resource).expect("validated lease");
        lease.handoff_to = Some(target.into());
        lease.updated_at = now_ms();
        Ok(lease.clone())
    }

    pub fn take_handoff(
        &mut self,
        resource: &str,
        target: &str,
        ttl_ms: u64,
    ) -> Result<Lease, LeaseError> {
        let now = now_ms();
        let lease = self.leases.get_mut(resource).ok_or(LeaseError::NotFound)?;
        if lease.handoff_to.as_deref() != Some(target) {
            return Err(LeaseError::InvalidHandoff);
        }
        let fence = self.fences.get(resource).copied().unwrap_or(lease.fence) + 1;
        self.fences.insert(resource.to_owned(), fence);
        lease.owner = target.to_owned();
        lease.token = format!("token_{}", Uuid::new_v4().simple());
        lease.fence = fence;
        lease.acquired_at = now;
        lease.updated_at = now;
        lease.expires_at = now.saturating_add(ttl_ms);
        lease.handoff_to = None;
        Ok(lease.clone())
    }

    pub fn release(
        &mut self,
        resource: &str,
        owner: &str,
        token: &str,
    ) -> Result<Lease, LeaseError> {
        self.validate(resource, owner, token)?;
        self.leases.remove(resource).ok_or(LeaseError::NotFound)
    }

    pub fn snapshot(&self) -> Vec<Lease> {
        self.leases.values().cloned().collect()
    }

    pub fn fence_snapshot(&self) -> BTreeMap<String, u64> {
        self.fences.clone()
    }

    pub fn reap_expired(&mut self) -> Vec<Lease> {
        let now = now_ms();
        let resources: Vec<String> = self
            .leases
            .values()
            .filter(|lease| lease.expires_at <= now)
            .map(|lease| lease.resource.clone())
            .collect();
        resources
            .into_iter()
            .filter_map(|resource| self.leases.remove(&resource))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_handoff_and_stale_writer_rejection() {
        let mut table = LeaseTable::default();
        let first = table
            .acquire(LeaseKind::Driver, "workspace:a", "agent-a", 60_000)
            .unwrap();
        assert!(matches!(
            table.acquire(LeaseKind::Driver, "workspace:a", "agent-b", 60_000),
            Err(LeaseError::Active { .. })
        ));
        table
            .handoff("workspace:a", "agent-a", &first.token, "agent-b")
            .unwrap();
        let second = table
            .take_handoff("workspace:a", "agent-b", 60_000)
            .unwrap();
        assert_ne!(first.token, second.token);
        assert!(matches!(
            table.validate("workspace:a", "agent-a", &first.token),
            Err(LeaseError::OwnedByOther)
        ));
        table
            .release("workspace:a", "agent-b", &second.token)
            .unwrap();
    }

    #[test]
    fn expired_resources_are_reaped_without_renewal() {
        let mut table = LeaseTable::default();
        table
            .acquire(LeaseKind::Resource, "port:1", "controller", 0)
            .unwrap();
        assert_eq!(table.reap_expired().len(), 1);
        assert!(table.get("port:1").is_none());
    }

    #[test]
    fn released_fence_survives_snapshot_restore() {
        let mut table = LeaseTable::default();
        let first = table
            .acquire(LeaseKind::Resource, "file:a", "controller-a", 60_000)
            .unwrap();
        table
            .release("file:a", "controller-a", &first.token)
            .unwrap();
        let mut restored = LeaseTable::from_snapshot(table.snapshot(), table.fence_snapshot());
        let second = restored
            .acquire(LeaseKind::Resource, "file:a", "controller-b", 60_000)
            .unwrap();
        assert_eq!(second.fence, first.fence + 1);
    }
}
