use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const API_VERSION: &str = "machine-fabric.dev/v1";

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub id: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceSession {
    pub api_version: String,
    pub metadata: Metadata,
    pub objective: String,
    #[serde(default)]
    pub state: SessionState,
    #[serde(default)]
    pub authority: Option<SessionAuthority>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionAuthority {
    pub controller_id: String,
    pub epoch: u64,
    #[serde(default)]
    pub pending_controller_id: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SessionState {
    #[default]
    Active,
    Completed,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Executor {
    pub api_version: String,
    pub metadata: Metadata,
    pub endpoint: ExecutorEndpoint,
    pub capabilities: Vec<CapabilityDescriptor>,
    #[serde(default)]
    pub allowed_roots: Vec<String>,
    #[serde(default)]
    pub health: HealthStatus,
}

/// A routable Controller service. Endpoints are transport-level and independent
/// from the Controller role, so one node-level peer connection can carry both
/// Controller and Executor traffic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ControllerPeer {
    pub api_version: String,
    pub metadata: Metadata,
    pub endpoint: ExecutorEndpoint,
    #[serde(default)]
    pub health: HealthStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "kebab-case")]
pub enum ExecutorEndpoint {
    Local {
        socket: String,
    },
    Ssh {
        host: String,
        socket: String,
        #[serde(default)]
        control_path: Option<String>,
    },
    Command {
        executable: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        cwd: Option<String>,
    },
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HealthStatus {
    Ready,
    Degraded,
    Offline,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CapabilityDescriptor {
    pub name: String,
    pub version: String,
    pub input_schema: Value,
    pub output_schema: Value,
    #[serde(default)]
    pub required_executor_features: Vec<String>,
    #[serde(default)]
    pub locks: Vec<LockTemplate>,
    pub idempotency: IdempotencyContract,
    pub timeout_ms: u64,
    #[serde(default)]
    pub retry: RetryPolicy,
    pub rollback: RollbackStrategy,
    pub health_check: HealthCheck,
    #[serde(default)]
    pub emitted_evidence: Vec<String>,
    #[serde(default)]
    pub effect: Effect,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    #[serde(default = "default_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub retryable_errors: Vec<String>,
    #[serde(default)]
    pub backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            retryable_errors: Vec::new(),
            backoff_ms: 0,
        }
    }
}

fn default_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LockTemplate {
    pub key: String,
    #[serde(default)]
    pub mode: LockMode,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LockMode {
    Shared,
    #[default]
    Exclusive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IdempotencyContract {
    #[serde(default)]
    pub key_fields: Vec<String>,
    #[serde(default = "default_idempotency_ttl")]
    pub retention_ms: u64,
}

fn default_idempotency_ttl() -> u64 {
    86_400_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "strategy", rename_all = "kebab-case")]
pub enum RollbackStrategy {
    None,
    Compensate { capability: String },
    RetainPreviousGeneration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum HealthCheck {
    None,
    Capability { name: String },
    Process,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Effect {
    #[default]
    ReadOnly,
    Mutating,
    Destructive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub id: String,
    pub kind: LeaseKind,
    pub resource: String,
    pub owner: String,
    pub token: String,
    pub fence: u64,
    pub acquired_at: u64,
    pub updated_at: u64,
    pub expires_at: u64,
    #[serde(default)]
    pub handoff_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum LeaseKind {
    Driver,
    Resource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub workspace_session_id: String,
    pub executor_id: String,
    pub capability: String,
    pub input: Value,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub error: Option<TaskError>,
    pub idempotency_key: String,
    pub state: TaskState,
    pub attempt: u32,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default)]
    pub events: Vec<TaskEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    OutcomeUnknown,
}

impl TaskState {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded
                | Self::Failed
                | Self::Cancelled
                | Self::TimedOut
                | Self::OutcomeUnknown
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskEvent {
    pub sequence: u64,
    pub timestamp: u64,
    pub event_type: String,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub digest: String,
    pub artifact_type: String,
    pub schema: String,
    pub size: u64,
    pub locations: Vec<ArtifactLocation>,
    pub provenance: Provenance,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Generation {
    pub id: String,
    pub workspace_session_id: String,
    pub application_type: String,
    pub root: String,
    pub state: GenerationState,
    pub baseline: BaselineIdentity,
    #[serde(default)]
    pub applied_artifacts: Vec<String>,
    pub digest: String,
    pub created_at: u64,
    #[serde(default)]
    pub activated_at: Option<u64>,
    #[serde(default)]
    pub validation: Option<EvidenceResult>,
    #[serde(default)]
    pub finalization: Option<EvidenceResult>,
    #[serde(default)]
    pub smoke: Option<EvidenceResult>,
    #[serde(default)]
    pub failure: Option<TaskError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GenerationState {
    Reserved,
    Materializing,
    Materialized,
    Validated,
    Finalized,
    SmokePassed,
    Active,
    Superseded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BaselineIdentity {
    pub digest: String,
    pub source: String,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceResult {
    pub passed: bool,
    pub checked_at: u64,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ActivationTransaction {
    pub id: String,
    pub workspace_session_id: String,
    pub idempotency_key: String,
    pub target: String,
    pub generation_id: String,
    #[serde(default)]
    pub previous_generation_id: Option<String>,
    pub state: TransactionState,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default)]
    pub completed_steps: Vec<String>,
    #[serde(default)]
    pub journal: Vec<TransactionJournalEntry>,
    #[serde(default)]
    pub lease_fence: Option<u64>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub error: Option<TaskError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TransactionState {
    Planned,
    Running,
    OutcomeUnknown,
    Activated,
    RolledBack,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TransactionJournalEntry {
    pub sequence: u64,
    pub step: String,
    pub state: TransactionStepState,
    pub attempt: u32,
    pub started_at: u64,
    #[serde(default)]
    pub finished_at: Option<u64>,
    #[serde(default)]
    pub input_digest: Option<String>,
    #[serde(default)]
    pub output_digest: Option<String>,
    #[serde(default)]
    pub executor_id: Option<String>,
    #[serde(default)]
    pub fence: Option<u64>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TransactionStepState {
    Planned,
    Started,
    Succeeded,
    Failed,
    Compensated,
    OutcomeUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ArtifactLocation {
    File {
        #[serde(rename = "executorId", alias = "executor_id")]
        executor_id: String,
        path: String,
    },
    Url {
        url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    pub workspace_session_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub source_digests: Vec<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AgentInstance {
    pub id: String,
    pub workspace_session_id: String,
    pub executor_id: String,
    pub role: String,
    pub provider: String,
    pub state: AgentState,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AgentState {
    Starting,
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Handoff {
    pub id: String,
    pub workspace_session_id: String,
    pub objective: String,
    pub from: AgentRoleRef,
    pub to: AgentRoleRef,
    #[serde(default)]
    pub artifacts: Vec<HandoffArtifact>,
    #[serde(default)]
    pub evidence: Vec<Value>,
    #[serde(default)]
    pub completed_actions: Vec<PendingAction>,
    #[serde(default)]
    pub pending_actions: Vec<PendingAction>,
    #[serde(default)]
    pub acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    pub created_at: u64,
    #[serde(default)]
    pub acknowledged_at: Option<u64>,
    #[serde(default)]
    pub completed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HandoffArtifact {
    pub artifact_type: String,
    pub digest: String,
    #[serde(default)]
    pub generation: Option<String>,
    #[serde(default)]
    pub provenance: Value,
    #[serde(default)]
    pub location: Option<ArtifactLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgentRoleRef {
    #[serde(default)]
    pub agent_id: Option<String>,
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PendingAction {
    pub capability: String,
    #[serde(default)]
    pub input: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Approval {
    pub id: String,
    pub digest: String,
    pub owner: String,
    pub reason: String,
    pub state: ApprovalState,
    pub created_at: u64,
    #[serde(default)]
    pub approved_at: Option<u64>,
    #[serde(default)]
    pub consumed_at: Option<u64>,
    #[serde(default)]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalState {
    Pending,
    Approved,
    Consumed,
    Revoked,
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn handoff_schema_uses_the_runtime_artifact_and_location_shape() {
        let schema: Value =
            serde_json::from_str(include_str!("../../../schemas/handoff.schema.json")).unwrap();
        let artifact = &schema["properties"]["artifacts"]["items"];
        assert!(
            artifact["required"]
                .as_array()
                .unwrap()
                .contains(&json!("artifactType"))
        );
        assert!(artifact["properties"]["location"]["oneOf"].is_array());

        let handoff: Handoff = serde_json::from_value(json!({
            "id": "",
            "workspaceSessionId": "session",
            "objective": "accept",
            "from": {"role": "coding"},
            "to": {"role": "gui-acceptance"},
            "artifacts": [{
                "artifactType": "application-generation",
                "digest": format!("sha256:{}", "a".repeat(64)),
                "generation": "generation-1",
                "provenance": {},
                "location": {"kind": "file", "executorId": "mac", "path": "/state/App.app"}
            }],
            "evidence": [],
            "completedActions": [],
            "pendingActions": [],
            "acceptanceCriteria": [],
            "constraints": [],
            "blockers": [],
            "createdAt": 0
        }))
        .unwrap();
        assert_eq!(handoff.artifacts[0].artifact_type, "application-generation");
        let legacy_location: ArtifactLocation = serde_json::from_value(json!({
            "kind": "file",
            "executor_id": "legacy-mac",
            "path": "/state/Legacy.app"
        }))
        .unwrap();
        let migrated = serde_json::to_value(legacy_location).unwrap();
        assert_eq!(migrated["executorId"], "legacy-mac");
        assert!(migrated.get("executor_id").is_none());
    }
}
