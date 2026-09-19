use machine_fabric_core::{
    FabricState, JsonStore, LeaseError, LeaseTable, TaskTable, now_ms, sha256_bytes,
};
use machine_fabric_protocol::{Request, Response, RpcError};
use machine_fabric_schema::{
    ActivationTransaction, AgentInstance, AgentState, Approval, ApprovalState, Artifact,
    ArtifactLocation, CapabilityDescriptor, ControllerPeer, Executor, ExecutorEndpoint, Generation,
    GenerationState, Handoff, HealthStatus, LeaseKind, Metadata, Provenance, SessionAuthority,
    SessionState, Task, TaskState, TransactionJournalEntry, TransactionState, TransactionStepState,
    WorkspaceSession,
};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::telemetry::{event_fields, request_event};
use crate::transport::call_executor;

#[derive(Clone, Default)]
struct TraceContext {
    correlation_id: String,
    request_id: String,
}

thread_local! { static CURRENT_TRACE: RefCell<Option<TraceContext>> = const { RefCell::new(None) }; }

fn traced_request(action: impl Into<String>, params: Value) -> Request {
    let mut request = Request::new(action, params);
    CURRENT_TRACE.with(|current| {
        if let Some(trace) = current.borrow().as_ref() {
            request.correlation_id = Some(trace.correlation_id.clone());
            request.parent_request_id = Some(trace.request_id.clone());
        }
    });
    request
}

fn current_trace() -> (Option<String>, Option<String>) {
    CURRENT_TRACE.with(|current| {
        current
            .borrow()
            .as_ref()
            .map(|trace| {
                (
                    Some(trace.correlation_id.clone()),
                    Some(trace.request_id.clone()),
                )
            })
            .unwrap_or_default()
    })
}

fn transfer_event(stage: &str, fields: Value) {
    let (correlation_id, request_id) = current_trace();
    event_fields(
        "info",
        "artifact.transfer.stage",
        json!({"stage": stage, "correlationId": correlation_id, "requestId": request_id, "fields": fields}),
    );
}

fn transfer_error(mut error: RpcError, stage: &str, offset: u64, chunks: u64) -> RpcError {
    let original_details = std::mem::take(&mut error.details);
    error.details = json!({
        "stage": stage,
        "failureOffset": offset,
        "transferredBytes": offset,
        "chunks": chunks,
        "retryCount": 0,
        "cause": original_details,
    });
    transfer_event(
        "failed",
        json!({
            "stage": stage,
            "failureOffset": offset,
            "transferredBytes": offset,
            "chunks": chunks,
            "retryCount": 0,
            "errorCode": error.code,
            "errorMessage": error.message,
        }),
    );
    error
}

pub struct Controller {
    id: String,
    store: JsonStore,
    state: Mutex<FabricState>,
    leases: Mutex<LeaseTable>,
    tasks: Mutex<TaskTable>,
    session_gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl Controller {
    pub fn open(store: JsonStore) -> Result<Self, machine_fabric_core::StoreError> {
        Self::open_with_id(store, None)
    }

    pub fn open_with_id(
        store: JsonStore,
        configured_id: Option<String>,
    ) -> Result<Self, machine_fabric_core::StoreError> {
        let mut state = store.load()?;
        if let (Some(stored), Some(configured)) = (&state.controller_id, &configured_id)
            && stored != configured
        {
            return Err(
                machine_fabric_core::StoreError::ControllerIdentityMismatch {
                    stored: stored.clone(),
                    configured: configured.clone(),
                },
            );
        }
        let id = configured_id
            .or_else(|| state.controller_id.clone())
            .unwrap_or_else(|| format!("controller_{}", Uuid::new_v4().simple()));
        let mut identity_changed = state.controller_id.as_deref() != Some(&id);
        state.controller_id = Some(id.clone());
        for session in &mut state.sessions {
            if session.authority.is_none() {
                session.authority = Some(SessionAuthority {
                    controller_id: id.clone(),
                    epoch: 1,
                    pending_controller_id: None,
                });
                identity_changed = true;
            }
        }
        let compacted = compact_persisted_evidence(&mut state);
        let mut leases =
            LeaseTable::from_snapshot(state.leases.clone(), state.lease_fences.clone());
        let reaped = leases.reap_expired();
        let mut tasks = TaskTable::from_tasks(state.tasks.clone());
        let recovered = tasks.recover_orphans();
        if identity_changed || compacted || !reaped.is_empty() || !recovered.is_empty() {
            state.leases = leases.snapshot();
            state.lease_fences = leases.fence_snapshot();
            state.tasks = tasks.snapshot();
            store.save(&state)?;
        }
        Ok(Self {
            id,
            leases: Mutex::new(leases),
            tasks: Mutex::new(tasks),
            session_gates: Mutex::new(HashMap::new()),
            state: Mutex::new(state),
            store,
        })
    }

    pub fn handle(&self, request: Request) -> Response {
        let correlation_id = request
            .correlation_id
            .clone()
            .unwrap_or_else(|| request.request_id.clone());
        let previous = CURRENT_TRACE.with(|current| {
            current.replace(Some(TraceContext {
                correlation_id,
                request_id: request.request_id.clone(),
            }))
        });
        let started = Instant::now();
        request_event("info", "request.started", &request, json!({}));
        let finish_fields = json!({
            "requestId": request.request_id.clone(),
            "correlationId": request.correlation_id.as_deref().unwrap_or(&request.request_id),
            "parentRequestId": request.parent_request_id.clone(),
            "action": request.action.clone(),
        });
        let session_gate = self
            .session_id_for_action(&request.action, &request.params)
            .map(|session_id| self.session_gate(&session_id));
        let _session_guard = session_gate
            .as_ref()
            .map(|gate| gate.lock().expect("session gate"));
        let reaped = self.leases.lock().expect("lease lock").reap_expired();
        let result = if reaped.is_empty() {
            self.dispatch(&request.action, request.params)
        } else {
            self.persist()
                .and_then(|()| self.dispatch(&request.action, request.params))
        };
        let response = match result {
            Ok(value) => Response::success(request.request_id.clone(), value),
            Err(error) => Response::failure(request.request_id.clone(), error),
        };
        event_fields(
            if response.ok { "info" } else { "error" },
            "request.finished",
            json!({"request": finish_fields, "ok": response.ok, "durationMs": started.elapsed().as_millis()}),
        );
        CURRENT_TRACE.with(|current| {
            current.replace(previous);
        });
        response
    }

    fn dispatch(&self, action: &str, params: Value) -> Result<Value, RpcError> {
        if action.starts_with("desktop.") {
            let executor_id = required_str(&params, "executorId")?.to_owned();
            return self.call_registered_executor(&executor_id, action, params);
        }
        if let Some(routed) = self.route_session_action(action, &params)? {
            return Ok(routed);
        }
        match action {
            "ping" => Ok(json!({
                "controller": {"id": self.id, "status": "ready"},
            })),
            "fabric.context" => {
                let requested_executor = params.get("executorId").and_then(Value::as_str);
                let requested_capability = params.get("capability").and_then(Value::as_str);
                let resources = params
                    .get("resources")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let executors = self.state.lock().expect("state lock").executors.clone();
                let mut nodes = Vec::new();
                for executor in executors {
                    if requested_executor.is_some_and(|id| id != executor.metadata.id) {
                        continue;
                    }
                    let capabilities: Vec<Value> = executor
                        .capabilities
                        .iter()
                        .filter(|capability| {
                            requested_capability.is_none_or(|name| name == capability.name)
                        })
                        .map(|capability| {
                            if requested_capability.is_some() {
                                json!({
                                    "name": capability.name,
                                    "effect": capability.effect,
                                    "requiredExecutorFeatures": capability.required_executor_features,
                                    "locks": capability.locks,
                                    "timeoutMs": capability.timeout_ms,
                                })
                            } else {
                                json!(capability.name)
                            }
                        })
                        .collect();
                    if requested_capability.is_some() && capabilities.is_empty() {
                        continue;
                    }
                    let availability = call_executor(
                        &executor.endpoint,
                        &traced_request("availability", json!({"resources": resources})),
                    )
                    .ok()
                    .filter(|response| response.ok)
                    .and_then(|response| response.result)
                    .unwrap_or_else(|| json!({"available": false, "status": "unreachable"}));
                    nodes.push(json!({
                        "executorId": executor.metadata.id,
                        "health": executor.health,
                        "availability": availability,
                        "capabilityCount": executor.capabilities.len(),
                        "capabilities": capabilities,
                    }));
                }
                Ok(json!({
                    "controllerId": self.id,
                    "taskOwnership": "this Controller owns tasks submitted by its local Agent",
                    "executionAuthority": "each target Executor owns node-local resource admission and queue state",
                    "executors": nodes,
                }))
            }
            "capability.describe" => {
                let executor_id = required_str(&params, "executorId")?;
                let capability_name = required_str(&params, "capability")?;
                let state = self.state.lock().expect("state lock");
                let executor = state
                    .executors
                    .iter()
                    .find(|executor| executor.metadata.id == executor_id)
                    .ok_or_else(|| {
                        RpcError::new(
                            "EXECUTOR_NOT_FOUND",
                            format!("unknown executor: {executor_id}"),
                        )
                    })?;
                let capability = executor
                    .capabilities
                    .iter()
                    .find(|capability| capability.name == capability_name)
                    .ok_or_else(|| {
                        RpcError::new(
                            "CAPABILITY_NOT_FOUND",
                            format!("executor {executor_id} does not provide {capability_name}"),
                        )
                    })?;
                Ok(serde_json::to_value(capability).expect("capability serializes"))
            }
            "status" => {
                let state = self.state.lock().expect("state lock");
                let mut controllers = state.controllers.clone();
                let mut executors = state.executors.clone();
                refresh_local_endpoint_health(&mut controllers, &mut executors);
                Ok(json!({
                    "controller": {"id": self.id, "status": "ready"},
                    "controllers": controllers,
                    "executors": executors,
                    "leases": self.leases.lock().expect("lease lock").snapshot(),
                    "tasks": self.tasks.lock().expect("task lock").snapshot(),
                }))
            }
            "executor.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").executors,
            )
            .expect("executors serialize")),
            "controller.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").controllers,
            )
            .expect("controllers serialize")),
            "executor.unregister" => {
                let id = required_str(&params, "executorId")?;
                let mut state = self.state.lock().expect("state lock");
                let before = state.executors.len();
                state
                    .executors
                    .retain(|executor| executor.metadata.id != id);
                let removed = state.executors.len() != before;
                drop(state);
                if removed {
                    self.persist()?;
                }
                Ok(json!({"executorId": id, "removed": removed}))
            }
            "controller.unregister" => {
                let id = required_str(&params, "controllerId")?;
                if id == self.id {
                    return Err(RpcError::new(
                        "CONTROLLER_SELF_UNREGISTER",
                        "a Controller cannot unregister itself",
                    ));
                }
                let mut state = self.state.lock().expect("state lock");
                let before = state.controllers.len();
                state
                    .controllers
                    .retain(|controller| controller.metadata.id != id);
                let removed = state.controllers.len() != before;
                drop(state);
                if removed {
                    self.persist()?;
                }
                Ok(json!({"controllerId": id, "removed": removed}))
            }
            "controller.register" => {
                let id = required_str(&params, "controllerId")?.to_owned();
                let endpoint: ExecutorEndpoint = serde_json::from_value(
                    params
                        .get("endpoint")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "endpoint is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let response = call_executor(&endpoint, &traced_request("ping", Value::Null))
                    .map_err(|error| {
                        let mut rpc = RpcError::new("CONTROLLER_UNAVAILABLE", error.to_string());
                        rpc.retryable = true;
                        rpc
                    })?;
                if !response.ok {
                    return Err(response.error.unwrap_or_else(|| {
                        RpcError::new("CONTROLLER_FAILED", "controller failed")
                    }));
                }
                let status = response.result.unwrap_or(Value::Null);
                let actual_id = status
                    .get("controller")
                    .and_then(|controller| controller.get("id"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        RpcError::new(
                            "CONTROLLER_INVALID",
                            "controller status did not report an identity",
                        )
                    })?;
                if actual_id != id {
                    return Err(RpcError::new(
                        "CONTROLLER_IDENTITY_MISMATCH",
                        format!("endpoint reported Controller {actual_id}, expected {id}"),
                    ));
                }
                let now = now_ms();
                let controller = ControllerPeer {
                    api_version: "machine-fabric.dev/v1".to_owned(),
                    metadata: Metadata {
                        id: id.clone(),
                        labels: Default::default(),
                        created_at: now,
                        updated_at: now,
                    },
                    endpoint,
                    health: HealthStatus::Ready,
                };
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.controllers, controller.clone(), |existing| {
                    existing.metadata.id == id
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(controller).expect("controller serializes"))
            }
            "controller.call" => {
                let id = required_str(&params, "controllerId")?;
                let controller = self
                    .state
                    .lock()
                    .expect("state lock")
                    .controllers
                    .iter()
                    .find(|controller| controller.metadata.id == id)
                    .cloned()
                    .ok_or_else(|| {
                        RpcError::new("CONTROLLER_NOT_FOUND", format!("unknown controller: {id}"))
                    })?;
                let nested = traced_request(
                    required_str(&params, "action")?,
                    params.get("params").cloned().unwrap_or(Value::Null),
                );
                let response = call_executor(&controller.endpoint, &nested).map_err(|error| {
                    let mut rpc = RpcError::new("CONTROLLER_UNAVAILABLE", error.to_string());
                    rpc.retryable = true;
                    rpc
                })?;
                if response.ok {
                    Ok(response.result.unwrap_or(Value::Null))
                } else {
                    Err(response
                        .error
                        .unwrap_or_else(|| RpcError::new("CONTROLLER_FAILED", "controller failed")))
                }
            }
            "doctor" => {
                let executors = self.state.lock().expect("state lock").executors.clone();
                let checks: Vec<Value> = executors
                    .iter()
                    .map(|executor| {
                        match call_executor(
                            &executor.endpoint,
                            &traced_request("status", Value::Null),
                        ) {
                            Ok(response) if response.ok => json!({
                                "executorId": executor.metadata.id,
                                "status": "ready",
                                "capabilities": executor.capabilities.len()
                            }),
                            Ok(response) => json!({
                                "executorId": executor.metadata.id,
                                "status": "failed",
                                "error": response.error
                            }),
                            Err(error) => json!({
                                "executorId": executor.metadata.id,
                                "status": "offline",
                                "error": error.to_string()
                            }),
                        }
                    })
                    .collect();
                let healthy = checks.iter().all(|check| check["status"] == "ready");
                Ok(json!({
                    "healthy": healthy,
                    "controller": {"id": self.id, "status": "ready"},
                    "checks": checks
                }))
            }
            "dashboard.snapshot" => {
                let state = self.state.lock().expect("state lock").clone();
                Ok(json!({
                    "generatedAt": now_ms(),
                    "controller": {"id": self.id, "status": "ready"},
                    "controllers": state.controllers,
                    "sessions": state.sessions,
                    "executors": state.executors,
                    "leases": self.leases.lock().expect("lease lock").snapshot(),
                    "tasks": self.tasks.lock().expect("task lock").snapshot(),
                    "artifacts": state.artifacts,
                    "generations": state.generations,
                    "transactions": state.transactions,
                    "agents": state.agents,
                    "handoffs": state.handoffs
                }))
            }
            "session.put" => {
                let mut session: WorkspaceSession = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                match &session.authority {
                    None => {
                        session.authority = Some(SessionAuthority {
                            controller_id: self.id.clone(),
                            epoch: 1,
                            pending_controller_id: None,
                        });
                    }
                    Some(authority) if authority.controller_id == self.id => {
                        let existing = self
                            .state
                            .lock()
                            .expect("state lock")
                            .sessions
                            .iter()
                            .find(|existing| existing.metadata.id == session.metadata.id)
                            .and_then(|existing| existing.authority.as_ref())
                            .cloned();
                        if existing.as_ref() != Some(authority) {
                            return Err(RpcError::new(
                                "INVALID_AUTHORITY",
                                "session authority can only change through handoff",
                            ));
                        }
                    }
                    Some(authority) => {
                        return Err(RpcError::new(
                            "NOT_SESSION_AUTHORITY",
                            format!(
                                "session {} is owned by controller {}",
                                session.metadata.id, authority.controller_id
                            ),
                        ));
                    }
                }
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.sessions, session.clone(), |item| {
                    item.metadata.id == session.metadata.id
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(session).expect("session serializes"))
            }
            "session.accept-handoff" => {
                let session: WorkspaceSession = serde_json::from_value(
                    params
                        .get("session")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "session is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let authority = session.authority.as_ref().ok_or_else(|| {
                    RpcError::new("INVALID_AUTHORITY", "handoff has no session authority")
                })?;
                if authority.controller_id != self.id {
                    return Err(RpcError::new(
                        "INVALID_AUTHORITY",
                        "handoff target does not match this controller",
                    ));
                }
                let mut state = self.state.lock().expect("state lock");
                if let Some(existing) = state
                    .sessions
                    .iter()
                    .find(|existing| existing.metadata.id == session.metadata.id)
                {
                    if existing.authority.as_ref() == Some(authority) {
                        return Ok(serde_json::to_value(existing).expect("session serializes"));
                    }
                    if existing
                        .authority
                        .as_ref()
                        .is_some_and(|current| current.epoch >= authority.epoch)
                    {
                        return Err(RpcError::new(
                            "STALE_AUTHORITY_EPOCH",
                            "handoff epoch must increase",
                        ));
                    }
                }
                upsert_by(&mut state.sessions, session.clone(), |existing| {
                    existing.metadata.id == session.metadata.id
                });
                for task in optional_array::<Task>(&params, "tasks")? {
                    let id = task.id.clone();
                    upsert_by(&mut state.tasks, task, |existing| existing.id == id);
                }
                for artifact in optional_array::<Artifact>(&params, "artifacts")? {
                    let digest = artifact.digest.clone();
                    upsert_by(&mut state.artifacts, artifact, |existing| {
                        existing.digest == digest
                    });
                }
                for generation in optional_array::<Generation>(&params, "generations")? {
                    let id = generation.id.clone();
                    upsert_by(&mut state.generations, generation, |existing| {
                        existing.id == id
                    });
                }
                for transaction in optional_array::<ActivationTransaction>(&params, "transactions")?
                {
                    let id = transaction.id.clone();
                    upsert_by(&mut state.transactions, transaction, |existing| {
                        existing.id == id
                    });
                }
                for agent in optional_array::<AgentInstance>(&params, "agents")? {
                    let id = agent.id.clone();
                    upsert_by(&mut state.agents, agent, |existing| existing.id == id);
                }
                for handoff in optional_array::<Handoff>(&params, "handoffs")? {
                    let id = handoff.id.clone();
                    upsert_by(&mut state.handoffs, handoff, |existing| existing.id == id);
                }
                *self.tasks.lock().expect("task lock") = TaskTable::from_tasks(state.tasks.clone());
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(session).expect("session serializes"))
            }
            "session.handoff" => {
                let session_id = required_str(&params, "sessionId")?;
                let target_id = required_str(&params, "targetControllerId")?;
                let target = self
                    .state
                    .lock()
                    .expect("state lock")
                    .controllers
                    .iter()
                    .find(|controller| controller.metadata.id == target_id)
                    .cloned()
                    .ok_or_else(|| {
                        RpcError::new("CONTROLLER_NOT_FOUND", "target not registered")
                    })?;
                if self
                    .tasks
                    .lock()
                    .expect("task lock")
                    .snapshot()
                    .iter()
                    .any(|task| {
                        task.workspace_session_id == session_id
                            && !matches!(
                                task.state,
                                TaskState::Succeeded
                                    | TaskState::Failed
                                    | TaskState::Cancelled
                                    | TaskState::TimedOut
                                    | TaskState::OutcomeUnknown
                            )
                    })
                {
                    return Err(RpcError::new(
                        "SESSION_NOT_QUIESCENT",
                        "session has queued or running tasks",
                    ));
                }
                let mut session = self
                    .state
                    .lock()
                    .expect("state lock")
                    .sessions
                    .iter()
                    .find(|session| session.metadata.id == session_id)
                    .cloned()
                    .ok_or_else(|| RpcError::new("SESSION_NOT_FOUND", "session not found"))?;
                let current = session.authority.clone().ok_or_else(|| {
                    RpcError::new("INVALID_AUTHORITY", "session has no authority")
                })?;
                if current.controller_id != self.id {
                    return Err(RpcError::new(
                        "NOT_SESSION_AUTHORITY",
                        format!("session is owned by {}", current.controller_id),
                    ));
                }
                if current
                    .pending_controller_id
                    .as_deref()
                    .is_some_and(|pending| pending != target_id)
                {
                    return Err(RpcError::new(
                        "HANDOFF_IN_PROGRESS",
                        format!(
                            "handoff is already pending to {:?}",
                            current.pending_controller_id
                        ),
                    ));
                }
                if current.pending_controller_id.is_none() {
                    session.authority = Some(SessionAuthority {
                        controller_id: self.id.clone(),
                        epoch: current.epoch,
                        pending_controller_id: Some(target_id.to_owned()),
                    });
                    let mut state = self.state.lock().expect("state lock");
                    upsert_by(&mut state.sessions, session.clone(), |existing| {
                        existing.metadata.id == session.metadata.id
                    });
                    drop(state);
                    self.persist()?;
                }
                session.authority = Some(SessionAuthority {
                    controller_id: target_id.to_owned(),
                    epoch: current.epoch.saturating_add(1),
                    pending_controller_id: None,
                });
                session.metadata.updated_at = now_ms();
                let bundle = self.session_bundle(&session);
                let response = call_executor(
                    &target.endpoint,
                    &traced_request("session.accept-handoff", bundle),
                )
                .map_err(|error| RpcError::new("CONTROLLER_UNAVAILABLE", error.to_string()))?;
                if !response.ok {
                    return Err(response.error.unwrap_or_else(|| {
                        RpcError::new("HANDOFF_REJECTED", "target rejected session handoff")
                    }));
                }
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.sessions, session.clone(), |existing| {
                    existing.metadata.id == session.metadata.id
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(session).expect("session serializes"))
            }
            "session.get" => {
                let id = required_str(&params, "sessionId")?;
                let state = self.state.lock().expect("state lock");
                let value = state
                    .sessions
                    .iter()
                    .find(|item| item.metadata.id == id)
                    .ok_or_else(|| {
                        RpcError::new("SESSION_NOT_FOUND", format!("unknown session: {id}"))
                    })?;
                Ok(serde_json::to_value(value).expect("session serializes"))
            }
            "session.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").sessions,
            )
            .expect("sessions serialize")),
            "session.transition" => {
                let id = required_str(&params, "sessionId")?;
                let session_state: SessionState = serde_json::from_value(
                    params
                        .get("state")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "state is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                let session = state
                    .sessions
                    .iter_mut()
                    .find(|item| item.metadata.id == id)
                    .ok_or_else(|| {
                        RpcError::new("SESSION_NOT_FOUND", format!("unknown session: {id}"))
                    })?;
                session.state = session_state;
                session.metadata.updated_at = now_ms();
                let result = session.clone();
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(result).expect("session serializes"))
            }
            "artifact.put" => {
                let artifact: Artifact = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.artifacts, artifact.clone(), |item| {
                    item.digest == artifact.digest
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(artifact).expect("artifact serializes"))
            }
            "artifact.get" => {
                let digest = required_str(&params, "digest")?;
                let state = self.state.lock().expect("state lock");
                let value = state
                    .artifacts
                    .iter()
                    .find(|item| item.digest == digest)
                    .ok_or_else(|| {
                        RpcError::new("ARTIFACT_NOT_FOUND", format!("unknown artifact: {digest}"))
                    })?;
                Ok(serde_json::to_value(value).expect("artifact serializes"))
            }
            "artifact.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").artifacts,
            )
            .expect("artifacts serialize")),
            "artifact.transfer" => self.relay_artifact(&params),
            "generation.put" => {
                let generation: Generation = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                if let Some(existing) = state
                    .generations
                    .iter()
                    .find(|item| item.id == generation.id)
                {
                    if existing.workspace_session_id != generation.workspace_session_id
                        || existing.root != generation.root
                        || existing.baseline.digest != generation.baseline.digest
                    {
                        return Err(RpcError::new(
                            "GENERATION_IDENTITY_CONFLICT",
                            "generation identity fields are immutable",
                        ));
                    }
                    if !generation_transition_allowed(&existing.state, &generation.state) {
                        return Err(RpcError::new(
                            "INVALID_GENERATION_STATE",
                            format!(
                                "invalid generation transition {:?} -> {:?}",
                                existing.state, generation.state
                            ),
                        ));
                    }
                }
                upsert_by(&mut state.generations, generation.clone(), |item| {
                    item.id == generation.id
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(generation).expect("generation serializes"))
            }
            "generation.get" => {
                let id = required_str(&params, "generationId")?;
                let state = self.state.lock().expect("state lock");
                let value = state
                    .generations
                    .iter()
                    .find(|item| item.id == id)
                    .ok_or_else(|| {
                        RpcError::new("GENERATION_NOT_FOUND", format!("unknown generation: {id}"))
                    })?;
                Ok(serde_json::to_value(value).expect("generation serializes"))
            }
            "generation.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").generations,
            )
            .expect("generations serialize")),
            "agent.put" => {
                let agent: AgentInstance = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.agents, agent.clone(), |item| item.id == agent.id);
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(agent).expect("agent serializes"))
            }
            "agent.get" => {
                let id = required_str(&params, "agentId")?;
                let state = self.state.lock().expect("state lock");
                let value = state
                    .agents
                    .iter()
                    .find(|item| item.id == id)
                    .ok_or_else(|| {
                        RpcError::new("AGENT_NOT_FOUND", format!("unknown agent: {id}"))
                    })?;
                Ok(serde_json::to_value(value).expect("agent serializes"))
            }
            "agent.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").agents,
            )
            .expect("agents serialize")),
            "agent.transition" => {
                let id = required_str(&params, "agentId")?;
                let agent_state: AgentState = serde_json::from_value(
                    params
                        .get("state")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "state is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                let agent = state
                    .agents
                    .iter_mut()
                    .find(|item| item.id == id)
                    .ok_or_else(|| {
                        RpcError::new("AGENT_NOT_FOUND", format!("unknown agent: {id}"))
                    })?;
                agent.state = agent_state;
                let result = agent.clone();
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(result).expect("agent serializes"))
            }
            "executor.put" => {
                let executor: Executor = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                if let Some(existing) = state
                    .executors
                    .iter_mut()
                    .find(|existing| existing.metadata.id == executor.metadata.id)
                {
                    *existing = executor.clone();
                } else {
                    state.executors.push(executor.clone());
                }
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(executor).expect("executor serializes"))
            }
            "executor.register" => {
                let id = required_str(&params, "executorId")?.to_owned();
                let endpoint: ExecutorEndpoint = serde_json::from_value(
                    params
                        .get("endpoint")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "endpoint is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let response = call_executor(&endpoint, &traced_request("status", Value::Null))
                    .map_err(|error| {
                        let mut rpc = RpcError::new("EXECUTOR_UNAVAILABLE", error.to_string());
                        rpc.retryable = true;
                        rpc
                    })?;
                if !response.ok {
                    return Err(response
                        .error
                        .unwrap_or_else(|| RpcError::new("EXECUTOR_FAILED", "executor failed")));
                }
                let status = response.result.unwrap_or(Value::Null);
                let actual_id = status
                    .get("executorId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        RpcError::new(
                            "EXECUTOR_INVALID",
                            "executor status did not report an identity",
                        )
                    })?;
                if actual_id != id {
                    return Err(RpcError::new(
                        "EXECUTOR_IDENTITY_MISMATCH",
                        format!("endpoint reported Executor {actual_id}, expected {id}"),
                    ));
                }
                let capabilities: Vec<CapabilityDescriptor> = serde_json::from_value(
                    status
                        .get("capabilities")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|error| RpcError::new("EXECUTOR_INVALID", error.to_string()))?;
                let allowed_roots: Vec<String> = serde_json::from_value(
                    status
                        .get("allowedRoots")
                        .cloned()
                        .unwrap_or_else(|| json!([])),
                )
                .map_err(|error| RpcError::new("EXECUTOR_INVALID", error.to_string()))?;
                let now = now_ms();
                let executor = Executor {
                    api_version: "machine-fabric.dev/v1".to_owned(),
                    metadata: Metadata {
                        id: id.clone(),
                        labels: Default::default(),
                        created_at: now,
                        updated_at: now,
                    },
                    endpoint,
                    capabilities,
                    allowed_roots,
                    health: HealthStatus::Ready,
                };
                let mut state = self.state.lock().expect("state lock");
                upsert_by(&mut state.executors, executor.clone(), |existing| {
                    existing.metadata.id == id
                });
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(executor).expect("executor serializes"))
            }
            "executor.remove" => {
                let id = required_str(&params, "executorId")?;
                let mut state = self.state.lock().expect("state lock");
                let before = state.executors.len();
                state
                    .executors
                    .retain(|executor| executor.metadata.id != id);
                if state.executors.len() == before {
                    return Err(RpcError::new(
                        "EXECUTOR_NOT_FOUND",
                        format!("unknown executor: {id}"),
                    ));
                }
                drop(state);
                self.persist()?;
                Ok(json!({"executorId": id, "removed": true}))
            }
            "executor.call" => {
                let id = required_str(&params, "executorId")?;
                let executor = self
                    .state
                    .lock()
                    .expect("state lock")
                    .executors
                    .iter()
                    .find(|executor| executor.metadata.id == id)
                    .cloned()
                    .ok_or_else(|| {
                        RpcError::new("EXECUTOR_NOT_FOUND", format!("unknown executor: {id}"))
                    })?;
                let nested_action = required_str(&params, "action")?;
                let mut nested_params = params.get("params").cloned().unwrap_or(Value::Null);
                let lease_params = params
                    .get("leases")
                    .and_then(Value::as_array)
                    .cloned()
                    .or_else(|| {
                        params.get("leaseResource").map(|resource| {
                            vec![json!({
                                "resource": resource,
                                "owner": params.get("owner"),
                                "token": params.get("token"),
                            })]
                        })
                    });
                if let Some(lease_params) = lease_params {
                    let leases = self.leases.lock().expect("lease lock");
                    let authorities = lease_params
                        .iter()
                        .map(|item| {
                            let lease = leases
                                .validate(
                                    required_str(item, "resource")?,
                                    required_str(item, "owner")?,
                                    required_str(item, "token")?,
                                )
                                .map_err(map_lease_error)?;
                            Ok(json!({
                                "controllerId": self.id,
                                "resource": lease.resource,
                                "fence": lease.fence,
                            }))
                        })
                        .collect::<Result<Vec<_>, RpcError>>()?;
                    nested_params["_authority"] = Value::Array(authorities);
                }
                let nested = traced_request(nested_action, nested_params);
                let response = call_executor(&executor.endpoint, &nested).map_err(|error| {
                    let mut rpc = RpcError::new("EXECUTOR_UNAVAILABLE", error.to_string());
                    rpc.retryable = true;
                    rpc
                })?;
                if response.ok {
                    Ok(response.result.unwrap_or(Value::Null))
                } else {
                    Err(response
                        .error
                        .unwrap_or_else(|| RpcError::new("EXECUTOR_FAILED", "executor failed")))
                }
            }
            "approval.request" => {
                let digest = required_str(&params, "digest")?.to_owned();
                let owner = required_str(&params, "owner")?.to_owned();
                let mut state = self.state.lock().expect("state lock");
                if let Some(existing) = state.approvals.iter().find(|approval| {
                    approval.digest == digest
                        && approval.owner == owner
                        && matches!(
                            approval.state,
                            ApprovalState::Pending | ApprovalState::Approved
                        )
                }) {
                    return Ok(serde_json::to_value(existing).expect("approval serializes"));
                }
                let approval = Approval {
                    id: format!("approval_{}", Uuid::new_v4().simple()),
                    digest,
                    owner,
                    reason: required_str(&params, "reason")?.to_owned(),
                    state: ApprovalState::Pending,
                    created_at: now_ms(),
                    approved_at: None,
                    consumed_at: None,
                    expires_at: None,
                };
                state.approvals.push(approval.clone());
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(approval).expect("approval serializes"))
            }
            "approval.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").approvals,
            )
            .expect("approvals serialize")),
            "approval.approve" | "approval.revoke" => {
                let id = required_str(&params, "approvalId")?;
                let mut state = self.state.lock().expect("state lock");
                let approval = state
                    .approvals
                    .iter_mut()
                    .find(|approval| approval.id == id)
                    .ok_or_else(|| {
                        RpcError::new("APPROVAL_NOT_FOUND", format!("unknown approval: {id}"))
                    })?;
                if action == "approval.approve" {
                    if !matches!(approval.state, ApprovalState::Pending) {
                        return Err(RpcError::new(
                            "APPROVAL_NOT_PENDING",
                            "approval is not pending",
                        ));
                    }
                    let now = now_ms();
                    approval.state = ApprovalState::Approved;
                    approval.approved_at = Some(now);
                    approval.expires_at = Some(
                        now.saturating_add(
                            params
                                .get("ttlMs")
                                .and_then(Value::as_u64)
                                .unwrap_or(300_000),
                        ),
                    );
                } else {
                    approval.state = ApprovalState::Revoked;
                }
                let result = approval.clone();
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(result).expect("approval serializes"))
            }
            "capability.invoke" => {
                let executor_id = required_str(&params, "executorId")?;
                let capability_name = required_str(&params, "capability")?;
                let workspace_session_id = required_str(&params, "workspaceSessionId")?;
                let session_authority = self
                    .state
                    .lock()
                    .expect("state lock")
                    .sessions
                    .iter()
                    .find(|session| session.metadata.id == workspace_session_id)
                    .and_then(|session| session.authority.clone())
                    .ok_or_else(|| {
                        RpcError::new(
                            "SESSION_NOT_FOUND",
                            format!("unknown session: {workspace_session_id}"),
                        )
                    })?;
                if session_authority.controller_id != self.id {
                    return Err(RpcError::new(
                        "NOT_SESSION_AUTHORITY",
                        format!("session is owned by {}", session_authority.controller_id),
                    ));
                }
                let owner = required_str(&params, "owner")?;
                let idempotency_key = required_str(&params, "idempotencyKey")?;
                let mut input = params.get("input").cloned().unwrap_or_else(|| json!({}));
                let executor = self
                    .state
                    .lock()
                    .expect("state lock")
                    .executors
                    .iter()
                    .find(|executor| executor.metadata.id == executor_id)
                    .cloned()
                    .ok_or_else(|| {
                        RpcError::new(
                            "EXECUTOR_NOT_FOUND",
                            format!("unknown executor: {executor_id}"),
                        )
                    })?;
                let contract = executor
                    .capabilities
                    .iter()
                    .find(|capability| capability.name == capability_name)
                    .cloned()
                    .ok_or_else(|| {
                        RpcError::new(
                            "CAPABILITY_NOT_FOUND",
                            format!("executor does not provide {capability_name}"),
                        )
                    })?;
                if !matches!(contract.effect, machine_fabric_schema::Effect::ReadOnly) {
                    self.leases
                        .lock()
                        .expect("lease lock")
                        .validate(
                            &format!("workspace:{workspace_session_id}"),
                            owner,
                            required_str(&params, "driverToken")?,
                        )
                        .map_err(map_lease_error)?;
                }
                validate_schema(&contract.input_schema, &input, "input")?;
                if matches!(
                    capability_name,
                    "process.start" | "command.run" | "artifact.build" | "agent.start"
                ) && command_requires_approval(&input)
                {
                    let digest = command_digest(&input)?;
                    let approval_id = required_str(&params, "approvalId")?;
                    let mut state = self.state.lock().expect("state lock");
                    let approval = state
                        .approvals
                        .iter_mut()
                        .find(|approval| approval.id == approval_id)
                        .ok_or_else(|| RpcError::new("APPROVAL_NOT_FOUND", "approval not found"))?;
                    if approval.digest != digest
                        || approval.owner != owner
                        || !matches!(approval.state, ApprovalState::Approved)
                        || approval
                            .expires_at
                            .is_none_or(|expires| expires <= now_ms())
                    {
                        return Err(RpcError::new(
                            "APPROVAL_INVALID",
                            "approval does not authorize this exact command",
                        ));
                    }
                    approval.state = ApprovalState::Consumed;
                    approval.consumed_at = Some(now_ms());
                    input["approvalDigest"] = Value::String(digest);
                    drop(state);
                    self.persist()?;
                }
                let (correlation_id, request_id) = current_trace();
                let (task, reused) = self.tasks.lock().expect("task lock").submit_traced(
                    workspace_session_id,
                    executor_id,
                    capability_name,
                    input.clone(),
                    idempotency_key,
                    correlation_id,
                    request_id,
                );
                let task = if reused {
                    match task.state {
                        TaskState::Succeeded => {
                            return Ok(
                                json!({"task": task, "reused": true, "result": task.output}),
                            );
                        }
                        TaskState::Failed | TaskState::TimedOut | TaskState::OutcomeUnknown
                            if task.attempt < contract.retry.max_attempts =>
                        {
                            self.tasks
                                .lock()
                                .expect("task lock")
                                .retry(&task.id)
                                .map_err(|error| {
                                    RpcError::new("INVALID_TASK_STATE", error.to_string())
                                })?
                        }
                        TaskState::Queued | TaskState::Running => {
                            return Err(RpcError::new(
                                "TASK_IN_PROGRESS",
                                format!("task {} is already in progress", task.id),
                            ));
                        }
                        _ => {
                            return Err(RpcError::new(
                                "TASK_RETRY_EXHAUSTED",
                                format!("task {} exhausted retry policy", task.id),
                            ));
                        }
                    }
                } else {
                    task
                };
                let task = self
                    .tasks
                    .lock()
                    .expect("task lock")
                    .transition(&task.id, TaskState::Running, None, None)
                    .map_err(|error| RpcError::new("INVALID_TASK_STATE", error.to_string()))?;
                if !matches!(contract.effect, machine_fabric_schema::Effect::ReadOnly) {
                    input["_workspaceSessionId"] = Value::String(workspace_session_id.to_owned());
                }
                let response = call_executor(
                    &executor.endpoint,
                    &traced_request(capability_name, input.clone()),
                );
                match response {
                    Ok(response) if response.ok => {
                        let mut result = response.result.unwrap_or(Value::Null);
                        if capability_name == "process.start"
                            && input.get("waitReady").and_then(Value::as_bool) == Some(true)
                        {
                            match wait_for_process_readiness(&executor.endpoint, &input, result) {
                                Ok(ready) => result = ready,
                                Err(error) => {
                                    let _ = self.tasks.lock().expect("task lock").transition(
                                        &task.id,
                                        TaskState::Failed,
                                        None,
                                        Some(machine_fabric_schema::TaskError {
                                            code: error.code.clone(),
                                            message: error.message.clone(),
                                            retryable: error.retryable,
                                            details: error.details.clone(),
                                        }),
                                    );
                                    self.persist()?;
                                    return Err(error);
                                }
                            }
                        }
                        if let Err(error) =
                            validate_schema(&contract.output_schema, &result, "output")
                        {
                            let _ = self.tasks.lock().expect("task lock").transition(
                                &task.id,
                                TaskState::Failed,
                                None,
                                Some(machine_fabric_schema::TaskError {
                                    code: error.code.clone(),
                                    message: error.message.clone(),
                                    retryable: false,
                                    details: error.details.clone(),
                                }),
                            );
                            self.persist()?;
                            return Err(error);
                        }
                        let retained_output = retained_task_output(capability_name, &result);
                        let task = self
                            .tasks
                            .lock()
                            .expect("task lock")
                            .transition(&task.id, TaskState::Succeeded, Some(retained_output), None)
                            .map_err(|error| {
                                RpcError::new("INVALID_TASK_STATE", error.to_string())
                            })?;
                        if let Some(artifact) = artifact_from_result(
                            capability_name,
                            &input,
                            &result,
                            workspace_session_id,
                            executor_id,
                            &task.id,
                        ) {
                            let mut state = self.state.lock().expect("state lock");
                            upsert_by(&mut state.artifacts, artifact.clone(), |existing| {
                                existing.digest == artifact.digest
                            });
                        }
                        if capability_name == "agent.start" {
                            let agent = AgentInstance {
                                id: required_str(&input, "agentId")?.to_owned(),
                                workspace_session_id: workspace_session_id.to_owned(),
                                executor_id: executor_id.to_owned(),
                                role: required_str(&input, "role")?.to_owned(),
                                provider: input
                                    .get("provider")
                                    .and_then(Value::as_str)
                                    .unwrap_or("process")
                                    .to_owned(),
                                state: AgentState::Running,
                                metadata: result.clone(),
                            };
                            let mut state = self.state.lock().expect("state lock");
                            upsert_by(&mut state.agents, agent.clone(), |existing| {
                                existing.id == agent.id
                            });
                        } else if capability_name == "agent.stop"
                            && let Some(agent) = self
                                .state
                                .lock()
                                .expect("state lock")
                                .agents
                                .iter_mut()
                                .find(|agent| {
                                    agent.id
                                        == input
                                            .get("agentId")
                                            .and_then(Value::as_str)
                                            .unwrap_or_default()
                                })
                        {
                            agent.state = AgentState::Stopped;
                            agent.metadata = result.clone();
                        }
                        self.persist()?;
                        Ok(json!({"task": task, "reused": false, "result": result}))
                    }
                    Ok(response) => {
                        let error = response
                            .error
                            .unwrap_or_else(|| RpcError::new("EXECUTOR_FAILED", "executor failed"));
                        let _ = self.tasks.lock().expect("task lock").transition(
                            &task.id,
                            TaskState::Failed,
                            None,
                            Some(machine_fabric_schema::TaskError {
                                code: error.code.clone(),
                                message: error.message.clone(),
                                retryable: error.retryable,
                                details: error.details.clone(),
                            }),
                        );
                        self.persist()?;
                        Err(error)
                    }
                    Err(error) => {
                        let rpc = RpcError::new("EXECUTOR_UNAVAILABLE", error.to_string());
                        let _ = self.tasks.lock().expect("task lock").transition(
                            &task.id,
                            TaskState::Failed,
                            None,
                            Some(machine_fabric_schema::TaskError {
                                code: rpc.code.clone(),
                                message: rpc.message.clone(),
                                retryable: true,
                                details: Value::Null,
                            }),
                        );
                        self.persist()?;
                        Err(rpc)
                    }
                }
            }
            "driver.status" | "lease.status" => {
                let resource = required_str(&params, "resource")?;
                Ok(
                    serde_json::to_value(self.leases.lock().expect("lease lock").get(resource))
                        .expect("lease serializes"),
                )
            }
            "driver.acquire" | "lease.acquire" => {
                let resource = required_str(&params, "resource")?;
                let owner = required_str(&params, "owner")?;
                let ttl_ms = params
                    .get("ttlMs")
                    .and_then(Value::as_u64)
                    .unwrap_or(300_000);
                let kind = if action.starts_with("driver") {
                    LeaseKind::Driver
                } else {
                    LeaseKind::Resource
                };
                let lease = self
                    .leases
                    .lock()
                    .expect("lease lock")
                    .acquire(kind, resource, owner, ttl_ms)
                    .map_err(map_lease_error)?;
                self.persist()?;
                Ok(serde_json::to_value(lease).expect("lease serializes"))
            }
            "driver.renew" | "lease.renew" => {
                let lease = self
                    .leases
                    .lock()
                    .expect("lease lock")
                    .renew(
                        required_str(&params, "resource")?,
                        required_str(&params, "owner")?,
                        required_str(&params, "token")?,
                        params
                            .get("ttlMs")
                            .and_then(Value::as_u64)
                            .unwrap_or(300_000),
                    )
                    .map_err(map_lease_error)?;
                self.persist()?;
                Ok(serde_json::to_value(lease).expect("lease serializes"))
            }
            "driver.handoff" => {
                let lease = self
                    .leases
                    .lock()
                    .expect("lease lock")
                    .handoff(
                        required_str(&params, "resource")?,
                        required_str(&params, "owner")?,
                        required_str(&params, "token")?,
                        required_str(&params, "target")?,
                    )
                    .map_err(map_lease_error)?;
                self.persist()?;
                Ok(serde_json::to_value(lease).expect("lease serializes"))
            }
            "driver.take" => {
                let resource = required_str(&params, "resource")?;
                let owner = required_str(&params, "owner")?;
                let ttl_ms = params
                    .get("ttlMs")
                    .and_then(Value::as_u64)
                    .unwrap_or(300_000);
                let mut leases = self.leases.lock().expect("lease lock");
                let lease = if leases.get(resource).is_some() {
                    leases.take_handoff(resource, owner, ttl_ms)
                } else {
                    leases.acquire(LeaseKind::Driver, resource, owner, ttl_ms)
                }
                .map_err(map_lease_error)?;
                drop(leases);
                self.persist()?;
                Ok(serde_json::to_value(lease).expect("lease serializes"))
            }
            "driver.release" | "lease.release" => {
                let lease = self
                    .leases
                    .lock()
                    .expect("lease lock")
                    .release(
                        required_str(&params, "resource")?,
                        required_str(&params, "owner")?,
                        required_str(&params, "token")?,
                    )
                    .map_err(map_lease_error)?;
                self.persist()?;
                Ok(serde_json::to_value(lease).expect("lease serializes"))
            }
            "task.submit" => {
                let (correlation_id, request_id) = current_trace();
                let (task, reused) = self.tasks.lock().expect("task lock").submit_traced(
                    required_str(&params, "workspaceSessionId")?,
                    required_str(&params, "executorId")?,
                    required_str(&params, "capability")?,
                    params.get("input").cloned().unwrap_or(Value::Null),
                    required_str(&params, "idempotencyKey")?,
                    correlation_id,
                    request_id,
                );
                self.persist()?;
                Ok(json!({"task": task, "reused": reused}))
            }
            "task.get" => {
                let id = required_str(&params, "taskId")?;
                let tasks = self.tasks.lock().expect("task lock");
                let task = tasks.get(id).ok_or_else(|| {
                    RpcError::new("TASK_NOT_FOUND", format!("unknown task: {id}"))
                })?;
                Ok(serde_json::to_value(task).expect("task serializes"))
            }
            "task.list" => Ok(serde_json::to_value(
                self.tasks.lock().expect("task lock").snapshot(),
            )
            .expect("tasks serialize")),
            "task.events" => {
                let id = required_str(&params, "taskId")?;
                let tasks = self.tasks.lock().expect("task lock");
                let task = tasks.get(id).ok_or_else(|| {
                    RpcError::new("TASK_NOT_FOUND", format!("unknown task: {id}"))
                })?;
                Ok(json!({"taskId": id, "events": task.events}))
            }
            "task.wait" => {
                let id = required_str(&params, "taskId")?.to_owned();
                let timeout_ms = params
                    .get("timeoutMs")
                    .and_then(Value::as_u64)
                    .unwrap_or(30_000)
                    .min(3_600_000);
                let deadline = Instant::now() + Duration::from_millis(timeout_ms);
                loop {
                    let task = self
                        .tasks
                        .lock()
                        .expect("task lock")
                        .get(&id)
                        .cloned()
                        .ok_or_else(|| {
                            RpcError::new("TASK_NOT_FOUND", format!("unknown task: {id}"))
                        })?;
                    if task.state.terminal() {
                        break Ok(serde_json::to_value(task).expect("task serializes"));
                    }
                    if Instant::now() >= deadline {
                        break Err(RpcError::new(
                            "TASK_WAIT_TIMEOUT",
                            format!("task {id} did not finish before timeout"),
                        ));
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
            "task.prune" => {
                let cutoff = params
                    .get("before")
                    .and_then(Value::as_u64)
                    .unwrap_or_else(now_ms);
                let removed = self
                    .tasks
                    .lock()
                    .expect("task lock")
                    .prune_terminal_before(cutoff);
                self.persist()?;
                Ok(json!({
                    "pruned": removed.len(),
                    "taskIds": removed.into_iter().map(|task| task.id).collect::<Vec<_>>()
                }))
            }
            "task.transition" => {
                let state: TaskState = serde_json::from_value(
                    params
                        .get("state")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "state is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let task = self
                    .tasks
                    .lock()
                    .expect("task lock")
                    .transition(
                        required_str(&params, "taskId")?,
                        state,
                        params.get("output").cloned(),
                        None,
                    )
                    .map_err(|error| RpcError::new("INVALID_TASK_STATE", error.to_string()))?;
                self.persist()?;
                Ok(serde_json::to_value(task).expect("task serializes"))
            }
            "port.list" => Ok(serde_json::to_value(
                self.leases
                    .lock()
                    .expect("lease lock")
                    .snapshot()
                    .into_iter()
                    .filter(|lease| lease.resource.starts_with("port:"))
                    .collect::<Vec<_>>(),
            )
            .expect("port leases serialize")),
            "port.allocate" => {
                let executor_id = required_str(&params, "executorId")?;
                let owner = required_str(&params, "owner")?;
                let start = params
                    .get("start")
                    .and_then(Value::as_u64)
                    .unwrap_or(20_000);
                let end = params.get("end").and_then(Value::as_u64).unwrap_or(40_000);
                if start == 0 || end > 65_535 || start > end {
                    return Err(RpcError::new("INVALID_PARAMS", "invalid port range"));
                }
                let executor = self
                    .state
                    .lock()
                    .expect("state lock")
                    .executors
                    .iter()
                    .find(|executor| executor.metadata.id == executor_id)
                    .cloned()
                    .ok_or_else(|| RpcError::new("EXECUTOR_NOT_FOUND", "executor not found"))?;
                for port in start..=end {
                    let resource = format!("port:{executor_id}:{port}");
                    let lease = match self.leases.lock().expect("lease lock").acquire(
                        LeaseKind::Resource,
                        &resource,
                        owner,
                        params
                            .get("ttlMs")
                            .and_then(Value::as_u64)
                            .unwrap_or(300_000),
                    ) {
                        Ok(lease) => lease,
                        Err(LeaseError::Active { .. }) => continue,
                        Err(error) => return Err(map_lease_error(error)),
                    };
                    let response = call_executor(
                        &executor.endpoint,
                        &traced_request("port.check", json!({"port": port})),
                    );
                    let available = response
                        .ok()
                        .and_then(|response| response.result)
                        .and_then(|result| result.get("available").and_then(Value::as_bool))
                        .unwrap_or(false);
                    if available {
                        self.persist()?;
                        return Ok(
                            json!({"executorId": executor_id, "port": port, "lease": lease}),
                        );
                    }
                    let _ = self.leases.lock().expect("lease lock").release(
                        &resource,
                        owner,
                        &lease.token,
                    );
                }
                Err(RpcError::new(
                    "PORT_UNAVAILABLE",
                    format!("no available port in {start}..={end}"),
                ))
            }
            "transaction.begin" => {
                let idempotency_key = required_str(&params, "idempotencyKey")?;
                let mut state = self.state.lock().expect("state lock");
                if let Some(existing) = state
                    .transactions
                    .iter()
                    .find(|transaction| transaction.idempotency_key == idempotency_key)
                {
                    return Ok(json!({"transaction": existing, "reused": true}));
                }
                let now = now_ms();
                let transaction = ActivationTransaction {
                    id: format!("transaction_{}", Uuid::new_v4().simple()),
                    workspace_session_id: required_str(&params, "workspaceSessionId")?.to_owned(),
                    idempotency_key: idempotency_key.to_owned(),
                    target: required_str(&params, "target")?.to_owned(),
                    generation_id: required_str(&params, "generationId")?.to_owned(),
                    previous_generation_id: params
                        .get("previousGenerationId")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    state: TransactionState::Planned,
                    created_at: now,
                    updated_at: now,
                    completed_steps: Vec::new(),
                    journal: Vec::new(),
                    lease_fence: params.get("leaseFence").and_then(Value::as_u64),
                    evidence: Vec::new(),
                    error: None,
                };
                state.transactions.push(transaction.clone());
                drop(state);
                self.persist()?;
                Ok(json!({"transaction": transaction, "reused": false}))
            }
            "transaction.get" => {
                let id = required_str(&params, "transactionId")?;
                let state = self.state.lock().expect("state lock");
                let transaction = state
                    .transactions
                    .iter()
                    .find(|transaction| transaction.id == id)
                    .ok_or_else(|| {
                        RpcError::new(
                            "TRANSACTION_NOT_FOUND",
                            format!("unknown transaction: {id}"),
                        )
                    })?;
                Ok(serde_json::to_value(transaction).expect("transaction serializes"))
            }
            "transaction.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").transactions,
            )
            .expect("transactions serialize")),
            "transaction.record" => {
                let id = required_str(&params, "transactionId")?;
                let step = required_str(&params, "step")?.to_owned();
                let step_state: TransactionStepState = serde_json::from_value(
                    params
                        .get("state")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "state is required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut state = self.state.lock().expect("state lock");
                let transaction = state
                    .transactions
                    .iter_mut()
                    .find(|transaction| transaction.id == id)
                    .ok_or_else(|| {
                        RpcError::new(
                            "TRANSACTION_NOT_FOUND",
                            format!("unknown transaction: {id}"),
                        )
                    })?;
                if let (Some(expected), Some(actual)) = (
                    transaction.lease_fence,
                    params.get("fence").and_then(Value::as_u64),
                ) && expected != actual
                {
                    return Err(RpcError::new(
                        "STALE_FENCING_TOKEN",
                        format!("transaction fence is {expected}, request used {actual}"),
                    ));
                }
                let now = now_ms();
                let attempt = params.get("attempt").and_then(Value::as_u64).unwrap_or(1) as u32;
                transaction.journal.push(TransactionJournalEntry {
                    sequence: transaction.journal.len() as u64 + 1,
                    step: step.clone(),
                    state: step_state.clone(),
                    attempt,
                    started_at: params
                        .get("startedAt")
                        .and_then(Value::as_u64)
                        .unwrap_or(now),
                    finished_at: if matches!(
                        step_state,
                        TransactionStepState::Succeeded
                            | TransactionStepState::Failed
                            | TransactionStepState::Compensated
                            | TransactionStepState::OutcomeUnknown
                    ) {
                        Some(now)
                    } else {
                        None
                    },
                    input_digest: params
                        .get("inputDigest")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    output_digest: params
                        .get("outputDigest")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    executor_id: params
                        .get("executorId")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    fence: params.get("fence").and_then(Value::as_u64),
                    details: params.get("details").cloned().unwrap_or(Value::Null),
                });
                transaction.updated_at = now;
                if step == "activate"
                    && let Some(previous) =
                        params.get("previousGenerationId").and_then(Value::as_str)
                {
                    transaction.previous_generation_id = Some(previous.to_owned());
                }
                match step_state {
                    TransactionStepState::Started => transaction.state = TransactionState::Running,
                    TransactionStepState::Succeeded => {
                        if !transaction.completed_steps.contains(&step) {
                            transaction.completed_steps.push(step.clone());
                        }
                        if step == "activate" {
                            transaction.state = TransactionState::Activated;
                        }
                    }
                    TransactionStepState::Failed => transaction.state = TransactionState::Failed,
                    TransactionStepState::OutcomeUnknown => {
                        transaction.state = TransactionState::OutcomeUnknown
                    }
                    TransactionStepState::Compensated => {
                        transaction.state = TransactionState::RolledBack
                    }
                    TransactionStepState::Planned => {}
                }
                let result = transaction.clone();
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(result).expect("transaction serializes"))
            }
            "handoff.create" => {
                let mut handoff: Handoff = serde_json::from_value(params)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                if handoff.id.is_empty() {
                    handoff.id = format!("handoff_{}", Uuid::new_v4().simple());
                }
                if handoff.created_at == 0 {
                    handoff.created_at = now_ms();
                }
                self.state
                    .lock()
                    .expect("state lock")
                    .handoffs
                    .push(handoff.clone());
                self.persist()?;
                Ok(serde_json::to_value(handoff).expect("handoff serializes"))
            }
            "handoff.get" => {
                let id = required_str(&params, "handoffId")?;
                let state = self.state.lock().expect("state lock");
                let handoff = state
                    .handoffs
                    .iter()
                    .find(|handoff| handoff.id == id)
                    .ok_or_else(|| {
                        RpcError::new("HANDOFF_NOT_FOUND", format!("unknown handoff: {id}"))
                    })?;
                Ok(serde_json::to_value(handoff).expect("handoff serializes"))
            }
            "handoff.list" => Ok(serde_json::to_value(
                &self.state.lock().expect("state lock").handoffs,
            )
            .expect("handoffs serialize")),
            "handoff.acknowledge" | "handoff.complete" => {
                let id = required_str(&params, "handoffId")?;
                let mut state = self.state.lock().expect("state lock");
                let handoff = state
                    .handoffs
                    .iter_mut()
                    .find(|handoff| handoff.id == id)
                    .ok_or_else(|| {
                        RpcError::new("HANDOFF_NOT_FOUND", format!("unknown handoff: {id}"))
                    })?;
                let now = now_ms();
                if action == "handoff.acknowledge" {
                    if handoff.acknowledged_at.is_some() {
                        return Err(RpcError::new(
                            "HANDOFF_ALREADY_ACKNOWLEDGED",
                            format!("handoff already acknowledged: {id}"),
                        ));
                    }
                    handoff.acknowledged_at = Some(now);
                } else {
                    if handoff.acknowledged_at.is_none() {
                        return Err(RpcError::new(
                            "HANDOFF_NOT_ACKNOWLEDGED",
                            "handoff must be acknowledged before completion",
                        ));
                    }
                    handoff.completed_at = Some(now);
                }
                let result = handoff.clone();
                drop(state);
                self.persist()?;
                Ok(serde_json::to_value(result).expect("handoff serializes"))
            }
            _ => Err(RpcError::new(
                "UNKNOWN_ACTION",
                format!("unknown action: {action}"),
            )),
        }
    }

    fn call_registered_executor(
        &self,
        executor_id: &str,
        action: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
        let executor = self
            .state
            .lock()
            .expect("state lock")
            .executors
            .iter()
            .find(|executor| executor.metadata.id == executor_id)
            .cloned()
            .ok_or_else(|| {
                RpcError::new(
                    "EXECUTOR_NOT_FOUND",
                    format!("unknown executor: {executor_id}"),
                )
            })?;
        let response = call_executor(&executor.endpoint, &traced_request(action, params)).map_err(
            |error| {
                let mut rpc = RpcError::new("EXECUTOR_UNAVAILABLE", error.to_string());
                rpc.retryable = true;
                rpc
            },
        )?;
        if response.ok {
            Ok(response.result.unwrap_or(Value::Null))
        } else {
            Err(response
                .error
                .unwrap_or_else(|| RpcError::new("EXECUTOR_FAILED", "executor failed")))
        }
    }

    fn relay_artifact(&self, params: &Value) -> Result<Value, RpcError> {
        let transfer_started = Instant::now();
        let source = params
            .get("source")
            .ok_or_else(|| RpcError::new("INVALID_PARAMS", "source is required"))?;
        let destination = params
            .get("destination")
            .ok_or_else(|| RpcError::new("INVALID_PARAMS", "destination is required"))?;
        let source_executor = required_str(source, "executorId")?;
        let source_path = required_str(source, "path")?;
        let destination_executor = required_str(destination, "executorId")?;
        let destination_path = required_str(destination, "path")?;
        let mode = params
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("mirror");
        if mode != "mirror" {
            return Err(RpcError::new(
                "INVALID_PARAMS",
                "Controller artifact relay currently requires mode=mirror",
            ));
        }
        transfer_event(
            "manifesting",
            json!({"sourceExecutorId": source_executor, "destinationExecutorId": destination_executor, "sourcePath": source_path, "destinationPath": destination_path}),
        );
        let archive_started = Instant::now();
        match self.call_registered_executor(
            source_executor,
            "artifact.relay.archive.create",
            json!({"path": source_path}),
        ) {
            Ok(archive) => {
                transfer_event(
                    "archived",
                    json!({"durationMs": archive_started.elapsed().as_millis(), "archiveSize": archive.get("archiveSize"), "size": archive.get("size"), "files": archive.get("files")}),
                );
                let token = required_str(&archive, "token")?.to_owned();
                let result = self.relay_archive(
                    source_executor,
                    destination_executor,
                    destination_path,
                    mode,
                    archive,
                );
                let _ = self.call_registered_executor(
                    source_executor,
                    "artifact.relay.archive.remove",
                    json!({"token": token}),
                );
                return result.map(|mut value| {
                    value["transfer"]["totalDurationMs"] =
                        json!(transfer_started.elapsed().as_millis());
                    value
                });
            }
            Err(error) if error.code == "UNKNOWN_ACTION" => {}
            Err(error) => return Err(error),
        }
        let mut manifest = self.call_registered_executor(
            source_executor,
            "artifact.relay.manifest",
            json!({"path": source_path}),
        )?;
        let stability_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            thread::sleep(Duration::from_millis(250));
            let next = self.call_registered_executor(
                source_executor,
                "artifact.relay.manifest",
                json!({"path": source_path}),
            )?;
            if next.get("digest") == manifest.get("digest")
                && next.get("entries") == manifest.get("entries")
            {
                manifest = next;
                break;
            }
            if Instant::now() >= stability_deadline {
                return Err(RpcError::new(
                    "ARTIFACT_NOT_STABLE",
                    "source artifact changed during the stability window",
                ));
            }
            manifest = next;
        }
        let entries = manifest
            .get("entries")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| RpcError::new("INVALID_ARTIFACT", "source manifest has no entries"))?;
        if params.get("rejectEmptyFiles").and_then(Value::as_bool) == Some(true)
            && entries.iter().any(|entry| {
                entry.get("kind").and_then(Value::as_str) == Some("file")
                    && entry.get("size").and_then(Value::as_u64) == Some(0)
            })
        {
            return Err(RpcError::new(
                "ARTIFACT_INCOMPLETE",
                "source artifact contains an empty file",
            ));
        }
        let expected_digest = required_str(&manifest, "digest")?.to_owned();
        let resource = format!("artifact-relay:{destination_path}");
        let owner = format!("controller:{}", self.id);
        let lease = self
            .leases
            .lock()
            .expect("lease lock")
            .acquire(
                LeaseKind::Resource,
                resource.clone(),
                owner.clone(),
                3_600_000,
            )
            .map_err(map_lease_error)?;
        self.persist()?;
        let staging = format!(
            "{destination_path}.machine-fabric-relay-{}",
            Uuid::new_v4().simple()
        );
        let authority =
            json!([{"controllerId": self.id, "resource": resource, "fence": lease.fence}]);
        let result = (|| {
            self.call_registered_executor(
                destination_executor,
                "artifact.relay.prepare",
                json!({"destination": destination_path, "staging": staging, "entries": entries, "_authority": authority}),
            )?;
            for entry in &entries {
                if required_str(entry, "kind")? != "file" {
                    continue;
                }
                let relative = required_str(entry, "path")?;
                let size = entry
                    .get("size")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_ARTIFACT", "file size is required"))?;
                let mut offset = 0_u64;
                while offset < size {
                    let chunk = self.call_registered_executor(
                        source_executor,
                        "artifact.relay.read",
                        json!({"path": source_path, "relativePath": relative, "offset": offset, "limit": 1024 * 1024}),
                    )?;
                    let bytes = chunk.get("bytes").and_then(Value::as_u64).ok_or_else(|| {
                        RpcError::new("INVALID_ARTIFACT", "relay chunk has no byte count")
                    })?;
                    if bytes == 0 {
                        return Err(RpcError::new(
                            "ARTIFACT_TRANSFER_FAILED",
                            format!("unexpected EOF while reading {relative}"),
                        ));
                    }
                    self.call_registered_executor(
                        destination_executor,
                        "artifact.relay.write",
                        json!({"destination": destination_path, "staging": staging, "relativePath": relative, "offset": offset, "data": chunk["data"], "_authority": authority}),
                    )?;
                    offset = offset.saturating_add(bytes);
                }
            }
            let committed = self.call_registered_executor(
                destination_executor,
                "artifact.relay.commit",
                json!({"destination": destination_path, "staging": staging, "expectedDigest": expected_digest, "_authority": authority}),
            )?;
            Ok(json!({
                "source": {"executorId": source_executor, "path": source_path},
                "destination": {"executorId": destination_executor, "path": destination_path},
                "mode": mode,
                "digest": committed["digest"], "size": committed["size"], "files": committed["files"]
            }))
        })();
        let _ = self
            .leases
            .lock()
            .expect("lease lock")
            .release(&resource, &owner, &lease.token);
        self.persist()?;
        result
    }

    fn relay_archive(
        &self,
        source_executor: &str,
        destination_executor: &str,
        destination_path: &str,
        mode: &str,
        archive: Value,
    ) -> Result<Value, RpcError> {
        let token = required_str(&archive, "token")?.to_owned();
        let expected_digest = required_str(&archive, "digest")?.to_owned();
        let archive_size = archive
            .get("archiveSize")
            .and_then(Value::as_u64)
            .ok_or_else(|| RpcError::new("INVALID_ARTIFACT", "archive size is required"))?;
        let size = archive
            .get("size")
            .and_then(Value::as_u64)
            .ok_or_else(|| RpcError::new("INVALID_ARTIFACT", "artifact size is required"))?;
        let files = archive
            .get("files")
            .and_then(Value::as_u64)
            .ok_or_else(|| RpcError::new("INVALID_ARTIFACT", "artifact file count is required"))?;
        let resource = format!("artifact-relay:{destination_path}");
        let owner = format!("controller:{}", self.id);
        let lease = self
            .leases
            .lock()
            .expect("lease lock")
            .acquire(
                LeaseKind::Resource,
                resource.clone(),
                owner.clone(),
                3_600_000,
            )
            .map_err(map_lease_error)?;
        self.persist()?;
        let staging = format!(
            "{destination_path}.machine-fabric-relay-{}",
            Uuid::new_v4().simple()
        );
        let authority =
            json!([{"controllerId": self.id, "resource": resource, "fence": lease.fence}]);
        let result = (|| {
            let prepare_started = Instant::now();
            transfer_event(
                "preparing",
                json!({"destinationExecutorId": destination_executor, "archiveSize": archive_size}),
            );
            self.call_registered_executor(
                destination_executor,
                "artifact.relay.archive.prepare",
                json!({"destination": destination_path, "staging": staging, "_authority": authority}),
            )
            .map_err(|error| transfer_error(error, "preparing", 0, 0))?;
            transfer_event(
                "prepared",
                json!({"durationMs": prepare_started.elapsed().as_millis()}),
            );
            let mut offset = 0_u64;
            let mut chunks = 0_u64;
            let mut encoded_bytes = 0_u64;
            let relay_started = Instant::now();
            transfer_event(
                "relay-started",
                json!({"chunkSize": 8 * 1024 * 1024, "archiveSize": archive_size}),
            );
            while offset < archive_size {
                let chunk = self
                    .call_registered_executor(
                        source_executor,
                        "artifact.relay.archive.read",
                        json!({"token": token, "offset": offset, "limit": 8 * 1024 * 1024}),
                    )
                    .map_err(|error| transfer_error(error, "reading", offset, chunks))?;
                let bytes = chunk.get("bytes").and_then(Value::as_u64).ok_or_else(|| {
                    RpcError::new("INVALID_ARTIFACT", "archive chunk has no byte count")
                })?;
                if bytes == 0 {
                    return Err(transfer_error(
                        RpcError::new(
                            "ARTIFACT_TRANSFER_FAILED",
                            "unexpected EOF while reading archive",
                        ),
                        "reading",
                        offset,
                        chunks,
                    ));
                }
                chunks += 1;
                encoded_bytes += chunk
                    .get("data")
                    .and_then(Value::as_str)
                    .map(|data| data.len() as u64)
                    .unwrap_or(0);
                self.call_registered_executor(
                    destination_executor,
                    "artifact.relay.archive.write",
                    json!({"destination": destination_path, "staging": staging, "offset": offset, "data": chunk["data"], "_authority": authority}),
                )
                .map_err(|error| transfer_error(error, "writing", offset, chunks))?;
                offset = offset.saturating_add(bytes);
                transfer_event(
                    "chunk-transferred",
                    json!({"chunk": chunks, "bytes": bytes, "transferredBytes": offset.min(archive_size), "archiveSize": archive_size}),
                );
            }
            let relay_duration_ms = relay_started.elapsed().as_millis();
            transfer_event(
                "relay-completed",
                json!({"chunks": chunks, "transferredBytes": offset, "encodedBytes": encoded_bytes, "durationMs": relay_duration_ms}),
            );
            let commit_started = Instant::now();
            transfer_event("verifying", json!({"expectedDigest": expected_digest}));
            let committed = self
                .call_registered_executor(
                    destination_executor,
                    "artifact.relay.archive.commit",
                    json!({
                        "destination": destination_path, "staging": staging,
                        "expectedDigest": expected_digest, "archiveSize": archive_size,
                        "size": size, "files": files, "_authority": authority
                    }),
                )
                .map_err(|error| transfer_error(error, "committing", offset, chunks))?;
            let commit_duration_ms = commit_started.elapsed().as_millis();
            transfer_event(
                "committed",
                json!({"durationMs": commit_duration_ms, "digest": committed.get("digest")}),
            );
            Ok(json!({
                "source": {"executorId": source_executor},
                "destination": {"executorId": destination_executor, "path": destination_path},
                "mode": mode, "transport": "archive", "compression": "gzip",
                "archiveSize": archive_size,
                "digest": committed["digest"], "size": committed["size"], "files": committed["files"],
                "transfer": {
                    "rawBytes": size, "archiveBytes": archive_size, "transferredBytes": offset,
                    "encodedBytes": encoded_bytes, "chunks": chunks, "chunkSize": 8 * 1024 * 1024,
                    "retryCount": 0,
                    "relayDurationMs": relay_duration_ms, "commitDurationMs": commit_duration_ms,
                    "throughputBytesPerSecond": if relay_duration_ms == 0 { archive_size * 1000 } else { archive_size.saturating_mul(1000) / relay_duration_ms as u64 }
                }
            }))
        })();
        let _ = self
            .leases
            .lock()
            .expect("lease lock")
            .release(&resource, &owner, &lease.token);
        self.persist()?;
        result
    }

    fn route_session_action(
        &self,
        action: &str,
        params: &Value,
    ) -> Result<Option<Value>, RpcError> {
        if matches!(action, "session.put" | "session.accept-handoff") {
            return Ok(None);
        }
        let session_id = self.session_id_for_action(action, params);
        let Some(session_id) = session_id else {
            return Ok(None);
        };
        let authority_id = self
            .state
            .lock()
            .expect("state lock")
            .sessions
            .iter()
            .find(|session| session.metadata.id == session_id)
            .and_then(|session| session.authority.as_ref())
            .map(|authority| authority.controller_id.clone());
        let pending = self
            .state
            .lock()
            .expect("state lock")
            .sessions
            .iter()
            .find(|session| session.metadata.id == session_id)
            .and_then(|session| session.authority.as_ref())
            .and_then(|authority| authority.pending_controller_id.clone());
        if let Some(target) = pending
            && !matches!(action, "session.get" | "session.handoff")
        {
            return Err(RpcError::new(
                "SESSION_HANDOFF_IN_PROGRESS",
                format!("session handoff to {target} is in progress"),
            ));
        }
        let Some(authority_id) = authority_id else {
            return Ok(None);
        };
        if authority_id == self.id {
            return Ok(None);
        }
        let controller = self
            .state
            .lock()
            .expect("state lock")
            .controllers
            .iter()
            .find(|controller| controller.metadata.id == authority_id)
            .cloned()
            .ok_or_else(|| {
                RpcError::new(
                    "SESSION_AUTHORITY_UNAVAILABLE",
                    format!("home controller {authority_id} is not registered"),
                )
            })?;
        let response = call_executor(
            &controller.endpoint,
            &traced_request(action, params.clone()),
        )
        .map_err(|error| RpcError::new("SESSION_AUTHORITY_UNAVAILABLE", error.to_string()))?;
        if response.ok {
            Ok(Some(response.result.unwrap_or(Value::Null)))
        } else {
            Err(response.error.unwrap_or_else(|| {
                RpcError::new(
                    "SESSION_AUTHORITY_FAILED",
                    "home controller rejected request",
                )
            }))
        }
    }

    fn session_gate(&self, session_id: &str) -> Arc<Mutex<()>> {
        let mut gates = self.session_gates.lock().expect("session gates lock");
        Arc::clone(
            gates
                .entry(session_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
    }

    fn session_id_for_action(&self, action: &str, params: &Value) -> Option<String> {
        let mut session_id = params
            .get("workspaceSessionId")
            .or_else(|| params.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        if session_id.is_none() && action == "session.put" {
            session_id = params
                .pointer("/metadata/id")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        if session_id.is_none() && action == "session.accept-handoff" {
            session_id = params
                .pointer("/session/metadata/id")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        if session_id.is_none() && action == "artifact.put" {
            session_id = params
                .pointer("/provenance/workspaceSessionId")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        if session_id.is_none() && (action.starts_with("driver.") || action.starts_with("lease.")) {
            session_id = params
                .get("resource")
                .and_then(Value::as_str)
                .and_then(|resource| resource.strip_prefix("workspace:"))
                .map(str::to_owned);
        }
        if session_id.is_none() {
            let state = self.state.lock().expect("state lock");
            session_id = match action {
                name if name.starts_with("agent.") => params
                    .get("agentId")
                    .and_then(Value::as_str)
                    .and_then(|id| state.agents.iter().find(|agent| agent.id == id))
                    .map(|agent| agent.workspace_session_id.clone()),
                name if name.starts_with("task.") => params
                    .get("taskId")
                    .and_then(Value::as_str)
                    .and_then(|id| state.tasks.iter().find(|task| task.id == id))
                    .map(|task| task.workspace_session_id.clone()),
                name if name.starts_with("transaction.") => params
                    .get("transactionId")
                    .and_then(Value::as_str)
                    .and_then(|id| {
                        state
                            .transactions
                            .iter()
                            .find(|transaction| transaction.id == id)
                    })
                    .map(|transaction| transaction.workspace_session_id.clone()),
                name if name.starts_with("handoff.") => params
                    .get("handoffId")
                    .and_then(Value::as_str)
                    .and_then(|id| state.handoffs.iter().find(|handoff| handoff.id == id))
                    .map(|handoff| handoff.workspace_session_id.clone()),
                _ => None,
            };
        }
        session_id
    }

    fn session_bundle(&self, session: &WorkspaceSession) -> Value {
        let session_id = &session.metadata.id;
        let state = self.state.lock().expect("state lock");
        json!({
            "session": session,
            "tasks": self.tasks.lock().expect("task lock").snapshot().into_iter()
                .filter(|task| &task.workspace_session_id == session_id).collect::<Vec<_>>(),
            "artifacts": state.artifacts.iter()
                .filter(|artifact| &artifact.provenance.workspace_session_id == session_id).cloned().collect::<Vec<_>>(),
            "generations": state.generations.iter()
                .filter(|generation| &generation.workspace_session_id == session_id).cloned().collect::<Vec<_>>(),
            "transactions": state.transactions.iter()
                .filter(|transaction| &transaction.workspace_session_id == session_id).cloned().collect::<Vec<_>>(),
            "agents": state.agents.iter()
                .filter(|agent| &agent.workspace_session_id == session_id).cloned().collect::<Vec<_>>(),
            "handoffs": state.handoffs.iter()
                .filter(|handoff| &handoff.workspace_session_id == session_id).cloned().collect::<Vec<_>>(),
        })
    }

    fn persist(&self) -> Result<(), RpcError> {
        let mut state = self.state.lock().expect("state lock");
        let leases = self.leases.lock().expect("lease lock");
        state.leases = leases.snapshot();
        state.lease_fences = leases.fence_snapshot();
        drop(leases);
        state.tasks = self.tasks.lock().expect("task lock").snapshot();
        self.store
            .save(&state)
            .map_err(|error| RpcError::new("STATE_WRITE_FAILED", error.to_string()))
    }
}

fn required_str<'a>(params: &'a Value, key: &str) -> Result<&'a str, RpcError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", format!("{key} is required")))
}

fn optional_array<T: serde::de::DeserializeOwned>(
    params: &Value,
    key: &str,
) -> Result<Vec<T>, RpcError> {
    params
        .get(key)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| RpcError::new("INVALID_PARAMS", format!("{key}: {error}")))
        .map(Option::unwrap_or_default)
}

fn validate_schema(schema: &Value, value: &Value, path: &str) -> Result<(), RpcError> {
    if schema.as_object().is_none_or(|object| object.is_empty()) {
        return Ok(());
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(schema_error(path, "value is not in enum"));
    }
    if let Some(types) = schema.get("type") {
        let matches = match types {
            Value::String(kind) => value_matches_type(value, kind),
            Value::Array(kinds) => kinds
                .iter()
                .filter_map(Value::as_str)
                .any(|kind| value_matches_type(value, kind)),
            _ => false,
        };
        if !matches {
            return Err(schema_error(path, &format!("does not match type {types}")));
        }
    }
    if let Some(minimum) = schema.get("minimum").and_then(Value::as_f64)
        && value.as_f64().is_some_and(|number| number < minimum)
    {
        return Err(schema_error(path, &format!("is below minimum {minimum}")));
    }
    if let Some(maximum) = schema.get("maximum").and_then(Value::as_f64)
        && value.as_f64().is_some_and(|number| number > maximum)
    {
        return Err(schema_error(path, &format!("is above maximum {maximum}")));
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    return Err(schema_error(&format!("{path}.{key}"), "is required"));
                }
            }
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (key, child) in object {
                if let Some(child_schema) = properties.get(key) {
                    validate_schema(child_schema, child, &format!("{path}.{key}"))?;
                } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                    return Err(schema_error(&format!("{path}.{key}"), "is not allowed"));
                }
            }
        }
    }
    if let (Some(items), Some(values)) = (schema.get("items"), value.as_array()) {
        for (index, child) in values.iter().enumerate() {
            validate_schema(items, child, &format!("{path}[{index}]"))?;
        }
    }
    Ok(())
}

fn value_matches_type(value: &Value, kind: &str) -> bool {
    match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "number" => value.is_number(),
        "string" => value.is_string(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn retained_task_output(capability: &str, result: &Value) -> Value {
    if capability != "ui.native-inspect" {
        return result.clone();
    }
    json!({
        "applicationPath": result.get("applicationPath").cloned().unwrap_or(Value::Null),
        "pids": result.get("pids").cloned().unwrap_or_else(|| json!([])),
        "inspection": {
            "accessibilityTrusted": result.pointer("/inspection/accessibilityTrusted")
                .cloned().unwrap_or(Value::Bool(false)),
            "processCount": result.pointer("/inspection/processes")
                .and_then(Value::as_array).map_or(0, Vec::len),
        },
        "inspectedAt": result.get("inspectedAt").cloned().unwrap_or(Value::Null),
    })
}

fn compact_task_outputs(tasks: &mut [Task]) -> bool {
    let mut changed = false;
    for task in tasks {
        if task.capability == "ui.native-inspect"
            && let Some(output) = task.output.as_ref()
        {
            let compact = retained_task_output(&task.capability, output);
            if &compact != output {
                task.output = Some(compact);
                changed = true;
            }
        }
    }
    changed
}

fn compact_persisted_evidence(state: &mut FabricState) -> bool {
    let mut changed = compact_task_outputs(&mut state.tasks);
    for task in &mut state.tasks {
        changed |= compact_value(&mut task.input);
        if let Some(output) = &mut task.output {
            changed |= compact_value(output);
        }
        if let Some(error) = &mut task.error {
            changed |= compact_value(&mut error.details);
        }
    }
    for transaction in &mut state.transactions {
        for entry in &mut transaction.journal {
            changed |= compact_value(&mut entry.details);
        }
        if let Some(error) = &mut transaction.error {
            changed |= compact_value(&mut error.details);
        }
    }
    for generation in &mut state.generations {
        for evidence in [
            &mut generation.validation,
            &mut generation.finalization,
            &mut generation.smoke,
        ]
        .into_iter()
        .flatten()
        {
            changed |= compact_value(&mut evidence.details);
        }
        if let Some(error) = &mut generation.failure {
            changed |= compact_value(&mut error.details);
        }
    }
    for handoff in &mut state.handoffs {
        for evidence in &mut handoff.evidence {
            changed |= compact_value(evidence);
        }
    }
    for agent in &mut state.agents {
        changed |= compact_value(&mut agent.metadata);
    }
    changed
}

fn compact_value(value: &mut Value) -> bool {
    let mut changed = false;
    match value {
        Value::Object(object) => {
            if let Some(evaluations) = object.remove("nativeEvaluations") {
                let count = evaluations.as_array().map_or(0, Vec::len);
                object.insert("nativeObservationCount".to_owned(), json!(count));
                changed = true;
            }
            for child in object.values_mut() {
                changed |= compact_value(child);
            }
        }
        Value::Array(array) => {
            for child in array {
                changed |= compact_value(child);
            }
        }
        _ => {}
    }
    changed
}

fn schema_error(path: &str, message: &str) -> RpcError {
    RpcError::new("SCHEMA_VALIDATION_FAILED", format!("{path} {message}"))
}

fn artifact_from_result(
    capability: &str,
    input: &Value,
    result: &Value,
    workspace_session_id: &str,
    executor_id: &str,
    task_id: &str,
) -> Option<Artifact> {
    let value = match capability {
        "artifact.build" => result.get("artifact")?,
        "artifact.describe" | "artifact.transfer" => result,
        _ => return None,
    };
    let digest = value.get("digest")?.as_str()?.to_owned();
    let size = value.get("size").and_then(Value::as_u64).unwrap_or(0);
    let path = value
        .get("path")
        .or_else(|| value.get("destination"))?
        .as_str()?
        .to_owned();
    let artifact_type = input
        .get("artifactType")
        .and_then(Value::as_str)
        .unwrap_or("generic")
        .to_owned();
    let source_digests = input
        .get("digest")
        .and_then(Value::as_str)
        .map(|digest| vec![digest.to_owned()])
        .unwrap_or_default();
    Some(Artifact {
        digest,
        artifact_type,
        schema: "machine-fabric.dev/artifact/v1".to_owned(),
        size,
        locations: vec![ArtifactLocation::File {
            executor_id: executor_id.to_owned(),
            path,
        }],
        provenance: Provenance {
            workspace_session_id: workspace_session_id.to_owned(),
            task_id: Some(task_id.to_owned()),
            source_digests,
            attributes: Default::default(),
        },
        created_at: now_ms(),
    })
}

fn generation_transition_allowed(from: &GenerationState, to: &GenerationState) -> bool {
    from == to
        || matches!(
            (from, to),
            (GenerationState::Reserved, GenerationState::Materializing)
                | (GenerationState::Reserved, GenerationState::Failed)
                | (
                    GenerationState::Materializing,
                    GenerationState::Materialized
                )
                | (GenerationState::Materializing, GenerationState::Failed)
                | (GenerationState::Materialized, GenerationState::Validated)
                | (GenerationState::Materialized, GenerationState::Failed)
                | (GenerationState::Validated, GenerationState::Finalized)
                | (GenerationState::Validated, GenerationState::Failed)
                | (GenerationState::Finalized, GenerationState::SmokePassed)
                | (GenerationState::Finalized, GenerationState::Failed)
                | (GenerationState::SmokePassed, GenerationState::Active)
                | (GenerationState::SmokePassed, GenerationState::Failed)
                | (GenerationState::Active, GenerationState::Superseded)
        )
}

fn upsert_by<T>(items: &mut Vec<T>, value: T, predicate: impl Fn(&T) -> bool) {
    if let Some(existing) = items.iter_mut().find(|item| predicate(item)) {
        *existing = value;
    } else {
        items.push(value);
    }
}

fn wait_for_process_readiness(
    endpoint: &ExecutorEndpoint,
    input: &Value,
    mut result: Value,
) -> Result<Value, RpcError> {
    let process_id = required_str(input, "processId")?;
    let timeout_ms = input
        .get("readinessTimeoutMs")
        .and_then(Value::as_u64)
        .or_else(|| {
            input
                .get("readiness")
                .and_then(|value| value.get("timeoutMs"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(180_000);
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let readiness = result
            .get("readiness")
            .and_then(|value| value.get("state"))
            .and_then(Value::as_str);
        let process_state = result.get("state").and_then(Value::as_str);
        if readiness == Some("ready") {
            return Ok(result);
        }
        if matches!(readiness, Some("failed" | "timeout")) || process_state != Some("running") {
            return Err(RpcError::new(
                "PROCESS_NOT_READY",
                format!(
                    "process readiness ended in {}",
                    readiness.or(process_state).unwrap_or("unknown")
                ),
            ));
        }
        if Instant::now() >= deadline {
            let mut error = RpcError::new(
                "READINESS_TIMEOUT",
                format!("process did not become ready: {process_id}"),
            );
            error.retryable = true;
            return Err(error);
        }
        thread::sleep(Duration::from_millis(250));
        let polled = call_executor(
            endpoint,
            &traced_request("process.get", json!({"processId": process_id})),
        )
        .map_err(|error| RpcError::new("EXECUTOR_UNAVAILABLE", error.to_string()))?;
        if !polled.ok {
            return Err(polled
                .error
                .unwrap_or_else(|| RpcError::new("EXECUTOR_FAILED", "process.get failed")));
        }
        result = polled.result.unwrap_or(Value::Null);
    }
}

fn command_requires_approval(input: &Value) -> bool {
    input
        .get("argv")
        .and_then(Value::as_array)
        .and_then(|argv| argv.first())
        .and_then(Value::as_str)
        .and_then(|path| std::path::Path::new(path).file_name())
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, "rm" | "sudo" | "su" | "dd" | "mkfs"))
}

fn command_digest(input: &Value) -> Result<String, RpcError> {
    let canonical = json!({
        "cwd": input.get("cwd").ok_or_else(|| RpcError::new("INVALID_PARAMS", "cwd is required"))?,
        "argv": input.get("argv").ok_or_else(|| RpcError::new("INVALID_PARAMS", "argv is required"))?,
    });
    serde_json::to_vec(&canonical)
        .map(|bytes| sha256_bytes(&bytes))
        .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))
}

fn map_lease_error(error: LeaseError) -> RpcError {
    let code = match error {
        LeaseError::Active { .. } => "LEASE_ACTIVE",
        LeaseError::NotFound => "LEASE_NOT_FOUND",
        LeaseError::OwnedByOther => "LEASE_OWNED_BY_OTHER",
        LeaseError::InvalidHandoff => "INVALID_HANDOFF",
    };
    RpcError::new(code, error.to_string())
}

#[cfg(unix)]
fn refresh_local_endpoint_health(controllers: &mut [ControllerPeer], executors: &mut [Executor]) {
    for controller in controllers {
        if let ExecutorEndpoint::Local { socket } = &controller.endpoint {
            controller.health = if std::path::Path::new(socket).exists() {
                HealthStatus::Ready
            } else {
                HealthStatus::Offline
            };
        }
    }
    for executor in executors {
        if let ExecutorEndpoint::Local { socket } = &executor.endpoint {
            executor.health = if std::path::Path::new(socket).exists() {
                HealthStatus::Ready
            } else {
                HealthStatus::Offline
            };
        }
    }
}

#[cfg(not(unix))]
fn refresh_local_endpoint_health(_controllers: &mut [ControllerPeer], _executors: &mut [Executor]) {
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RpcServer;
    use machine_fabric_protocol::Request;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};

    #[test]
    fn unregister_removes_persisted_peer_registrations_idempotently() {
        let directory = tempfile::tempdir().unwrap();
        let store = JsonStore::new(directory.path().join("controller.json"));
        let controller = Controller::open_with_id(store.clone(), Some("local".to_owned())).unwrap();
        let now = now_ms();
        controller
            .state
            .lock()
            .unwrap()
            .controllers
            .push(ControllerPeer {
                api_version: "machine-fabric.dev/v1".to_owned(),
                metadata: Metadata {
                    id: "stale".to_owned(),
                    labels: Default::default(),
                    created_at: now,
                    updated_at: now,
                },
                endpoint: ExecutorEndpoint::Local {
                    socket: "missing-controller.sock".to_owned(),
                },
                health: HealthStatus::Offline,
            });
        controller.state.lock().unwrap().executors.push(Executor {
            api_version: "machine-fabric.dev/v1".to_owned(),
            metadata: Metadata {
                id: "stale-rust".to_owned(),
                labels: Default::default(),
                created_at: now,
                updated_at: now,
            },
            endpoint: ExecutorEndpoint::Local {
                socket: "missing-executor.sock".to_owned(),
            },
            capabilities: Vec::new(),
            allowed_roots: Vec::new(),
            health: HealthStatus::Offline,
        });
        controller.persist().unwrap();
        assert!(
            controller
                .handle(Request::new(
                    "controller.unregister",
                    json!({"controllerId": "stale"})
                ))
                .ok
        );
        assert!(
            controller
                .handle(Request::new(
                    "executor.unregister",
                    json!({"executorId": "stale-rust"})
                ))
                .ok
        );
        let reopened = Controller::open_with_id(store, Some("local".to_owned())).unwrap();
        assert!(reopened.state.lock().unwrap().controllers.is_empty());
        assert!(reopened.state.lock().unwrap().executors.is_empty());
        assert_eq!(
            reopened
                .handle(Request::new(
                    "controller.unregister",
                    json!({"controllerId": "stale"})
                ))
                .result
                .unwrap()["removed"],
            false
        );
    }

    #[cfg(unix)]
    #[test]
    fn status_marks_missing_peer_socket_offline_without_forgetting_registration() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("controller.json"))).unwrap();
        controller.state.lock().unwrap().executors.push(Executor {
            api_version: "machine-fabric.dev/v1".to_owned(),
            metadata: Metadata {
                id: "peer-rust".to_owned(),
                labels: Default::default(),
                created_at: 1,
                updated_at: 1,
            },
            endpoint: ExecutorEndpoint::Local {
                socket: directory
                    .path()
                    .join("disconnected-peer.sock")
                    .to_string_lossy()
                    .into_owned(),
            },
            capabilities: Vec::new(),
            allowed_roots: Vec::new(),
            health: HealthStatus::Ready,
        });
        let status = controller.handle(Request::new("status", Value::Null));
        assert!(status.ok, "{:?}", status.error);
        assert_eq!(status.result.unwrap()["executors"][0]["health"], "offline");
        assert_eq!(controller.state.lock().unwrap().executors.len(), 1);
    }

    #[test]
    fn agent_context_lists_capabilities_without_sending_their_schemas() {
        let directory = tempfile::tempdir().unwrap();
        let controller = Controller::open_with_id(
            JsonStore::new(directory.path().join("controller.json")),
            Some("mac-agent".to_owned()),
        )
        .unwrap();
        let now = now_ms();
        controller.state.lock().unwrap().executors.push(Executor {
            api_version: "machine-fabric.dev/v1".to_owned(),
            metadata: Metadata {
                id: "linux-build".to_owned(),
                labels: Default::default(),
                created_at: now,
                updated_at: now,
            },
            endpoint: ExecutorEndpoint::Local {
                socket: directory.path().join("missing.sock").display().to_string(),
            },
            capabilities: crate::capability_catalog(),
            allowed_roots: vec!["/workspace".to_owned()],
            health: HealthStatus::Ready,
        });

        let context = controller.handle(Request::new(
            "fabric.context",
            json!({"executorId": "linux-build", "capability": "command.run"}),
        ));
        assert!(context.ok, "{:?}", context.error);
        let executor = &context.result.unwrap()["executors"][0];
        assert_eq!(executor["executorId"], "linux-build");
        assert_eq!(executor["capabilities"][0]["name"], "command.run");
        assert!(executor["capabilities"][0].get("inputSchema").is_none());

        let description = controller.handle(Request::new(
            "capability.describe",
            json!({"executorId": "linux-build", "capability": "command.run"}),
        ));
        assert!(description.ok, "{:?}", description.error);
        assert!(description.result.unwrap().get("inputSchema").is_some());
    }

    #[test]
    fn readiness_wait_polls_until_the_executor_reports_ready() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("readiness.sock");
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);
        let server_socket = socket.clone();
        std::thread::spawn(move || {
            RpcServer::new(server_socket).serve(move |request| {
            let call = server_calls.fetch_add(1, Ordering::SeqCst);
            Response::success(request.request_id, json!({
                "id": "process-1", "state": "running",
                "readiness": {"state": if call == 0 { "starting" } else { "ready" }, "attempts": call + 1}
            }))
        }).unwrap()
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let endpoint: ExecutorEndpoint =
            serde_json::from_value(json!({"transport": "local", "socket": socket})).unwrap();
        let ready = wait_for_process_readiness(
            &endpoint,
            &json!({"processId": "process-1", "readinessTimeoutMs": 2_000}),
            json!({"id": "process-1", "state": "running", "readiness": {"state": "starting", "attempts": 0}}),
        ).unwrap();
        assert_eq!(ready["readiness"]["state"], "ready");
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn driver_ttl_is_not_silently_capped_at_five_minutes() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("controller.json"))).unwrap();
        let acquired = controller.handle(Request::new(
            "driver.take",
            json!({"resource": "workspace:test", "owner": "agent", "ttlMs": 900_000}),
        ));
        assert!(acquired.ok, "{:?}", acquired.error);
        let lease = acquired.result.unwrap();
        assert_eq!(
            lease["expiresAt"].as_u64().unwrap() - lease["acquiredAt"].as_u64().unwrap(),
            900_000
        );
        let renewed = controller.handle(Request::new(
            "driver.renew",
            json!({"resource": "workspace:test", "owner": "agent", "token": lease["token"], "ttlMs": 900_000}),
        ));
        let lease = renewed.result.unwrap();
        assert_eq!(
            lease["expiresAt"].as_u64().unwrap() - lease["updatedAt"].as_u64().unwrap(),
            900_000
        );
    }

    #[test]
    fn controller_relays_artifacts_between_executors_without_hostnames() {
        let directory = tempfile::tempdir().unwrap();
        let source_root = directory.path().join("source");
        let destination_root = directory.path().join("destination");
        std::fs::create_dir_all(&source_root).unwrap();
        std::fs::create_dir_all(&destination_root).unwrap();
        std::fs::write(
            source_root.join("chunk.js"),
            vec![7_u8; 2 * 1024 * 1024 + 17],
        )
        .unwrap();
        let source_runtime =
            Arc::new(crate::ExecutorRuntime::new("source", vec![source_root.clone()]).unwrap());
        let destination_runtime = Arc::new(
            crate::ExecutorRuntime::new("destination", vec![destination_root.clone()]).unwrap(),
        );
        let source_socket = directory.path().join("source.sock");
        let destination_socket = directory.path().join("destination.sock");
        for (runtime, socket) in [
            (source_runtime, source_socket.clone()),
            (destination_runtime, destination_socket.clone()),
        ] {
            std::thread::spawn(move || {
                RpcServer::new(socket)
                    .serve(move |request| runtime.handle(request))
                    .unwrap()
            });
        }
        for _ in 0..100 {
            if source_socket.exists() && destination_socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let controller =
            Controller::open(JsonStore::new(directory.path().join("controller.json"))).unwrap();
        for (id, socket) in [
            ("source", source_socket),
            ("destination", destination_socket),
        ] {
            assert!(controller.handle(Request::new("executor.register", json!({"executorId": id, "endpoint": {"transport": "local", "socket": socket}}))).ok);
        }
        let transferred = controller.handle(Request::new(
            "artifact.transfer",
            json!({
                "source": {"executorId": "source", "path": source_root},
                "destination": {"executorId": "destination", "path": destination_root.join("copy")},
                "mode": "mirror"
            }),
        ));
        assert!(transferred.ok, "{:?}", transferred.error);
        let transferred = transferred.result.unwrap();
        assert_eq!(transferred["transport"], "archive");
        assert_eq!(transferred["compression"], "gzip");
        assert_eq!(
            transferred["transfer"]["transferredBytes"],
            transferred["archiveSize"]
        );
        assert!(transferred["transfer"]["chunks"].as_u64().unwrap() >= 1);
        assert!(
            transferred["transfer"]["encodedBytes"].as_u64().unwrap()
                >= transferred["archiveSize"].as_u64().unwrap()
        );
        assert!(transferred["transfer"].get("relayDurationMs").is_some());
        assert!(transferred["transfer"].get("commitDurationMs").is_some());
        assert!(transferred["transfer"].get("totalDurationMs").is_some());
        assert!(
            transferred["archiveSize"].as_u64().unwrap() < transferred["size"].as_u64().unwrap()
        );
        assert_eq!(
            std::fs::metadata(destination_root.join("copy/chunk.js"))
                .unwrap()
                .len(),
            2 * 1024 * 1024 + 17
        );
        let reverse = controller.handle(Request::new(
            "artifact.transfer",
            json!({
                "source": {"executorId": "destination", "path": destination_root.join("copy")},
                "destination": {"executorId": "source", "path": source_root.join("roundtrip")},
                "mode": "mirror"
            }),
        ));
        assert!(reverse.ok, "{:?}", reverse.error);
        assert_eq!(
            std::fs::metadata(source_root.join("roundtrip/chunk.js"))
                .unwrap()
                .len(),
            2 * 1024 * 1024 + 17
        );
    }

    #[test]
    fn registers_and_calls_a_peer_controller_independently_of_transport_role() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("peer-controller.sock");
        let server_socket = socket.clone();
        std::thread::spawn(move || {
            RpcServer::new(server_socket)
                .serve(|request| {
                    Response::success(
                        request.request_id,
                        json!({"peer": "ready", "controller": {"id": "peer-b", "status": "ready"}}),
                    )
                })
                .unwrap();
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let controller = Arc::new(
            Controller::open(JsonStore::new(directory.path().join("controller.json"))).unwrap(),
        );
        let mismatched = controller.handle(Request::new(
            "controller.register",
            json!({
                "controllerId": "wrong-peer",
                "endpoint": {"transport": "local", "socket": socket}
            }),
        ));
        assert_eq!(
            mismatched.error.unwrap().code,
            "CONTROLLER_IDENTITY_MISMATCH"
        );
        let registered = controller.handle(Request::new(
            "controller.register",
            json!({
                "controllerId": "peer-b",
                "endpoint": {"transport": "local", "socket": socket}
            }),
        ));
        assert!(registered.ok, "{:?}", registered.error);
        let called = controller.handle(Request::new(
            "controller.call",
            json!({"controllerId": "peer-b", "action": "ping", "params": {}}),
        ));
        assert!(called.ok, "{:?}", called.error);
        assert_eq!(called.result.unwrap()["peer"], "ready");
    }

    #[test]
    fn executor_registration_rejects_an_endpoint_with_another_identity() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("peer-executor.sock");
        let server_socket = socket.clone();
        std::thread::spawn(move || {
            RpcServer::new(server_socket)
                .serve(|request| {
                    Response::success(
                        request.request_id,
                        json!({
                            "executorId": "executor-b",
                            "status": "ready",
                            "capabilities": [],
                            "allowedRoots": []
                        }),
                    )
                })
                .unwrap();
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let controller =
            Controller::open(JsonStore::new(directory.path().join("controller.json"))).unwrap();
        let mismatched = controller.handle(Request::new(
            "executor.register",
            json!({
                "executorId": "wrong-executor",
                "endpoint": {"transport": "local", "socket": socket}
            }),
        ));
        assert_eq!(mismatched.error.unwrap().code, "EXECUTOR_IDENTITY_MISMATCH");
        let registered = controller.handle(Request::new(
            "executor.register",
            json!({
                "executorId": "executor-b",
                "endpoint": {"transport": "local", "socket": socket}
            }),
        ));
        assert!(registered.ok, "{:?}", registered.error);
    }

    #[test]
    fn controller_identity_persists_across_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("controller.json");
        let first =
            Controller::open_with_id(JsonStore::new(&path), Some("node-a".to_owned())).unwrap();
        assert_eq!(first.id, "node-a");
        drop(first);
        let reopened = Controller::open(JsonStore::new(path)).unwrap();
        assert_eq!(reopened.id, "node-a");
    }

    #[test]
    fn session_handoff_moves_authority_and_routes_through_old_home() {
        let directory = tempfile::tempdir().unwrap();
        let controller_b = Arc::new(
            Controller::open_with_id(
                JsonStore::new(directory.path().join("b.json")),
                Some("node-b".to_owned()),
            )
            .unwrap(),
        );
        let socket_b = directory.path().join("b.sock");
        let server_b = Arc::clone(&controller_b);
        let listen_b = socket_b.clone();
        std::thread::spawn(move || {
            RpcServer::new(listen_b)
                .serve(move |request| server_b.handle(request))
                .unwrap();
        });
        for _ in 0..100 {
            if socket_b.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let controller_a = Controller::open_with_id(
            JsonStore::new(directory.path().join("a.json")),
            Some("node-a".to_owned()),
        )
        .unwrap();
        assert!(
            controller_a
                .handle(Request::new(
                    "controller.register",
                    json!({"controllerId": "node-b", "endpoint": {"transport": "local", "socket": socket_b}}),
                ))
                .ok
        );
        let session = controller_a.handle(Request::new(
            "session.put",
            json!({
                "apiVersion": "machine-fabric.dev/v1",
                "metadata": {"id": "session-1", "labels": {}, "createdAt": 1, "updatedAt": 1},
                "objective": "test",
                "state": "active"
            }),
        ));
        assert_eq!(
            session.result.unwrap()["authority"]["controllerId"],
            "node-a"
        );
        let submitted = controller_a.handle(Request::new(
            "task.submit",
            json!({
                "workspaceSessionId": "session-1",
                "executorId": "executor-1",
                "capability": "test",
                "input": {},
                "idempotencyKey": "handoff-task"
            }),
        ));
        let task_id = submitted.result.unwrap()["task"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        for state in ["running", "succeeded"] {
            assert!(
                controller_a
                    .handle(Request::new(
                        "task.transition",
                        json!({"taskId": task_id, "state": state})
                    ))
                    .ok
            );
        }
        let moved = controller_a.handle(Request::new(
            "session.handoff",
            json!({"sessionId": "session-1", "targetControllerId": "node-b"}),
        ));
        assert!(moved.ok, "{:?}", moved.error);
        assert_eq!(moved.result.unwrap()["authority"]["epoch"], 2);
        assert_eq!(
            controller_b
                .handle(Request::new("task.get", json!({"taskId": task_id})))
                .result
                .unwrap()["state"],
            "succeeded"
        );
        let transitioned = controller_a.handle(Request::new(
            "session.transition",
            json!({"sessionId": "session-1", "state": "completed"}),
        ));
        assert!(transitioned.ok, "{:?}", transitioned.error);
        assert_eq!(transitioned.result.unwrap()["state"], "completed");
        assert_eq!(
            controller_b
                .handle(Request::new(
                    "session.get",
                    json!({"sessionId": "session-1"})
                ))
                .result
                .unwrap()["state"],
            "completed"
        );
    }

    #[test]
    fn session_gate_prevents_mutation_from_crossing_a_handoff() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("target.sock");
        let server_socket = socket.clone();
        let (accept_started_tx, accept_started_rx) = mpsc::channel();
        let (release_accept_tx, release_accept_rx) = mpsc::channel();
        let release_accept_rx = Arc::new(Mutex::new(release_accept_rx));
        std::thread::spawn(move || {
            RpcServer::new(server_socket)
                .serve(move |request| match request.action.as_str() {
                    "session.accept-handoff" => {
                        let _ = accept_started_tx.send(());
                        release_accept_rx
                            .lock()
                            .expect("release receiver")
                            .recv()
                            .expect("handoff release");
                        Response::success(request.request_id, json!({"accepted": true}))
                    }
                    "session.transition" => Response::success(
                        request.request_id,
                        json!({"id": "session-gated", "state": "completed"}),
                    ),
                    _ => Response::success(
                        request.request_id,
                        json!({"controller": {"id": "node-b", "status": "ready"}}),
                    ),
                })
                .unwrap();
        });
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let controller = Arc::new(
            Controller::open_with_id(
                JsonStore::new(directory.path().join("source.json")),
                Some("node-a".to_owned()),
            )
            .unwrap(),
        );
        assert!(
            controller
                .handle(Request::new(
                    "controller.register",
                    json!({"controllerId": "node-b", "endpoint": {"transport": "local", "socket": socket}}),
                ))
                .ok
        );
        assert!(
            controller
                .handle(Request::new(
                    "session.put",
                    json!({
                        "apiVersion": "machine-fabric.dev/v1",
                        "metadata": {"id": "session-gated", "labels": {}, "createdAt": 1, "updatedAt": 1},
                        "objective": "test gate",
                        "state": "active"
                    }),
                ))
                .ok
        );
        let handoff_controller = Arc::clone(&controller);
        let handoff = std::thread::spawn(move || {
            handoff_controller.handle(Request::new(
                "session.handoff",
                json!({"sessionId": "session-gated", "targetControllerId": "node-b"}),
            ))
        });
        accept_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handoff reached target");
        let transition_controller = Arc::clone(&controller);
        let (transition_tx, transition_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let response = transition_controller.handle(Request::new(
                "session.transition",
                json!({"sessionId": "session-gated", "state": "completed"}),
            ));
            transition_tx.send(response).unwrap();
        });
        assert!(
            transition_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );
        release_accept_tx.send(()).unwrap();
        assert!(handoff.join().unwrap().ok);
        let transitioned = transition_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("transition unblocked after handoff");
        assert!(transitioned.ok, "{:?}", transitioned.error);
    }

    #[test]
    fn handoff_requires_acknowledgement_before_completion() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("state.json"))).unwrap();
        let created = controller.handle(Request::new(
            "handoff.create",
            json!({
                "id": "handoff-1",
                "workspaceSessionId": "session-1",
                "objective": "accept",
                "from": {"role": "coding"},
                "to": {"role": "gui-acceptance"},
                "createdAt": 0
            }),
        ));
        assert!(created.ok);
        let premature = controller.handle(Request::new(
            "handoff.complete",
            json!({"handoffId": "handoff-1"}),
        ));
        assert_eq!(premature.error.unwrap().code, "HANDOFF_NOT_ACKNOWLEDGED");
        assert!(
            controller
                .handle(Request::new(
                    "handoff.acknowledge",
                    json!({"handoffId": "handoff-1"}),
                ))
                .ok
        );
        assert!(
            controller
                .handle(Request::new(
                    "handoff.complete",
                    json!({"handoffId": "handoff-1"}),
                ))
                .ok
        );
    }

    #[test]
    fn transaction_journal_is_idempotent_and_fenced() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("state.json"))).unwrap();
        let begin = json!({
            "workspaceSessionId": "session-1",
            "idempotencyKey": "publish-1",
            "target": "client/current",
            "generationId": "generation-1",
            "leaseFence": 7
        });
        let first = controller.handle(Request::new("transaction.begin", begin.clone()));
        assert!(first.ok);
        let transaction_id = first.result.unwrap()["transaction"]["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let reused = controller.handle(Request::new("transaction.begin", begin));
        assert!(reused.result.unwrap()["reused"].as_bool().unwrap());
        let stale = controller.handle(Request::new(
            "transaction.record",
            json!({
                "transactionId": transaction_id,
                "step": "materialize",
                "state": "started",
                "fence": 6
            }),
        ));
        assert_eq!(stale.error.unwrap().code, "STALE_FENCING_TOKEN");
        let recorded = controller.handle(Request::new(
            "transaction.record",
            json!({
                "transactionId": transaction_id,
                "step": "materialize",
                "state": "succeeded",
                "fence": 7,
                "outputDigest": "sha256:ok"
            }),
        ));
        assert!(recorded.ok);
        let activated = controller.handle(Request::new(
            "transaction.record",
            json!({
                "transactionId": transaction_id,
                "step": "activate",
                "state": "succeeded",
                "fence": 7,
                "previousGenerationId": "generation-0"
            }),
        ));
        assert_eq!(
            activated.result.unwrap()["previousGenerationId"],
            "generation-0"
        );
    }

    #[test]
    fn approvals_are_exact_expiring_objects() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("state.json"))).unwrap();
        let requested = controller.handle(Request::new(
            "approval.request",
            json!({"digest": "sha256:exact", "owner": "agent", "reason": "test"}),
        ));
        assert!(requested.ok);
        let id = requested.result.unwrap()["id"].as_str().unwrap().to_owned();
        let approved = controller.handle(Request::new(
            "approval.approve",
            json!({"approvalId": id, "ttlMs": 1000}),
        ));
        assert_eq!(approved.result.unwrap()["state"], "approved");
        let duplicate =
            controller.handle(Request::new("approval.approve", json!({"approvalId": id})));
        assert_eq!(duplicate.error.unwrap().code, "APPROVAL_NOT_PENDING");
    }

    #[test]
    fn capability_schema_validation_checks_required_types_and_bounds() {
        let schema = json!({
            "type": "object",
            "required": ["port", "args"],
            "properties": {
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                "args": {"type": "array", "items": {"type": "string"}}
            },
            "additionalProperties": false
        });
        assert!(
            validate_schema(&schema, &json!({"port": 9222, "args": ["--safe"]}), "input").is_ok()
        );
        assert_eq!(
            validate_schema(&schema, &json!({"port": 0, "args": ["--safe"]}), "input")
                .unwrap_err()
                .code,
            "SCHEMA_VALIDATION_FAILED"
        );
        assert!(validate_schema(&schema, &json!({"port": 9222}), "input").is_err());
        assert!(validate_schema(&schema, &json!({"port": 9222, "args": [7]}), "input").is_err());
    }

    #[test]
    fn generation_identity_and_state_transitions_are_enforced() {
        let directory = tempfile::tempdir().unwrap();
        let controller =
            Controller::open(JsonStore::new(directory.path().join("state.json"))).unwrap();
        let generation = json!({
            "id": "g1", "workspaceSessionId": "s1", "applicationType": "application",
            "root": "/state/g1", "state": "materializing",
            "baseline": {"digest": "sha256:base", "source": "/baseline"},
            "appliedArtifacts": [], "digest": "sha256:base", "createdAt": 1
        });
        assert!(
            controller
                .handle(Request::new("generation.put", generation.clone()))
                .ok
        );
        let mut active = generation;
        active["state"] = Value::String("active".to_owned());
        let invalid = controller.handle(Request::new("generation.put", active));
        assert_eq!(invalid.error.unwrap().code, "INVALID_GENERATION_STATE");
    }

    #[test]
    fn native_inspection_task_output_retains_only_a_bounded_summary() {
        let output = json!({
            "applicationPath": "/state/App.app",
            "pids": [42],
            "inspection": {
                "accessibilityTrusted": true,
                "processes": [{"pid": 42, "windows": [{"children": [null, null]}]}]
            },
            "inspectedAt": 123
        });
        let compact = retained_task_output("ui.native-inspect", &output);
        assert_eq!(compact["inspection"]["accessibilityTrusted"], true);
        assert_eq!(compact["inspection"]["processCount"], 1);
        assert!(compact["inspection"].get("processes").is_none());
        assert_eq!(retained_task_output("ui.evaluate", &output), output);
    }
}
