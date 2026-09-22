use crate::now_ms;
use machine_fabric_schema::{Task, TaskError as TaskFailure, TaskEvent, TaskPayloadRef, TaskState};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskError {
    #[error("task not found")]
    NotFound,
    #[error("invalid transition from {from:?} to {to:?}")]
    InvalidTransition { from: TaskState, to: TaskState },
}

#[derive(Debug, Default)]
pub struct TaskTable {
    tasks: BTreeMap<String, Task>,
    idempotency: BTreeMap<String, String>,
}

impl TaskTable {
    pub fn from_tasks(tasks: impl IntoIterator<Item = Task>) -> Self {
        let mut table = Self::default();
        for task in tasks {
            table.idempotency.insert(
                scoped_idempotency_key(
                    &task.workspace_session_id,
                    &task.executor_id,
                    &task.capability,
                    &task.idempotency_key,
                ),
                task.id.clone(),
            );
            table.tasks.insert(task.id.clone(), task);
        }
        table
    }

    pub fn submit(
        &mut self,
        workspace_session_id: impl Into<String>,
        executor_id: impl Into<String>,
        capability: impl Into<String>,
        input: Value,
        idempotency_key: impl Into<String>,
    ) -> (Task, bool) {
        self.submit_traced(
            workspace_session_id,
            executor_id,
            capability,
            input,
            idempotency_key,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn submit_traced(
        &mut self,
        workspace_session_id: impl Into<String>,
        executor_id: impl Into<String>,
        capability: impl Into<String>,
        input: Value,
        idempotency_key: impl Into<String>,
        correlation_id: Option<String>,
        request_id: Option<String>,
    ) -> (Task, bool) {
        let workspace_session_id = workspace_session_id.into();
        let executor_id = executor_id.into();
        let capability = capability.into();
        let idempotency_key = idempotency_key.into();
        let scoped_key = scoped_idempotency_key(
            &workspace_session_id,
            &executor_id,
            &capability,
            &idempotency_key,
        );
        if let Some(id) = self.idempotency.get(&scoped_key) {
            return (self.tasks[id].clone(), true);
        }
        let now = now_ms();
        let mut task = Task {
            id: format!("task_{}", Uuid::new_v4().simple()),
            correlation_id,
            request_id,
            workspace_session_id,
            executor_id,
            capability,
            input,
            input_ref: None,
            output: None,
            output_ref: None,
            error: None,
            idempotency_key: idempotency_key.clone(),
            state: TaskState::Queued,
            attempt: 0,
            revision: 1,
            created_at: now,
            updated_at: now,
            events: Vec::new(),
        };
        Self::push_event(&mut task, "task.queued", Value::Null);
        self.idempotency.insert(scoped_key, task.id.clone());
        self.tasks.insert(task.id.clone(), task.clone());
        (task, false)
    }

    pub fn transition(
        &mut self,
        id: &str,
        state: TaskState,
        output: Option<Value>,
        error: Option<TaskFailure>,
    ) -> Result<Task, TaskError> {
        let task = self.tasks.get_mut(id).ok_or(TaskError::NotFound)?;
        let allowed = matches!(
            (&task.state, &state),
            (TaskState::Queued, TaskState::Running)
                | (TaskState::Queued, TaskState::Cancelled)
                | (TaskState::Running, TaskState::Succeeded)
                | (TaskState::Running, TaskState::Failed)
                | (TaskState::Running, TaskState::Cancelled)
                | (TaskState::Running, TaskState::TimedOut)
                | (TaskState::Running, TaskState::OutcomeUnknown)
        );
        if !allowed {
            return Err(TaskError::InvalidTransition {
                from: task.state.clone(),
                to: state,
            });
        }
        if matches!(state, TaskState::Running) {
            task.attempt += 1;
        }
        task.revision = task.revision.saturating_add(1);
        task.state = state;
        task.output = output;
        task.output_ref = None;
        task.error = error;
        task.updated_at = now_ms();
        let event_type = format!(
            "task.{}",
            serde_json::to_value(&task.state).unwrap().as_str().unwrap()
        );
        Self::push_event(task, &event_type, Value::Null);
        Ok(task.clone())
    }

    pub fn get(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    pub fn retry(&mut self, id: &str) -> Result<Task, TaskError> {
        let task = self.tasks.get_mut(id).ok_or(TaskError::NotFound)?;
        if !matches!(
            task.state,
            TaskState::Failed | TaskState::TimedOut | TaskState::OutcomeUnknown
        ) {
            return Err(TaskError::InvalidTransition {
                from: task.state.clone(),
                to: TaskState::Queued,
            });
        }
        task.state = TaskState::Queued;
        task.revision = task.revision.saturating_add(1);
        task.output = None;
        task.output_ref = None;
        task.error = None;
        task.updated_at = now_ms();
        Self::push_event(task, "task.retried", Value::Null);
        Ok(task.clone())
    }

    /// Restore validated persisted input before a failed task becomes executable again.
    pub fn retry_with_input(&mut self, id: &str, input: Value) -> Result<Task, TaskError> {
        self.retry(id)?;
        let task = self.tasks.get_mut(id).ok_or(TaskError::NotFound)?;
        task.input = input;
        task.input_ref = None;
        Ok(task.clone())
    }

    pub fn snapshot(&self) -> Vec<Task> {
        self.tasks.values().cloned().collect()
    }

    pub fn page_by<F>(&self, cursor: usize, limit: usize, predicate: F) -> (usize, Vec<&Task>)
    where
        F: Fn(&Task) -> bool,
    {
        let mut total = 0;
        let mut page = Vec::new();
        for task in self.tasks.values().filter(|task| predicate(task)) {
            if total >= cursor && page.len() < limit {
                page.push(task);
            }
            total += 1;
        }
        (total, page)
    }

    pub fn replace_input_ref_if_unchanged(
        &mut self,
        id: &str,
        revision: u64,
        marker: Value,
        reference: TaskPayloadRef,
    ) -> bool {
        let Some(task) = self.tasks.get_mut(id) else {
            return false;
        };
        if task.revision != revision || task.input_ref.is_some() {
            return false;
        }
        task.input = marker;
        task.input_ref = Some(reference);
        true
    }

    pub fn replace_output_ref_if_unchanged(
        &mut self,
        id: &str,
        revision: u64,
        marker: Value,
        reference: TaskPayloadRef,
    ) -> bool {
        let Some(task) = self.tasks.get_mut(id) else {
            return false;
        };
        if task.revision != revision || task.output_ref.is_some() {
            return false;
        }
        task.output = Some(marker);
        task.output_ref = Some(reference);
        true
    }

    pub fn prune_terminal_before(&mut self, cutoff: u64) -> Vec<Task> {
        let ids: Vec<String> = self
            .tasks
            .values()
            .filter(|task| task.state.terminal() && task.updated_at < cutoff)
            .map(|task| task.id.clone())
            .collect();
        let mut removed = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(task) = self.tasks.remove(&id) {
                self.idempotency.remove(&scoped_idempotency_key(
                    &task.workspace_session_id,
                    &task.executor_id,
                    &task.capability,
                    &task.idempotency_key,
                ));
                removed.push(task);
            }
        }
        removed
    }

    pub fn recover_orphans(&mut self) -> Vec<Task> {
        let mut recovered = Vec::new();
        for task in self
            .tasks
            .values_mut()
            .filter(|task| matches!(task.state, TaskState::Queued | TaskState::Running))
        {
            task.state = TaskState::OutcomeUnknown;
            task.revision = task.revision.saturating_add(1);
            task.error = Some(TaskFailure {
                code: "CONTROLLER_RESTARTED".to_owned(),
                message: "controller restarted before the task outcome was recorded".to_owned(),
                retryable: true,
                details: Value::Null,
            });
            task.updated_at = now_ms();
            Self::push_event(task, "task.outcome-unknown", Value::Null);
            recovered.push(task.clone());
        }
        recovered
    }

    fn push_event(task: &mut Task, event_type: &str, details: Value) {
        task.events.push(TaskEvent {
            sequence: task.events.len() as u64 + 1,
            timestamp: now_ms(),
            event_type: event_type.to_owned(),
            details,
        });
    }
}

fn scoped_idempotency_key(
    workspace_session_id: &str,
    executor_id: &str,
    capability: &str,
    key: &str,
) -> String {
    format!("{workspace_session_id}\0{executor_id}\0{capability}\0{key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submission_is_idempotent_and_transitions_are_checked() {
        let mut table = TaskTable::default();
        let (first, reused) = table.submit("s", "e", "artifact.build", Value::Null, "same");
        assert!(!reused);
        let (second, reused) = table.submit("s", "e", "artifact.build", Value::Null, "same");
        assert!(reused);
        assert_eq!(first.id, second.id);
        table
            .transition(&first.id, TaskState::Running, None, None)
            .unwrap();
        table
            .transition(
                &first.id,
                TaskState::Succeeded,
                Some(Value::Bool(true)),
                None,
            )
            .unwrap();
        assert!(table.get(&first.id).unwrap().state.terminal());

        let (different_capability, reused) =
            table.submit("s", "e", "artifact.transfer", Value::Null, "same");
        assert!(!reused);
        assert_ne!(different_capability.id, first.id);

        let removed = table.prune_terminal_before(now_ms().saturating_add(1));
        assert_eq!(removed.len(), 1);
        let (resubmitted, reused) = table.submit("s", "e", "artifact.build", Value::Null, "same");
        assert!(!reused);
        assert_ne!(resubmitted.id, first.id);
    }

    #[test]
    fn restart_marks_inflight_tasks_outcome_unknown_and_retryable() {
        let mut table = TaskTable::default();
        let (task, _) = table.submit("s", "e", "artifact.build", Value::Null, "build");
        table
            .transition(&task.id, TaskState::Running, None, None)
            .unwrap();
        let recovered = table.recover_orphans();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].state, TaskState::OutcomeUnknown);
        assert!(recovered[0].error.as_ref().unwrap().retryable);
    }

    #[test]
    fn payload_reference_revision_rejects_same_millisecond_retry() {
        let mut table = TaskTable::default();
        let (task, _) = table.submit("s", "e", "artifact.read", Value::Null, "revision");
        table
            .transition(&task.id, TaskState::Running, None, None)
            .unwrap();
        let terminal = table
            .transition(
                &task.id,
                TaskState::Failed,
                Some(Value::String("large".to_owned())),
                None,
            )
            .unwrap();
        let reference = TaskPayloadRef {
            digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            bytes: 5,
            locator: "task-payloads/placeholder.json".to_owned(),
            chunk: None,
        };
        assert!(table.replace_output_ref_if_unchanged(
            &task.id,
            terminal.revision,
            Value::String("$ref".to_owned()),
            reference.clone(),
        ));
        let before_retry = table.get(&task.id).unwrap().revision;
        table.retry(&task.id).unwrap();
        assert!(table.get(&task.id).unwrap().revision > before_retry);
        assert!(!table.replace_output_ref_if_unchanged(
            &task.id,
            before_retry,
            Value::String("stale".to_owned()),
            reference,
        ));
        assert!(table.get(&task.id).unwrap().output.is_none());
    }
}
