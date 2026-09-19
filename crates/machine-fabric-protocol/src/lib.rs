use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const RPC_VERSION: &str = "machine-fabric.dev/v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub api_version: String,
    pub request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_request_id: Option<String>,
    pub action: String,
    #[serde(default)]
    pub params: Value,
}

impl Request {
    pub fn new(action: impl Into<String>, params: Value) -> Self {
        let request_id = format!("req_{}", Uuid::new_v4().simple());
        Self {
            api_version: RPC_VERSION.to_owned(),
            correlation_id: Some(request_id.clone()),
            parent_request_id: None,
            request_id,
            action: action.into(),
            params,
        }
    }

    pub fn child(parent: &Self, action: impl Into<String>, params: Value) -> Self {
        let mut request = Self::new(action, params);
        request.correlation_id = Some(
            parent
                .correlation_id
                .clone()
                .unwrap_or_else(|| parent.request_id.clone()),
        );
        request.parent_request_id = Some(parent.request_id.clone());
        request
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub ok: bool,
    pub api_version: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl Response {
    pub fn success(request_id: impl Into<String>, result: impl Serialize) -> Self {
        Self {
            ok: true,
            api_version: RPC_VERSION.to_owned(),
            request_id: request_id.into(),
            result: Some(serde_json::to_value(result).expect("serializable protocol result")),
            error: None,
        }
    }

    pub fn failure(request_id: impl Into<String>, error: RpcError) -> Self {
        Self {
            ok: false,
            api_version: RPC_VERSION.to_owned(),
            request_id: request_id.into(),
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RpcError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default)]
    pub details: Value,
}

impl RpcError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_envelope_omits_unused_branch() {
        let response = Response::success("req_1", serde_json::json!({"ready": true}));
        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["ok"], true);
        assert!(value.get("error").is_none());
    }

    #[test]
    fn child_request_preserves_correlation_and_records_parent() {
        let parent = Request::new("status", Value::Null);
        let child = Request::child(&parent, "ping", Value::Null);
        assert_eq!(child.correlation_id, parent.correlation_id);
        assert_eq!(
            child.parent_request_id.as_deref(),
            Some(parent.request_id.as_str())
        );
        assert_ne!(child.request_id, parent.request_id);
    }
}
