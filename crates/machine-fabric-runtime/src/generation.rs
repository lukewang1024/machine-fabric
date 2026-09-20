use machine_fabric_core::{atomic_replace, now_ms};
use machine_fabric_protocol::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MaterializedGeneration {
    pub generation_id: String,
    pub root: PathBuf,
    pub application_path: PathBuf,
    pub marker_path: PathBuf,
    pub created_at: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Overlay {
    pub source: PathBuf,
    pub target_relative_path: PathBuf,
    #[serde(default)]
    pub replace: bool,
}

pub fn materialize(
    generation_root: &Path,
    generation_id: &str,
    baseline: &Path,
) -> Result<MaterializedGeneration, RpcError> {
    validate_generation_id(generation_id)?;
    if generation_root
        .file_name()
        .is_some_and(|name| name == "generations")
    {
        return Err(RpcError::new(
            "INVALID_GENERATION_ROOT",
            "generationRoot is the deployment root; do not include the trailing generations directory",
        ));
    }
    let generations = generation_root.join("generations");
    fs::create_dir_all(&generations)
        .map_err(|error| io_error("GENERATION_CREATE_FAILED", &generations, error))?;
    let root = generations.join(generation_id);
    if root.exists() {
        let marker_path = root.join("generation.json");
        let marker: Value = serde_json::from_slice(
            &fs::read(&marker_path)
                .map_err(|error| io_error("GENERATION_INVALID", &marker_path, error))?,
        )
        .map_err(|error| RpcError::new("GENERATION_INVALID", error.to_string()))?;
        if marker.get("state").and_then(Value::as_str) == Some("materialized") {
            return Ok(MaterializedGeneration {
                generation_id: generation_id.to_owned(),
                root: root.clone(),
                application_path: root.join(
                    marker
                        .get("applicationName")
                        .and_then(Value::as_str)
                        .unwrap_or("application"),
                ),
                marker_path,
                created_at: marker.get("createdAt").and_then(Value::as_u64).unwrap_or(0),
            });
        }
        return Err(RpcError::new(
            "GENERATION_EXISTS",
            format!("generation already exists but is not reusable: {generation_id}"),
        ));
    }
    let temporary = generations.join(format!(".{generation_id}.{}.tmp", std::process::id()));
    fs::create_dir(&temporary)
        .map_err(|error| io_error("GENERATION_CREATE_FAILED", &temporary, error))?;
    let baseline_name = baseline
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| RpcError::new("BASELINE_INVALID", "baseline has no UTF-8 name"))?;
    let application_name = baseline_name
        .find(".app")
        .map(|position| &baseline_name[..position + 4])
        .unwrap_or(baseline_name);
    let application_path = temporary.join(application_name);
    if let Err(error) = copy_tree(baseline, &application_path) {
        let marker = serde_json::json!({
            "generationId": generation_id,
            "state": "failed",
            "error": error.message,
            "createdAt": now_ms(),
        });
        let _ = fs::write(
            temporary.join("generation.json"),
            serde_json::to_vec_pretty(&marker).expect("marker serializes"),
        );
        let _ = fs::rename(&temporary, &root);
        return Err(error);
    }
    let created_at = now_ms();
    let marker = serde_json::json!({
        "generationId": generation_id,
        "state": "materialized",
        "applicationName": application_name,
        "baseline": baseline,
        "createdAt": created_at,
    });
    let marker_path = temporary.join("generation.json");
    fs::write(
        &marker_path,
        serde_json::to_vec_pretty(&marker).expect("marker serializes"),
    )
    .map_err(|error| io_error("GENERATION_CREATE_FAILED", &marker_path, error))?;
    fs::rename(&temporary, &root)
        .map_err(|error| io_error("GENERATION_CREATE_FAILED", &root, error))?;
    Ok(MaterializedGeneration {
        generation_id: generation_id.to_owned(),
        application_path: root.join(application_name),
        marker_path: root.join("generation.json"),
        root,
        created_at,
    })
}

pub fn apply_overlays(application_path: &Path, overlays: &[Overlay]) -> Result<Value, RpcError> {
    let mut applied = Vec::new();
    for overlay in overlays {
        validate_relative(&overlay.target_relative_path)?;
        let target = application_path.join(&overlay.target_relative_path);
        // Partial overlays retain baseline files; complete artifacts explicitly
        // request replacement so obsolete chunks cannot survive deployment.
        if overlay.replace {
            replace_tree(&overlay.source, &target)?;
        } else {
            copy_tree(&overlay.source, &target)?;
        }
        applied.push(serde_json::json!({
            "source": overlay.source,
            "target": target,
            "replace": overlay.replace,
        }));
    }
    Ok(serde_json::json!({"applied": applied}))
}

fn replace_tree(source: &Path, target: &Path) -> Result<(), RpcError> {
    let parent = target
        .parent()
        .ok_or_else(|| RpcError::new("INVALID_OVERLAY_PATH", "overlay target has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| io_error("OVERLAY_REPLACE_FAILED", parent, error))?;
    let source_root = fs::canonicalize(source)
        .map_err(|error| io_error("OVERLAY_REPLACE_FAILED", source, error))?;
    let parent_root = fs::canonicalize(parent)
        .map_err(|error| io_error("OVERLAY_REPLACE_FAILED", parent, error))?;
    if parent_root.starts_with(&source_root) {
        return Err(RpcError::new(
            "INVALID_OVERLAY_PATH",
            "replacement staging must not be inside its source",
        ));
    }
    let staging = parent.join(format!(".overlay-{}-{}", std::process::id(), now_ms()));
    fs::create_dir(&staging)
        .map_err(|error| io_error("OVERLAY_REPLACE_FAILED", &staging, error))?;
    let incoming = staging.join("incoming");
    let previous = staging.join("previous");
    if let Err(error) = copy_tree(source, &incoming) {
        let _ = fs::remove_dir_all(&staging);
        return Err(error);
    }
    let had_previous = target.exists() || target.is_symlink();
    if had_previous && let Err(error) = fs::rename(target, &previous) {
        let _ = fs::remove_dir_all(&staging);
        return Err(io_error("OVERLAY_REPLACE_FAILED", target, error));
    }
    if let Err(error) = fs::rename(&incoming, target) {
        if had_previous {
            // Leave the recovery copy in place if restoration itself fails.
            fs::rename(&previous, target)
                .map_err(|error| io_error("OVERLAY_RESTORE_FAILED", &previous, error))?;
        }
        let _ = fs::remove_dir_all(&staging);
        return Err(io_error("OVERLAY_REPLACE_FAILED", target, error));
    }
    fs::remove_dir_all(&staging)
        .map_err(|error| io_error("OVERLAY_CLEANUP_FAILED", &staging, error))?;
    Ok(())
}

pub fn record_state(
    generation_root: &Path,
    generation_id: &str,
    state: &str,
    evidence: Value,
) -> Result<Value, RpcError> {
    validate_generation_id(generation_id)?;
    const STATES: &[&str] = &[
        "materialized",
        "applied",
        "validated",
        "finalized",
        "smoke-passed",
        "ready",
        "active",
        "failed",
    ];
    if !STATES.contains(&state) {
        return Err(RpcError::new(
            "INVALID_GENERATION_STATE",
            format!("unsupported generation state: {state}"),
        ));
    }
    let marker_path = generation_root
        .join("generations")
        .join(generation_id)
        .join("generation.json");
    let mut marker: Value = serde_json::from_slice(
        &fs::read(&marker_path)
            .map_err(|error| io_error("GENERATION_INVALID", &marker_path, error))?,
    )
    .map_err(|error| RpcError::new("GENERATION_INVALID", error.to_string()))?;
    marker["state"] = Value::String(state.to_owned());
    marker["updatedAt"] = Value::from(now_ms());
    marker["evidence"] = evidence;
    let temporary = marker_path.with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(&marker).expect("marker serializes"),
    )
    .map_err(|error| io_error("GENERATION_UPDATE_FAILED", &temporary, error))?;
    atomic_replace(&temporary, &marker_path)
        .map_err(|error| io_error("GENERATION_UPDATE_FAILED", &marker_path, error))?;
    Ok(marker)
}

pub fn activate(generation_root: &Path, generation_id: &str) -> Result<Value, RpcError> {
    validate_generation_id(generation_id)?;
    let generations = generation_root.join("generations");
    let target = generations.join(generation_id);
    let marker_path = target.join("generation.json");
    if !marker_path.is_file() {
        return Err(RpcError::new(
            "GENERATION_NOT_READY",
            format!("generation has no marker: {generation_id}"),
        ));
    }
    let marker: Value = serde_json::from_slice(
        &fs::read(&marker_path)
            .map_err(|error| io_error("GENERATION_INVALID", &marker_path, error))?,
    )
    .map_err(|error| RpcError::new("GENERATION_INVALID", error.to_string()))?;
    if marker.get("state").and_then(Value::as_str) != Some("ready") {
        return Err(RpcError::new(
            "GENERATION_NOT_READY",
            format!(
                "generation state is {}, expected ready",
                marker
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            ),
        ));
    }
    fs::create_dir_all(generation_root)
        .map_err(|error| io_error("ACTIVATION_FAILED", generation_root, error))?;
    let current = generation_root.join("current");
    let previous = generation_root.join("previous");
    let next = generation_root.join(format!(".current.{}.tmp", std::process::id()));
    let old_target = read_directory_link(&current).ok();
    if next.exists() {
        remove_directory_link(&next)
            .map_err(|error| io_error("ACTIVATION_FAILED", &next, error))?;
    }
    symlink_dir(Path::new("generations").join(generation_id), &next)
        .map_err(|error| io_error("ACTIVATION_FAILED", &next, error))?;
    replace_directory_link(&next, &current)
        .map_err(|error| io_error("ACTIVATION_FAILED", &current, error))?;
    if let Some(ref old) = old_target {
        let previous_next = generation_root.join(format!(".previous.{}.tmp", std::process::id()));
        let _ = remove_directory_link(&previous_next);
        symlink_dir(old, &previous_next)
            .map_err(|error| io_error("ACTIVATION_FAILED", &previous_next, error))?;
        replace_directory_link(&previous_next, &previous)
            .map_err(|error| io_error("ACTIVATION_FAILED", &previous, error))?;
    }
    Ok(serde_json::json!({
        "generationId": generation_id,
        "current": current,
        "previous": old_target,
        "activatedAt": now_ms(),
    }))
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), RpcError> {
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| io_error("MATERIALIZE_FAILED", source, error))?;
    if metadata.file_type().is_symlink() {
        let link =
            fs::read_link(source).map_err(|error| io_error("MATERIALIZE_FAILED", source, error))?;
        if target.exists() || target.is_symlink() {
            fs::remove_file(target)
                .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))?;
        }
        symlink_path(&link, target)
            .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))?;
    } else if metadata.is_dir() {
        fs::create_dir_all(target)
            .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))?;
        copy_permissions(&metadata, target)?;
        for entry in
            fs::read_dir(source).map_err(|error| io_error("MATERIALIZE_FAILED", source, error))?
        {
            let entry = entry.map_err(|error| io_error("MATERIALIZE_FAILED", source, error))?;
            copy_tree(&entry.path(), &target.join(entry.file_name()))?;
        }
    } else if metadata.is_file() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| io_error("MATERIALIZE_FAILED", parent, error))?;
        }
        clone_or_copy(source, target)?;
        copy_permissions(&metadata, target)?;
    } else {
        return Err(RpcError::new(
            "MATERIALIZE_FAILED",
            format!("unsupported baseline entry: {}", source.display()),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn symlink_dir(source: impl AsRef<Path>, target: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, target)
}

#[cfg(unix)]
fn remove_directory_link(path: &Path) -> std::io::Result<()> {
    fs::remove_file(path)
}

#[cfg(unix)]
fn read_directory_link(path: &Path) -> std::io::Result<PathBuf> {
    fs::read_link(path)
}

#[cfg(windows)]
fn read_directory_link(path: &Path) -> std::io::Result<PathBuf> {
    junction::get_target(path)
}

#[cfg(windows)]
fn remove_directory_link(path: &Path) -> std::io::Result<()> {
    match junction::delete(path) {
        Ok(()) => fs::remove_dir(path),
        Err(error) => fs::remove_dir(path).map_err(|_| error),
    }
}

#[cfg(unix)]
fn replace_directory_link(source: &Path, target: &Path) -> std::io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn replace_directory_link(source: &Path, target: &Path) -> std::io::Result<()> {
    let link_target = junction::get_target(source)?;
    if fs::symlink_metadata(target).is_ok() {
        remove_directory_link(target)?;
    }
    junction::create(link_target, target)?;
    junction::delete(source)
}

#[cfg(windows)]
fn symlink_dir(source: impl AsRef<Path>, target: &Path) -> std::io::Result<()> {
    let source = source.as_ref();
    let resolved = if source.is_absolute() {
        source.to_path_buf()
    } else {
        target
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(source)
    };
    junction::create(resolved, target)
}

#[cfg(unix)]
fn symlink_path(source: &Path, target: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, target)
}

#[cfg(windows)]
fn symlink_path(source: &Path, target: &Path) -> std::io::Result<()> {
    let resolved = target
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(source);
    if resolved.is_dir() {
        junction::create(resolved, target)
    } else {
        std::os::windows::fs::symlink_file(source, target)
    }
}

#[cfg(unix)]
fn copy_permissions(metadata: &fs::Metadata, target: &Path) -> Result<(), RpcError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        target,
        fs::Permissions::from_mode(metadata.permissions().mode()),
    )
    .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))
}

#[cfg(windows)]
fn copy_permissions(_metadata: &fs::Metadata, _target: &Path) -> Result<(), RpcError> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn clone_or_copy(source: &Path, target: &Path) -> Result<(), RpcError> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let source_c = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| RpcError::new("MATERIALIZE_FAILED", "source path contains NUL"))?;
    let target_c = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| RpcError::new("MATERIALIZE_FAILED", "target path contains NUL"))?;
    if unsafe { libc::clonefile(source_c.as_ptr(), target_c.as_ptr(), 0) } == 0 {
        return Ok(());
    }
    fs::copy(source, target)
        .map(|_| ())
        .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))
}

#[cfg(not(target_os = "macos"))]
fn clone_or_copy(source: &Path, target: &Path) -> Result<(), RpcError> {
    fs::copy(source, target)
        .map(|_| ())
        .map_err(|error| io_error("MATERIALIZE_FAILED", target, error))
}

fn validate_generation_id(value: &str) -> Result<(), RpcError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(RpcError::new(
            "INVALID_GENERATION_ID",
            "generation ID must be a safe path component",
        ));
    }
    Ok(())
}

fn validate_relative(path: &Path) -> Result<(), RpcError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(RpcError::new(
            "INVALID_OVERLAY_PATH",
            format!(
                "overlay target must be a safe relative path: {}",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn io_error(code: &str, path: &Path, error: std::io::Error) -> RpcError {
    RpcError::new(code, format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_root_rejects_the_internal_generations_directory() {
        let directory = tempfile::tempdir().unwrap();
        let baseline = directory.path().join("Baseline.app");
        fs::create_dir_all(&baseline).unwrap();
        let error =
            materialize(&directory.path().join("generations"), "g1", &baseline).unwrap_err();
        assert_eq!(error.code, "INVALID_GENERATION_ROOT");
    }

    #[test]
    fn overlay_merges_and_activation_retains_previous() {
        let directory = tempfile::tempdir().unwrap();
        let baseline = directory.path().join("Example.app");
        fs::create_dir(&baseline).unwrap();
        fs::write(baseline.join("kept"), "baseline").unwrap();
        let overlay = directory.path().join("overlay");
        fs::create_dir(&overlay).unwrap();
        fs::write(overlay.join("added"), "new").unwrap();
        let root = directory.path().join("client");
        let first = materialize(&root, "one", &baseline).unwrap();
        apply_overlays(
            &first.application_path,
            &[Overlay {
                source: overlay.clone(),
                target_relative_path: PathBuf::from("runtime"),
                replace: false,
            }],
        )
        .unwrap();
        assert!(first.application_path.join("kept").exists());
        record_state(&root, "one", "ready", Value::Null).unwrap();
        assert!(first.application_path.join("runtime/added").exists());
        activate(&root, "one").unwrap();
        materialize(&root, "two", &baseline).unwrap();
        record_state(&root, "two", "ready", Value::Null).unwrap();
        activate(&root, "two").unwrap();
        assert!(
            read_directory_link(&root.join("current"))
                .unwrap()
                .ends_with(Path::new("generations/two"))
        );
        assert!(
            read_directory_link(&root.join("previous"))
                .unwrap()
                .ends_with(Path::new("generations/one"))
        );
    }

    #[test]
    fn complete_overlay_removes_stale_chunks_but_default_overlay_merges() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        let runtime = app.join("runtime");
        let source = directory.path().join("source");
        fs::create_dir_all(&runtime).unwrap();
        fs::create_dir(&source).unwrap();
        fs::write(runtime.join("stale.js"), "old").unwrap();
        fs::write(source.join("current.js"), "new").unwrap();
        let mut overlay: Overlay = serde_json::from_value(serde_json::json!({
            "source": source, "targetRelativePath": "runtime"
        }))
        .unwrap();
        apply_overlays(&app, &[overlay.clone()]).unwrap();
        assert!(runtime.join("stale.js").exists());
        overlay.replace = true;
        apply_overlays(&app, &[overlay]).unwrap();
        assert!(!runtime.join("stale.js").exists());
        assert_eq!(
            fs::read_to_string(runtime.join("current.js")).unwrap(),
            "new"
        );
    }

    #[test]
    fn failed_replacement_preserves_existing_runtime() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().join("app");
        fs::create_dir_all(app.join("runtime")).unwrap();
        fs::write(app.join("runtime/current.js"), "working").unwrap();
        let overlay = Overlay {
            source: directory.path().join("missing"),
            target_relative_path: PathBuf::from("runtime"),
            replace: true,
        };
        assert!(apply_overlays(&app, &[overlay]).is_err());
        assert_eq!(
            fs::read_to_string(app.join("runtime/current.js")).unwrap(),
            "working"
        );
        assert_eq!(fs::read_dir(&app).unwrap().count(), 1);
        assert!(validate_relative(Path::new("")).is_err());
    }

    #[test]
    fn materialize_normalizes_provenance_suffix_after_app() {
        let directory = tempfile::tempdir().unwrap();
        let baseline = directory.path().join("Example.app.validated-123");
        fs::create_dir(&baseline).unwrap();
        fs::write(baseline.join("payload"), b"ok").unwrap();
        let root = directory.path().join("client");
        let generation = materialize(&root, "generation-1", &baseline).unwrap();
        assert_eq!(
            generation.application_path.file_name().unwrap(),
            "Example.app"
        );
        assert!(generation.application_path.join("payload").is_file());
    }
}
