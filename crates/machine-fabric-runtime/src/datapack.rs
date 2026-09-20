// Chromium DataPack codec migrated from distributed-workbench (MIT, Luke Wang).
use machine_fabric_core::atomic_replace;
use machine_fabric_protocol::RpcError;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataPackResourceTree {
    pub root_relative: PathBuf,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub exclude_source_maps: bool,
    #[serde(default)]
    pub exclude_root_files: Vec<String>,
}

/// Rebuild the Chromium DataPack consumed by Chromium applications from the
/// final expanded resource tree. This intentionally runs after every overlay,
/// so the pack and the loose runtime cannot represent different generations.
#[allow(clippy::too_many_arguments)]
pub fn pack_chromium_datapack(
    root_path: &Path,
    resource_trees: &[DataPackResourceTree],
    output_relative: &Path,
    platform: &str,
    arch: &str,
    bundle_name: &str,
    base_pack_path: Option<&Path>,
    base_pack_digest: Option<&str>,
    changed_prefixes: &[String],
) -> Result<Value, RpcError> {
    validate_relative(output_relative)?;
    let mut resources = Vec::new();
    for tree in resource_trees {
        validate_relative(&tree.root_relative)?;
        if !tree.prefix.is_empty()
            && (tree.prefix.starts_with('/')
                || tree.prefix.contains('\\')
                || tree
                    .prefix
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == ".."))
        {
            return Err(RpcError::new(
                "DATAPACK_PREFIX_INVALID",
                format!(
                    "DataPack prefix is not a safe resource path: {}",
                    tree.prefix
                ),
            ));
        }
        let tree_root = root_path.join(&tree.root_relative);
        if !tree_root.is_dir() {
            return Err(RpcError::new(
                "DATAPACK_INPUT_MISSING",
                format!("DataPack resource root is missing: {}", tree_root.display()),
            ));
        }
        collect_pack_tree(
            &tree_root,
            &tree_root,
            &tree.prefix,
            tree.exclude_source_maps,
            &tree.exclude_root_files,
            &mut resources,
        )?;
    }
    resources.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    for pair in resources.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(RpcError::new(
                "DATAPACK_DUPLICATE_PATH",
                format!("duplicate packed resource path: {}", pair[0].0),
            ));
        }
    }
    if resources.is_empty() {
        return Err(RpcError::new(
            "DATAPACK_EMPTY",
            "no resource resources to pack",
        ));
    }
    let base_entries = if let Some(base_path) = base_pack_path {
        let expected = base_pack_digest.ok_or_else(|| {
            RpcError::new(
                "BASE_DATAPACK_DIGEST_REQUIRED",
                "incremental DataPack requires the base pak digest",
            )
        })?;
        let mut digest = Sha256::new();
        hash_file_into(base_path, &mut digest)?;
        let actual = format!("sha256:{}", hex::encode(digest.finalize()));
        if actual != expected {
            return Err(RpcError::new(
                "BASE_DATAPACK_DRIFT",
                format!("base DataPack digest changed: expected {expected}, got {actual}"),
            ));
        }
        Some((base_path.to_path_buf(), parse_datapack_entries(base_path)?))
    } else {
        None
    };
    if base_entries.is_some() && changed_prefixes.is_empty() {
        return Err(RpcError::new(
            "DATAPACK_CHANGED_PREFIX_REQUIRED",
            "incremental DataPack requires at least one changed prefix",
        ));
    }
    let resource_count = resources.len() + 1;
    if resource_count > u16::MAX as usize {
        return Err(RpcError::new(
            "DATAPACK_RESOURCE_LIMIT",
            format!("DataPack resource limit exceeded: {resource_count}"),
        ));
    }

    let mut content_digest = Sha256::new();
    for (relative, path) in &resources {
        content_digest.update(relative.as_bytes());
        content_digest.update([0]);
        if let Some((base_path, entries)) = &base_entries
            && !path_matches_prefix(relative, changed_prefixes)
        {
            let (offset, length) = entries.get(relative).ok_or_else(|| {
                RpcError::new(
                    "BASE_DATAPACK_ENTRY_MISSING",
                    format!("unchanged entry is absent from base pak: {relative}"),
                )
            })?;
            hash_file_range_into(base_path, *offset, *length, &mut content_digest)?;
        } else {
            hash_file_into(path, &mut content_digest)?;
        }
    }
    let content_hash = hex::encode(content_digest.finalize());
    let manifest = serde_json::to_vec(&serde_json::json!({
        "arch": arch,
        "bundle_name": bundle_name,
        "code_cache": Value::Null,
        "content_hash": content_hash.clone(),
        "entries": resources.iter().enumerate().map(|(index, (relative, _))| {
            serde_json::json!({"id": index + 2, "path": relative})
        }).collect::<Vec<_>>(),
        "platform": platform,
        "tool_version": "1",
        "v8_version": "",
    }))
    .expect("resource pack manifest serializes");

    let header_size = 12_u64;
    let index_size = ((resource_count + 1) * 6) as u64;
    let mut offsets = Vec::with_capacity(resource_count + 1);
    offsets.push(header_size + index_size);
    offsets.push(offsets[0] + manifest.len() as u64);
    for (relative, path) in &resources {
        let size = if let Some((_, entries)) = &base_entries
            && !path_matches_prefix(relative, changed_prefixes)
        {
            entries
                .get(relative)
                .ok_or_else(|| {
                    RpcError::new(
                        "BASE_DATAPACK_ENTRY_MISSING",
                        format!("unchanged entry is absent from base pak: {relative}"),
                    )
                })?
                .1
        } else {
            fs::metadata(path)
                .map_err(|error| io_error("DATAPACK_READ_FAILED", path, error))?
                .len()
        };
        offsets.push(offsets.last().copied().unwrap_or(0) + size);
    }
    if offsets.last().copied().unwrap_or(0) > u32::MAX as u64 {
        return Err(RpcError::new(
            "DATAPACK_SIZE_LIMIT",
            format!(
                "DataPack exceeds 4 GiB offset limit: {}",
                offsets.last().unwrap()
            ),
        ));
    }

    let output = root_path.join(output_relative);
    let parent = output.parent().ok_or_else(|| {
        RpcError::new(
            "DATAPACK_OUTPUT_INVALID",
            "resource pack output has no parent",
        )
    })?;
    fs::create_dir_all(parent).map_err(|error| io_error("DATAPACK_WRITE_FAILED", parent, error))?;
    let temporary = parent.join(format!(".resources.pak.{}.tmp", std::process::id()));
    let result = (|| -> Result<(), RpcError> {
        let mut target = fs::File::create(&temporary)
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&5_u32.to_le_bytes())
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&[0, 0, 0, 0])
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&(resource_count as u16).to_le_bytes())
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&0_u16.to_le_bytes())
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        for (index, offset) in offsets.iter().take(resource_count).enumerate() {
            target
                .write_all(&((index + 1) as u16).to_le_bytes())
                .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
            target
                .write_all(&(*offset as u32).to_le_bytes())
                .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        }
        target
            .write_all(&0_u16.to_le_bytes())
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&(offsets[resource_count] as u32).to_le_bytes())
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        target
            .write_all(&manifest)
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        for (relative, path) in &resources {
            if let Some((base_path, entries)) = &base_entries
                && !path_matches_prefix(relative, changed_prefixes)
            {
                let (offset, length) = entries.get(relative).expect("base entry validated");
                copy_file_range(base_path, *offset, *length, &mut target)?;
            } else {
                let mut source = fs::File::open(path)
                    .map_err(|error| io_error("DATAPACK_READ_FAILED", path, error))?;
                std::io::copy(&mut source, &mut target)
                    .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
            }
        }
        target
            .sync_all()
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &temporary, error))?;
        atomic_replace(&temporary, &output)
            .map_err(|error| io_error("DATAPACK_WRITE_FAILED", &output, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;

    let mut digest = Sha256::new();
    hash_file_into(&output, &mut digest)?;
    Ok(serde_json::json!({
        "output": output,
        "resources": resource_count,
        "entries": resources.len(),
        "size": fs::metadata(&output).map_err(|error| io_error("DATAPACK_READ_FAILED", &output, error))?.len(),
        "sha256": format!("sha256:{}", hex::encode(digest.finalize())),
        "contentHash": format!("sha256:{content_hash}"),
        "incremental": base_entries.is_some(),
        "basePack": base_pack_path,
        "changedPrefixes": changed_prefixes,
    }))
}

fn path_matches_prefix(path: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|prefix| {
        path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

fn parse_datapack_entries(path: &Path) -> Result<HashMap<String, (u64, u64)>, RpcError> {
    let mut source =
        fs::File::open(path).map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let mut header = [0_u8; 12];
    source
        .read_exact(&mut header)
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    if u32::from_le_bytes(header[0..4].try_into().unwrap()) != 5 {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak is not DataPack version 5",
        ));
    }
    let count = u16::from_le_bytes(header[8..10].try_into().unwrap()) as usize;
    if count < 1 {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak has no manifest entry",
        ));
    }
    let mut index = vec![0_u8; (count + 1) * 6];
    source
        .read_exact(&mut index)
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let offsets = (0..=count)
        .map(|position| {
            let start = position * 6 + 2;
            u32::from_le_bytes(index[start..start + 4].try_into().unwrap()) as u64
        })
        .collect::<Vec<_>>();
    if offsets.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak offsets are not monotonic",
        ));
    }
    let file_size = source
        .metadata()
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?
        .len();
    if offsets.last().copied().unwrap_or(0) > file_size {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak offsets exceed file size",
        ));
    }
    source
        .seek(SeekFrom::Start(offsets[0]))
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let mut manifest = vec![0_u8; (offsets[1] - offsets[0]) as usize];
    source
        .read_exact(&mut manifest)
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let manifest: Value = serde_json::from_slice(&manifest)
        .map_err(|error| RpcError::new("BASE_DATAPACK_INVALID", error.to_string()))?;
    let entries = manifest
        .get("entries")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            RpcError::new("BASE_DATAPACK_INVALID", "base pak manifest has no entries")
        })?;
    if entries.len() + 1 != count {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak manifest/index entry counts differ",
        ));
    }
    let mut result = HashMap::new();
    for (position, entry) in entries.iter().enumerate() {
        let name = entry
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::new("BASE_DATAPACK_INVALID", "base pak entry has no path"))?;
        result.insert(
            name.to_owned(),
            (
                offsets[position + 1],
                offsets[position + 2] - offsets[position + 1],
            ),
        );
    }
    Ok(result)
}

fn hash_file_range_into(
    path: &Path,
    offset: u64,
    length: u64,
    digest: &mut Sha256,
) -> Result<(), RpcError> {
    let mut source =
        fs::File::open(path).map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    source
        .seek(SeekFrom::Start(offset))
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let mut limited = source.take(length);
    let copied = std::io::copy(&mut limited, &mut DigestWriter(digest))
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    if copied != length {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak entry is truncated",
        ));
    }
    Ok(())
}

struct DigestWriter<'a>(&'a mut Sha256);
impl Write for DigestWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn copy_file_range(
    path: &Path,
    offset: u64,
    length: u64,
    target: &mut fs::File,
) -> Result<(), RpcError> {
    let mut source =
        fs::File::open(path).map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    source
        .seek(SeekFrom::Start(offset))
        .map_err(|error| io_error("BASE_DATAPACK_INVALID", path, error))?;
    let copied = std::io::copy(&mut source.take(length), target)
        .map_err(|error| io_error("DATAPACK_WRITE_FAILED", path, error))?;
    if copied != length {
        return Err(RpcError::new(
            "BASE_DATAPACK_INVALID",
            "base pak entry is truncated",
        ));
    }
    Ok(())
}

fn collect_pack_tree(
    root: &Path,
    current: &Path,
    prefix: &str,
    exclude_source_maps: bool,
    exclude_root_files: &[String],
    resources: &mut Vec<(String, PathBuf)>,
) -> Result<(), RpcError> {
    let mut entries = fs::read_dir(current)
        .map_err(|error| io_error("DATAPACK_READ_FAILED", current, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error("DATAPACK_READ_FAILED", current, error))?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let resolved = path
            .canonicalize()
            .map_err(|error| io_error("DATAPACK_READ_FAILED", &path, error))?;
        let canonical_root = root
            .canonicalize()
            .map_err(|error| io_error("DATAPACK_READ_FAILED", root, error))?;
        if !resolved.starts_with(&canonical_root)
            || entry
                .file_type()
                .map_err(|error| io_error("DATAPACK_READ_FAILED", &path, error))?
                .is_symlink()
        {
            return Err(RpcError::new(
                "DATAPACK_PATH_INVALID",
                "resource trees must not contain symbolic links",
            ));
        }
        let metadata = entry
            .metadata()
            .map_err(|error| io_error("DATAPACK_READ_FAILED", &path, error))?;
        if metadata.is_dir() {
            collect_pack_tree(
                root,
                &path,
                prefix,
                exclude_source_maps,
                exclude_root_files,
                resources,
            )?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root).expect("walk remains under root");
            let relative = relative.to_string_lossy().replace('\\', "/");
            if (!relative.contains('/')
                && exclude_root_files
                    .iter()
                    .any(|excluded| excluded == &relative))
                || (exclude_source_maps && relative.ends_with(".map"))
            {
                continue;
            }
            resources.push((
                if prefix.is_empty() {
                    relative
                } else {
                    format!("{prefix}/{relative}")
                },
                path,
            ));
        }
    }
    Ok(())
}

fn hash_file_into(path: &Path, digest: &mut Sha256) -> Result<(), RpcError> {
    let mut source =
        fs::File::open(path).map_err(|error| io_error("DATAPACK_READ_FAILED", path, error))?;
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = source
            .read(&mut buffer)
            .map_err(|error| io_error("DATAPACK_READ_FAILED", path, error))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(())
}

fn validate_relative(path: &Path) -> Result<(), RpcError> {
    if path.is_absolute()
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
    #[cfg(unix)]
    #[test]
    fn resource_symlink_escape_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("resources");
        fs::create_dir(&root).unwrap();
        let outside = directory.path().join("outside.txt");
        fs::write(&outside, "outside").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("linked.txt")).unwrap();
        let mut resources = Vec::new();
        let error = collect_pack_tree(&root, &root, "", false, &[], &mut resources).unwrap_err();
        assert_eq!(error.code, "DATAPACK_PATH_INVALID");
    }

    #[test]
    fn pack_uses_final_overlay_tree_and_incremental_resources() {
        let directory = tempfile::tempdir().unwrap();
        let application = directory.path().join("Example");
        let webcontents = application.join("resources/local_webcontents");
        let office = webcontents.join("apps/example-app");
        let word = office.join("static/v/w");
        let biz = webcontents.join("biz/static/js");
        fs::create_dir_all(&word).unwrap();
        fs::create_dir_all(&biz).unwrap();
        fs::write(office.join("index.html"), "office").unwrap();
        fs::write(word.join("runtime.js"), "selected-runtime").unwrap();
        fs::write(word.join("formula.js"), "equation").unwrap();
        fs::write(biz.join("entry.js"), "flow").unwrap();
        fs::write(biz.join("entry.js.map"), "source-map").unwrap();
        fs::write(office.join("resources.pak"), "stale-pack").unwrap();

        let result = pack_chromium_datapack(
            &application,
            &[
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/apps/example-app"),
                    prefix: String::new(),
                    exclude_source_maps: false,
                    exclude_root_files: vec!["resources.pak".to_owned()],
                },
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/biz"),
                    prefix: "biz".to_owned(),
                    exclude_source_maps: true,
                    exclude_root_files: Vec::new(),
                },
            ],
            Path::new("resources/local_webcontents/apps/example-app/resources.pak"),
            "win",
            "x64",
            "example-app",
            None,
            None,
            &[],
        )
        .unwrap();

        assert_eq!(result["entries"], 4);
        assert!(result["sha256"].as_str().unwrap().starts_with("sha256:"));
        let packed = fs::read(result["output"].as_str().unwrap()).unwrap();
        assert_eq!(u32::from_le_bytes(packed[0..4].try_into().unwrap()), 5);
        let manifest_offset = u32::from_le_bytes(packed[14..18].try_into().unwrap()) as usize;
        let next_offset = u32::from_le_bytes(packed[20..24].try_into().unwrap()) as usize;
        let manifest: Value =
            serde_json::from_slice(&packed[manifest_offset..next_offset]).unwrap();
        let paths = manifest["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["path"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(paths.contains(&"static/v/w/runtime.js"));
        assert!(paths.contains(&"static/v/w/formula.js"));
        assert!(paths.contains(&"biz/static/js/entry.js"));
        assert!(!paths.contains(&"biz/static/js/entry.js.map"));
        assert!(!paths.contains(&"resources.pak"));

        let base_pack = directory.path().join("base-example-app.pak");
        fs::copy(result["output"].as_str().unwrap(), &base_pack).unwrap();
        let base_digest = result["sha256"].as_str().unwrap().to_owned();
        fs::write(word.join("runtime.js"), "selected-runtime-v2-longer").unwrap();
        let full = pack_chromium_datapack(
            &application,
            &[
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/apps/example-app"),
                    prefix: String::new(),
                    exclude_source_maps: false,
                    exclude_root_files: vec!["resources.pak".to_owned()],
                },
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/biz"),
                    prefix: "biz".to_owned(),
                    exclude_source_maps: true,
                    exclude_root_files: Vec::new(),
                },
            ],
            Path::new("resources/local_webcontents/apps/example-app/resources.pak"),
            "win",
            "x64",
            "example-app",
            None,
            None,
            &[],
        )
        .unwrap();
        let full_bytes = fs::read(full["output"].as_str().unwrap()).unwrap();
        let incremental = pack_chromium_datapack(
            &application,
            &[
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/apps/example-app"),
                    prefix: String::new(),
                    exclude_source_maps: false,
                    exclude_root_files: vec!["resources.pak".to_owned()],
                },
                DataPackResourceTree {
                    root_relative: PathBuf::from("resources/local_webcontents/biz"),
                    prefix: "biz".to_owned(),
                    exclude_source_maps: true,
                    exclude_root_files: Vec::new(),
                },
            ],
            Path::new("resources/local_webcontents/apps/example-app/resources.pak"),
            "win",
            "x64",
            "example-app",
            Some(&base_pack),
            Some(&base_digest),
            &["static/v/w".to_owned()],
        )
        .unwrap();
        assert!(incremental["incremental"].as_bool().unwrap());
        assert_eq!(
            fs::read(incremental["output"].as_str().unwrap()).unwrap(),
            full_bytes
        );
        assert_eq!(incremental["sha256"], full["sha256"]);
        assert_eq!(incremental["contentHash"], full["contentHash"]);
    }
}
