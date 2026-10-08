use machine_fabric_protocol::RpcError;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PathAccess {
    Read,
    Write,
}

#[derive(Debug)]
pub(crate) struct DesktopPathPolicy {
    read_denied: Vec<PathBuf>,
    write_allowed: Vec<PathBuf>,
    logical_write_allowed: Vec<PathBuf>,
    managed_mappings: Vec<(PathBuf, PathBuf)>,
}

impl DesktopPathPolicy {
    pub(crate) fn new(home: &Path) -> Result<Self, RpcError> {
        let original_home = home.to_path_buf();
        let home = home.canonicalize().map_err(|error| {
            RpcError::new("INVALID_ROOT", format!("{}: {error}", home.display()))
        })?;
        let mut read_denied = Vec::new();
        for relative in [
            ".ssh",
            ".gnupg",
            ".aws",
            ".azure",
            ".kube",
            ".docker",
            ".config/gcloud",
            ".config/gh",
            ".netrc",
            ".npmrc",
        ] {
            add_read_root(&mut read_denied, &original_home.join(relative))?;
            add_read_root(&mut read_denied, &home.join(relative))?;
        }
        let mut write_allowed = Vec::new();
        let mut logical_write_allowed = Vec::new();
        for relative in [
            "Code",
            "Workspace",
            "Downloads",
            "Documents",
            "Desktop",
            "Pictures",
            ".cache",
            ".local/state",
            ".local/share",
            ".config",
        ] {
            write_allowed.push(resolve_path(&home.join(relative))?);
            logical_write_allowed.push(logical_path(&original_home.join(relative))?);
        }

        #[cfg(target_os = "macos")]
        {
            for path in [
                "/private/etc",
                "/private/var/db",
                "/private/var/root",
                "/Library/Keychains",
                "/System/Library/Keychains",
                "/dev",
            ] {
                add_read_root(&mut read_denied, Path::new(path))?;
            }
            for relative in [
                "Library/Keychains",
                "Library/Application Support/com.apple.TCC",
            ] {
                add_read_root(&mut read_denied, &original_home.join(relative))?;
                add_read_root(&mut read_denied, &home.join(relative))?;
            }
            for relative in [
                "Applications",
                "Library/Caches",
                "Library/Application Support",
                "Library/Preferences",
                "Library/Logs",
            ] {
                write_allowed.push(resolve_path(&home.join(relative))?);
                logical_write_allowed.push(logical_path(&original_home.join(relative))?);
            }
        }

        #[cfg(windows)]
        {
            let system_root = std::env::var_os("SystemRoot")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
            for relative in ["System32/config", "System32/drivers/etc", "ServiceProfiles"] {
                add_read_root(&mut read_denied, &system_root.join(relative))?;
            }
            let program_data = std::env::var_os("ProgramData")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    system_root
                        .parent()
                        .unwrap_or(Path::new(r"C:\"))
                        .join("ProgramData")
                });
            for relative in ["Microsoft/Crypto", "Microsoft/Protect", "Microsoft/Vault"] {
                add_read_root(&mut read_denied, &program_data.join(relative))?;
            }
            for relative in [
                "AppData/Roaming/Microsoft/Credentials",
                "AppData/Roaming/Microsoft/Protect",
                "AppData/Roaming/Microsoft/Vault",
                "AppData/Local/Microsoft/Credentials",
                "AppData/Local/Microsoft/Protect",
                "AppData/Local/Microsoft/Vault",
            ] {
                add_read_root(&mut read_denied, &original_home.join(relative))?;
                add_read_root(&mut read_denied, &home.join(relative))?;
            }
            for relative in ["AppData/Local", "AppData/Roaming"] {
                write_allowed.push(resolve_path(&home.join(relative))?);
                logical_write_allowed.push(logical_path(&original_home.join(relative))?);
            }
        }

        for variable in ["XDG_CACHE_HOME", "XDG_STATE_HOME", "XDG_CONFIG_HOME"] {
            if let Some(value) = std::env::var_os(variable) {
                let root = PathBuf::from(value);
                if root.is_absolute() {
                    let root = resolve_path(&root)?;
                    if within(&root, &home) {
                        logical_write_allowed.push(logical_path(&root)?);
                        write_allowed.push(root);
                    }
                }
            }
        }
        Ok(Self {
            read_denied,
            write_allowed,
            logical_write_allowed,
            managed_mappings: Vec::new(),
        })
    }

    pub(crate) fn register_mappings(
        &mut self,
        mappings: Vec<(PathBuf, PathBuf)>,
    ) -> Result<(), RpcError> {
        let mut verified: Vec<(PathBuf, PathBuf)> = Vec::new();
        for (logical, physical) in mappings {
            let logical = logical_path(&logical)?;
            let physical = resolve_path(&physical)?;
            self.check(&logical, PathAccess::Read)?;
            self.check(&physical, PathAccess::Read)?;
            if !self
                .logical_write_allowed
                .iter()
                .any(|root| within(&logical, root))
                || !logical.is_dir()
                || !physical.is_dir()
                || !same_path(&resolve_path(&logical)?, &physical)
                || verified
                    .iter()
                    .any(|(root, _)| within(&logical, root) || within(root, &logical))
            {
                return Err(RpcError::new(
                    "INVALID_ROOT",
                    "invalid or overlapping managed path mapping",
                ));
            }
            verified.push((logical, physical));
        }
        self.managed_mappings = verified;
        Ok(())
    }

    pub(crate) fn check_resolved(
        &self,
        logical: &Path,
        resolved: &Path,
        access: PathAccess,
    ) -> Result<(), RpcError> {
        self.check(logical, PathAccess::Read)?;
        self.check(resolved, PathAccess::Read)?;
        let logical = logical_path(logical)?;
        self.check(&logical, PathAccess::Read)?;
        if access == PathAccess::Write {
            for (root, target) in &self.managed_mappings {
                if within(&logical, root) {
                    let mut expected = target.clone();
                    for component in logical.components().skip(root.components().count()) {
                        expected.push(component.as_os_str());
                    }
                    if !same_path(&resolve_path(root)?, target) || !same_path(resolved, &expected) {
                        return Err(RpcError::new(
                            "PATH_WRITE_DENIED",
                            "managed mapping changed or contains an unregistered redirect",
                        ));
                    }
                    return Ok(());
                }
            }
        }
        self.check(resolved, access)
    }

    pub(crate) fn check(&self, path: &Path, access: PathAccess) -> Result<(), RpcError> {
        if self.read_denied.iter().any(|root| within(path, root)) {
            return Err(RpcError::new(
                "PATH_READ_DENIED",
                format!(
                    "{} is in a protected system or credential path",
                    path.display()
                ),
            ));
        }
        if access == PathAccess::Write && !self.write_allowed.iter().any(|root| within(path, root))
        {
            return Err(RpcError::new(
                "PATH_WRITE_DENIED",
                format!("{} is outside executor write roots", path.display()),
            ));
        }
        Ok(())
    }

    pub(crate) fn read_denied(&self) -> &[PathBuf] {
        &self.read_denied
    }
    pub(crate) fn write_allowed(&self) -> &[PathBuf] {
        &self.write_allowed
    }
    pub(crate) fn managed_mappings(&self) -> &[(PathBuf, PathBuf)] {
        &self.managed_mappings
    }
}

pub(crate) fn logical_path(path: &Path) -> Result<PathBuf, RpcError> {
    if !path.is_absolute() {
        return Err(RpcError::new("PATH_INVALID", "path must be absolute"));
    }
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if result.file_name().is_none() || !result.pop() {
                    return Err(RpcError::new(
                        "PATH_INVALID",
                        "path escapes its filesystem root",
                    ));
                }
            }
            #[cfg(windows)]
            Component::Prefix(prefix) => {
                use std::path::Prefix;
                match prefix.kind() {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        result.push(format!("{}:", char::from(drive)))
                    }
                    Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                        result.push(format!(
                            r"\\{}\{}",
                            server.to_string_lossy(),
                            share.to_string_lossy()
                        ));
                    }
                    _ => {
                        return Err(RpcError::new(
                            "PATH_INVALID",
                            "device paths are not supported",
                        ));
                    }
                }
            }
            Component::Normal(name) => {
                #[cfg(windows)]
                {
                    let text = name.to_string_lossy();
                    if text.contains(':') || text.ends_with('.') || text.ends_with(' ') {
                        return Err(RpcError::new(
                            "PATH_INVALID",
                            "ambiguous Windows path component",
                        ));
                    }
                    let stem = text.split('.').next().unwrap_or("").to_ascii_uppercase();
                    let numbered_device = ["COM", "LPT"].iter().any(|prefix| {
                        stem.strip_prefix(prefix).is_some_and(|suffix| {
                            matches!(
                                suffix,
                                "1" | "2"
                                    | "3"
                                    | "4"
                                    | "5"
                                    | "6"
                                    | "7"
                                    | "8"
                                    | "9"
                                    | "¹"
                                    | "²"
                                    | "³"
                            )
                        })
                    });
                    if numbered_device
                        || matches!(
                            stem.as_str(),
                            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
                        )
                    {
                        return Err(RpcError::new(
                            "PATH_INVALID",
                            "reserved Windows device name",
                        ));
                    }
                }
                result.push(name);
            }
            _ => result.push(component.as_os_str()),
        }
    }
    Ok(result)
}

fn same_path(left: &Path, right: &Path) -> bool {
    let (Ok(left), Ok(right)) = (logical_path(left), logical_path(right)) else {
        return false;
    };
    within(&left, &right) && within(&right, &left)
}

fn add_read_root(roots: &mut Vec<PathBuf>, path: &Path) -> Result<(), RpcError> {
    if !path.is_absolute() {
        return Err(RpcError::new(
            "INVALID_ROOT",
            "read-deny root must be absolute",
        ));
    }
    let raw = path.to_path_buf();
    if !roots.contains(&raw) {
        roots.push(raw);
    }
    // Protected OS directories may not be canonicalizable by the service user.
    if let Ok(resolved) = resolve_path(path)
        && !roots.contains(&resolved)
    {
        roots.push(resolved);
    }
    Ok(())
}

pub(crate) fn resolve_path(path: &Path) -> Result<PathBuf, RpcError> {
    if !path.is_absolute() {
        return Err(RpcError::new("PATH_INVALID", "path must be absolute"));
    }
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(RpcError::new(
            "PATH_INVALID",
            "path contains parent traversal",
        ));
    }
    if path.exists() {
        return path.canonicalize().map_err(|error| {
            RpcError::new("PATH_INVALID", format!("{}: {error}", path.display()))
        });
    }
    let mut ancestor = path;
    let mut missing = Vec::new();
    while !ancestor.exists() {
        let name = ancestor
            .file_name()
            .ok_or_else(|| RpcError::new("PATH_INVALID", "path has no existing ancestor"))?;
        if name == "." || name == ".." {
            return Err(RpcError::new(
                "PATH_INVALID",
                "path contains unresolved traversal",
            ));
        }
        missing.push(name.to_owned());
        ancestor = ancestor
            .parent()
            .ok_or_else(|| RpcError::new("PATH_INVALID", "path has no existing ancestor"))?;
    }
    let mut resolved = fs::canonicalize(ancestor)
        .map_err(|error| RpcError::new("PATH_INVALID", format!("{}: {error}", path.display())))?;
    for name in missing.into_iter().rev() {
        resolved.push(name);
    }
    Ok(resolved)
}

fn within(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        let mut components = path.components();
        for expected in root.components() {
            let Some(actual) = components.next() else {
                return false;
            };
            if !actual
                .as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case(&expected.as_os_str().to_string_lossy())
            {
                return false;
            }
        }
        true
    }
    #[cfg(not(windows))]
    {
        path.starts_with(root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(unix, windows))]
    #[test]
    fn explicit_mapping_allows_relocation_but_rejects_nested_escape_and_retarget() {
        fn redirect(target: &Path, link: &Path) {
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, link).unwrap();
            #[cfg(windows)]
            {
                let output = std::process::Command::new("cmd.exe")
                    .args(["/D", "/C", "mklink", "/J"])
                    // mklink treats forward slashes as switches even though
                    // Windows filesystem APIs accept them in path arguments.
                    .arg(link.as_os_str().to_string_lossy().replace('/', "\\"))
                    .arg(target.as_os_str().to_string_lossy().replace('/', "\\"))
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "junction creation failed: {:?}",
                    output
                );
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let target = temp.path().join("disk-data");
        let outside = temp.path().join("outside");
        fs::create_dir_all(home.join("Documents")).unwrap();
        fs::create_dir_all(home.join(".ssh")).unwrap();
        fs::create_dir_all(&target).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let logical = home.join("Documents/managed");
        redirect(&target, &logical);
        let mut policy = DesktopPathPolicy::new(&home).unwrap();
        assert!(
            policy
                .check_resolved(
                    &logical.join("new"),
                    &resolve_path(&logical.join("new")).unwrap(),
                    PathAccess::Write
                )
                .is_err()
        );
        policy
            .register_mappings(vec![(logical.clone(), target.clone())])
            .unwrap();
        assert!(
            policy
                .check_resolved(
                    &logical.join("new"),
                    &resolve_path(&logical.join("new")).unwrap(),
                    PathAccess::Write
                )
                .is_ok()
        );
        redirect(&outside, &target.join("escape"));
        assert!(
            policy
                .check_resolved(
                    &logical.join("escape/new"),
                    &resolve_path(&logical.join("escape/new")).unwrap(),
                    PathAccess::Write
                )
                .is_err()
        );
        redirect(&home.join(".ssh"), &target.join("credentials"));
        assert_eq!(
            policy
                .check_resolved(
                    &logical.join("credentials/key"),
                    &resolve_path(&logical.join("credentials/key")).unwrap(),
                    PathAccess::Write
                )
                .unwrap_err()
                .code,
            "PATH_READ_DENIED"
        );
        #[cfg(unix)]
        fs::remove_file(&logical).unwrap();
        #[cfg(windows)]
        fs::remove_dir(&logical).unwrap();
        redirect(&outside, &logical);
        assert!(
            policy
                .check_resolved(
                    &logical.join("new"),
                    &resolve_path(&logical.join("new")).unwrap(),
                    PathAccess::Write
                )
                .is_err()
        );
    }

    #[test]
    fn logical_normalization_collapses_parents_without_crossing_root() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(
            logical_path(&temp.path().join("Documents/a/../b")).unwrap(),
            temp.path().join("Documents/b")
        );
        assert!(logical_path(Path::new("relative/path")).is_err());
        #[cfg(unix)]
        assert!(logical_path(Path::new("/../outside")).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_logical_paths_have_unambiguous_drive_and_component_identity() {
        assert!(same_path(
            Path::new(r"C:\Users\Example\Documents\a"),
            Path::new("c:/users/example/documents/a")
        ));
        assert!(same_path(
            Path::new(r"\\?\C:\Users\Example\Documents\a"),
            Path::new("C:/Users/Example/Documents/a")
        ));
        assert_eq!(
            logical_path(Path::new("C:/Documents/a/../b")).unwrap(),
            PathBuf::from(r"C:\Documents\b")
        );
        for invalid in [
            r"C:\..\outside",
            r"C:\Documents\file:stream",
            r"C:\Documents\file.",
            r"C:\Documents\file ",
            r"\\.\PhysicalDrive0",
            r"C:\Documents\NUL.txt",
            r"C:\Documents\COM1",
        ] {
            assert!(
                logical_path(Path::new(invalid)).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn desktop_reads_are_open_except_credentials_and_writes_are_allowlisted() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().canonicalize().unwrap();
        let policy = DesktopPathPolicy::new(&home).unwrap();
        assert!(
            policy
                .check(&home.join("Other/readme.txt"), PathAccess::Read)
                .is_ok()
        );
        assert!(
            policy
                .check(&home.join("Code/new.txt"), PathAccess::Write)
                .is_ok()
        );
        assert!(
            policy
                .check(&home.join("Downloads/new.txt"), PathAccess::Write)
                .is_ok()
        );
        assert!(
            policy
                .check(&home.join("Documents/new.txt"), PathAccess::Write)
                .is_ok()
        );
        assert_eq!(
            policy
                .check(&home.join("Other/new.txt"), PathAccess::Write)
                .unwrap_err()
                .code,
            "PATH_WRITE_DENIED"
        );
        assert_eq!(
            policy
                .check(&home.join(".ssh/id_ed25519"), PathAccess::Read)
                .unwrap_err()
                .code,
            "PATH_READ_DENIED"
        );
        assert_eq!(
            policy
                .check(&home.join(".ssh/id_ed25519"), PathAccess::Write)
                .unwrap_err()
                .code,
            "PATH_READ_DENIED"
        );
    }

    #[test]
    fn missing_path_cannot_escape_allowlist_through_parent_components() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("Code/missing/../../../outside");
        assert!(resolve_path(&path).is_err());
    }
}
