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
            }
        }

        for variable in ["XDG_CACHE_HOME", "XDG_STATE_HOME", "XDG_CONFIG_HOME"] {
            if let Some(value) = std::env::var_os(variable) {
                let root = PathBuf::from(value);
                if root.is_absolute() {
                    let root = resolve_path(&root)?;
                    if within(&root, &home) {
                        write_allowed.push(root);
                    }
                }
            }
        }
        Ok(Self {
            read_denied,
            write_allowed,
        })
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
