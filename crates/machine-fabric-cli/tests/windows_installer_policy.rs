#[cfg(windows)]
#[test]
fn native_powershell_installer_policy_and_quoting() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let output = std::process::Command::new("powershell.exe")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
        ])
        .arg(root.join("scripts/test-install-windows-policy.ps1"))
        .arg("-Binary")
        .arg(env!("CARGO_BIN_EXE_machine-fabric"))
        .output()
        .expect("Windows PowerShell must be available");
    assert!(
        output.status.success(),
        "PowerShell installer tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
