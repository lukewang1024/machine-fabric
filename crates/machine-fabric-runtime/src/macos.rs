use base64::Engine;
use machine_fabric_core::now_ms;
use machine_fabric_protocol::RpcError;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::{Message, connect};

#[cfg(target_os = "macos")]
mod accessibility {
    use libc::{c_char, c_void, pid_t};
    use serde_json::{Value, json};
    use std::collections::BTreeSet;
    use std::ffi::CStr;
    use std::ptr;

    type CFTypeRef = *const c_void;
    type CFArrayRef = *const c_void;
    type CFStringRef = *const c_void;
    type AXUIElementRef = *const c_void;
    type CFIndex = isize;
    type CFTypeID = usize;

    const UTF8: u32 = 0x0800_0100;

    #[link(name = "ApplicationServices", kind = "framework")]
    unsafe extern "C" {
        fn AXIsProcessTrusted() -> bool;
        fn AXIsProcessTrustedWithOptions(options: CFTypeRef) -> bool;
        fn AXUIElementCreateApplication(pid: pid_t) -> AXUIElementRef;
        fn AXUIElementCopyAttributeValue(
            element: AXUIElementRef,
            attribute: CFStringRef,
            value: *mut CFTypeRef,
        ) -> i32;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        static kCFBooleanTrue: CFTypeRef;
        fn CFRelease(value: CFTypeRef);
        fn CFGetTypeID(value: CFTypeRef) -> CFTypeID;
        fn CFArrayGetTypeID() -> CFTypeID;
        fn CFArrayGetCount(array: CFArrayRef) -> CFIndex;
        fn CFArrayGetValueAtIndex(array: CFArrayRef, index: CFIndex) -> CFTypeRef;
        fn CFStringGetTypeID() -> CFTypeID;
        fn CFStringGetCStringPtr(string: CFStringRef, encoding: u32) -> *const c_char;
        fn CFStringGetCString(
            string: CFStringRef,
            buffer: *mut c_char,
            buffer_size: CFIndex,
            encoding: u32,
        ) -> bool;
        fn CFStringGetLength(string: CFStringRef) -> CFIndex;
        fn CFStringGetMaximumSizeForEncoding(length: CFIndex, encoding: u32) -> CFIndex;
        fn CFStringCreateWithCString(
            allocator: CFTypeRef,
            value: *const c_char,
            encoding: u32,
        ) -> CFStringRef;
        fn CFDictionaryCreate(
            allocator: CFTypeRef,
            keys: *const *const c_void,
            values: *const *const c_void,
            count: CFIndex,
            key_callbacks: *const c_void,
            value_callbacks: *const c_void,
        ) -> CFTypeRef;
        static kCFTypeDictionaryKeyCallBacks: c_void;
        static kCFTypeDictionaryValueCallBacks: c_void;
    }

    struct Owned(CFTypeRef);
    impl Drop for Owned {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { CFRelease(self.0) };
            }
        }
    }

    unsafe fn cf_string(value: CFTypeRef) -> Option<String> {
        if value.is_null() || unsafe { CFGetTypeID(value) } != unsafe { CFStringGetTypeID() } {
            return None;
        }
        let string = value as CFStringRef;
        let direct = unsafe { CFStringGetCStringPtr(string, UTF8) };
        if !direct.is_null() {
            return Some(
                unsafe { CStr::from_ptr(direct) }
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        let length = unsafe { CFStringGetLength(string) };
        let capacity = unsafe { CFStringGetMaximumSizeForEncoding(length, UTF8) } + 1;
        let mut buffer = vec![0_i8; capacity.max(1) as usize];
        unsafe { CFStringGetCString(string, buffer.as_mut_ptr(), capacity, UTF8) }.then(|| {
            unsafe { CStr::from_ptr(buffer.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        })
    }

    unsafe fn attribute(element: AXUIElementRef, name: CFStringRef) -> Option<Owned> {
        let mut value = ptr::null();
        (unsafe { AXUIElementCopyAttributeValue(element, name, &mut value) } == 0
            && !value.is_null())
        .then_some(Owned(value))
    }

    unsafe fn named_attribute(element: AXUIElementRef, name: &CStr) -> Option<Owned> {
        let key = Owned(unsafe { CFStringCreateWithCString(ptr::null(), name.as_ptr(), UTF8) });
        unsafe { attribute(element, key.0) }
    }

    unsafe fn named_string_attribute(element: AXUIElementRef, name: &CStr) -> Option<String> {
        let value = unsafe { named_attribute(element, name) }?;
        unsafe { cf_string(value.0) }
    }

    unsafe fn snapshot(element: AXUIElementRef, depth: usize, budget: &mut usize) -> Value {
        if *budget == 0 {
            return Value::Null;
        }
        *budget -= 1;
        let role = unsafe { named_string_attribute(element, c"AXRole") };
        let title = unsafe { named_string_attribute(element, c"AXTitle") };
        let value = unsafe { named_string_attribute(element, c"AXValue") };
        let description = unsafe { named_string_attribute(element, c"AXDescription") };
        let mut children = Vec::new();
        if depth < 8
            && let Some(array) = unsafe { named_attribute(element, c"AXChildren") }
            && unsafe { CFGetTypeID(array.0) } == unsafe { CFArrayGetTypeID() }
        {
            let count = unsafe { CFArrayGetCount(array.0 as CFArrayRef) }.clamp(0, 200);
            for index in 0..count {
                if *budget == 0 {
                    break;
                }
                let child = unsafe { CFArrayGetValueAtIndex(array.0 as CFArrayRef, index) };
                if !child.is_null() {
                    children.push(unsafe { snapshot(child, depth + 1, budget) });
                }
            }
        }
        json!({
            "role": role,
            "title": title,
            "value": value,
            "description": description,
            "children": children,
        })
    }

    fn request_accessibility_permission() {
        let key = unsafe {
            CFStringCreateWithCString(ptr::null(), c"AXTrustedCheckOptionPrompt".as_ptr(), UTF8)
        };
        if key.is_null() {
            return;
        }
        let key = Owned(key);
        let keys = [key.0];
        let values = [unsafe { kCFBooleanTrue }];
        let options = unsafe {
            CFDictionaryCreate(
                ptr::null(),
                keys.as_ptr(),
                values.as_ptr(),
                1,
                &kCFTypeDictionaryKeyCallBacks,
                &kCFTypeDictionaryValueCallBacks,
            )
        };
        if !options.is_null() {
            let options = Owned(options);
            unsafe { AXIsProcessTrustedWithOptions(options.0) };
        }
    }

    pub fn inspect(pids: &[u64], request_permission: bool) -> Value {
        if request_permission {
            request_accessibility_permission();
        }
        let trusted = unsafe { AXIsProcessTrusted() };
        if !trusted {
            return json!({"accessibilityTrusted": false, "processes": []});
        }
        let mut seen = BTreeSet::new();
        let mut processes = Vec::new();
        for pid in pids.iter().copied().filter(|pid| seen.insert(*pid)) {
            let application = Owned(unsafe { AXUIElementCreateApplication(pid as pid_t) });
            let mut windows = Vec::new();
            if let Some(array) = unsafe { named_attribute(application.0, c"AXWindows") }
                && unsafe { CFGetTypeID(array.0) } == unsafe { CFArrayGetTypeID() }
            {
                let count = unsafe { CFArrayGetCount(array.0 as CFArrayRef) }.clamp(0, 100);
                for index in 0..count {
                    let window = unsafe { CFArrayGetValueAtIndex(array.0 as CFArrayRef, index) };
                    let mut budget = 1000;
                    windows.push(unsafe { snapshot(window, 0, &mut budget) });
                }
            }
            processes.push(json!({"pid": pid, "windows": windows}));
        }
        json!({"accessibilityTrusted": true, "processes": processes})
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SigningUnit {
    pub path: PathBuf,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub entitlements: Option<PathBuf>,
    #[serde(default)]
    pub options_runtime: bool,
    #[serde(default)]
    pub requirements: Option<String>,
}

pub fn inspect(application: &Path) -> Result<Value, RpcError> {
    let info = application.join("Contents/Info.plist");
    let output = Command::new("/usr/bin/plutil")
        .args(["-convert", "json", "-o", "-"])
        .arg(&info)
        .output()
        .map_err(|error| command_error("APPLICATION_INSPECT_FAILED", "plutil", error))?;
    if !output.status.success() {
        return Err(failed_output("APPLICATION_INSPECT_FAILED", &output));
    }
    let value: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| RpcError::new("APPLICATION_INSPECT_FAILED", error.to_string()))?;
    let signature = Command::new("/usr/bin/codesign")
        .args(["--verify", "--deep", "--strict"])
        .arg(application)
        .output()
        .map_err(|error| command_error("APPLICATION_INSPECT_FAILED", "codesign", error))?;
    let local_webcontents = find_directory(application, "local_webcontents");
    Ok(json!({
        "path": application,
        "bundleIdentifier": value.get("CFBundleIdentifier"),
        "version": value.get("CFBundleShortVersionString"),
        "build": value.get("CFBundleVersion"),
        "documentTypes": value.get("CFBundleDocumentTypes"),
        "signature": {"valid": signature.status.success()},
        "localWebcontents": local_webcontents,
    }))
}

fn find_directory(root: &Path, name: &str) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|value| value.to_str()) == Some(name) {
                    return Some(path);
                }
                pending.push(path);
            }
        }
    }
    None
}

pub fn finalize(
    application: &Path,
    units: Vec<SigningUnit>,
    identity: &str,
    signing_keychain: Option<&Path>,
    signing_keychain_password_file: Option<&Path>,
) -> Result<Value, RpcError> {
    if identity != "-" && (signing_keychain.is_none() || signing_keychain_password_file.is_none()) {
        return Err(RpcError::new(
            "SIGNING_IDENTITY_NOT_ALLOWED",
            "non-ad-hoc signing requires an explicit keychain and password file",
        ));
    }
    if let (Some(keychain), Some(password_file)) =
        (signing_keychain, signing_keychain_password_file)
    {
        let password = std::fs::read_to_string(password_file).map_err(|error| {
            command_error("SIGNING_KEYCHAIN_FAILED", "read password file", error)
        })?;
        let output = Command::new("/usr/bin/security")
            .args(["unlock-keychain", "-p", password.trim()])
            .arg(keychain)
            .stdin(Stdio::null())
            .output()
            .map_err(|error| command_error("SIGNING_KEYCHAIN_FAILED", "security", error))?;
        if !output.status.success() {
            return Err(failed_output("SIGNING_KEYCHAIN_FAILED", &output));
        }
    }
    let mut signed = Vec::new();
    for unit in units {
        if !unit.path.starts_with(application) {
            return Err(RpcError::new(
                "SIGNING_PATH_OUTSIDE_APPLICATION",
                format!(
                    "{} is outside {}",
                    unit.path.display(),
                    application.display()
                ),
            ));
        }
        let mut command = Command::new("/usr/bin/codesign");
        command.args(["--force", "--sign", identity]);
        if let Some(keychain) = signing_keychain {
            command.arg("--keychain").arg(keychain);
        }
        if unit.options_runtime {
            command.args(["--options", "runtime"]);
        }
        if let Some(identifier) = unit.identifier {
            command.args(["--identifier", &identifier]);
        }
        if let Some(requirements) = unit.requirements {
            command.args(["--requirements", &requirements]);
        }
        if let Some(entitlements) = unit.entitlements {
            command.arg("--entitlements").arg(entitlements);
        }
        command.arg(&unit.path);
        let output = command
            .stdin(Stdio::null())
            .output()
            .map_err(|error| command_error("SIGNING_FAILED", "codesign", error))?;
        if !output.status.success() {
            return Err(failed_output("SIGNING_FAILED", &output));
        }
        signed.push(unit.path);
    }
    let verification = Command::new("/usr/bin/codesign")
        .args(["--verify", "--deep", "--strict", "--verbose=2"])
        .arg(application)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| command_error("SIGNATURE_INVALID", "codesign", error))?;
    if !verification.status.success() {
        return Err(failed_output("SIGNATURE_INVALID", &verification));
    }
    Ok(json!({
        "applicationPath": application,
        "signed": signed,
        "identity": if identity == "-" { "ad-hoc" } else { identity },
        "verified": true,
        "verifiedAt": now_ms(),
    }))
}

pub struct LaunchOptions<'a> {
    pub env: &'a std::collections::BTreeMap<String, String>,
    pub user_data_dir: Option<&'a Path>,
    pub chromium_local_state_patch: Option<&'a Value>,
    pub browser_executable_relative: Option<&'a str>,
}

pub fn launch(
    application: &Path,
    args: &[String],
    bundle_identifier: &str,
    terminate_conflicts: bool,
    cdp_port: u16,
    options: LaunchOptions<'_>,
) -> Result<Value, RpcError> {
    if !args.iter().any(|arg| arg == "--use-mock-keychain") {
        return Err(RpcError::new(
            "UNSAFE_CREDENTIAL_MODE",
            "macOS application launch requires --use-mock-keychain",
        ));
    }
    let executable = application_executable(application)?;
    let conflicts = conflicting_instances(bundle_identifier, &executable)?;
    if !conflicts.is_empty() && !terminate_conflicts {
        let application_count = conflicts
            .iter()
            .filter_map(|process| process["applicationRoot"].as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();
        let mut error = RpcError::new(
            "CONFLICTING_APPLICATION_INSTANCE",
            format!(
                "{} conflicting process(es) across {} application bundle(s) are running",
                conflicts.len(),
                application_count,
            ),
        );
        error.details = json!({
            "processCount": conflicts.len(),
            "applicationCount": application_count,
            "processes": conflicts,
        });
        return Err(error);
    }
    for conflict in &conflicts {
        let pid = conflict["pid"].as_u64().unwrap_or(0) as i32;
        if pid > 0 {
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
    if !conflicts.is_empty() {
        wait_processes_exit(&conflicts, Duration::from_secs(8));
    }
    if let (Some(profile), Some(patch)) =
        (options.user_data_dir, options.chromium_local_state_patch)
    {
        patch_chromium_local_state(profile, patch)?;
    }
    let mut child = Command::new(&executable)
        .envs(options.env)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| command_error("APPLICATION_LAUNCH_FAILED", "application", error))?;
    // Chromium may spend tens of seconds rebuilding profile state after a
    // signing or application generation change. Keep the process readiness
    // deadline above that cold-start window while retaining bounded startup.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(60);
    let mut browser_child = None;
    let version = loop {
        match cdp_json(cdp_port, "/json/version") {
            Ok(value) => break value,
            Err(_) if Instant::now() < deadline => {
                if browser_child.is_none()
                    && started.elapsed() >= Duration::from_secs(10)
                    && let Some(relative) = options.browser_executable_relative
                {
                    let relative = Path::new(relative);
                    if relative.is_absolute()
                        || relative
                            .components()
                            .any(|part| matches!(part, std::path::Component::ParentDir))
                    {
                        return Err(RpcError::new(
                            "INVALID_PARAMS",
                            "browserExecutableRelative must stay inside the application",
                        ));
                    }
                    let browser = application.join(relative);
                    if !browser.is_file() {
                        return Err(RpcError::new(
                            "BROWSER_FALLBACK_MISSING",
                            format!("browser fallback is missing: {}", browser.display()),
                        ));
                    }
                    let mut browser_args = args.to_vec();
                    browser_args.push(format!("--saman-from-chat={}", child.id()));
                    browser_child = Some(
                        Command::new(&browser)
                            .envs(options.env)
                            .args(&browser_args)
                            .stdin(Stdio::null())
                            .stdout(Stdio::null())
                            .stderr(Stdio::null())
                            .spawn()
                            .map_err(|error| {
                                command_error(
                                    "BROWSER_FALLBACK_LAUNCH_FAILED",
                                    "browser fallback",
                                    error,
                                )
                            })?,
                    );
                }
                if let Some(status) = child.try_wait().map_err(|error| {
                    command_error("APPLICATION_LAUNCH_FAILED", "application", error)
                })? {
                    return Err(RpcError::new(
                        "APPLICATION_EXITED",
                        format!("application exited before CDP became ready: {status}"),
                    ));
                }
                thread::sleep(Duration::from_millis(250));
            }
            Err(error) => return Err(error),
        }
    };
    Ok(json!({
        "applicationPath": application,
        "executable": executable,
        "pid": child.id(),
        "browserFallbackPid": browser_child.as_ref().map(std::process::Child::id),
        "args": args,
        "terminatedConflicts": conflicts,
        "cdp": version,
        "readyAt": now_ms(),
    }))
}

fn merge_json(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => {
            for (key, value) in patch {
                merge_json(target.entry(key.clone()).or_insert(Value::Null), value);
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

fn patch_chromium_local_state(profile: &Path, patch: &Value) -> Result<(), RpcError> {
    if !patch.is_object() {
        return Err(RpcError::new(
            "INVALID_PARAMS",
            "chromiumLocalStatePatch must be an object",
        ));
    }
    std::fs::create_dir_all(profile)
        .map_err(|error| command_error("PROFILE_WRITE_FAILED", "create profile", error))?;
    let local_state = profile.join("Local State");
    let mut state = if local_state.exists() {
        let bytes = std::fs::read(&local_state)
            .map_err(|error| command_error("PROFILE_WRITE_FAILED", "read Local State", error))?;
        serde_json::from_slice(&bytes).map_err(|error| {
            RpcError::new(
                "PROFILE_INVALID",
                format!("cannot parse {}: {error}", local_state.display()),
            )
        })?
    } else {
        json!({})
    };
    if !state.is_object() {
        return Err(RpcError::new(
            "PROFILE_INVALID",
            format!("{} must contain a JSON object", local_state.display()),
        ));
    }
    merge_json(&mut state, patch);
    let temporary = profile.join(format!(".Local State.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&state)
        .map_err(|error| RpcError::new("PROFILE_WRITE_FAILED", error.to_string()))?;
    std::fs::write(&temporary, bytes)
        .map_err(|error| command_error("PROFILE_WRITE_FAILED", "write Local State", error))?;
    std::fs::rename(&temporary, &local_state)
        .map_err(|error| command_error("PROFILE_WRITE_FAILED", "replace Local State", error))?;
    Ok(())
}

pub fn open_file(
    application: &Path,
    file: &Path,
    handler: Option<&Path>,
) -> Result<Value, RpcError> {
    if !file.is_file() {
        return Err(RpcError::new(
            "FILE_NOT_FOUND",
            format!("file not found: {}", file.display()),
        ));
    }
    let handler = handler.unwrap_or(application);
    let output = Command::new("/usr/bin/open")
        .arg("-a")
        .arg(handler)
        .arg(file)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| command_error("OPEN_FILE_FAILED", "open", error))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("error -600") {
            return Err(failed_output("OPEN_FILE_FAILED", &output));
        }

        // The generated app's outer shell may exit after handing control to
        // its embedded Browser. LaunchServices then sees no running app and
        // returns procNotFound (-600). Bypass its bundle-registration and
        // singleton resolution: a fresh outer executable receives the file
        // argument and forwards it to the already-running Browser process.
        let executable = application_executable(application)?;
        let mut retry = Command::new(&executable)
            .arg(file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| command_error("OPEN_FILE_FAILED", "open", error))?;
        thread::sleep(Duration::from_millis(500));
        if let Some(status) = retry
            .try_wait()
            .map_err(|error| command_error("OPEN_FILE_FAILED", "open", error))?
            && !status.success()
        {
            return Err(RpcError::new(
                "OPEN_FILE_FAILED",
                format!("application document handoff exited with {status}"),
            ));
        }
    }
    Ok(json!({
        "applicationPath": application,
        "handlerPath": handler,
        "file": file,
        "openedAt": now_ms()
    }))
}

pub fn stop(application: &Path) -> Result<Value, RpcError> {
    let processes = processes_for_roots(&[application.to_path_buf()], false)?;
    for process in &processes {
        let pid = process["pid"].as_u64().unwrap_or(0) as i32;
        if pid > 0 {
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
    wait_processes_exit(&processes, Duration::from_secs(8));
    Ok(json!({"applicationPath": application, "stopped": processes, "stoppedAt": now_ms()}))
}

pub fn native_inspect(application: &Path, request_permission: bool) -> Result<Value, RpcError> {
    let executable = application_executable(application)?;
    let pids = matching_processes(&executable)?;
    let inspection = accessibility::inspect(&pids, request_permission);
    Ok(json!({
        "applicationPath": application,
        "executable": executable,
        "pids": pids,
        "inspection": inspection,
        "inspectedAt": now_ms(),
    }))
}

pub fn cdp_pages(port: u16) -> Result<Value, RpcError> {
    cdp_json(port, "/json")
}

pub fn cdp_evaluate(
    port: u16,
    target_url_prefix: Option<&str>,
    expression: &str,
) -> Result<Value, RpcError> {
    cdp_command(
        port,
        target_url_prefix,
        "Runtime.evaluate",
        json!({
            "expression": expression,
            "returnByValue": true,
            "awaitPromise": true
        }),
    )
    .map(|result| {
        json!({
            "target": result["target"],
            "value": result.pointer("/result/result/value").cloned().unwrap_or(Value::Null),
            "evaluatedAt": now_ms()
        })
    })
}

pub fn cdp_automate(
    port: u16,
    target_url_prefix: Option<&str>,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    const ALLOWED: &[&str] = &[
        "Input.dispatchMouseEvent",
        "Input.dispatchKeyEvent",
        "Input.insertText",
        "Page.captureScreenshot",
    ];
    if !ALLOWED.contains(&method) {
        return Err(RpcError::new(
            "CDP_METHOD_NOT_ALLOWED",
            format!("CDP method is not allowed: {method}"),
        ));
    }
    cdp_command(port, target_url_prefix, method, params)
}

pub fn cdp_capture(
    port: u16,
    target_url_prefix: Option<&str>,
    output: &Path,
) -> Result<Value, RpcError> {
    let response = cdp_automate(
        port,
        target_url_prefix,
        "Page.captureScreenshot",
        json!({"format": "png", "captureBeyondViewport": false}),
    )?;
    let encoded = response
        .pointer("/result/data")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::new("CDP_INVALID_RESPONSE", "screenshot data is missing"))?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|error| RpcError::new("CDP_INVALID_RESPONSE", error.to_string()))?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| command_error("SCREENSHOT_WRITE_FAILED", "mkdir", error))?;
    }
    std::fs::write(output, &bytes)
        .map_err(|error| command_error("SCREENSHOT_WRITE_FAILED", "write", error))?;
    Ok(json!({
        "target": response["target"],
        "path": output,
        "bytes": bytes.len(),
        "capturedAt": now_ms()
    }))
}

fn cdp_command(
    port: u16,
    target_url_prefix: Option<&str>,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    let pages = cdp_pages(port)?;
    let page = pages
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .filter(|item| {
                    item.get("type").and_then(Value::as_str) == Some("page")
                        && target_url_prefix.is_none_or(|prefix| {
                            item.get("url")
                                .and_then(Value::as_str)
                                .is_some_and(|url| url.starts_with(prefix))
                        })
                })
                .max_by_key(|item| {
                    item.get("url")
                        .and_then(Value::as_str)
                        .and_then(open_received_at)
                        .unwrap_or(0)
                })
        })
        .ok_or_else(|| RpcError::new("CDP_TARGET_NOT_FOUND", "matching page target not found"))?;
    let websocket_url = page
        .get("webSocketDebuggerUrl")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::new("CDP_TARGET_INVALID", "target has no debugger URL"))?;
    let (mut socket, _) = connect(websocket_url)
        .map_err(|error| RpcError::new("CDP_WEBSOCKET_FAILED", error.to_string()))?;
    let request_id = 1_u64;
    socket
        .send(Message::Text(
            json!({
                "id": request_id,
                "method": method,
                "params": params
            })
            .to_string()
            .into(),
        ))
        .map_err(|error| RpcError::new("CDP_WEBSOCKET_FAILED", error.to_string()))?;
    loop {
        let message = socket
            .read()
            .map_err(|error| RpcError::new("CDP_WEBSOCKET_FAILED", error.to_string()))?;
        let Message::Text(text) = message else {
            continue;
        };
        let response: Value = serde_json::from_str(&text)
            .map_err(|error| RpcError::new("CDP_INVALID_RESPONSE", error.to_string()))?;
        if response.get("id").and_then(Value::as_u64) != Some(request_id) {
            continue;
        }
        if let Some(error) = response.get("error") {
            return Err(RpcError::new("CDP_EVALUATION_FAILED", error.to_string()));
        }
        if let Some(exception) = response.pointer("/result/exceptionDetails") {
            return Err(RpcError::new(
                "CDP_EVALUATION_FAILED",
                exception.to_string(),
            ));
        }
        return Ok(json!({"target": page, "result": response["result"], "completedAt": now_ms()}));
    }
}

fn open_received_at(url: &str) -> Option<u64> {
    url.split_once('?')?.1.split('&').find_map(|field| {
        let (key, value) = field.split_once('=')?;
        (key == "openReceivedAt")
            .then(|| value.parse().ok())
            .flatten()
    })
}

fn application_executable(application: &Path) -> Result<PathBuf, RpcError> {
    let info = inspect(application)?;
    let plist_output = Command::new("/usr/libexec/PlistBuddy")
        .args(["-c", "Print :CFBundleExecutable"])
        .arg(application.join("Contents/Info.plist"))
        .output()
        .map_err(|error| command_error("APPLICATION_INVALID", "PlistBuddy", error))?;
    if !plist_output.status.success() {
        return Err(RpcError::new(
            "APPLICATION_INVALID",
            format!("application has no executable: {info}"),
        ));
    }
    let name = String::from_utf8_lossy(&plist_output.stdout)
        .trim()
        .to_owned();
    let executable = application.join("Contents/MacOS").join(name);
    if !executable.is_file() {
        return Err(RpcError::new(
            "APPLICATION_INVALID",
            format!("executable missing: {}", executable.display()),
        ));
    }
    Ok(executable)
}

fn matching_processes(executable: &Path) -> Result<Vec<u64>, RpcError> {
    let output = Command::new("/bin/ps")
        .args(["-axo", "pid=,comm="])
        .output()
        .map_err(|error| command_error("PROCESS_DISCOVERY_FAILED", "ps", error))?;
    if !output.status.success() {
        return Err(failed_output("PROCESS_DISCOVERY_FAILED", &output));
    }
    let expected = executable.to_string_lossy();
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.trim().splitn(2, char::is_whitespace);
            let pid = parts.next()?.parse::<u64>().ok()?;
            (parts.next()?.trim() == expected.as_ref()).then_some(pid)
        })
        .collect())
}

fn conflicting_instances(bundle_identifier: &str, expected: &Path) -> Result<Vec<Value>, RpcError> {
    let target_root = outer_application_root(expected)
        .ok_or_else(|| RpcError::new("APPLICATION_INVALID", "executable is outside an app"))?;
    let processes = process_snapshot()?;
    let mut roots = vec![target_root];
    for process in &processes {
        let command = Path::new(process["command"].as_str().unwrap_or_default());
        let Some(root) = outer_application_root(command) else {
            continue;
        };
        // The outer Doubao process commonly exits after handing off to its
        // embedded Browser. Discover a conflicting bundle from any surviving
        // helper, otherwise a later generation can mistake the old CDP port
        // for its own readiness endpoint.
        if application_bundle_identifier(&root).ok().as_deref() == Some(bundle_identifier)
            && !roots.contains(&root)
        {
            roots.push(root);
        }
    }
    processes_for_snapshot_roots(&processes, &roots, true, bundle_identifier)
}

fn process_snapshot() -> Result<Vec<Value>, RpcError> {
    let output = Command::new("/bin/ps")
        .args(["-axo", "pid=,comm="])
        .output()
        .map_err(|error| command_error("PROCESS_DISCOVERY_FAILED", "ps", error))?;
    if !output.status.success() {
        return Err(failed_output("PROCESS_DISCOVERY_FAILED", &output));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.trim().splitn(2, char::is_whitespace);
            let pid = parts.next()?.parse::<u64>().ok()?;
            let command = parts.next()?.trim();
            Some(json!({"pid": pid, "command": command}))
        })
        .collect())
}

fn processes_for_roots(
    roots: &[PathBuf],
    include_orphan_repair: bool,
) -> Result<Vec<Value>, RpcError> {
    let identifier = roots
        .first()
        .map(|root| application_bundle_identifier(root))
        .transpose()?
        .unwrap_or_default();
    processes_for_snapshot_roots(
        &process_snapshot()?,
        roots,
        include_orphan_repair,
        &identifier,
    )
}

fn processes_for_snapshot_roots(
    processes: &[Value],
    roots: &[PathBuf],
    include_orphan_repair: bool,
    bundle_identifier: &str,
) -> Result<Vec<Value>, RpcError> {
    let mut values = Vec::new();
    for process in processes {
        let pid = process["pid"].as_u64().unwrap_or(0);
        let command = process["command"].as_str().unwrap_or_default();
        let Some(application_root) = outer_application_root(Path::new(command)) else {
            continue;
        };
        let in_selected_root = roots.contains(&application_root);
        let orphan_repair = include_orphan_repair
            && command.contains("Browser Helper (Repair).app/Contents/MacOS/")
            && application_bundle_identifier(&application_root)
                .ok()
                .as_deref()
                == Some(bundle_identifier);
        if in_selected_root || orphan_repair {
            values.push(json!({
                "pid": pid,
                "command": command,
                "applicationRoot": application_root,
                "bundleIdentifier": bundle_identifier
            }));
        }
    }
    Ok(values)
}

fn outer_application_root(executable: &Path) -> Option<PathBuf> {
    let text = executable.to_str()?;
    let end = text.find(".app/")?.saturating_add(4);
    Some(PathBuf::from(&text[..end]))
}

fn application_bundle_identifier(application: &Path) -> Result<String, RpcError> {
    let output = Command::new("/usr/libexec/PlistBuddy")
        .args(["-c", "Print :CFBundleIdentifier"])
        .arg(application.join("Contents/Info.plist"))
        .output()
        .map_err(|error| command_error("APPLICATION_INVALID", "PlistBuddy", error))?;
    if !output.status.success() {
        return Err(failed_output("APPLICATION_INVALID", &output));
    }
    let identifier = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if identifier.is_empty() {
        return Err(RpcError::new(
            "APPLICATION_INVALID",
            "application has no bundle identifier",
        ));
    }
    Ok(identifier)
}

fn wait_processes_exit(processes: &[Value], timeout: Duration) {
    let deadline = Instant::now() + timeout;
    for process in processes {
        let pid = process["pid"].as_u64().unwrap_or(0) as i32;
        while pid > 0 && unsafe { libc::kill(pid, 0) } == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(100));
        }
        if pid > 0 && unsafe { libc::kill(pid, 0) } == 0 {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}

fn cdp_json(port: u16, path: &str) -> Result<Value, RpcError> {
    let mut stream = TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}")
            .parse()
            .expect("valid loopback address"),
        Duration::from_millis(500),
    )
    .map_err(|error| RpcError::new("CDP_UNAVAILABLE", error.to_string()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|error| RpcError::new("CDP_UNAVAILABLE", error.to_string()))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|error| RpcError::new("CDP_UNAVAILABLE", error.to_string()))?;
    let mut response = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => response.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) && !response.is_empty() =>
            {
                break;
            }
            Err(error) => return Err(RpcError::new("CDP_UNAVAILABLE", error.to_string())),
        }
    }
    let body = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| &response[position + 4..])
        .ok_or_else(|| RpcError::new("CDP_INVALID_RESPONSE", "missing HTTP response body"))?;
    serde_json::from_slice(body)
        .map_err(|error| RpcError::new("CDP_INVALID_RESPONSE", error.to_string()))
}

fn command_error(code: &str, command: &str, error: std::io::Error) -> RpcError {
    RpcError::new(code, format!("cannot execute {command}: {error}"))
}

fn failed_output(code: &str, output: &std::process::Output) -> RpcError {
    let message = String::from_utf8_lossy(if output.stderr.is_empty() {
        &output.stdout
    } else {
        &output.stderr
    });
    RpcError::new(code, message.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_document_open_timestamp_for_fresh_target_selection() {
        assert_eq!(
            open_received_at(
                "doubao://doubao-office/docx/id?type=docx&openReceivedAt=1786990651541&viewId=2"
            ),
            Some(1_786_990_651_541)
        );
        assert_eq!(
            open_received_at("doubao://doubao-office/docx/id?type=docx"),
            None
        );
        assert_eq!(
            open_received_at("doubao://doubao-office/docx/id?openReceivedAt=bad"),
            None
        );
    }

    #[test]
    fn helper_executables_resolve_to_the_outer_application_bundle() {
        let executable = Path::new(
            "/state/g/Demo.app/Contents/Helpers/Browser.app/Contents/Frameworks/F.framework/Helpers/Repair.app/Contents/MacOS/Repair",
        );
        assert_eq!(
            outer_application_root(executable).unwrap(),
            PathBuf::from("/state/g/Demo.app")
        );
    }

    #[test]
    fn process_selection_does_not_touch_unselected_extensions() {
        let target = PathBuf::from("/state/Target.app");
        let processes = vec![
            json!({"pid": 1, "command": "/state/Target.app/Contents/MacOS/Target"}),
            json!({"pid": 2, "command": "/state/Target.app/Contents/Helpers/Repair"}),
            json!({"pid": 3, "command": "/Applications/Other.app/Contents/PlugIns/finder.appex/Contents/MacOS/finder"}),
        ];
        let selected =
            processes_for_snapshot_roots(&processes, &[target], false, "example.target").unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(|item| item["pid"] != 3));
    }

    #[test]
    fn chromium_local_state_patch_preserves_existing_preferences() {
        let profile =
            std::env::temp_dir().join(format!("machine-fabric-profile-{}", std::process::id()));
        std::fs::create_dir_all(&profile).unwrap();
        std::fs::write(
            profile.join("Local State"),
            br#"{"existing":{"keep":true},"saman":{"shortcut":{"other":{"key":"A"}}}}"#,
        )
        .unwrap();

        patch_chromium_local_state(
            &profile,
            &json!({
                "saman": {"shortcut": {"launcher": {"key": ""}}}
            }),
        )
        .unwrap();

        let state: Value =
            serde_json::from_slice(&std::fs::read(profile.join("Local State")).unwrap()).unwrap();
        assert_eq!(state["existing"]["keep"], true);
        assert_eq!(state["saman"]["shortcut"]["other"]["key"], "A");
        assert_eq!(state["saman"]["shortcut"]["launcher"]["key"], "");
        std::fs::remove_dir_all(profile).unwrap();
    }
}
