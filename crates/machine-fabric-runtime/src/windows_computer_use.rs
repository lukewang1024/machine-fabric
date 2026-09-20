use machine_fabric_protocol::RpcError;
use std::ffi::OsString;
use std::os::windows::ffi::OsStringExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Path, PathBuf};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Security::{DuplicateTokenEx, SecurityImpersonation, TOKEN_ALL_ACCESS, TokenPrimary},
    System::{
        Environment::{CreateEnvironmentBlock, DestroyEnvironmentBlock},
        RemoteDesktop::{
            WTS_CURRENT_SERVER_HANDLE, WTS_SESSION_INFOW, WTSActive, WTSDisconnected,
            WTSEnumerateSessionsW, WTSFreeMemory, WTSQueryUserToken,
        },
        Threading::{
            CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT, CreateProcessAsUserW, GetExitCodeProcess,
            PROCESS_INFORMATION, STARTUPINFOW, TerminateProcess, WaitForSingleObject,
        },
    },
    UI::Shell::GetUserProfileDirectoryW,
};

pub(crate) struct HostProcess(OwnedHandle);

impl HostProcess {
    pub(crate) fn try_wait(&mut self) -> std::io::Result<Option<u32>> {
        match unsafe { WaitForSingleObject(self.0.as_raw_handle(), 0) } {
            WAIT_OBJECT_0 => self.exit_code().map(Some),
            WAIT_TIMEOUT => Ok(None),
            _ => Err(std::io::Error::last_os_error()),
        }
    }

    fn exit_code(&self) -> std::io::Result<u32> {
        let mut code = 0;
        if unsafe { GetExitCodeProcess(self.0.as_raw_handle(), &mut code) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(code)
    }

    pub(crate) fn kill(&mut self) -> std::io::Result<()> {
        if self.try_wait()?.is_some() {
            return Ok(());
        }
        if unsafe { TerminateProcess(self.0.as_raw_handle(), 1) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn wait(&mut self) -> std::io::Result<u32> {
        if unsafe { WaitForSingleObject(self.0.as_raw_handle(), u32::MAX) } != WAIT_OBJECT_0 {
            return Err(std::io::Error::last_os_error());
        }
        self.exit_code()
    }
}

pub(crate) fn interactive_computer_use_state_root() -> Result<PathBuf, RpcError> {
    let mut sessions = std::ptr::null_mut::<WTS_SESSION_INFOW>();
    let mut count = 0_u32;
    if unsafe { WTSEnumerateSessionsW(WTS_CURRENT_SERVER_HANDLE, 0, 1, &mut sessions, &mut count) }
        == 0
    {
        return Err(failed("INTERACTIVE_SESSION_UNAVAILABLE"));
    }
    let entries = unsafe { std::slice::from_raw_parts(sessions, count as usize) };
    let mut token: HANDLE = std::ptr::null_mut();
    let mut last_token_error = None;
    for session in entries
        .iter()
        .filter(|session| session.State == WTSActive)
        .chain(
            entries
                .iter()
                .filter(|session| session.State == WTSDisconnected),
        )
    {
        if unsafe { WTSQueryUserToken(session.SessionId, &mut token) } != 0 {
            break;
        }
        last_token_error = Some(std::io::Error::last_os_error().to_string());
    }
    unsafe { WTSFreeMemory(sessions.cast()) };
    if token.is_null() {
        return Err(RpcError::new(
            "INTERACTIVE_SESSION_TOKEN_FAILED",
            last_token_error.unwrap_or_else(|| "no interactive user token".into()),
        ));
    }
    let mut length = 0_u32;
    unsafe { GetUserProfileDirectoryW(token, std::ptr::null_mut(), &mut length) };
    if length == 0 {
        let error = failed("INTERACTIVE_PROFILE_UNAVAILABLE");
        unsafe { CloseHandle(token) };
        return Err(error);
    }
    let mut profile = vec![0_u16; length as usize];
    let result = unsafe { GetUserProfileDirectoryW(token, profile.as_mut_ptr(), &mut length) };
    let error = (result == 0).then(|| failed("INTERACTIVE_PROFILE_UNAVAILABLE"));
    unsafe { CloseHandle(token) };
    if let Some(error) = error {
        return Err(error);
    }
    let end = profile
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(profile.len());
    let profile = PathBuf::from(OsString::from_wide(&profile[..end]));
    Ok(profile
        .join("AppData")
        .join("Local")
        .join("machine-fabric/state/computer-use"))
}

pub(crate) fn spawn_hidden_in_active_session(
    executable: &Path,
    args: &[String],
    cwd: &Path,
) -> Result<HostProcess, RpcError> {
    let mut sessions = std::ptr::null_mut::<WTS_SESSION_INFOW>();
    let mut count = 0_u32;
    if unsafe { WTSEnumerateSessionsW(WTS_CURRENT_SERVER_HANDLE, 0, 1, &mut sessions, &mut count) }
        == 0
    {
        return Err(failed("INTERACTIVE_SESSION_UNAVAILABLE"));
    }
    let entries = unsafe { std::slice::from_raw_parts(sessions, count as usize) };
    let mut user_token: HANDLE = std::ptr::null_mut();
    let mut candidate_found = false;
    for session in entries
        .iter()
        .filter(|session| session.State == WTSActive)
        .chain(
            entries
                .iter()
                .filter(|session| session.State == WTSDisconnected),
        )
    {
        candidate_found = true;
        if unsafe { WTSQueryUserToken(session.SessionId, &mut user_token) } != 0 {
            break;
        }
    }
    unsafe { WTSFreeMemory(sessions.cast()) };
    if user_token.is_null() {
        return Err(RpcError::new(
            if candidate_found {
                "INTERACTIVE_SESSION_TOKEN_FAILED"
            } else {
                "INTERACTIVE_SESSION_UNAVAILABLE"
            },
            std::io::Error::last_os_error().to_string(),
        ));
    }

    let mut primary_token: HANDLE = std::ptr::null_mut();
    let duplicated = unsafe {
        DuplicateTokenEx(
            user_token,
            TOKEN_ALL_ACCESS,
            std::ptr::null(),
            SecurityImpersonation,
            TokenPrimary,
            &mut primary_token,
        )
    };
    unsafe { CloseHandle(user_token) };
    if duplicated == 0 {
        return Err(failed("INTERACTIVE_SESSION_TOKEN_FAILED"));
    }

    let command_line = std::iter::once(executable.to_string_lossy().into_owned())
        .chain(args.iter().cloned())
        .map(|value| format!("\"{}\"", value.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(" ")
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut command_line = command_line;
    let mut desktop = "winsta0\\default\0".encode_utf16().collect::<Vec<_>>();
    let cwd = cwd
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut startup: STARTUPINFOW = unsafe { std::mem::zeroed() };
    startup.cb = std::mem::size_of::<STARTUPINFOW>() as u32;
    startup.lpDesktop = desktop.as_mut_ptr();
    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let mut environment = std::ptr::null_mut();
    if unsafe { CreateEnvironmentBlock(&mut environment, primary_token, 0) } == 0 {
        unsafe { CloseHandle(primary_token) };
        return Err(failed("INTERACTIVE_ENVIRONMENT_FAILED"));
    }
    let created = unsafe {
        CreateProcessAsUserW(
            primary_token,
            std::ptr::null(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW,
            environment,
            cwd.as_ptr(),
            &startup,
            &mut process,
        )
    };
    unsafe { DestroyEnvironmentBlock(environment) };
    unsafe { CloseHandle(primary_token) };
    if created == 0 {
        return Err(failed("INTERACTIVE_PROCESS_CREATE_FAILED"));
    }
    unsafe {
        CloseHandle(process.hThread);
    }
    // Keep the exact process object, rather than a reusable PID, so idle
    // host exit can be detected before submitting another desktop request.
    Ok(HostProcess(unsafe {
        OwnedHandle::from_raw_handle(process.hProcess)
    }))
}

fn failed(code: &'static str) -> RpcError {
    RpcError::new(code, std::io::Error::last_os_error().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::windows::io::BorrowedHandle;
    use std::os::windows::process::CommandExt;
    use std::process::Command;

    #[test]
    fn retained_handle_observes_exit_and_terminates_only_its_process() {
        for command in ["exit 0", "ping -n 30 127.0.0.1 > NUL"] {
            let mut child = Command::new("cmd.exe")
                .args(["/C", command])
                .creation_flags(CREATE_NO_WINDOW)
                .spawn()
                .unwrap();
            let handle = unsafe { BorrowedHandle::borrow_raw(child.as_raw_handle()) }
                .try_clone_to_owned()
                .unwrap();
            let mut tracked = HostProcess(handle);
            if command == "exit 0" {
                assert!(child.wait().unwrap().success());
                assert_eq!(tracked.try_wait().unwrap(), Some(0));
                tracked.kill().unwrap();
            } else {
                assert_eq!(tracked.try_wait().unwrap(), None);
                tracked.kill().unwrap();
                assert_ne!(tracked.wait().unwrap(), 0);
                child.wait().unwrap();
            }
        }
    }
}
