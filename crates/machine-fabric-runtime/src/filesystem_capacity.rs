use std::{io, path::Path};

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Capacity {
    pub(crate) available_bytes: u64,
    pub(crate) free_bytes: u64,
    pub(crate) total_bytes: u64,
}

pub(crate) fn capacity(path: &Path) -> io::Result<Capacity> {
    let metadata = path.metadata()?;
    let directory = if metadata.is_dir() {
        path
    } else if metadata.is_file() {
        path.parent()
            .ok_or_else(|| io::Error::other("file has no parent directory"))?
    } else {
        return Err(io::Error::other("capacity requires a file or directory"));
    };
    platform_capacity(directory)
}

#[cfg(unix)]
fn platform_capacity(path: &Path) -> io::Result<Capacity> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::other("capacity path contains a NUL"))?;
    // statvfs only reads filesystem statistics; it neither creates a journal
    // nor launches a command, so disk-pressure diagnosis needs no write fence.
    let mut statistics = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), statistics.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let statistics = unsafe { statistics.assume_init() };
    let block_size = if statistics.f_frsize == 0 {
        statistics.f_bsize
    } else {
        statistics.f_frsize
    };
    let bytes = |blocks| {
        u64::try_from(u128::from(blocks) * u128::from(block_size))
            .map_err(|_| io::Error::other("filesystem capacity exceeds u64 bytes"))
    };
    Ok(Capacity {
        available_bytes: bytes(statistics.f_bavail)?,
        free_bytes: bytes(statistics.f_bfree)?,
        total_bytes: bytes(statistics.f_blocks)?,
    })
}

#[cfg(windows)]
fn platform_capacity(path: &Path) -> io::Result<Capacity> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
    let path = path
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let mut result = Capacity {
        available_bytes: 0,
        free_bytes: 0,
        total_bytes: 0,
    };
    if unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut result.available_bytes,
            &mut result.total_bytes,
            &mut result.free_bytes,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(result)
}

#[cfg(not(any(unix, windows)))]
fn platform_capacity(_path: &Path) -> io::Result<Capacity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem capacity is unsupported",
    ))
}
