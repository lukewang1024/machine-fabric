//! Worker-owned admission. Wakeups are hints: every reconciliation rereads the
//! short-lived local snapshot. Darwin file-descriptor delivery works without a
//! main-thread Cocoa runloop; periodic checks cover lost events/provider death.
use crate::desktop::DesktopQueue;
use machine_fabric_protocol::RpcError;
use std::sync::{Mutex, Weak};
use std::time::Duration;

type Queue = Weak<Mutex<DesktopQueue>>;
fn reconcile(queue: &Queue, notification: bool) -> bool {
    let Some(queue) = queue.upgrade() else {
        return false;
    };
    if let Ok(mut queue) = queue.lock()
        && queue.worker_reconcile(notification).is_err()
    {
        return true;
    }
    true
}
pub(crate) fn start(queue: Queue) -> Result<(), RpcError> {
    std::thread::Builder::new()
        .name("desktop-queue".into())
        .spawn(move || {
            #[cfg(target_os = "macos")]
            let mut notifications =
                macos::Notifications::new(c"dev.machine-fabric.desktop-availability-changed");
            let mut notified = false;
            while reconcile(&queue, notified) {
                #[cfg(target_os = "macos")]
                {
                    notified = if let Some(source) = &mut notifications {
                        source.wait(Duration::from_millis(500))
                    } else {
                        std::thread::sleep(Duration::from_millis(500));
                        false
                    };
                }
                #[cfg(not(target_os = "macos"))]
                {
                    std::thread::sleep(Duration::from_millis(500));
                    notified = false;
                }
            }
        })
        .map_err(|e| RpcError::new("DESKTOP_WORKER_FAILED", e.to_string()))?;
    Ok(())
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::ffi::{CStr, c_char};
    unsafe extern "C" {
        fn notify_register_file_descriptor(
            name: *const c_char,
            fd: *mut i32,
            flags: i32,
            token: *mut i32,
        ) -> u32;
        fn notify_cancel(token: i32) -> u32;
        #[cfg(test)]
        fn notify_post(name: *const c_char) -> u32;
    }
    pub(super) struct Notifications {
        fd: i32,
        token: i32,
    }
    impl Notifications {
        pub(super) fn new(name: &CStr) -> Option<Self> {
            let (mut fd, mut token) = (-1, -1);
            let status =
                unsafe { notify_register_file_descriptor(name.as_ptr(), &mut fd, 0, &mut token) };
            if status == 0 && fd >= 0 {
                Some(Self { fd, token })
            } else {
                None
            }
        }
        pub(super) fn wait(&mut self, duration: Duration) -> bool {
            let mut descriptor = libc::pollfd {
                fd: self.fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let ready = unsafe {
                libc::poll(
                    &mut descriptor,
                    1,
                    duration.as_millis().min(i32::MAX as u128) as i32,
                )
            };
            if ready <= 0 {
                return false;
            }
            if descriptor.revents & libc::POLLIN == 0 {
                std::thread::sleep(duration);
                return false;
            }
            let mut bytes = [0u8; 4];
            let size = unsafe { libc::read(self.fd, bytes.as_mut_ptr().cast(), bytes.len()) };
            size == 4 && u32::from_be_bytes(bytes) == self.token as u32
        }
    }
    impl Drop for Notifications {
        fn drop(&mut self) {
            // notify_cancel closes this non-reused descriptor; no double close.
            unsafe {
                notify_cancel(self.token);
            }
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn event_arrives_on_background_thread_without_main_runloop() {
            let name =
                std::ffi::CString::new(format!("dev.machine-fabric.test.{}", uuid::Uuid::new_v4()))
                    .unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let other = name.clone();
            let worker = std::thread::spawn(move || {
                let mut source =
                    Notifications::new(&other).expect("Darwin descriptor registration");
                tx.send(()).unwrap();
                source.wait(Duration::from_secs(3))
            });
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
            assert_eq!(unsafe { notify_post(name.as_ptr()) }, 0);
            assert!(
                worker.join().unwrap(),
                "actual native notification was not delivered"
            );
        }
    }
}
