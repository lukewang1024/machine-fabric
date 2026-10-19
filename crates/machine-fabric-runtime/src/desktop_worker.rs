//! Node-local queue wakeup. A notification never grants permission: promotion
//! always rereads the short-lived provider snapshot under the desktop mutex.
use crate::desktop::DesktopQueue;
use machine_fabric_protocol::RpcError;
use std::sync::{Mutex, Weak};
use std::time::Duration;

type Queue = Weak<Mutex<DesktopQueue>>;
fn reconcile(queue: &Queue, notification: bool) -> bool {
    let Some(queue) = queue.upgrade() else {
        return false;
    };
    if let Ok(mut queue) = queue.lock() {
        // A failed persistence operation must not authorize desktop input.
        if queue.worker_reconcile(notification).is_err() {
            return true;
        }
    }
    true
}
pub(crate) fn start(queue: Queue) -> Result<(), RpcError> {
    std::thread::Builder::new()
        .name("desktop-queue".into())
        .spawn(move || {
            #[cfg(target_os = "macos")]
            let _notifications = macos::Notifications::new(queue.clone());
            while reconcile(&queue, false) {
                // Reconcile even when notifications were lost or the provider died.
                #[cfg(target_os = "macos")]
                macos::wait(Duration::from_millis(500));
                #[cfg(not(target_os = "macos"))]
                std::thread::sleep(Duration::from_millis(500));
            }
        })
        .map_err(|e| RpcError::new("DESKTOP_WORKER_FAILED", e.to_string()))?;
    Ok(())
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::ffi::c_void;
    type Ref = *const c_void;
    type Callback = unsafe extern "C" fn(Ref, *mut c_void, Ref, Ref, Ref);
    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFNotificationCenterGetDistributedCenter() -> Ref;
        fn CFNotificationCenterAddObserver(
            center: Ref,
            observer: *const c_void,
            callback: Callback,
            name: Ref,
            object: Ref,
            behavior: isize,
        );
        fn CFNotificationCenterRemoveObserver(
            center: Ref,
            observer: *const c_void,
            name: Ref,
            object: Ref,
        );
        fn CFStringCreateWithCString(
            allocator: Ref,
            string: *const std::ffi::c_char,
            encoding: u32,
        ) -> Ref;
        fn CFRelease(object: Ref);
        fn CFRunLoopRunInMode(mode: Ref, seconds: f64, return_after_source: bool) -> i32;
        static kCFRunLoopDefaultMode: Ref;
    }
    unsafe extern "C" fn changed(_: Ref, observer: *mut c_void, _: Ref, _: Ref, _: Ref) {
        // The Box is stable until the observer is removed on this runloop thread.
        let queue = unsafe { &*observer.cast::<Queue>() };
        reconcile(queue, true);
    }
    pub(super) struct Notifications {
        queue: Box<Queue>,
        name: Ref,
        center: Ref,
    }
    impl Notifications {
        pub(super) fn new(queue: Queue) -> Self {
            let queue = Box::new(queue);
            unsafe {
                let center = CFNotificationCenterGetDistributedCenter();
                let name = CFStringCreateWithCString(
                    std::ptr::null(),
                    c"dev.machine-fabric.desktop-availability-changed".as_ptr(),
                    0x08000100,
                );
                CFNotificationCenterAddObserver(
                    center,
                    (&*queue as *const Queue).cast(),
                    changed,
                    name,
                    std::ptr::null(),
                    4,
                );
                Self {
                    queue,
                    name,
                    center,
                }
            }
        }
    }
    impl Drop for Notifications {
        fn drop(&mut self) {
            unsafe {
                CFNotificationCenterRemoveObserver(
                    self.center,
                    (&*self.queue as *const Queue).cast(),
                    self.name,
                    std::ptr::null(),
                );
                CFRelease(self.name);
            }
        }
    }
    pub(super) fn wait(duration: Duration) {
        unsafe {
            if CFRunLoopRunInMode(kCFRunLoopDefaultMode, duration.as_secs_f64(), true) == 1 {
                std::thread::sleep(duration);
            }
        }
    }
}
