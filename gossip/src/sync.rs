use std::sync::{LockResult, Mutex, PoisonError, RwLockReadGuard, RwLockWriteGuard};

#[derive(Default)]
pub struct RwLock<T> {
    inner: std::sync::RwLock<T>,
    mutex: Mutex<()>,
}

impl<T> RwLock<T> {
    pub fn new(t: T) -> Self {
        Self {
            inner: std::sync::RwLock::new(t),
            mutex: Mutex::default(),
        }
    }

    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let mutex_lock = self.mutex.lock();
        let inner = self.inner.read();
        match mutex_lock {
            Err(_) => inner.and_then(|r| Err(PoisonError::new(r))),
            Ok(_) => inner,
        }
    }

    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let mutex_lock = self.mutex.lock();
        let inner = self.inner.write();
        match mutex_lock {
            Err(_) => inner.and_then(|r| Err(PoisonError::new(r))),
            Ok(_) => inner,
        }
    }
}
