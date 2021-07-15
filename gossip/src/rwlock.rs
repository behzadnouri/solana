pub use std::sync::{LockResult, RwLockReadGuard};
use std::{
    ops::{Deref, DerefMut},
    sync::{Mutex, MutexGuard, PoisonError},
};

#[derive(Default)]
pub struct RwLock<T> {
    inner: std::sync::RwLock<T>,
    mutex: Mutex<()>,
}

pub struct RwLockWriteGuard<'a, T: 'a> {
    inner: std::sync::RwLockWriteGuard<'a, T>,
    _mutex_guard: MutexGuard<'a, ()>,
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
            Ok(_) => inner,
            Err(_) => inner.and_then(|r| Err(PoisonError::new(r))),
        }
    }

    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let mutex_lock = self.mutex.lock();
        let inner = self.inner.write();
        let err = mutex_lock.is_err() || inner.is_err();
        let guard = RwLockWriteGuard {
            inner: inner.unwrap_or_else(|err| err.into_inner()),
            _mutex_guard: mutex_lock.unwrap_or_else(|err| err.into_inner()),
        };
        if err {
            Err(PoisonError::new(guard))
        } else {
            Ok(guard)
        }
    }
}

impl<'a, T: 'a> Deref for RwLockWriteGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<'a, T: 'a> DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}
