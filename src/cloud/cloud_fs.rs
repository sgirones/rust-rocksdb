use libc::c_int;
use std::pin::Pin;
use std::sync::Arc;

use crate::{env::EnvWrapper, ffi, CloudFileSystemOptions, Env, Error};

/// Cloud FileSystem.
#[derive(Clone)]
pub struct CloudFileSystem(pub(crate) Pin<Arc<CloudFileSystemWrapper>>);

pub(crate) struct CloudFileSystemWrapper {
    pub(crate) inner: *mut ffi::rocksdb_cloud_fs_t,
    pub(crate) opts: CloudFileSystemOptions,
}

unsafe impl Send for CloudFileSystemWrapper {}
unsafe impl Sync for CloudFileSystemWrapper {}

impl Drop for CloudFileSystemWrapper {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_cloud_fs_destroy(self.inner);
        }
    }
}

impl CloudFileSystem {
    pub fn new(opts: &CloudFileSystemOptions) -> Result<Self, Error> {
        let cloud_fs = Self::create_cloud_fs(&opts);
        if let Ok(cloud_fs) = cloud_fs {
            Ok(Self(Arc::pin(CloudFileSystemWrapper {
                inner: cloud_fs,
                opts: opts.clone(),
            })))
        } else {
            Err(Error::new("Could not create cloud file system".to_owned()))
        }
    }

    fn create_cloud_fs(
        opts: &CloudFileSystemOptions,
    ) -> Result<*mut ffi::rocksdb_cloud_fs_t, Error> {
        unsafe {
            let o = opts.0.lock().unwrap();
            let cloud_fs = ffi_try!(ffi::rocksdb_cloud_fs_create(o.inner, o.log_level as c_int));
            Ok(cloud_fs)
        }
    }

    pub fn create_cloud_env(&self) -> Result<Env, Error> {
        let a = self.clone();
        let a = a.0.inner;
        let env = unsafe { ffi::rocksdb_cloud_env_create(a) };

        if env.is_null() {
            Err(Error::new("Could not create cloud env".to_owned()))
        } else {
            Ok(Env(Arc::pin(EnvWrapper { inner: env })))
        }
    }

    pub fn opts(&self) -> &CloudFileSystemOptions {
        &self.0.opts
    }
}
