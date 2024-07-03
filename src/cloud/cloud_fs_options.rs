use std::sync::{Arc, Mutex};

use crate::ffi;

use super::cloud_bucket_options::CloudBucketOptions;
use super::kafka_log_options::KafkaLogOptions;

/// Cloud System options.
///
/// # Examples
///
/// ```
/// use rocksdb::{CloudFileSystemOptions, CloudFileSystem};
///
/// let mut opts = CloudFileSystemOptions::new("db-path");
/// opts.set_persistent_cache_path("db-path");
/// opts.set_persistent_cache_size(1);
///
/// let cloud_fs = CloudFileSystem::new(opts);
/// ```
#[derive(Clone)]
pub struct CloudFileSystemOptions(pub(crate) Arc<Mutex<CloudFileSystemOptionsWrapper>>);

pub struct CloudFileSystemOptionsWrapper {
    pub(crate) inner: *mut ffi::rocksdb_cloud_fs_options_t,
    pub(crate) persistent_cache_path: Option<String>,
    pub(crate) persistent_cache_size_gb: Option<usize>,
    pub(crate) log_level: crate::LogLevel,
}

unsafe impl Send for CloudFileSystemOptionsWrapper {}
unsafe impl Sync for CloudFileSystemOptionsWrapper {}

impl Drop for CloudFileSystemOptionsWrapper {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_cloud_fs_options_destroy(self.inner);
        }
    }
}

impl CloudFileSystemOptions {
    /// Set the source bucket for the cloud file system.
    pub fn set_src_bucket(&mut self, bucket: CloudBucketOptions) {
        unsafe {
            ffi::rocksdb_cloud_fs_options_set_src_bucket(
                self.0.lock().unwrap().inner,
                bucket.inner,
            );
        }
    }

    /// Set the destination bucket for the cloud file system.
    pub fn set_dst_bucket(&mut self, bucket: CloudBucketOptions) {
        unsafe {
            ffi::rocksdb_cloud_fs_options_set_dest_bucket(
                self.0.lock().unwrap().inner,
                bucket.inner,
            );
        }
    }

    // Enables or disables `keep_local_sst_files` option.
    //
    // If enabled, sst files are stored locally and uploaded to the cloud in
    // the background. On restart, all files from the cloud that are not present
    // locally are downloaded.
    //
    // If disabled, local sst files are created, uploaded to cloud immediately,
    // and local file is deleted. All reads are satisfied by fetching
    // data from the cloud.
    //
    // Default:  false
    pub fn set_keep_local_sst_files(&mut self, keep: bool) {
        unsafe {
            ffi::rocksdb_cloud_fs_options_set_keep_local_sst_files(
                self.0.lock().unwrap().inner,
                keep,
            );
        }
    }

    /// Set the kafka log options for the cloud file system.
    pub fn set_kafka_log(&mut self, kafka_log: &KafkaLogOptions) {
        unsafe {
            ffi::rocksdb_cloud_fs_options_set_kafka_log(
                self.0.lock().unwrap().inner,
                kafka_log.clone().0.inner,
            );
        }
    }

    pub fn set_persistent_cache_path(&mut self, path: &str) {
        self.0.lock().unwrap().persistent_cache_path = Some(path.to_owned());
    }

    // Set the size of the persistent cache in gigabytes.
    pub fn set_persistent_cache_size_gb(&mut self, size: usize) {
        self.0.lock().unwrap().persistent_cache_size_gb = Some(size);
    }

    pub fn set_log_level(&mut self, level: crate::LogLevel) {
        self.0.lock().unwrap().log_level = level;
    }

    pub fn persistent_cache_path(&self) -> Option<String> {
        self.0.lock().unwrap().persistent_cache_path.clone()
    }

    pub fn persistent_cache_size_gb(&self) -> Option<usize> {
        self.0.lock().unwrap().persistent_cache_size_gb
    }

    pub fn log_level(&self) -> crate::LogLevel {
        self.0.lock().unwrap().log_level
    }
}

impl Default for CloudFileSystemOptions {
    fn default() -> Self {
        unsafe {
            let opts = ffi::rocksdb_cloud_fs_options_create();
            assert!(!opts.is_null(), "Could not create RocksDB options");

            Self(Arc::new(Mutex::new(CloudFileSystemOptionsWrapper {
                inner: opts,
                persistent_cache_path: None,
                persistent_cache_size_gb: None,
                log_level: crate::LogLevel::Info,
            })))
        }
    }
}
