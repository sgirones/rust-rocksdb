use std::ffi::CStr;

use crate::{ffi, ffi_util::CStrLike};

/// Cloud Bucket options.
pub struct KafkaLogOptions {
    pub(crate) inner: *mut ffi::rocksdb_cloud_kafka_log_options_t,
}

const DEFAULT_ENV_PREFIX: &str = "ROCKSDB_CLOUD_KAFKA_LOG";

unsafe impl Send for KafkaLogOptions {}
unsafe impl Sync for KafkaLogOptions {}

impl Drop for KafkaLogOptions {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_cloud_kafka_log_options_destroy(self.inner);
        }
    }
}

impl Clone for KafkaLogOptions {
    fn clone(&self) -> Self {
        let inner = unsafe { ffi::rocksdb_cloud_kafka_log_options_create_copy(self.inner) };
        assert!(
            !inner.is_null(),
            "Could not copy RocksDB Cloud Kafka Log options"
        );

        Self { inner }
    }
}

impl KafkaLogOptions {
    pub fn read_from_env(&self, env_prefix: &str) -> Self {
        let mut result = self.clone();
        std::env::vars().for_each(|(key, value)| match key {
            _ if key == format!("{env_prefix}_BROKER_LIST") => result.set_broker_list(&value),
            _ => {}
        });

        result
    }
    pub fn get_broker_list(&self) -> String {
        unsafe {
            let ptr = ffi::rocksdb_cloud_kafka_log_options_get_broker_list(self.inner);
            String::from_utf8_lossy(CStr::from_ptr(ptr).to_bytes()).to_string()
        }
    }
    pub fn set_broker_list(&mut self, name: impl CStrLike) {
        let name = name.into_c_string().unwrap();
        unsafe {
            ffi::rocksdb_cloud_kafka_log_options_set_broker_list(self.inner, name.as_ptr());
        }
    }
    pub fn is_valid(&self) -> bool {
        unsafe { ffi::rocksdb_cloud_kafka_log_options_is_valid(self.inner) }
    }
}

impl Default for KafkaLogOptions {
    fn default() -> Self {
        let opts = unsafe { ffi::rocksdb_cloud_kafka_log_options_create() };

        if opts.is_null() {
            panic!("Could not create RocksDB Cloud Kafka Log options");
        };

        Self { inner: opts }.read_from_env(DEFAULT_ENV_PREFIX)
    }
}
