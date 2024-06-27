use std::pin::Pin;
use std::{ffi::CStr, sync::Arc};

use crate::{ffi, ffi_util::CStrLike};

#[derive(Clone)]
pub struct KafkaLogOptions(pub(crate) Pin<Arc<KafkaLogOptionsWrapper>>);

/// Cloud Bucket options.
pub struct KafkaLogOptionsWrapper {
    pub(crate) inner: *mut ffi::rocksdb_cloud_kafka_log_options_t,
}

const DEFAULT_ENV_PREFIX: &str = "ROCKSDB_CLOUD_KAFKA_LOG";

unsafe impl Send for KafkaLogOptionsWrapper {}
unsafe impl Sync for KafkaLogOptionsWrapper {}

impl Drop for KafkaLogOptionsWrapper {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_cloud_kafka_log_options_destroy(self.inner);
        }
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
            let ptr = ffi::rocksdb_cloud_kafka_log_options_get_broker_list(self.0.inner);
            String::from_utf8_lossy(CStr::from_ptr(ptr).to_bytes()).to_string()
        }
    }
    pub fn set_broker_list(&mut self, name: impl CStrLike) {
        let name = name.into_c_string().unwrap();
        unsafe {
            ffi::rocksdb_cloud_kafka_log_options_set_broker_list(self.0.inner, name.as_ptr());
        }
    }
    pub fn set_api_version_request(&mut self, enabled: bool) {
        unsafe {
            ffi::rocksdb_cloud_kafka_log_options_set_api_version_request(self.0.inner, enabled);
        }
    }
    pub fn is_valid(&self) -> bool {
        unsafe { ffi::rocksdb_cloud_kafka_log_options_is_valid(self.0.inner) }
    }
}

impl Default for KafkaLogOptions {
    fn default() -> Self {
        unsafe {
            let opts = ffi::rocksdb_cloud_kafka_log_options_create();

            if opts.is_null() {
                panic!("Could not create RocksDB Cloud Kafka Log options");
            };

            Self(Arc::pin(KafkaLogOptionsWrapper { inner: opts })).read_from_env(DEFAULT_ENV_PREFIX)
        }
    }
}
