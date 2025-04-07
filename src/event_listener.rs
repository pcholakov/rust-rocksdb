// Copyright 2023
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::sync::Arc;

use libc::c_void;

use crate::ffi;
use crate::ffi::{
    rocksdb_compactionjobinfo_t, rocksdb_flushinfo_t, rocksdb_memtableinfo_t, rocksdb_t,
    rocksdb_tablefilecreationbriefinfo_t, rocksdb_tablefilecreationinfo_t,
    rocksdb_tablefiledeletioninfo_t, rocksdb_writestallinfo_t,
};
use crate::ffi_util::from_cstr;

/// Trait for RocksDB event listeners
pub trait EventListener: Send + Sync {
    /// Called whenever a registered RocksDB finishes flushing a file
    fn on_flush_completed(&self, _flush_job_info: &FlushJobInfo) {}

    /// Called before a RocksDB starts to flush memtables
    fn on_flush_begin(&self, _cf_name: &str) {}

    /// Called whenever a SST file is deleted
    fn on_table_file_deleted(&self, _file_path: &str) {}

    /// Called before RocksDB starts to compact
    fn on_compaction_begin(&self, _cf_name: &str) {}

    /// Called whenever RocksDB finishes compacting a file
    fn on_compaction_completed(&self, _cf_name: &str) {}

    /// Called whenever a SST file is created
    fn on_table_file_created(&self, _file_path: &str) {}

    /// Called before a SST file is being created
    fn on_table_file_creation_started(&self, _file_path: &str) {}

    /// Called before a memtable is made immutable
    fn on_memtable_sealed(&self, _cf_name: &str) {}

    /// Called whenever a stall condition changes
    fn on_stall_conditions_changed(&self, _cf_name: &str) {}

    // More callback methods could be added here for other event types
}

pub struct EventListenerHandle {
    _inner: Arc<dyn EventListener>,
}

pub struct EventListenerCallback {
    inner: Arc<dyn EventListener>,
}

/// Information about a flush job.
pub struct FlushJobInfo {
    /// The name of the column family.
    pub cf_name: String,
    // /// The reason for the flush.
    // pub flush_reason: FlushReason,
}

/// Reasons for a flush operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// Flushing was triggered by the user calling DB::Flush().
    Manual,
    /// Flushing was triggered by the memtable being full.
    WriteBufferFull,
    /// Flushing was triggered by the DB reaching the designated size.
    WriteBufferManager,
    /// Flushing was triggered by the close of a column family or DB.
    Shutdown,
    /// Flushing was triggered by the external file ingestion.
    ExternalFileIngestion,
    /// Flushing was triggered by starting a new WAL.
    WalFull,
    /// Automatic flush to avoid memtable being too big.
    GetLiveFiles,
    /// Flushing was triggered to prepare for point-in-time consistency.
    FlushPointRelease,
    /// Flushing was triggered to prepare for atomically swapping column families.
    ColumnFamilySwitch,
    /// Automatic flush to reclaim the memory usage of memtable when it is high.
    ReclaimMemory,
}

impl EventListenerCallback {
    pub fn new(listener: Arc<dyn EventListener>) -> Self {
        Self { inner: listener }
    }

    pub unsafe extern "C" fn destructor_callback(raw_cb: *mut c_void) {
        drop(Box::from_raw(raw_cb as *mut Self));
    }

    pub unsafe extern "C" fn on_flush_completed_callback(
        raw_cb: *mut c_void,
        __db: *mut rocksdb_t,
        flush_info: *const rocksdb_flushinfo_t,
    ) {
        let flush_job_info = FlushJobInfo {
            cf_name: {
                // Get the column family name from the flush_info
                let cf_name_ptr = ffi::rocksdb_flushinfo_cf_name(flush_info);
                if !cf_name_ptr.is_null() {
                    from_cstr(cf_name_ptr)
                } else {
                    String::new()
                }
            },
            // flush_reason: {
            //     // Convert the C flush reason enum to our Rust enum
            //     let reason = ffi::rocksdb_flushinfo_flush_reason(flush_info);
            //     match reason {
            //         0 => FlushReason::Manual,
            //         1 => FlushReason::WriteBufferFull,
            //         2 => FlushReason::WriteBufferManager,
            //         3 => FlushReason::Shutdown,
            //         4 => FlushReason::ExternalFileIngestion,
            //         5 => FlushReason::WalFull,
            //         6 => FlushReason::GetLiveFiles,
            //         7 => FlushReason::FlushPointRelease,
            //         8 => FlushReason::ColumnFamilySwitch,
            //         9 => FlushReason::ReclaimMemory,
            //         _ => FlushReason::Manual, // default to Manual for unknown values
            //     }
            // },
        };

        let cb = &mut *(raw_cb as *mut Self);
        cb.inner.on_flush_completed(&flush_job_info);
    }

    pub unsafe extern "C" fn on_flush_begin_callback(
        raw_cb: *mut c_void,
        _db: *mut rocksdb_t,
        info: *const rocksdb_flushinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let cf_name = get_cf_name(info as *const c_void);

        cb.inner.on_flush_begin(&cf_name);
    }

    pub unsafe extern "C" fn on_table_file_deleted_callback(
        raw_cb: *mut c_void,
        info: *const rocksdb_tablefiledeletioninfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let file_path = get_file_path(info as *const c_void);

        cb.inner.on_table_file_deleted(&file_path);
    }

    pub unsafe extern "C" fn on_compaction_begin_callback(
        raw_cb: *mut c_void,
        _db: *mut rocksdb_t,
        info: *const rocksdb_compactionjobinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let cf_name = get_cf_name(info as *const c_void);

        cb.inner.on_compaction_begin(&cf_name);
    }

    pub unsafe extern "C" fn on_compaction_completed_callback(
        raw_cb: *mut c_void,
        _db: *mut rocksdb_t,
        info: *const rocksdb_compactionjobinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let cf_name = get_cf_name(info as *const c_void);

        cb.inner.on_compaction_completed(&cf_name);
    }

    pub unsafe extern "C" fn on_table_file_created_callback(
        raw_cb: *mut c_void,
        info: *const rocksdb_tablefilecreationinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let file_path = get_file_path(info as *const c_void);

        cb.inner.on_table_file_created(&file_path);
    }

    pub unsafe extern "C" fn on_table_file_creation_started_callback(
        raw_cb: *mut c_void,
        info: *const rocksdb_tablefilecreationbriefinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let file_path = get_file_path(info as *const c_void);

        cb.inner.on_table_file_creation_started(&file_path);
    }

    pub unsafe extern "C" fn on_memtable_sealed_callback(
        raw_cb: *mut c_void,
        info: *const rocksdb_memtableinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let cf_name = get_cf_name(info as *const c_void);

        cb.inner.on_memtable_sealed(&cf_name);
    }

    pub unsafe extern "C" fn on_stall_conditions_changed_callback(
        raw_cb: *mut c_void,
        info: *const rocksdb_writestallinfo_t,
    ) {
        let cb = &mut *(raw_cb as *mut Self);
        let cf_name = get_cf_name(info as *const c_void);

        cb.inner.on_stall_conditions_changed(&cf_name);
    }
}

unsafe fn get_cf_name(info: *const c_void) -> String {
    // Extract cf_name from the C struct
    // This is placeholder - actual implementation depends on the C struct
    String::from("cf_name")
}

unsafe fn get_file_path(info: *const c_void) -> String {
    // Extract file_path from the C struct
    // This is placeholder - actual implementation depends on the C struct
    String::from("file_path")
}

pub trait EventListenerExt {
    fn add_event_listener(&mut self, listener: Arc<dyn EventListener>) -> EventListenerHandle;
}

impl EventListenerExt for crate::Options {
    fn add_event_listener(&mut self, listener: Arc<dyn EventListener>) -> EventListenerHandle {
        unsafe {
            let cb = Box::new(EventListenerCallback::new(listener.clone()));
            let cb_ptr = Box::into_raw(cb) as *mut c_void;

            // Create the rocksdb_event_listener_t
            let event_listener = ffi::rocksdb_event_listener_create(
                cb_ptr,
                Some(EventListenerCallback::destructor_callback),
            );

            // Set the callback functions with proper type casting
            ffi::rocksdb_event_listener_set_flush_completed(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_flush_completed_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *mut rocksdb_t,
                            *const rocksdb_flushinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_flush_begin(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_flush_begin_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *mut rocksdb_t,
                            *const rocksdb_flushinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_table_file_deleted(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_table_file_deleted_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *const rocksdb_tablefiledeletioninfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_compaction_begin(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_compaction_begin_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *mut rocksdb_t,
                            *const rocksdb_compactionjobinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_compaction_completed(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_compaction_completed_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *mut rocksdb_t,
                            *const rocksdb_compactionjobinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_table_file_created(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_table_file_created_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *const rocksdb_tablefilecreationinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_table_file_creation_started(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_table_file_creation_started_callback
                        as unsafe extern "C" fn(
                            *mut c_void,
                            *const rocksdb_tablefilecreationbriefinfo_t,
                        ),
                ),
            );

            ffi::rocksdb_event_listener_set_memtable_sealed(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_memtable_sealed_callback
                        as unsafe extern "C" fn(*mut c_void, *const rocksdb_memtableinfo_t),
                ),
            );

            ffi::rocksdb_event_listener_set_stall_conditions_changed(
                event_listener,
                std::mem::transmute(
                    EventListenerCallback::on_stall_conditions_changed_callback
                        as unsafe extern "C" fn(*mut c_void, *const rocksdb_writestallinfo_t),
                ),
            );

            // Add the event listener to the options
            ffi::rocksdb_options_add_event_listener(self.inner, event_listener);

            EventListenerHandle { _inner: listener }
        }
    }
}
