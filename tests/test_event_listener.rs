// Copyright 2020 Tyler Neely
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

mod util;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use rust_rocksdb::event_listener::{EventListener, EventListenerExt, FlushJobInfo};
use rust_rocksdb::{Options, DB};
use util::DBPath;

struct FlushListener {
    column_family_name: String,
    count: AtomicU32,
}

impl EventListener for FlushListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        assert_eq!(self.column_family_name, info.cf_name);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
pub fn test_event_listener() {
    const PATH_PREFIX: &str = "_rust_rocksdb_cp_single_";

    let db_path = DBPath::new(&format!("{PATH_PREFIX}db1"));

    let mut opts = Options::default();
    opts.create_if_missing(true);

    let listener = Arc::new(FlushListener {
        column_family_name: "default".to_owned(),
        count: 0.into(),
    });
    opts.add_event_listener(listener.clone());
    let db = DB::open(&opts, &db_path).unwrap();

    db.put(b"k1", b"v1").unwrap();
    db.flush().unwrap();
    assert_eq!(1, listener.count.load(Ordering::Relaxed));
    db.put(b"k2", b"v2").unwrap();
    db.flush().unwrap();
    assert_eq!(2, listener.count.load(Ordering::Relaxed));
    db.put(b"k3", b"v3").unwrap();
    db.flush().unwrap();
    assert_eq!(3, listener.count.load(Ordering::Relaxed));
    db.put(b"k4", b"v4").unwrap();
    db.flush().unwrap();
    db.flush().unwrap();
    db.flush().unwrap();
    db.flush().unwrap();
    assert_eq!(4, listener.count.load(Ordering::Relaxed));
}
