use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct HealthState {
    last_activity_secs: AtomicU64,
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            last_activity_secs: AtomicU64::new(now_secs()),
        }
    }

    pub fn record_activity(&self) {
        self.last_activity_secs
            .store(now_secs(), Ordering::Release);
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
