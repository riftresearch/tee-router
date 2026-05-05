use chrono::DateTime;
use chrono::Utc;

// Use mock_instant's global time when the `mock-time` feature is enabled.
// Otherwise, fall back to the real system clock.
#[cfg(feature = "mock-time")]
use mock_instant::global::{SystemTime, UNIX_EPOCH};

#[cfg(not(feature = "mock-time"))]
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> DateTime<Utc> {
    #[cfg(all(feature = "mock-time", not(test), not(debug_assertions)))]
    compile_error!("mock-time feature must not be enabled in production builds");

    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return DateTime::from_timestamp(0, 0).unwrap_or(DateTime::<Utc>::MIN_UTC);
    };
    let seconds = i64::try_from(now.as_secs()).unwrap_or(i64::MAX);
    DateTime::from_timestamp(seconds, now.subsec_nanos()).unwrap_or(DateTime::<Utc>::MAX_UTC)
}
