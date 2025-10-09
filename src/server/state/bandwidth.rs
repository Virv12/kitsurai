use std::sync::atomic::{AtomicI64, Ordering};

/// Holds the current available _"bandwidth"_.
///
/// Strictly speaking it has no unit and its value can be set semi-arbitrarily.<br>
/// In practice the unit chosen is the smallest amount that can be allocated by a table.
static BANDWIDTH: AtomicI64 = AtomicI64::new(0);

/// Sets the initial bandwidth as given by configuration.
pub fn init(initial: u64) {
    BANDWIDTH.store(initial as i64, Ordering::Relaxed);
    log::debug!("initialized with {}", BANDWIDTH.load(Ordering::Relaxed));
}

/// Allocates _"bandwidth"_ for a table.
///
/// May return less than requested if there is not enough available.
pub fn alloc(request: u64) -> u64 {
    let request = request as i64;
    let available = BANDWIDTH.fetch_sub(request, Ordering::Relaxed);
    let proposed = available.min(request).max(0);
    BANDWIDTH.fetch_add(request - proposed, Ordering::Relaxed);
    log::debug!(
        "requested {request}, alloc {proposed}, total after {}",
        BANDWIDTH.load(Ordering::Relaxed)
    );
    proposed as u64
}

/// Frees _"bandwidth"_ previously allocated by a table.
pub fn free(allocated: u64) {
    BANDWIDTH.fetch_add(allocated as i64, Ordering::Relaxed);
    log::debug!(
        "freed {allocated}, total after {}",
        BANDWIDTH.load(Ordering::Relaxed)
    );
}
