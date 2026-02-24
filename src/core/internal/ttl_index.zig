//! Internal TTL bookkeeping helpers shared by runtime and expiration paths.
//! Cost: O(1) timestamp comparisons and counter updates.
//! Allocator: Does not allocate.

/// Returns whether `expire_at` is at or before `now`.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn is_expired(expire_at: i64, now: i64) bool {
    return expire_at <= now;
}

/// Increments one TTL-entry counter after a successful insert.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn record_insert(ttl_entry_count: *usize) void {
    ttl_entry_count.* += 1;
}

/// Decrements one TTL-entry counter after a successful delete.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn record_delete(ttl_entry_count: *usize) void {
    if (ttl_entry_count.* > 0) ttl_entry_count.* -= 1;
}

test "record_delete does not underflow" {
    const testing = @import("std").testing;

    var count: usize = 0;
    record_delete(&count);
    try testing.expectEqual(@as(usize, 0), count);
}
