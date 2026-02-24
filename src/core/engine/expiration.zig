//! Expiration-semantics ownership boundary for TTL-visible behavior.
//! Cost: O(1) delegated TTL bookkeeping only in the skeleton.
//! Allocator: Does not allocate.

const internal_ttl_index = @import("../internal/ttl_index.zig");

/// Returns whether one TTL timestamp should be treated as expired.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
pub fn is_expired(expire_at: i64, now: i64) bool {
    return internal_ttl_index.is_expired(expire_at, now);
}
