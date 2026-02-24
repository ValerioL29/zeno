//! Batch-semantics ownership boundary for atomic plain and guarded writes.
//! Cost: O(n + b) for low-level physical validation, where `n` is batch size and `b` is total serialized value bytes.
//! Allocator: Uses explicit allocators only for delegated planning scratch.

const std = @import("std");
const internal_batch_plan = @import("../internal/batch_plan.zig");
const types = @import("../types.zig");

/// Performs low-level physical validation for one plain batch before engine semantics apply it.
///
/// Time Complexity: O(n + b), where `n` is `writes.len` and `b` is total serialized value bytes.
///
/// Allocator: Uses `allocator` only for delegated planning scratch.
pub fn validate_plain_batch(allocator: std.mem.Allocator, writes: []const types.PutWrite) !usize {
    const plan = try internal_batch_plan.plan_put_batch(allocator, writes);
    return plan.write_count;
}
