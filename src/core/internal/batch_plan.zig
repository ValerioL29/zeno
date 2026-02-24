//! Internal batch planning helpers for physical validation and deterministic ordering.
//! Cost: O(n + b), where `n` is batch size and `b` is total serialized value bytes measured during planning.
//! Allocator: Uses explicit allocators only for temporary serialization scratch.

const std = @import("std");
const batch = @import("../types/batch.zig");
const codec = @import("codec.zig");
const mutate = @import("mutate.zig");

/// Low-level batch planning error set.
pub const BatchPlanError = mutate.MutationError || error{
    ValueTooLarge,
    MaxDepthExceeded,
    OutOfMemory,
};

/// Summary of one validated batch before engine-owned apply semantics run.
pub const BatchPlan = struct {
    write_count: usize,
};

/// Validates one batch and returns a low-level planning summary.
///
/// Time Complexity: O(n + b), where `n` is `writes.len` and `b` is total serialized value bytes measured during validation.
///
/// Allocator: Uses `allocator` only for temporary serialization scratch growth.
pub fn plan_put_batch(allocator: std.mem.Allocator, writes: []const batch.PutWrite) BatchPlanError!BatchPlan {
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);

    for (writes) |write| {
        try mutate.validate_key(write.key);
        scratch.clearRetainingCapacity();
        try codec.serialize_value(allocator, write.value, &scratch, 0);
        if (scratch.items.len > codec.MAX_VAL_LEN) return error.ValueTooLarge;
    }

    return .{ .write_count = writes.len };
}

test "plan_put_batch validates keys and values without owning semantics" {
    const testing = std.testing;

    const value = @import("../types/value.zig").Value{ .integer = 1 };
    const plan = try plan_put_batch(testing.allocator, &.{
        .{ .key = "a", .value = &value },
        .{ .key = "b", .value = &value },
    });
    try testing.expectEqual(@as(usize, 2), plan.write_count);
}
