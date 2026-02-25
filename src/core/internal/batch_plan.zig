//! Internal batch planning helpers for physical validation and deterministic ordering.
//! Cost: O(n + b), where `n` is batch size and `b` is total serialized value bytes measured during planning.
//! Allocator: Uses explicit allocators only for temporary serialization scratch.

const std = @import("std");
const batch = @import("../types/batch.zig");
const codec = @import("codec.zig");
const mutate = @import("mutate.zig");
const runtime_shard = @import("../runtime/shard.zig");

/// Low-level batch planning error set.
pub const BatchPlanError = mutate.MutationError || error{
    ValueTooLarge,
    MaxDepthExceeded,
    OutOfMemory,
};

/// Summary of one validated batch before engine-owned apply semantics run.
pub const BatchPlan = struct {
    allocator: std.mem.Allocator,
    writes: []PlannedWrite,

    /// Releases planner-owned metadata.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Frees the planner-owned write array through `allocator`.
    pub fn deinit(self: *BatchPlan) void {
        self.allocator.free(self.writes);
        self.* = undefined;
    }
};

/// One validated survivor write kept for deterministic batch apply.
pub const PlannedWrite = struct {
    key: []const u8,
    value: *const @import("../types/value.zig").Value,
    shard_idx: usize,
};

/// Validates one batch and returns a low-level planning summary.
///
/// Time Complexity: O(n + b), where `n` is `writes.len` and `b` is total serialized value bytes measured during validation.
///
/// Allocator: Uses `allocator` for temporary serializer scratch, survivor tracking, and the returned write array.
pub fn plan_put_batch(allocator: std.mem.Allocator, writes: []const batch.PutWrite) BatchPlanError!BatchPlan {
    var scratch = std.ArrayList(u8).empty;
    defer scratch.deinit(allocator);
    var planned_writes = std.ArrayList(PlannedWrite).empty;
    defer planned_writes.deinit(allocator);
    var planned_index_by_key = std.StringHashMapUnmanaged(usize){};
    defer planned_index_by_key.deinit(allocator);

    for (writes) |write| {
        try mutate.validate_key(write.key);
        scratch.clearRetainingCapacity();
        try codec.serialize_value(allocator, write.value, &scratch, 0);
        if (scratch.items.len > codec.MAX_VAL_LEN) return error.ValueTooLarge;

        if (planned_index_by_key.get(write.key)) |planned_index| {
            planned_writes.items[planned_index].value = write.value;
        } else {
            try planned_writes.append(allocator, .{
                .key = write.key,
                .value = write.value,
                .shard_idx = runtime_shard.get_shard_index(write.key),
            });
            try planned_index_by_key.put(allocator, write.key, planned_writes.items.len - 1);
        }
    }

    const owned_writes = try planned_writes.toOwnedSlice(allocator);
    errdefer allocator.free(owned_writes);

    return .{
        .allocator = allocator,
        .writes = owned_writes,
    };
}

test "plan_put_batch keeps final values in first-declared key order" {
    const testing = std.testing;

    const one = @import("../types/value.zig").Value{ .integer = 1 };
    const two = @import("../types/value.zig").Value{ .integer = 2 };
    var plan = try plan_put_batch(testing.allocator, &.{
        .{ .key = "a", .value = &one },
        .{ .key = "b", .value = &one },
        .{ .key = "a", .value = &two },
    });
    defer plan.deinit();

    try testing.expectEqual(@as(usize, 2), plan.writes.len);
    try testing.expectEqualStrings("a", plan.writes[0].key);
    try testing.expectEqual(@as(i64, 2), plan.writes[0].value.*.integer);
    try testing.expectEqualStrings("b", plan.writes[1].key);
}
