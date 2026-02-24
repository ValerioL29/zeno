//! Public checked-batch contract types for guarded atomic writes.
//! Cost: O(1) type metadata only.
//! Allocator: Does not allocate.

const PutWrite = @import("batch.zig").PutWrite;
const Value = @import("value.zig").Value;

/// One physical guard evaluated before a checked batch becomes visible.
pub const CheckedBatchGuard = union(enum) {
    key_exists: []const u8,
    key_not_exists: []const u8,
    key_value_equals: struct {
        key: []const u8,
        value: *const Value,
    },
};

/// Public checked-batch request with ordered writes plus physical guards.
pub const CheckedBatch = struct {
    writes: []const PutWrite,
    guards: []const CheckedBatchGuard = &.{},
};
