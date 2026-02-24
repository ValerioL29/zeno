//! Public request types for zeno-core batch operations.
//! Cost: O(1) type metadata only.
//! Allocator: Does not allocate.

const Value = @import("value.zig").Value;

/// One plain key/value write request for `apply_batch`.
pub const PutWrite = struct {
    key: []const u8,
    value: *const Value,
};
