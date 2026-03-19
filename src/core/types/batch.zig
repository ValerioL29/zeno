//! Public request types for zeno-core batch operations.
//! Cost: O(1) type metadata only.
//! Allocator: Does not allocate.

const std = @import("std");
const Value = @import("value.zig").Value;

/// One plain key/value write request for `apply_batch`.
///
/// Ownership:
/// - `key` is borrowed for the duration of the call that consumes this write.
/// - `value` is borrowed for the duration of the call that consumes this write.
/// - Callers must keep both slices and pointed values valid and immutable until the consuming batch call returns.
pub const PutWrite = struct {
    key: []const u8,
    value: *const Value,
};

/// Owned result container for a `get_many` batch read.
///
/// `values[i]` corresponds to the key at index `i` in the original request slice.
/// A `null` entry means the key was absent or TTL-expired at read time.
///
/// Ownership: All non-null `Value` entries are owned by this container and must be
/// released by calling `deinit`.
pub const GetManyResult = struct {
    values: []?Value,

    /// Releases all owned value storage and the result slice itself.
    ///
    /// Time Complexity: O(n + v), where `n` is `values.len` and `v` is total
    /// nested value bytes across all present entries.
    ///
    /// Allocator: Frees through `allocator`, which must be the same allocator
    /// used to produce this result.
    pub fn deinit(self: *GetManyResult, allocator: std.mem.Allocator) void {
        for (self.values) |*entry| {
            if (entry.*) |*val| val.deinit(allocator);
        }
        allocator.free(self.values);
        self.* = undefined;
    }
};
