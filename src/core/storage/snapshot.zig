//! Storage-owned snapshot contract for checkpoint write and recovery load.
//! Cost: O(1) in the skeleton; durable snapshot I/O is not implemented yet.
//! Allocator: Does not allocate in the skeleton; load and write return `error.NotImplemented`.

const std = @import("std");
const runtime_state = @import("../runtime/state.zig");

/// Metadata returned after a successful snapshot write.
pub const SnapshotWriteResult = struct {
    checkpoint_lsn: u64,
    records_written: usize,
};

/// Metadata returned after a successful snapshot load.
pub const SnapshotLoadResult = struct {
    checkpoint_lsn: u64,
    records_loaded: usize,
};

/// Writes a consistent snapshot of runtime state to `path`.
///
/// Time Complexity: O(1) in the skeleton.
///
/// Allocator: Does not allocate in the skeleton; returns `error.NotImplemented`.
pub fn write(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    path: []const u8,
    checkpoint_lsn: u64,
) !SnapshotWriteResult {
    _ = state;
    _ = allocator;
    _ = path;
    _ = checkpoint_lsn;
    return error.NotImplemented;
}

/// Loads snapshot state from `path` into `state`.
///
/// Time Complexity: O(1) in the skeleton.
///
/// Allocator: Does not allocate in the skeleton; returns `error.NotImplemented`.
pub fn load(
    state: *runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    path: []const u8,
) !SnapshotLoadResult {
    _ = state;
    _ = allocator;
    _ = path;
    return error.NotImplemented;
}
