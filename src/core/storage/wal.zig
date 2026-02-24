//! Storage-owned WAL contract for lifecycle orchestration and replay wiring.
//! Cost: O(1) for handle bookkeeping in the skeleton; durable I/O is not implemented yet.
//! Allocator: Does not allocate in the skeleton; `open` returns `error.NotImplemented`.

const std = @import("std");
const Value = @import("../types/value.zig").Value;

/// Durability policy for WAL fsync behavior.
pub const FsyncMode = enum {
    always,
    none,
    batched_async,
};

/// WAL open/runtime configuration with replay floor and fsync policy.
pub const WalOptions = struct {
    fsync_mode: FsyncMode = .always,
    fsync_interval_ms: u32 = 2,
    min_lsn: u64 = 0,
};

/// Replay callback table used by WAL recovery.
pub const ReplayApplier = struct {
    ctx: *anyopaque,
    put: *const fn (ctx: *anyopaque, key: []const u8, value: *const Value) anyerror!void,
    delete: *const fn (ctx: *anyopaque, key: []const u8) anyerror!void,
    expire: *const fn (ctx: *anyopaque, key: []const u8, expire_at_sec: i64) anyerror!void,
    prune_shard: *const fn (ctx: *anyopaque, shard_idx: u8, prefix: []const u8) anyerror!void,
};

/// Storage-owned WAL handle.
pub const Wal = struct {
    options: WalOptions,
    path: []const u8,

    /// Flushes buffered WAL state to stable storage.
    ///
    /// Time Complexity: O(1) in the skeleton.
    ///
    /// Allocator: Does not allocate.
    pub fn fsync(self: *Wal) !void {
        _ = self;
        return error.NotImplemented;
    }

    /// Closes the WAL handle and releases storage-owned resources.
    ///
    /// Time Complexity: O(1) in the skeleton.
    ///
    /// Allocator: Does not allocate.
    pub fn close(self: *Wal) void {
        _ = self;
    }
};

/// Opens a WAL handle and replays persisted records through `applier`.
///
/// Time Complexity: O(1) in the skeleton.
///
/// Allocator: Does not allocate in the skeleton; returns `error.NotImplemented`.
pub fn open(
    path: []const u8,
    options: WalOptions,
    applier: ReplayApplier,
    allocator: std.mem.Allocator,
) !Wal {
    _ = path;
    _ = options;
    _ = applier;
    _ = allocator;
    return error.NotImplemented;
}
