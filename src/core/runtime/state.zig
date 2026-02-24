//! Runtime-owned database state for shards, visibility coordination, and local counters.
//! Cost: O(s) initialization and teardown over the shard count.
//! Allocator: Uses explicit allocators only for owning the enclosing engine handle; this state itself does not allocate in step 4.

const std = @import("std");
const runtime_shard = @import("shard.zig");
const runtime_visibility = @import("visibility.zig");

/// Number of shards in the runtime execution state.
pub const NUM_SHARDS: usize = 64;

/// Runtime counters kept local to engine state during the migration.
pub const RuntimeCounters = struct {
    ops_put_total: std.atomic.Value(u64),
    ops_get_total: std.atomic.Value(u64),
    ops_delete_total: std.atomic.Value(u64),
    ops_scan_total: std.atomic.Value(u64),
    ops_expire_total: std.atomic.Value(u64),

    fn init() RuntimeCounters {
        return .{
            .ops_put_total = std.atomic.Value(u64).init(0),
            .ops_get_total = std.atomic.Value(u64).init(0),
            .ops_delete_total = std.atomic.Value(u64).init(0),
            .ops_scan_total = std.atomic.Value(u64).init(0),
            .ops_expire_total = std.atomic.Value(u64).init(0),
        };
    }
};

/// Full database runtime state, including shards, visibility coordination, and local counters.
pub const DatabaseState = struct {
    base_allocator: std.mem.Allocator,
    visibility_gate: runtime_visibility.VisibilityGate,
    snapshot_path: ?[]const u8 = null,
    shards: [NUM_SHARDS]runtime_shard.Shard,
    counters: RuntimeCounters,

    /// Initializes runtime state for one engine handle.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate in the step 4 skeleton.
    ///
    /// Thread Safety: Must be called before the state is shared across threads.
    pub fn init(base_allocator: std.mem.Allocator, snapshot_path: ?[]const u8) DatabaseState {
        var state = DatabaseState{
            .base_allocator = base_allocator,
            .visibility_gate = .{},
            .snapshot_path = snapshot_path,
            .shards = undefined,
            .counters = RuntimeCounters.init(),
        };
        for (&state.shards) |*shard| {
            shard.* = runtime_shard.Shard.init();
        }
        return state;
    }

    /// Releases runtime state owned by one engine handle.
    ///
    /// Time Complexity: O(s), where `s` is `NUM_SHARDS`.
    ///
    /// Allocator: Does not allocate in the step 4 skeleton.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the enclosing engine handle.
    pub fn deinit(self: *DatabaseState) void {
        for (&self.shards) |*shard| {
            shard.deinit();
        }
        self.* = undefined;
    }
};
