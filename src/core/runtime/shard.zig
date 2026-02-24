//! Runtime-owned shard state for locks and long-lived in-memory bookkeeping.
//! Cost: O(1) initialization and teardown in the step 4 skeleton.
//! Allocator: Does not allocate in the step 4 skeleton.

const std = @import("std");

/// One shard of runtime state owned by the engine.
pub const Shard = struct {
    lock: std.Thread.RwLock = .{},
    ttl_entry_count: usize = 0,
    committed_batch_count: usize = 0,

    /// Initializes one shard-local runtime state container.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Must be called before the shard is shared across threads.
    pub fn init() Shard {
        return .{};
    }

    /// Releases shard-local runtime resources.
    ///
    /// Time Complexity: O(1) in the step 4 skeleton.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership.
    pub fn deinit(self: *Shard) void {
        self.* = undefined;
    }
};
