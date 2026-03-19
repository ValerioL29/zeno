//! Read-semantics ownership boundary for point reads and read-view-aware reads.
//! Cost: O(n + k + v) for point reads, where `n` is key length for shard routing, `k` is ART lookup work, and `v` is cloned value size.
//! Allocator: Uses explicit allocators only for returning owned cloned values to callers.

const std = @import("std");
const expiration = @import("expiration.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_shard = @import("../runtime/shard.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");

/// Clones the current plain value for `key` while relying on an already-held visibility window.
///
/// Time Complexity: O(k + v), where `k` is ART lookup work and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Requires a surrounding visibility window and acquires the selected shard's shared lock while reading and cloning the stored value.
fn clone_plain_value_no_visibility(
    state: *const runtime_state.DatabaseState,
    shard: *runtime_shard.Shard,
    allocator: std.mem.Allocator,
    key: []const u8,
) error_mod.EngineError!?types.Value {
    _ = state;

    while (true) {
        const v1 = shard.seq.load(.acquire);
        if ((v1 & 1) != 0) {
            std.atomic.spinLoopHint();
            continue;
        }

        const stored = shard.tree.lookup(key);

        const v2 = shard.seq.load(.acquire);
        if (v1 != v2) {
            std.atomic.spinLoopHint();
            continue;
        }

        const value_ptr = stored orelse return null;

        if (shard.has_ttl_entries) {
            const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return try value_ptr.clone(allocator);
            const now = runtime_shard.unix_now();
            if (expiration.is_expired(stored_expire_at, now)) return null;
        }

        return try value_ptr.clone(allocator);
    }
}

/// Checks whether `key` is present and TTL-visible without allocating.
///
/// Time Complexity: O(k), where `k` is ART lookup work.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Requires a surrounding visibility window and acquires the selected
/// shard's shared lock while reading.
fn check_key_exists_no_visibility(
    state: *const runtime_state.DatabaseState,
    shard: *runtime_shard.Shard,
    key: []const u8,
) bool {
    _ = state;

    while (true) {
        const v1 = shard.seq.load(.acquire);
        if ((v1 & 1) != 0) {
            std.atomic.spinLoopHint();
            continue;
        }

        const stored = shard.tree.lookup(key);

        const v2 = shard.seq.load(.acquire);
        if (v1 != v2) {
            std.atomic.spinLoopHint();
            continue;
        }

        if (stored == null) return false;

        if (shard.has_ttl_entries) {
            const stored_expire_at = internal_ttl_index.get_expire_at(shard, key) orelse return true;
            const now = runtime_shard.unix_now();
            if (expiration.is_expired(stored_expire_at, now)) return false;
        }

        return true;
    }
}

/// Opens one consistent read window over the current visible engine state.
///
/// Time Complexity: O(s), where `s` is the shard count.
///
/// Allocator: May allocate through the read-view token registry.
///
/// Ownership: Returns a handle that borrows the runtime state and visibility gates until `deinit` is called.
///
/// Thread Safety: Acquires the shared side of all shard-local visibility gates and keeps them held for the lifetime of the returned `ReadView`.
pub fn read_view(state: *const runtime_state.DatabaseState) error_mod.EngineError!types.ReadView {
    state.lock_all_shards_shared();

    return types.ReadView.init(
        state,
        @constCast(&state.active_read_views),
        runtime_shard.unix_now(),
    ) catch {
        state.unlock_all_shards_shared();
        return error.OutOfMemory;
    };
}

/// Clones the current plain value for `key` under the selected shard shared lock.
///
/// Time Complexity: O(n + k + v), where `n` is `key.len` for shard routing, `k` is ART lookup work, and `v` is the size of the cloned value tree.
///
/// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
///
/// Ownership: Returns a value owned by the caller when non-null. The caller must later call `deinit` with the same allocator.
///
/// Thread Safety: Acquires only the selected shard's shared lock through `clone_plain_value_no_visibility`.
pub fn get(state: *const runtime_state.DatabaseState, allocator: std.mem.Allocator, key: []const u8) error_mod.EngineError!?types.Value {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = @constCast(&state.shards[shard_idx]);

    const value = try clone_plain_value_no_visibility(state, shard, allocator, key);
    state.record_operation(.get, 1);
    return value;
}

/// Returns whether `key` is present and TTL-visible in the engine.
///
/// Time Complexity: O(n + k), where `n` is `key.len` for shard routing and `k` is ART lookup work.
///
/// Allocator: Does not allocate.
///
/// Thread Safety: Lock-free via the shard seqlock; safe for concurrent use with
/// point writes and scans.
pub fn exists(state: *const runtime_state.DatabaseState, key: []const u8) error_mod.EngineError!bool {
    internal_mutate.validate_key(key) catch |err| switch (err) {
        error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
    };

    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = @constCast(&state.shards[shard_idx]);

    const found = check_key_exists_no_visibility(state, shard, key);
    state.record_operation(.get, 1);
    return found;
}

/// Reads multiple keys in a single call, grouping by shard for efficiency.
///
/// Uses counting-sort bucketing to route each key to its shard in O(N) total,
/// then reads each shard exactly once without revisiting keys from other shards.
///
/// Time Complexity: O(N + s + v), where `N` is `keys.len`, `s` is the number
/// of shards (256, fixed), and `v` is the total size of all cloned values.
/// The seqlock retry cost is bounded by writer contention on each shard, not
/// by the number of keys.
///
/// Allocator: Allocates the result slice, a flat key-order index array of size N,
/// and all present cloned values through `allocator`.
///
/// Ownership: Returns a `GetManyResult` that owns all cloned values. The caller
/// must call `result.deinit(allocator)` when done.
///
/// Thread Safety: Lock-free via the shard seqlock per shard; safe for concurrent
/// use with point writes and scans.
pub fn getMany(
    state: *const runtime_state.DatabaseState,
    allocator: std.mem.Allocator,
    keys: []const []const u8,
) error_mod.EngineError!types.GetManyResult {

    // Validate all keys before allocating anything
    for (keys) |key| {
        internal_mutate.validate_key(key) catch |err| switch (err) {
            error.EmptyKey, error.KeyTooLarge => return error.KeyTooLarge,
        };
    }

    const values = allocator.alloc(?types.Value, keys.len) catch return error.OutOfMemory;
    errdefer allocator.free(values);
    @memset(values, null);

    if (keys.len == 0) {
        state.record_operation(.get, 0);
        return .{ .values = values };
    }

    // Flat array of original key indices, sorted into buckets per-shard
    const key_order = allocator.alloc(usize, keys.len) catch return error.OutOfMemory;
    defer allocator.free(key_order);

    const cloned_indices = allocator.alloc(usize, keys.len) catch return error.OutOfMemory;
    defer allocator.free(cloned_indices);

    // Track cloned indices for errdefer cleanup
    var cloned_count: usize = 0;
    errdefer {
        for (cloned_indices[0..cloned_count]) |index| {
            if (values[index]) |*val| val.deinit(allocator);
        }
    }

    // Count keys per shard
    var shard_counts = [_]u32{0} ** runtime_state.NUM_SHARDS;
    for (keys) |key| {
        shard_counts[runtime_shard.get_shard_index(key)] += 1;
    }

    // Prefix sums, bucket start positions
    var shard_starts = [_]u32{0} ** runtime_state.NUM_SHARDS;
    var running: u32 = 0;
    for (0..runtime_state.NUM_SHARDS) |s| {
        shard_starts[s] = running;
        running += shard_counts[s];
    }

    // Fill key_order — place each key's original index into its shard bucket
    var cursors = shard_starts;
    for (keys, 0..) |key, i| {
        const s = runtime_shard.get_shard_index(key);
        key_order[cursors[s]] = i;
        cursors[s] += 1;
    }

    // Process each touched shard exactly once, reading only its keys
    for (0..runtime_state.NUM_SHARDS) |shard_idx| {
        if (shard_counts[shard_idx] == 0) continue;

        const shard = @constCast(&state.shards[shard_idx]);
        const start = shard_starts[shard_idx];
        const end = start + shard_counts[shard_idx];

        for (key_order[start..end]) |original_idx| {
            values[original_idx] = try clone_plain_value_no_visibility(
                state,
                shard,
                allocator,
                keys[original_idx],
            );
            cloned_indices[cloned_count] = original_idx;
            cloned_count += 1;
        }
    }

    state.record_operation(.get, @intCast(keys.len));
    return .{ .values = values };
}
