//! Lifecycle ownership boundary for engine open, create, close, and checkpoint work.
//! Time Complexity: O(s + n + r + e) for persistent open, where `s` is the shard count, `n` is snapshot load work, `r` is replayed WAL work, and `e` is recovery-time expired-key purge work.
//! Allocator: Uses explicit allocators for engine-handle ownership plus snapshot-load and WAL-replay scratch.

const std = @import("std");
const engine_db = @import("db.zig");
const error_mod = @import("error.zig");
const internal_mutate = @import("../internal/mutate.zig");
const internal_ttl_index = @import("../internal/ttl_index.zig");
const runtime_state = @import("../runtime/state.zig");
const storage_snapshot = @import("../storage/snapshot.zig");
const storage_wal = @import("../storage/wal.zig");
const runtime_shard = @import("../runtime/shard.zig");
const types = @import("../types.zig");

/// Creates an in-memory engine handle without persistence.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle from `allocator`.
pub fn create(allocator: std.mem.Allocator) error_mod.EngineError!*engine_db.Database {
    return create_with_snapshot_path(allocator, null);
}

/// Opens an engine handle and routes persistence work through storage-owned modules.
///
/// Time Complexity: O(s + n + r + e), where `s` is the runtime shard count, `n` is snapshot load work, `r` is replayed WAL record work, and `e` is post-recovery expired-key purge work.
///
/// Allocator: Allocates the engine handle from `allocator` and uses explicit allocator paths for snapshot load and WAL replay scratch when persistence is configured.
///
/// Thread Safety: Not thread-safe during open; recovery mutates runtime state before the database handle is published to callers.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) error_mod.EngineError!*engine_db.Database {
    var db = try create_with_snapshot_path(allocator, options.snapshot_path);
    errdefer db.close() catch unreachable;

    var snapshot_lsn: u64 = 0;
    if (options.snapshot_path) |snapshot_path| {
        if (storage_snapshot.load(&db.state, allocator, snapshot_path)) |result| {
            snapshot_lsn = result.checkpoint_lsn;
        } else |err| switch (err) {
            error.FileNotFound => {},
            error.SnapshotCorrupted => {
                const wal_has_content = wal_file_has_content(options.wal_path);
                if (!wal_has_content) return error.SnapshotCorrupted;

                reset_runtime_shards_for_recovery(&db.state);
            },
            else => return error_mod.map_persistence_error(err),
        }
    }

    if (options.wal_path) |wal_path| {
        const replay_applier = storage_wal.ReplayApplier{
            .ctx = db,
            .put = replay_put,
            .delete = replay_delete,
            .expire = replay_expire,
        };
        db.state.wal = storage_wal.open(wal_path, .{
            .fsync_mode = options.fsync_mode,
            .fsync_interval_ms = options.fsync_interval_ms,
            .min_lsn = snapshot_lsn,
        }, replay_applier, allocator) catch |err| return error_mod.map_persistence_error(err);
    }

    try purge_expired_after_recovery(&db.state);
    return db;
}

fn create_with_snapshot_path(
    allocator: std.mem.Allocator,
    snapshot_path: ?[]const u8,
) error_mod.EngineError!*engine_db.Database {
    const db = allocator.create(engine_db.Database) catch return error.OutOfMemory;
    errdefer allocator.destroy(db);

    db.* = .{
        .allocator = allocator,
        .state = runtime_state.DatabaseState.init(allocator, snapshot_path),
    };
    return db;
}

/// Flushes and closes persistence handles, then releases runtime state and engine ownership.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns `error.ActiveReadViews` when any `ReadView` handles still borrow this database.
///
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
pub fn close(db: *engine_db.Database) error_mod.EngineError!void {
    if (db.state.active_read_views.load(.monotonic) != 0) return error.ActiveReadViews;
    if (db.state.wal) |*wal| {
        if (wal.needs_close_fsync()) {
            wal.fsync() catch return error.WalFlushFailed;
        }
    }
    db.state.deinit();
    db.allocator.destroy(db);
}

/// Writes one consistent checkpoint through the storage-owned snapshot boundary.
///
/// Time Complexity: O(1) until checkpoint behavior is implemented.
///
/// Allocator: Does not allocate; returns `error.NotImplemented` until checkpoint behavior is implemented.
///
/// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
pub fn checkpoint(db: *engine_db.Database) error_mod.EngineError!void {
    _ = db;
    return error.NotImplemented;
}

fn wal_file_has_content(wal_path: ?[]const u8) bool {
    const path = wal_path orelse return false;
    const file = std.fs.cwd().openFile(path, .{}) catch return false;
    defer file.close();

    const size = file.getEndPos() catch return false;
    return size > 0;
}

fn reset_runtime_shards_for_recovery(state: *runtime_state.DatabaseState) void {
    for (&state.shards) |*shard| {
        shard.deinit();
        shard.* = runtime_shard.Shard.init(state.base_allocator);
    }
}

fn purge_expired_after_recovery(state: *runtime_state.DatabaseState) !void {
    const now = runtime_shard.unix_now();

    for (&state.shards) |*shard| {
        shard.lock.lock();
        defer shard.lock.unlock();

        var expired_keys = std.ArrayList([]u8).init(state.base_allocator);
        defer {
            for (expired_keys.items) |key| state.base_allocator.free(key);
            expired_keys.deinit();
        }

        var iterator = shard.ttl_index.iterator();
        while (iterator.next()) |entry| {
            if (!internal_ttl_index.is_expired(entry.value_ptr.*, now)) continue;
            try expired_keys.append(try state.base_allocator.dupe(u8, entry.key_ptr.*));
        }

        for (expired_keys.items) |key| {
            _ = internal_mutate.remove_stored_value_unlocked(shard, state.base_allocator, key);
            internal_ttl_index.clear_ttl_entry(shard, key);
        }
    }
}

fn replay_put(ctx: *anyopaque, key: []const u8, value: *const @import("../types/value.zig").Value) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    const allocator = db.state.base_allocator;
    if (shard.values.getPtr(key)) |stored| {
        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        stored.deinit(allocator);
        stored.* = cloned;
    } else {
        try shard.values.ensureUnusedCapacity(allocator, 1);

        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const cloned = try value.clone(allocator);
        errdefer {
            var owned_value = cloned;
            owned_value.deinit(allocator);
        }

        shard.values.putAssumeCapacityNoClobber(owned_key, cloned);
    }

    internal_ttl_index.clear_ttl_entry(shard, key);
}

fn replay_delete(ctx: *anyopaque, key: []const u8) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    _ = internal_mutate.remove_stored_value_unlocked(shard, db.state.base_allocator, key);
    internal_ttl_index.clear_ttl_entry(shard, key);
}

fn replay_expire(ctx: *anyopaque, key: []const u8, expire_at_sec: i64) !void {
    const db: *engine_db.Database = @ptrCast(@alignCast(ctx));
    const shard_idx = runtime_shard.get_shard_index(key);
    const shard = &db.state.shards[shard_idx];

    shard.lock.lock();
    defer shard.lock.unlock();

    if (!shard.values.contains(key)) {
        internal_ttl_index.clear_ttl_entry(shard, key);
        return;
    }

    try internal_ttl_index.set_ttl_entry(shard, key, expire_at_sec);
}

test "create initializes runtime state without storage handles" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer close(db) catch unreachable;

    try testing.expect(db.state.wal == null);
    try testing.expect(db.state.snapshot_path == null);
}
