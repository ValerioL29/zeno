//! Engine coordination center for the zeno-core facade.
//! Cost: O(1) dispatch plus downstream runtime and storage work.
//! Allocator: Uses explicit allocators to own the engine handle, runtime state, and caller-visible cloned values.

const std = @import("std");
const batch_ops = @import("batch.zig");
const error_mod = @import("error.zig");
const internal_codec = @import("../internal/codec.zig");
const lifecycle = @import("lifecycle.zig");
const read = @import("read.zig");
const scan_ops = @import("scan.zig");
const runtime_state = @import("../runtime/state.zig");
const types = @import("../types.zig");
const write = @import("write.zig");

/// Shared error set for engine contract operations.
pub const EngineError = error_mod.EngineError;

/// Central engine handle coordinated by the future engine layer.
pub const Database = struct {
    allocator: std.mem.Allocator,
    state: runtime_state.DatabaseState,

    /// Flushes and closes engine-owned resources.
    ///
    /// Time Complexity: O(s), where `s` is the runtime shard count.
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns `error.ActiveReadViews` when any `ReadView` handles are still active.
    ///
    /// Thread Safety: Not thread-safe; caller must ensure exclusive ownership of the engine handle.
    pub fn close(self: *Database) EngineError!void {
        return lifecycle.close(self);
    }

    /// Writes a consistent checkpoint of engine-owned state.
    ///
    /// Time Complexity: O(1) until checkpoint persistence is implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until checkpoint persistence is implemented.
    pub fn checkpoint(self: *Database) EngineError!void {
        return lifecycle.checkpoint(self);
    }

    /// Reads one key from the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup work, and `v` is cloned value size when the key exists.
    ///
    /// Allocator: Allocates the returned cloned value through `allocator` when the key exists.
    ///
    /// Ownership: Returns a caller-owned cloned value when non-null. The caller must later call `deinit` with `allocator`.
    pub fn get(self: *const Database, allocator: std.mem.Allocator, key: []const u8) EngineError!?types.Value {
        return read.get(&self.state, allocator, key);
    }

    /// Writes one plain key/value pair through the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k + v), where `n` is `key.len` for shard routing, `k` is hash-map lookup or insert work, and `v` is cloned value size.
    ///
    /// Allocator: Clones owned key and value storage through the engine base allocator.
    ///
    /// Ownership: Clones `value` into engine-owned storage before the call returns.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn put(self: *Database, key: []const u8, value: *const types.Value) EngineError!void {
        return write.put(&self.state, key, value);
    }

    /// Deletes one plain key from the engine contract surface.
    ///
    /// Time Complexity: O(n^2 + k), where `n` is `key.len` for shard routing and `k` is hash-map lookup and removal work.
    ///
    /// Allocator: Does not allocate; frees engine-owned key and value storage when the key exists.
    ///
    /// Thread Safety: Safe for concurrent use with other point operations; acquires the global visibility gate exclusively before taking one shard-exclusive lock.
    pub fn delete(self: *Database, key: []const u8) bool {
        return write.delete(&self.state, key);
    }

    /// Sets or clears key expiration at an absolute unix-second timestamp.
    ///
    /// Time Complexity: O(1) until expiration semantics are implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until expiration semantics are implemented.
    pub fn expire_at(self: *Database, key: []const u8, unix_seconds: ?i64) EngineError!bool {
        _ = self;
        _ = key;
        _ = unix_seconds;
        return error.NotImplemented;
    }

    /// Returns Redis-style TTL for one plain key.
    ///
    /// Time Complexity: O(1) until expiration semantics are implemented.
    ///
    /// Allocator: Does not allocate; returns `error.NotImplemented` until expiration semantics are implemented.
    pub fn ttl(self: *const Database, key: []const u8) EngineError!i64 {
        _ = self;
        _ = key;
        return error.NotImplemented;
    }

    /// Performs a full prefix scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scan_prefix(
        self: *const Database,
        allocator: std.mem.Allocator,
        prefix: []const u8,
    ) EngineError!types.ScanResult {
        return scan_ops.scan_prefix(&self.state, allocator, prefix);
    }

    /// Performs a full range scan over the current visible state.
    ///
    /// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
    ///
    /// Allocator: Allocates owned entry keys and values plus result storage through `allocator`.
    ///
    /// Ownership: Returns a result that owns all returned keys and values until `deinit`.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate before taking shard shared locks to collect entries.
    pub fn scan_range(
        self: *const Database,
        allocator: std.mem.Allocator,
        range: types.KeyRange,
    ) EngineError!types.ScanResult {
        return scan_ops.scan_range(&self.state, allocator, range);
    }

    /// Applies one plain atomic batch.
    ///
    /// Time Complexity: O(n + b + v), where `n` is `writes.len`, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
    ///
    /// Allocator: Uses the engine base allocator for committed values and temporary planner scratch while validating and preparing the batch.
    ///
    /// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
    ///
    /// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full apply window.
    pub fn apply_batch(self: *Database, writes: []const types.PutWrite) EngineError!void {
        return batch_ops.apply_batch(&self.state, self.allocator, writes);
    }

    /// Opens one consistent read view.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a `ReadView` that keeps one registry-backed visibility hold alive until `deinit` is called.
    ///
    /// Thread Safety: Acquires the shared side of the global visibility gate and keeps it held for the lifetime of the returned `ReadView`.
    pub fn read_view(self: *Database) EngineError!types.ReadView {
        return read.read_view(&self.state);
    }
};

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(s), where `s` is the runtime shard count.
///
/// Allocator: Allocates the engine handle and runtime state from `allocator`.
pub fn create(allocator: std.mem.Allocator) EngineError!*Database {
    return lifecycle.create(allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(s), where `s` is the runtime shard count, when persistence is not requested.
///
/// Allocator: Allocates the engine handle from `allocator` when persistence is not requested.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) EngineError!*Database {
    return lifecycle.open(allocator, options);
}

test "create initializes runtime-owned database state" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectEqual(@as(usize, runtime_state.NUM_SHARDS), db.state.shards.len);
    try testing.expect(db.state.snapshot_path == null);
}

test "plain point operations store clone and delete values" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .string = "hello" };
    try db.put("alpha", &original);

    {
        var first_read = (try db.get(testing.allocator, "alpha")).?;
        defer first_read.deinit(testing.allocator);

        try testing.expectEqualStrings("hello", first_read.string);
    }

    var second_read = (try db.get(testing.allocator, "alpha")).?;
    defer second_read.deinit(testing.allocator);

    try testing.expectEqualStrings("hello", second_read.string);

    try testing.expect(db.delete("alpha"));
    try testing.expect(!db.delete("alpha"));
    try testing.expect((try db.get(testing.allocator, "alpha")) == null);
}

test "put overwrites existing plain value" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const first = types.Value{ .integer = 7 };
    try db.put("counter", &first);

    const second = types.Value{ .string = "updated" };
    try db.put("counter", &second);

    var stored = (try db.get(testing.allocator, "counter")).?;
    defer stored.deinit(testing.allocator);

    try testing.expectEqualStrings("updated", stored.string);
}

test "read view holds the visibility gate until released" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    defer if (view.token_id != 0) view.deinit();

    try testing.expect(!db.state.visibility_gate.try_lock_exclusive());

    view.deinit();
    try testing.expect(db.state.visibility_gate.try_lock_exclusive());
    db.state.visibility_gate.unlock_exclusive();
}

test "read view copies release the visibility gate only once" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    var copied = view;
    defer copied.deinit();
    defer view.deinit();

    try testing.expectEqual(@as(usize, 1), db.state.active_read_views.load(.monotonic));
    try testing.expect(!db.state.visibility_gate.try_lock_exclusive());

    view.deinit();

    try testing.expectEqual(@as(usize, 0), db.state.active_read_views.load(.monotonic));
    try testing.expect(db.state.visibility_gate.try_lock_exclusive());
    db.state.visibility_gate.unlock_exclusive();
}

test "in-view scans reject stale read view copies" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const value = types.Value{ .integer = 1 };
    try db.put("alpha", &value);

    var view = try db.read_view();
    var copied = view;
    defer copied.deinit();

    view.deinit();

    try testing.expectError(error.InvalidReadView, scan_prefix_from_in_view(&copied, testing.allocator, "alpha", null, 1));
}

test "close fails while a read view is still active" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    var view = try db.read_view();
    defer view.deinit();

    try testing.expectError(error.ActiveReadViews, db.close());
}

test "apply_batch keeps the final value in declared key order" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };

    try db.apply_batch(&.{
        .{ .key = "alpha", .value = &one },
        .{ .key = "beta", .value = &two },
        .{ .key = "alpha", .value = &three },
    });

    var alpha = (try db.get(testing.allocator, "alpha")).?;
    defer alpha.deinit(testing.allocator);
    var beta = (try db.get(testing.allocator, "beta")).?;
    defer beta.deinit(testing.allocator);

    try testing.expectEqual(@as(i64, 3), alpha.integer);
    try testing.expectEqual(@as(i64, 2), beta.integer);
}

test "apply_checked_batch keeps state unchanged when a guard fails" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const original = types.Value{ .string = "original" };
    try db.put("guarded", &original);

    const replacement = types.Value{ .string = "replacement" };
    const other = types.Value{ .integer = 9 };

    try testing.expectError(error.GuardFailed, apply_checked_batch(db, .{
        .writes = &.{
            .{ .key = "guarded", .value = &replacement },
            .{ .key = "other", .value = &other },
        },
        .guards = &.{
            .{ .key_not_exists = "guarded" },
        },
    }));

    var guarded = (try db.get(testing.allocator, "guarded")).?;
    defer guarded.deinit(testing.allocator);

    try testing.expectEqualStrings("original", guarded.string);
    try testing.expect((try db.get(testing.allocator, "other")) == null);
}

test "apply_checked_batch validates guard keys and expected values" {
    const testing = std.testing;
    const allocator = testing.allocator;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    try testing.expectError(error.KeyTooLarge, apply_checked_batch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_exists = "" },
        },
    }));

    const oversized_bytes = try allocator.alloc(u8, @as(usize, @intCast(internal_codec.MAX_VAL_LEN)) + 1);
    defer allocator.free(oversized_bytes);
    @memset(oversized_bytes, 'x');
    const oversized_value = types.Value{ .string = oversized_bytes };

    try testing.expectError(error.ValueTooLarge, apply_checked_batch(db, .{
        .writes = &.{},
        .guards = &.{
            .{ .key_value_equals = .{
                .key = "guarded",
                .value = &oversized_value,
            } },
        },
    }));
}

test "scan_prefix returns lexicographically ordered owned entries" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const alpha = types.Value{ .integer = 1 };
    const alpha_one = types.Value{ .integer = 2 };
    const beta = types.Value{ .integer = 3 };
    try db.put("alpha", &alpha);
    try db.put("alpha:1", &alpha_one);
    try db.put("beta", &beta);

    var result = try db.scan_prefix(testing.allocator, "alpha");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("alpha", result.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", result.entries.items[1].key);
    try testing.expectEqual(@as(i64, 1), result.entries.items[0].value.integer);
    try testing.expectEqual(@as(i64, 2), result.entries.items[1].value.integer);
}

test "scan_range uses inclusive start and exclusive end" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const a = types.Value{ .integer = 1 };
    const b = types.Value{ .integer = 2 };
    const c = types.Value{ .integer = 3 };
    try db.put("a", &a);
    try db.put("b", &b);
    try db.put("c", &c);

    var result = try db.scan_range(testing.allocator, .{
        .start = "a",
        .end = "c",
    });
    defer result.deinit();

    try testing.expectEqual(@as(usize, 2), result.entries.items.len);
    try testing.expectEqualStrings("a", result.entries.items[0].key);
    try testing.expectEqualStrings("b", result.entries.items[1].key);
}

test "scan_prefix_from_in_view paginates in key order" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    const three = types.Value{ .integer = 3 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);
    try db.put("alpha:2", &three);

    var view = try db.read_view();
    defer view.deinit();

    var first_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 2);
    defer first_page.deinit();

    try testing.expectEqual(@as(usize, 2), first_page.entries.items.len);
    try testing.expect(first_page.borrow_next_cursor() != null);
    try testing.expectEqualStrings("alpha", first_page.entries.items[0].key);
    try testing.expectEqualStrings("alpha:1", first_page.entries.items[1].key);

    var cursor = first_page.take_next_cursor().?;
    defer cursor.deinit();
    const cursor_view = cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", &cursor_view, 2);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expect(second_page.borrow_next_cursor() == null);
    try testing.expectEqualStrings("alpha:2", second_page.entries.items[0].key);
}

test "scan page can promote one borrowed continuation cursor into owned storage" {
    const testing = std.testing;

    const db = try create(testing.allocator);
    defer db.close() catch unreachable;

    const one = types.Value{ .integer = 1 };
    const two = types.Value{ .integer = 2 };
    try db.put("alpha", &one);
    try db.put("alpha:1", &two);

    var view = try db.read_view();
    defer view.deinit();

    var page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", null, 1);

    const borrowed_cursor = page.borrow_next_cursor().?;
    var owned_cursor = try borrowed_cursor.clone(testing.allocator);
    defer owned_cursor.deinit();

    page.deinit();

    const cursor_view = owned_cursor.as_cursor().?;
    var second_page = try scan_prefix_from_in_view(&view, testing.allocator, "alpha", &cursor_view, 1);
    defer second_page.deinit();

    try testing.expectEqual(@as(usize, 1), second_page.entries.items.len);
    try testing.expectEqualStrings("alpha:1", second_page.entries.items[0].key);
}

test "owned scan cursor copies release continuation bytes only once" {
    const testing = std.testing;

    var cursor = try types.OwnedScanCursor.init(testing.allocator, 0, "alpha");
    var copied = cursor;
    defer copied.deinit();
    defer cursor.deinit();

    try testing.expect(cursor.as_cursor() != null);

    cursor.deinit();

    try testing.expect(copied.as_cursor() == null);
}

/// Scans the next prefix page inside a consistent read view.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while collecting entries.
pub fn scan_prefix_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    prefix: []const u8,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    return scan_ops.scan_prefix_from_in_view(view, allocator, prefix, cursor, limit);
}

/// Scans the next range page inside a consistent read view.
///
/// Time Complexity: O(s + m log m + v), where `s` is shard count, `m` is matched entry count, and `v` is total cloned value size.
///
/// Allocator: Allocates owned entry keys and values plus any continuation cursor through `allocator`.
///
/// Ownership: `cursor` is borrowed when present and must remain valid for the duration of the call. The returned page exposes any continuation cursor through `borrow_next_cursor` and may transfer it into `OwnedScanCursor` through `take_next_cursor`.
///
/// Thread Safety: Relies on the caller-owned `ReadView` visibility hold and takes shard shared locks while collecting entries.
pub fn scan_range_from_in_view(
    view: *const types.ReadView,
    allocator: std.mem.Allocator,
    range: types.KeyRange,
    cursor: ?*const types.ScanCursor,
    limit: usize,
) EngineError!types.ScanPageResult {
    return scan_ops.scan_range_from_in_view(view, allocator, range, cursor, limit);
}

/// Applies one checked batch under the official advanced contract.
///
/// Time Complexity: O(g + n + b + v), where `g` is `batch.guards.len`, `n` is surviving write count, `b` is total serialized value bytes measured during planning, and `v` is total cloned value size for prepared writes.
///
/// Allocator: Uses the engine base allocator for committed values and temporary planner scratch while validating guards and preparing the batch.
///
/// Ownership: Clones all surviving write values into engine-owned storage before making the batch visible.
///
/// Thread Safety: Safe for concurrent use with point operations and read views; acquires the global visibility gate exclusively for the full guard-check and apply window.
pub fn apply_checked_batch(db: *Database, batch: types.CheckedBatch) EngineError!void {
    return batch_ops.apply_checked_batch(&db.state, db.allocator, batch);
}
