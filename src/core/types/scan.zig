//! Public scan descriptors, entries, and owned scan result containers.
//! Cost: Scan descriptors are O(1); owned result cleanup is O(n) over stored entries and cursor bytes.
//! Allocator: Uses explicit allocators for owned scan results and continuation cursors.

const std = @import("std");
const Value = @import("value.zig").Value;

/// Opaque token identity for one owned continuation cursor.
const OwnedScanCursorToken = enum(u64) {
    invalid = 0,
    _,
};

/// Private page-owned slot for one continuation cursor token.
const PageCursorSlot = enum(u64) {
    invalid = 0,
    _,
};

/// Token-table payload for one owned continuation cursor.
const OwnedScanCursorState = struct {
    allocator: std.mem.Allocator,
    shard_idx: u8,
    resume_key: []u8,
};

var next_owned_scan_cursor_token_id = std.atomic.Value(u64).init(1);
var owned_scan_cursor_mutex: std.Thread.Mutex = .{};
var owned_scan_cursors = std.AutoHashMapUnmanaged(u64, OwnedScanCursorState){};

/// Registers one owned continuation cursor state behind a stable token.
///
/// Time Complexity: O(k) expected, where `k` is `resume_key.len`.
///
/// Allocator: Duplicates `resume_key` through `allocator` and may grow the token table with `std.heap.page_allocator`.
///
/// Ownership: Transfers the duplicated continuation bytes into the token table until `release_owned_scan_cursor_state` removes them.
///
/// Thread Safety: Serializes token-table mutation through `owned_scan_cursor_mutex`.
fn register_owned_scan_cursor_state(
    allocator: std.mem.Allocator,
    shard_idx: u8,
    resume_key: []const u8,
) std.mem.Allocator.Error!u64 {
    const token_id = next_owned_scan_cursor_token_id.fetchAdd(1, .monotonic);
    const owned_key = try allocator.dupe(u8, resume_key);
    errdefer allocator.free(owned_key);

    owned_scan_cursor_mutex.lock();
    defer owned_scan_cursor_mutex.unlock();
    try owned_scan_cursors.put(std.heap.page_allocator, token_id, .{
        .allocator = allocator,
        .shard_idx = shard_idx,
        .resume_key = owned_key,
    });
    return token_id;
}

/// Resolves one active owned continuation cursor state without consuming it.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns borrowed state that remains valid only while the token stays active.
///
/// Thread Safety: Serializes token-table access through `owned_scan_cursor_mutex`.
fn get_owned_scan_cursor_state(token_id: u64) ?OwnedScanCursorState {
    owned_scan_cursor_mutex.lock();
    defer owned_scan_cursor_mutex.unlock();
    return owned_scan_cursors.get(token_id);
}

/// Removes one owned continuation cursor state from the token table.
///
/// Time Complexity: O(1) expected.
///
/// Allocator: Does not allocate.
///
/// Ownership: Transfers the removed owned continuation bytes to the caller, which must free them exactly once.
///
/// Thread Safety: Serializes token-table mutation through `owned_scan_cursor_mutex`.
fn release_owned_scan_cursor_state(token_id: u64) ?OwnedScanCursorState {
    owned_scan_cursor_mutex.lock();
    defer owned_scan_cursor_mutex.unlock();
    const removed = owned_scan_cursors.fetchRemove(token_id) orelse return null;
    return removed.value;
}

/// Inclusive-start, exclusive-end bounds for ordered-key scans.
///
/// Ownership:
/// - `start` and `end` are borrowed.
/// - Bound slices must remain valid for the duration of the consuming scan call.
pub const KeyRange = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

/// One owned key/value pair yielded during scan traversal.
///
/// Ownership:
/// - `key` is owned by the enclosing result container.
/// - `value` points to owned storage released by the enclosing result container.
pub const ScanEntry = struct {
    key: []const u8,
    value: *const Value,
};

/// Borrowed continuation view for a paginated scan.
///
/// Ownership:
/// - `resume_key` is borrowed.
/// - The cursor remains valid only while the owner of `resume_key` stays alive.
pub const ScanCursor = struct {
    shard_idx: u8,
    resume_key: []const u8,

    /// Clones one borrowed continuation cursor into owned storage.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Duplicates `resume_key` through `allocator`.
    ///
    /// Ownership: Returns one owned cursor that must later be released with `deinit`.
    pub fn clone(self: ScanCursor, allocator: std.mem.Allocator) std.mem.Allocator.Error!OwnedScanCursor {
        return OwnedScanCursor.init(allocator, self.shard_idx, self.resume_key);
    }
};

/// Owned continuation cursor retained independently of any page result.
///
/// Ownership:
/// - `resume_key` is owned by this cursor.
/// - Copies of this handle alias the same underlying token-table entry.
/// - The underlying continuation bytes remain alive until the first successful `deinit`.
pub const OwnedScanCursor = enum(u64) {
    invalid = 0,
    _,

    /// Creates one owned continuation cursor by cloning `resume_key`.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Duplicates `resume_key` through `allocator` and may grow the token table with `std.heap.page_allocator`.
    ///
    /// Ownership: Returns one owned cursor handle that keeps a token-table entry alive until `deinit`.
    pub fn init(
        allocator: std.mem.Allocator,
        shard_idx: u8,
        resume_key: []const u8,
    ) std.mem.Allocator.Error!OwnedScanCursor {
        return @enumFromInt(try register_owned_scan_cursor_state(allocator, shard_idx, resume_key));
    }

    /// Returns a borrowed continuation view over this owned cursor.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: The returned cursor borrows continuation bytes from the token table and remains valid only while this owned cursor stays active.
    pub fn as_cursor(self: OwnedScanCursor) ?ScanCursor {
        if (self == .invalid) return null;
        const state = get_owned_scan_cursor_state(@intFromEnum(self)) orelse return null;
        return .{
            .shard_idx = state.shard_idx,
            .resume_key = state.resume_key,
        };
    }

    /// Releases one owned continuation cursor.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Does not allocate; frees `resume_key` through the allocator captured in the token state.
    ///
    /// Ownership: Releases the token-table entry when still active. Releasing multiple copies is safe and frees the continuation bytes only once.
    pub fn deinit(self: *OwnedScanCursor) void {
        if (self.* == .invalid) return;
        const state = release_owned_scan_cursor_state(@intFromEnum(self.*)) orelse {
            self.* = .invalid;
            return;
        };
        state.allocator.free(state.resume_key);
        self.* = .invalid;
    }
};

/// Converts one owned cursor handle into the private page-owned slot representation.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
///
/// Ownership: Preserves ownership of the underlying token-table entry.
fn page_cursor_slot_from_owned_scan_cursor(cursor: OwnedScanCursor) PageCursorSlot {
    return @enumFromInt(@intFromEnum(cursor));
}

/// Rebuilds one owned cursor handle from the private page-owned slot representation.
///
/// Time Complexity: O(1).
///
/// Allocator: Does not allocate.
///
/// Ownership: Returns a handle that aliases the page-owned token-table entry.
fn owned_scan_cursor_from_page_cursor_slot(slot: PageCursorSlot) OwnedScanCursor {
    return @enumFromInt(@intFromEnum(slot));
}

/// Owned result of a scan that materializes all returned entries.
pub const ScanResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,

    /// Releases the owned entry buffer.
    ///
    /// Time Complexity: O(n + b), where `n` is `entries.items.len` and `b` is total teardown work for stored values.
    ///
    /// Allocator: Does not allocate; frees owned entry keys, values, and the entry buffer through `allocator`.
    ///
    /// Ownership: Releases all entry keys and values owned by the result container.
    pub fn deinit(self: *ScanResult) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
            const owned_value: *Value = @constCast(entry.value);
            owned_value.deinit(self.allocator);
            self.allocator.destroy(owned_value);
        }
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }
};

/// Owned result for one paginated scan page.
///
/// Ownership:
/// - The page owns all `entries`.
/// - Any continuation cursor remains page-owned until `take_next_cursor`.
pub const ScanPageResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,
    /// Private page-owned continuation cursor slot. Use `borrow_next_cursor` or `take_next_cursor` instead of depending on this representation.
    _cursor_slot: PageCursorSlot = .invalid,

    /// Returns a borrowed continuation cursor while this page result stays alive.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns a borrowed cursor that remains valid only while this page result stays alive or until `take_next_cursor` transfers ownership away.
    pub fn borrow_next_cursor(self: *const ScanPageResult) ?ScanCursor {
        if (self._cursor_slot == .invalid) return null;
        const owned_cursor = owned_scan_cursor_from_page_cursor_slot(self._cursor_slot);
        return owned_cursor.as_cursor();
    }

    /// Transfers ownership of the optional continuation cursor out of this page result.
    ///
    /// Time Complexity: O(1).
    ///
    /// Allocator: Does not allocate.
    ///
    /// Ownership: Returns one owned cursor when present and clears the page-owned slot so `deinit` will not release it twice.
    pub fn take_next_cursor(self: *ScanPageResult) ?OwnedScanCursor {
        if (self._cursor_slot == .invalid) return null;
        const cursor = owned_scan_cursor_from_page_cursor_slot(self._cursor_slot);
        self._cursor_slot = .invalid;
        return cursor;
    }

    /// Releases the owned entry buffer and optional continuation cursor.
    ///
    /// Time Complexity: O(n + b + k), where `n` is `entries.items.len`, `b` is total teardown work for stored values, and `k` is the continuation resume key length when present.
    ///
    /// Allocator: Does not allocate; frees owned buffers through `allocator`.
    ///
    /// Ownership: Releases all entry keys and values plus any continuation cursor bytes owned by this page result.
    pub fn deinit(self: *ScanPageResult) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
            const owned_value: *Value = @constCast(entry.value);
            owned_value.deinit(self.allocator);
            self.allocator.destroy(owned_value);
        }
        self.entries.deinit(self.allocator);
        if (self._cursor_slot != .invalid) {
            var cursor = owned_scan_cursor_from_page_cursor_slot(self._cursor_slot);
            cursor.deinit();
        }
        self.* = undefined;
    }
};
