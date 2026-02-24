//! Public scan descriptors, entries, and owned scan result containers.
//! Cost: Scan descriptors are O(1); owned result cleanup is O(n) over stored entries and cursor bytes.
//! Allocator: Uses explicit allocators for owned scan results and continuation cursors.

const std = @import("std");
const Value = @import("value.zig").Value;

/// Prefix or range bounds for ordered-key scans.
pub const KeyRange = struct {
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

/// One borrowed key/value pair yielded during scan traversal.
pub const ScanEntry = struct {
    key: []const u8,
    value: *const Value,
};

/// Owned result of a scan that materializes all returned entries.
pub const ScanResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,

    /// Releases the owned entry buffer.
    ///
    /// Time Complexity: O(1), delegated to `ArrayList` buffer teardown.
    ///
    /// Allocator: Frees the entry buffer through `allocator`.
    ///
    /// Ownership: Releases only storage owned by the result container; entry keys and values remain borrowed.
    pub fn deinit(self: *ScanResult) void {
        self.entries.deinit(self.allocator);
        self.* = undefined;
    }
};

/// Opaque continuation state for a paginated scan.
pub const ScanCursor = struct {
    shard_idx: u8,
    resume_key: []const u8,

    /// Releases the owned resume key buffer.
    ///
    /// Time Complexity: O(k), where `k` is `resume_key.len`.
    ///
    /// Allocator: Does not allocate; frees `resume_key` through `allocator`.
    ///
    /// Ownership: Releases the resume key bytes owned by this cursor.
    pub fn deinit(self: *ScanCursor, allocator: std.mem.Allocator) void {
        allocator.free(self.resume_key);
        self.* = undefined;
    }
};

/// Owned result for one paginated scan page.
pub const ScanPageResult = struct {
    entries: std.ArrayList(ScanEntry),
    allocator: std.mem.Allocator,
    next_cursor: ?ScanCursor = null,

    /// Releases the owned entry buffer and optional continuation cursor.
    ///
    /// Time Complexity: O(k), where `k` is the continuation resume key length when present, plus O(1) entry buffer teardown.
    ///
    /// Allocator: Does not allocate; frees owned buffers through `allocator`.
    ///
    /// Ownership: Releases only storage owned by this page result; entry keys and values remain borrowed.
    pub fn deinit(self: *ScanPageResult) void {
        self.entries.deinit(self.allocator);
        if (self.next_cursor) |*cursor| cursor.deinit(self.allocator);
        self.* = undefined;
    }
};
