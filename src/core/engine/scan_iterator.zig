//! Lazy iterator over a consistent prefix or range scan.
//! Cost: O(1) amortized per entry; O(page_size * (k + v)) per internal page fetch.
//! Allocator: Uses an explicit allocator for page allocation and entry cloning.

const std = @import("std");
const error_mod = @import("error.zig");
const scan_ops = @import("scan.zig");
const read_view_mod = @import("../types/read_view.zig");
const scan_types = @import("../types/scan.zig");

/// Query discriminator for the iterator — either a prefix string or a key range.
///
/// Ownership: Borrows `prefix` and range `start`/`end` slices for the lifetime
/// of the owning `ScanIterator`. Callers must keep those slices valid.
pub const IteratorQuery = union(enum) {
    prefix: []const u8,
    range: scan_types.KeyRange,
};

/// Lazy iterator over a consistent prefix or range scan.
///
/// Wraps `ReadView` + paginated scan internally. Each call to `next()` returns
/// one entry at a time without materializing the full result set. The returned
/// entry borrows from the iterator's internal page and is valid until the next
/// call to `next()` or `deinit()`.
///
/// Ownership: Owns the `ReadView`, the current page, and the continuation cursor.
/// The caller must call `deinit()` when done, even if `next()` returned `null`.
///
/// Thread Safety: Not thread-safe; must be used from a single thread only.
pub const ScanIterator = struct {
    allocator: std.mem.Allocator,
    query: IteratorQuery,
    page_size: usize,
    view: read_view_mod.ReadView,
    cursor: ?scan_types.OwnedScanCursor,
    page: ?scan_types.ScanPageResult,
    page_pos: usize,
    done: bool,

    /// Default number of entries fetched per internal page request.
    pub const default_page_size: usize = 64;

    /// Advances the iterator and returns the next entry, or `null` when exhausted.
    ///
    /// Time Complexity: O(1) amortized — O(page_size * (k + v)) every `page_size`
    /// calls when a new page is fetched; O(1) for all other calls.
    ///
    /// Allocator: Allocates a new page (entries + cursor) every `page_size` entries.
    ///
    /// Ownership: The returned `ScanEntry` borrows from the iterator's current page.
    /// It remains valid until the next call to `next()` or `deinit()`.
    pub fn next(self: *ScanIterator) error_mod.EngineError!?scan_types.ScanEntry {
        if (self.done) return null;

        // If we have entries left in the current page, return the next one.
        if (self.page) |*page| {
            if (self.page_pos < page.entries.items.len) {
                const entry = page.entries.items[self.page_pos];
                self.page_pos += 1;
                return entry;
            }

            // Page exhausted — check for a continuation cursor.
            const next_cursor = page.takeNextCursor();
            page.deinit();
            self.page = null;
            self.page_pos = 0;

            if (self.cursor) |*old| old.deinit();
            self.cursor = next_cursor;

            if (self.cursor == null) {
                self.done = true;
                return null;
            }
        }

        // Fetch the next page.
        var cursor_view: ?scan_types.ScanCursor = if (self.cursor) |*c| c.asCursor() else null;
        const cursor_ptr: ?*const scan_types.ScanCursor = if (cursor_view) |*cv| cv else null;

        var new_page = switch (self.query) {
            .prefix => |prefix| try scan_ops.scanPrefixFromInView(
                &self.view,
                self.allocator,
                prefix,
                cursor_ptr,
                self.page_size,
            ),
            .range => |range| try scan_ops.scanRangeFromInView(
                &self.view,
                self.allocator,
                range,
                cursor_ptr,
                self.page_size,
            ),
        };

        if (new_page.entries.items.len == 0) {
            new_page.deinit();
            self.done = true;
            return null;
        }

        self.page = new_page;
        self.page_pos = 1;
        return self.page.?.entries.items[0];
    }

    /// Releases all iterator-owned resources.
    ///
    /// Time Complexity: O(p + k), where `p` is entries remaining in the current
    /// page and `k` is the continuation cursor key length.
    ///
    /// Allocator: Frees through the allocator provided at creation.
    ///
    /// Ownership: Must be called exactly once. Safe to call after `next()` returned `null`.
    pub fn deinit(self: *ScanIterator) void {
        if (self.page) |*page| page.deinit();
        if (self.cursor) |*cursor| cursor.deinit();
        self.view.deinit();
        self.* = undefined;
    }
};
