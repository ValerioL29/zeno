//! Public access point for zeno-core contract types.
//! Cost: O(1) module reexports plus declared type metadata.
//! Allocator: Does not allocate.

const batch = @import("types/batch.zig");
const checked_batch = @import("types/checked_batch.zig");
const read_view = @import("types/read_view.zig");
const scan = @import("types/scan.zig");
const storage_wal = @import("storage/wal.zig");
const value_mod = @import("types/value.zig");

/// Durability policy for WAL fsync behavior.
pub const FsyncMode = storage_wal.FsyncMode;

/// Options for opening a database with WAL and optional snapshot support.
pub const DatabaseOptions = struct {
    /// Path to the WAL file. `null` keeps the engine in-memory only.
    wal_path: ?[]const u8 = null,
    /// Path to the snapshot file. `null` disables snapshot load and checkpoint writes.
    snapshot_path: ?[]const u8 = null,
    fsync_mode: FsyncMode = .always,
    fsync_interval_ms: u32 = 2,
};

/// Public engine value model.
pub const Value = value_mod.Value;

/// Public request type for plain-key batch writes.
pub const PutWrite = batch.PutWrite;

/// Public guarded batch request for official advanced atomic writes.
pub const CheckedBatch = checked_batch.CheckedBatch;

/// Public physical guard type for checked-batch execution.
pub const CheckedBatchGuard = checked_batch.CheckedBatchGuard;

/// Prefix or range bounds for ordered key scans.
pub const KeyRange = scan.KeyRange;

/// One borrowed key/value pair yielded by scan operations.
pub const ScanEntry = scan.ScanEntry;

/// Owned result container for full scan responses.
pub const ScanResult = scan.ScanResult;

/// Opaque continuation state for paginated scans.
pub const ScanCursor = scan.ScanCursor;

/// Owned result container for one paginated scan page.
pub const ScanPageResult = scan.ScanPageResult;

/// Consistent read window handle for official advanced reads.
pub const ReadView = read_view.ReadView;
