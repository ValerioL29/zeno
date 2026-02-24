//! Thin public facade for the zeno-core general engine contract.
//! Cost: O(1) facade delegation plus downstream engine work.
//! Allocator: Delegates allocation behavior to engine entry points.

const std = @import("std");
const engine_db = @import("engine/db.zig");
const types = @import("types.zig");

/// Public database handle for the general engine contract.
pub const Database = engine_db.Database;

/// Public error set used by the step 3 facade skeleton.
pub const Error = engine_db.EngineError;

/// Creates an in-memory engine handle.
///
/// Time Complexity: O(1) in the step 3 skeleton.
///
/// Allocator: Does not allocate in the step 3 skeleton; returns `error.NotImplemented`.
pub fn create(allocator: std.mem.Allocator) Error!*Database {
    return engine_db.create(allocator);
}

/// Opens an engine handle from the provided runtime options.
///
/// Time Complexity: O(1) in the step 3 skeleton.
///
/// Allocator: Does not allocate in the step 3 skeleton; returns `error.NotImplemented`.
pub fn open(allocator: std.mem.Allocator, options: types.DatabaseOptions) Error!*Database {
    return engine_db.open(allocator, options);
}
