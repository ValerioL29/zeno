//! Public value model for zeno-core payload storage.
//! Cost: Scalar access is O(1); clone and teardown are O(n) over nested nodes.
//! Allocator: Uses explicit allocators for deep clone and recursive teardown.

const std = @import("std");

/// Tagged value union for scalar and nested payloads stored by the engine.
pub const Value = union(enum) {
    null_val: void,
    boolean: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    array: std.ArrayList(Value),
    object: std.StringHashMapUnmanaged(Value),

    /// Deep-clones the full value tree into memory owned by `allocator`.
    ///
    /// Time Complexity: O(n + b), where `n` is nested value node count and `b` is total cloned string bytes.
    ///
    /// Allocator: Allocates duplicated strings plus array and object storage from `allocator`.
    ///
    /// Ownership: Caller owns the returned value and must later call `deinit` with the same allocator.
    pub fn clone(self: *const Value, allocator: std.mem.Allocator) !Value {
        return switch (self.*) {
            .null_val => .{ .null_val = {} },
            .boolean => |value| .{ .boolean = value },
            .integer => |value| .{ .integer = value },
            .float => |value| .{ .float = value },
            .string => |value| .{ .string = try allocator.dupe(u8, value) },
            .array => |items| {
                var cloned_items = try std.ArrayList(Value).initCapacity(allocator, items.items.len);
                for (items.items) |item| {
                    const cloned_item = try item.clone(allocator);
                    cloned_items.appendAssumeCapacity(cloned_item);
                }
                return .{ .array = cloned_items };
            },
            .object => |entries| {
                var cloned_entries = std.StringHashMapUnmanaged(Value){};
                try cloned_entries.ensureTotalCapacity(allocator, entries.count());
                var iterator = entries.iterator();
                while (iterator.next()) |entry| {
                    const cloned_key = try allocator.dupe(u8, entry.key_ptr.*);
                    const cloned_value = try entry.value_ptr.clone(allocator);
                    cloned_entries.putAssumeCapacity(cloned_key, cloned_value);
                }
                return .{ .object = cloned_entries };
            },
        };
    }

    /// Recursively releases memory owned by this value and all nested children.
    ///
    /// Time Complexity: O(n + b), where `n` is nested value node count and `b` is total freed string bytes.
    ///
    /// Allocator: Does not allocate; frees memory through `allocator`.
    ///
    /// Ownership: Releases only storage previously allocated for this value tree with `allocator`.
    pub fn deinit(self: *Value, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |value| allocator.free(value),
            .array => |*items| {
                for (items.items) |*item| item.deinit(allocator);
                items.deinit(allocator);
            },
            .object => |*entries| {
                var iterator = entries.iterator();
                while (iterator.next()) |entry| {
                    allocator.free(entry.key_ptr.*);
                    entry.value_ptr.deinit(allocator);
                }
                entries.deinit(allocator);
            },
            else => {},
        }
    }
};

test "value clone duplicates owned nested storage" {
    const testing = std.testing;
    const allocator = testing.allocator;

    var nested = std.StringHashMapUnmanaged(Value){};
    defer {
        var iterator = nested.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(allocator);
        }
        nested.deinit(allocator);
    }

    try nested.put(allocator, try allocator.dupe(u8, "message"), .{ .string = try allocator.dupe(u8, "hello") });

    var original = Value{ .object = nested };
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    switch (cloned) {
        .object => |entries| {
            const message = entries.get("message").?;
            try testing.expectEqualStrings("hello", message.string);
        },
        else => try testing.expect(false),
    }
}
