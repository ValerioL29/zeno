//! Internal physical mutation helpers shared by engine write and batch paths.
//! Cost: O(k) over key length for boundary validation.
//! Allocator: Does not allocate.

const codec = @import("codec.zig");

/// Error set for low-level physical mutation validation.
pub const MutationError = error{
    EmptyKey,
    KeyTooLarge,
};

/// Validates one physical key before it enters engine mutation planning.
///
/// Time Complexity: O(k), where `k` is `key.len`.
///
/// Allocator: Does not allocate.
pub fn validate_key(key: []const u8) MutationError!void {
    if (key.len == 0) return error.EmptyKey;
    if (key.len > codec.MAX_KEY_LEN) return error.KeyTooLarge;
}

test "validate_key rejects empty and oversized keys" {
    const testing = @import("std").testing;

    try testing.expectError(error.EmptyKey, validate_key(""));
    try testing.expectError(error.KeyTooLarge, validate_key(&[_]u8{'a'} ** (codec.MAX_KEY_LEN + 1)));
}
