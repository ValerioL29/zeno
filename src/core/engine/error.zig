//! Shared error contract for engine coordination and internal engine modules.
//! Cost: O(1) module reexports plus declared error metadata.
//! Allocator: Does not allocate.

/// Shared error set for engine contract operations.
pub const EngineError = error{
    NotImplemented,
    OutOfMemory,
    KeyTooLarge,
    ActiveReadViews,
    ValueTooLarge,
    ValueTooDeep,
    GuardFailed,
    InvalidReadView,
};
