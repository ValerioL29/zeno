//! Lifecycle ownership boundary for engine open, create, close, and checkpoint work.
//! Cost: O(1) module load only in step 3.
//! Allocator: Does not allocate in step 3.
