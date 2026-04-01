# AGENTS.md - Coding Guidelines for Zeno

Zeno is a high-performance key-value storage engine with implementations in Zig (core), Python, and Rust.

## Build Commands

### Zig (Core)
```bash
zig build                          # Build project
zig build test                     # Run all tests
zig test src/core/types/value.zig  # Run single test file
zig test src/zeno.zig              # Run specific module
zig build bench -Doptimize=ReleaseFast              # Run benchmarks
zig build bench -Doptimize=ReleaseFast -- --filter=GET  # Filter benchmarks
```

### Python
```bash
cd python/zeno
uv pip install -e .                # Install dependencies
uv run pytest                      # Run all tests
uv run pytest tests/test_value.py  # Run single test file
uv run pytest tests/test_value.py::TestValue::test_clone  # Run specific test
uv run pytest -v                   # Verbose output
```

### Rust
```bash
cd rust
cargo build                        # Build project
cargo test                         # Run all tests
cargo test test_name               # Run single test
cargo test module_name::           # Run module tests
cargo test -- --nocapture          # Verbose output
```

## Lint/Format

### Zig
```bash
zig fmt src/core/engine/db.zig     # Format file
zig fmt src/                       # Format all files
```

### Python
```bash
cd python/zeno
ruff format src/                   # Format
uv run ruff format zeno/           # Using uv
uv run mypy zeno/                  # Type check
```

### Rust
```bash
cd rust
cargo fmt                          # Format code
cargo fmt -- --check               # Check formatting
cargo clippy                       # Run lints
```

## Code Style Guidelines

### Documentation Comments

- Use `//!` for module-level documentation
- Include: Cost (time complexity), Allocator usage, Thread Safety
- Example:
  ```zig
  //! Engine coordination center.
  //! Cost: O(1) dispatch plus downstream work.
  //! Allocator: Uses explicit allocators.
  ```

### Imports

**Zig:**
- Standard library first: `const std = @import("std");`
- Internal modules in alphabetical order
- Use snake_case for aliases: `const runtime_state = @import("../runtime/state.zig");`

**Python:**
- Standard library → third-party → local
- Use absolute imports: `from zeno.types import Value`

**Rust:**
- Standard library → third-party → crate
- Example: `use std::collections::HashMap;`

### Naming Conventions

| Category | Zig | Python | Rust |
|----------|-----|--------|------|
| Files | snake_case | snake_case | snake_case |
| Structs/Types | PascalCase | PascalCase | PascalCase |
| Functions | camelCase | snake_case | snake_case |
| Variables | snake_case | snake_case | snake_case |
| Constants | snake_case | UPPER_SNAKE | UPPER_SNAKE |
| Errors | PascalCase + Error | PascalCase + Error | PascalCase + Error |

### Error Handling

**Zig:** Use explicit error unions: `EngineError!void` or `EngineError!?Value`

**Python:** Use custom exceptions inheriting from `ZenoError`

**Rust:** Use `Result<T, E>` for fallible operations

### Memory Management

**Zig:**
- Every allocating function must accept explicit `std.mem.Allocator`
- Document: "Allocator: Allocates X from allocator"
- Document ownership: "Caller owns the returned value"
- Use `defer` for cleanup immediately after allocation

**Python:**
- Rely on garbage collection
- Clone values for safe storage: `value.clone()`

**Rust:**
- Leverage ownership and borrowing
- Use RAII patterns with `Drop` trait

### Function Documentation Template

```zig
/// Brief description.
///
/// Time Complexity: O(n), where n is ...
///
/// Allocator: Document allocation behavior.
///
/// Ownership: Document ownership rules.
///
/// Thread Safety: Document thread safety.
pub fn functionName(...) Error!ReturnType {
```

### Testing

- Tests are inline at bottom of files or in test directories
- Use descriptive test names: `test "value clone duplicates owned nested storage"`
- Test both success and error paths

**Zig:** Use `std.testing`
**Python:** Use `pytest`
**Rust:** Use `#[test]` with `assert_eq!`, `assert!`

### Thread Safety

- Document thread safety for every public function
- Use atomics: `std.atomic.Value(T)` (Zig), `Atomic*` (Rust)
- Common patterns:
  - "Not thread-safe; caller must ensure exclusive access"
  - "Lock-free via seqlock"
  - "Thread-safe for concurrent readers"

## Project Structure

```
src/                      # Zig core
├── zeno.zig             # Root module
├── core/
│   ├── public.zig       # General API
│   ├── official.zig     # Advanced API
│   ├── types/           # Value, Scan, Batch
│   ├── engine/          # Core logic
│   ├── runtime/         # Shard management
│   ├── index/           # ART index
│   ├── storage/         # WAL, snapshots
│   └── internal/        # Utilities
└── bench/               # Benchmarks

python/zeno/             # Python
├── zeno/
│   ├── __init__.py      # Public API
│   ├── types.py         # Value type
│   ├── art/             # ART index
│   ├── shard.py         # Shard
│   └── database.py      # Database
└── tests/

rust/                    # Rust
├── src/
│   ├── lib.rs           # Library root
│   ├── value.rs         # Value type
│   ├── shard.rs         # Shard
│   └── database.rs      # Database
└── tests/
```

## Key Patterns

- **Facade pattern**: `public.zig` and `official.zig` are thin facades
- **Shard-first architecture**: 256 shards with independent locks
- **Zero-implicit allocation**: Every allocation is explicit
- **Seqlock for lock-free reads**: Concurrent readers never block
- **Tagged pointers**: For type safety in ART index

## Git Workflow

- **ALWAYS** create PRs to the user's fork (`ValerioL29/zeno`), NEVER to upstream (`zeno-core/zeno`)
- Keep only one remote: `origin` pointing to the user's fork
- Remove any `upstream` remote immediately if it exists
