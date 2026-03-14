# Zeno - Zig  Engine for Node Operations

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zig Version](https://img.shields.io/badge/Zig-0.15.2-orange.svg)](https://ziglang.org)

Zeno is a high-performance, embedded key-value storage engine written in pure Zig. Designed for modern workloads, it prioritizes predictable low latency, zero-implicit allocation, and efficient sharded concurrency.

Zeno began as a learning experiment into database storage internals and the Adaptive Radix Tree (ART). The results and the performance were promising, that it evolved into a standalone engine.

---

## 🚀 Key Features

*   **Adaptive Radix Tree (ART) Index**: O(k) lookups with SIMD-optimized node transitions (Node4 to Node256).
*   **Sharded Concurrency**: Lock-sharding architecture to maximize multi-core throughput while minimizing contention.
*   **Zero-Implicit Allocation**: Every function that allocates accepts an explicit `Allocator`, following strict Zig practices.
*   **Durable Persistence**: 
    *   **WAL (Write-Ahead Log)**: Batched-async durability mode for high-throughput writes.
    *   **Snapshots**: Efficient, streaming snapshot-backed recovery.
*   **Structured Values**: Support for complex values via `union(enum)` for strict runtime type safety.

## 📊 Performance Benchmarks

Zeno is built for speed. Below are numbers from the latest benchmark run:

| Operation | Throughput | Latency (Mean) |
| :--- | :--- | :--- |
| **DB PUT (steady)** | **21.59M ops/sec** | **46 ns** |
| **DB PUT Group16 (steady)** | **1.69M items/sec (0.11M ops/sec)** | **9.46 µs** |
| **DB GET (steady)** | **15.42M ops/sec** | **64 ns** |
| **ART Lookup (Hit)** | 97.95M ops/sec | 10 ns |
| **ART Insert (Sequential)**| 79.85M ops/sec | 12 ns |
| **WAL Append (Async)** | 0.61M ops/sec | 1.64 µs |
| **WAL Append Grouped16 (Async)** | 1.06M items/sec (0.07M ops/sec) | 15.05 µs |

Sharded scalability (latest run):

| Workload | 1 thread | 2 threads | 4 threads | 8 threads | 16 threads |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **GET (Shared)** | 14.48M | 27.59M | 49.50M | 100.02M | 147.31M ops/sec |
| **PUT (Sharded)** | 25.61M | 44.64M | 83.99M | 113.13M | 152.47M ops/sec |

*Benchmarks conducted on Ubuntu 24.04.4, AMD Ryzen 7 5700X, 32GB DDR4 @ 3200MHz*

Want to reproduce these numbers on your machine? From the repository root, run:

```bash
zig build bench -Doptimize=ReleaseFast
```

This executes the full benchmark suite (steady-state, throughput summary, and sharded scalability) and prints results directly to your terminal.

## 🛠 Usage

Add `zeno` to your `build.zig.zon`:

```zig
.{
    .name = "my-project",
    .version = "0.1.0",
    .dependencies = .{
        .zeno = .{
            .url = "https://github.com/zeno-core/zeno/archive/refs/heads/main.tar.gz",
        },
    },
}
```

Then, in your `build.zig`:

```zig
const zeno = b.dependency("zeno", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zeno", zeno.module("zeno"));
```

### Quick Example

```zig
const zeno = @import("zeno");
var db = try zeno.Database.open(allocator, .{
    .path = "./data",
    .wal_mode = .batched_async,
});
defer db.close();

try db.put("user:123", .{ .string = "Enzo" });
if (try db.get("user:123")) |val| {
    std.debug.print("Found: {s}\n", .{val.string});
}
```

## 🏗 Architecture

Zeno uses a shard-first architecture designed to keep hot paths predictable under concurrency:

- The keyspace is partitioned into independent shards (hash-routed by key), and each shard owns its own ART index, lock, and memory arena.
- Point operations (`put`, `get`, `delete`) are shard-local after routing, minimizing cross-core contention and avoiding a single global lock bottleneck.
- Read consistency is coordinated with visibility gates and `ReadView`, so scans and in-view reads can observe stable state while writes continue on other shards.
- Durability is handled by WAL + snapshot: WAL records live mutations for crash recovery, while snapshots provide faster restart and periodic state compaction.

This design gives strong single-key latency, good multicore scaling, and explicit trade-offs between throughput and durability policy (`fsync_mode`).

## ⚖️ License

Distributed under the MIT License. See `LICENSE` for more information.
