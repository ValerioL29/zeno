//! Benchmark entrypoint for coarse zeno-core engine performance checks.
//! Cost: Bench-dependent and intentionally end-to-end across public engine boundaries.
//! Allocator: Uses the benchmark-provided allocator for caller-owned result teardown and a process-wide page allocator for steady-state engine fixtures.

const std = @import("std");
const zbench = @import("zbench");
const zeno_core = @import("zeno_core");

const engine = zeno_core.public;
const official = zeno_core.official;
const types = zeno_core.types;

const scan_item_count: usize = 256;
const batch_item_count: usize = 64;
const batch_key_storage_bytes: usize = 32;

var bench_metrics_config: types.MetricsConfig = types.default_metrics_config();
var steady_put_db: ?*engine.Database = null;
var steady_get_db: ?*engine.Database = null;
var steady_scan_db: ?*engine.Database = null;
var steady_batch_overwrite_db: ?*engine.Database = null;
var steady_batch_insert_db: ?*engine.Database = null;
var steady_checked_batch_overwrite_db: ?*engine.Database = null;
var steady_checked_batch_insert_db: ?*engine.Database = null;
var steady_batch_insert_seed = std.atomic.Value(usize).init(0);
var steady_checked_batch_insert_seed = std.atomic.Value(usize).init(0);

const BenchCliConfig = struct {
    run_all_metrics_modes: bool = false,
    metrics_config: types.MetricsConfig = types.default_metrics_config(),
};

const default_sampled_latency_shift: u8 = 10;

fn open_bench_db(allocator: std.mem.Allocator) !*engine.Database {
    return engine.open(allocator, .{
        .metrics = bench_metrics_config,
    });
}

const PutFreshBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 1 };
        db.put("bench:put", &value) catch unreachable;
    }
};

const PutSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_put_db orelse unreachable;
        const value = types.Value{ .integer = 2 };
        db.put("bench:put", &value) catch unreachable;
    }
};

const GetExistingBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        const value = types.Value{ .integer = 42 };
        db.put("bench:get", &value) catch unreachable;

        var stored = (db.get(allocator, "bench:get") catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const GetExistingSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_get_db orelse unreachable;
        var stored = (db.get(allocator, "bench:get") catch unreachable).?;
        defer stored.deinit(allocator);
        std.mem.doNotOptimizeAway(stored.integer);
    }
};

const ScanPrefixBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        load_scan_fixture(db);

        var result = db.scan_prefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ScanPrefixSteadyBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = steady_scan_db orelse unreachable;
        var result = db.scan_prefix(allocator, "scan:") catch unreachable;
        defer result.deinit();
        std.mem.doNotOptimizeAway(result.entries.items.len);
    }
};

const ApplyBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "batch", 0, 0);

        db.apply_batch(&writes) catch unreachable;
    }
};

const ApplyBatchSteadyOverwriteBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_batch_overwrite_db orelse unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "batch", 1_000, 0);

        db.apply_batch(&writes) catch unreachable;
    }
};

const ApplyBatchSteadyInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_batch_insert_db orelse unreachable;
        const key_base = steady_batch_insert_seed.fetchAdd(batch_item_count, .monotonic);

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "batchi", 10_000, key_base);

        db.apply_batch(&writes) catch unreachable;
    }
};

const ApplyCheckedBatchBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        const db = open_bench_db(allocator) catch unreachable;
        defer db.close() catch unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "guard", 0, 0);

        official.apply_checked_batch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ApplyCheckedBatchSteadyOverwriteBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_checked_batch_overwrite_db orelse unreachable;

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "guard", 2_000, 0);

        official.apply_checked_batch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

const ApplyCheckedBatchSteadyInsertBenchmark = struct {
    pub fn run(_: *const @This(), allocator: std.mem.Allocator) void {
        _ = allocator;
        const db = steady_checked_batch_insert_db orelse unreachable;
        const key_base = steady_checked_batch_insert_seed.fetchAdd(batch_item_count, .monotonic);

        var values: [batch_item_count]types.Value = undefined;
        var writes: [batch_item_count]types.PutWrite = undefined;
        var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;

        fill_batch_writes(&values, &writes, &key_storage, "guardi", 20_000, key_base);

        official.apply_checked_batch(db, .{
            .writes = &writes,
            .guards = &.{},
        }) catch unreachable;
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();
    var stdout_buffer: [4 * 1024]u8 = undefined;
    var stdout = std.fs.File.stdout().writer(&stdout_buffer);
    const cli_config = try parse_cli_config(allocator);

    if (cli_config.run_all_metrics_modes) {
        const configs = [_]types.MetricsConfig{
            .{ .mode = .disabled },
            .{ .mode = .counters_only },
            .{
                .mode = .sampled_latency,
                .latency_sample_shift = cli_config.metrics_config.latency_sample_shift,
            },
            .{ .mode = .full },
        };

        for (configs, 0..) |config, index| {
            if (index != 0) try stdout.interface.print("\n", .{});
            try run_bench_suite(allocator, &stdout.interface, config);
        }
    } else {
        try run_bench_suite(allocator, &stdout.interface, cli_config.metrics_config);
    }

    try stdout.interface.flush();
}

fn parse_cli_config(allocator: std.mem.Allocator) !BenchCliConfig {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var cli = BenchCliConfig{};
    var saw_metrics_mode = false;
    var saw_latency_sample_shift = false;

    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--help")) {
            print_usage();
            std.process.exit(0);
        }
        if (std.mem.startsWith(u8, arg, "--metrics-mode=")) {
            const value = arg["--metrics-mode=".len..];
            if (std.mem.eql(u8, value, "all")) {
                cli.run_all_metrics_modes = true;
                saw_metrics_mode = true;
                continue;
            }
            cli.metrics_config.mode = parse_metrics_mode(value) orelse return error.InvalidArgument;
            saw_metrics_mode = true;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--latency-sample-shift=")) {
            const value = arg["--latency-sample-shift=".len..];
            cli.metrics_config.latency_sample_shift = std.fmt.parseUnsigned(u8, value, 10) catch return error.InvalidArgument;
            saw_latency_sample_shift = true;
            continue;
        }
        return error.InvalidArgument;
    }

    if (saw_metrics_mode and !saw_latency_sample_shift and (cli.run_all_metrics_modes or cli.metrics_config.mode == .sampled_latency)) {
        cli.metrics_config.latency_sample_shift = default_sampled_latency_shift;
    }

    return cli;
}

fn parse_metrics_mode(raw: []const u8) ?types.MetricsMode {
    if (std.mem.eql(u8, raw, "disabled")) return .disabled;
    if (std.mem.eql(u8, raw, "counters_only")) return .counters_only;
    if (std.mem.eql(u8, raw, "sampled_latency")) return .sampled_latency;
    if (std.mem.eql(u8, raw, "full")) return .full;
    return null;
}

fn print_usage() void {
    std.debug.print(
        \\usage: zeno-core-bench [--metrics-mode=disabled|counters_only|sampled_latency|full|all] [--latency-sample-shift=N]
        \\
    , .{});
}

fn run_bench_suite(
    allocator: std.mem.Allocator,
    writer: anytype,
    metrics_config: types.MetricsConfig,
) !void {
    bench_metrics_config = metrics_config;
    try init_steady_state_benches();
    defer deinit_steady_state_benches();

    var stable_bench = zbench.Benchmark.init(allocator, .{
        .max_iterations = 8_192,
        .time_budget_ns = 750 * std.time.ns_per_ms,
    });
    defer stable_bench.deinit();

    var growing_bench = zbench.Benchmark.init(allocator, .{
        .max_iterations = 8_192,
        .time_budget_ns = 750 * std.time.ns_per_ms,
    });
    defer growing_bench.deinit();

    const put_fresh = PutFreshBenchmark{};
    const put_steady = PutSteadyBenchmark{};
    const get_existing = GetExistingBenchmark{};
    const get_existing_steady = GetExistingSteadyBenchmark{};
    const scan_prefix = ScanPrefixBenchmark{};
    const scan_prefix_steady = ScanPrefixSteadyBenchmark{};
    const apply_batch = ApplyBatchBenchmark{};
    const apply_batch_steady_overwrite = ApplyBatchSteadyOverwriteBenchmark{};
    const apply_batch_steady_insert = ApplyBatchSteadyInsertBenchmark{};
    const apply_checked_batch = ApplyCheckedBatchBenchmark{};
    const apply_checked_batch_steady_overwrite = ApplyCheckedBatchSteadyOverwriteBenchmark{};
    const apply_checked_batch_steady_insert = ApplyCheckedBatchSteadyInsertBenchmark{};

    try stable_bench.addParam("put isolated", &put_fresh, .{});
    try stable_bench.addParam("put steady", &put_steady, .{});
    try stable_bench.addParam("get isolated", &get_existing, .{});
    try stable_bench.addParam("get steady", &get_existing_steady, .{});
    try stable_bench.addParam("scan256 isolated", &scan_prefix, .{});
    try stable_bench.addParam("scan256 steady", &scan_prefix_steady, .{});
    try stable_bench.addParam("batch64 isolated", &apply_batch, .{});
    try stable_bench.addParam("batch64 steady overwrite", &apply_batch_steady_overwrite, .{});
    try stable_bench.addParam("checked64 isolated", &apply_checked_batch, .{});
    try stable_bench.addParam("checked64 steady overwrite", &apply_checked_batch_steady_overwrite, .{});

    try growing_bench.addParam("batch64 growing insert", &apply_batch_steady_insert, .{});
    try growing_bench.addParam("checked64 growing insert", &apply_checked_batch_steady_insert, .{});

    try print_metrics_config(writer, metrics_config);
    try stable_bench.run(writer);
    try writer.print("\n", .{});
    try writer.print("growing workloads\n", .{});
    try growing_bench.run(writer);
}

fn print_metrics_config(writer: anytype, metrics_config: types.MetricsConfig) !void {
    switch (metrics_config.mode) {
        .sampled_latency => {
            try writer.print(
                "metrics mode: {s} (latency_sample_shift={d})\n",
                .{ @tagName(metrics_config.mode), metrics_config.latency_sample_shift },
            );
        },
        else => {
            try writer.print("metrics mode: {s}\n", .{@tagName(metrics_config.mode)});
        },
    }
}

fn load_scan_fixture(db: *engine.Database) void {
    var key_storage: [scan_item_count][16]u8 = undefined;
    for (0..scan_item_count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "scan:{d:0>4}", .{index}) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn init_steady_state_benches() !void {
    steady_put_db = try open_bench_db(std.heap.page_allocator);
    {
        const value = types.Value{ .integer = 1 };
        try steady_put_db.?.put("bench:put", &value);
    }

    steady_get_db = try open_bench_db(std.heap.page_allocator);
    {
        const value = types.Value{ .integer = 42 };
        try steady_get_db.?.put("bench:get", &value);
    }

    steady_scan_db = try open_bench_db(std.heap.page_allocator);
    load_scan_fixture(steady_scan_db.?);

    steady_batch_overwrite_db = try open_bench_db(std.heap.page_allocator);
    prime_batch_fixture(steady_batch_overwrite_db.?, "batch");

    steady_batch_insert_db = try open_bench_db(std.heap.page_allocator);
    steady_batch_insert_seed.store(0, .monotonic);

    steady_checked_batch_overwrite_db = try open_bench_db(std.heap.page_allocator);
    prime_batch_fixture(steady_checked_batch_overwrite_db.?, "guard");

    steady_checked_batch_insert_db = try open_bench_db(std.heap.page_allocator);
    steady_checked_batch_insert_seed.store(0, .monotonic);
}

fn deinit_steady_state_benches() void {
    if (steady_checked_batch_insert_db) |db| {
        db.close() catch unreachable;
        steady_checked_batch_insert_db = null;
    }
    if (steady_checked_batch_overwrite_db) |db| {
        db.close() catch unreachable;
        steady_checked_batch_overwrite_db = null;
    }
    if (steady_batch_insert_db) |db| {
        db.close() catch unreachable;
        steady_batch_insert_db = null;
    }
    if (steady_batch_overwrite_db) |db| {
        db.close() catch unreachable;
        steady_batch_overwrite_db = null;
    }
    if (steady_scan_db) |db| {
        db.close() catch unreachable;
        steady_scan_db = null;
    }
    if (steady_get_db) |db| {
        db.close() catch unreachable;
        steady_get_db = null;
    }
    if (steady_put_db) |db| {
        db.close() catch unreachable;
        steady_put_db = null;
    }
}

fn prime_batch_fixture(db: *engine.Database, prefix: []const u8) void {
    var key_storage: [batch_item_count][batch_key_storage_bytes]u8 = undefined;
    for (0..batch_item_count) |index| {
        const key = std.fmt.bufPrint(&key_storage[index], "{s}:{d:0>8}", .{ prefix, index }) catch unreachable;
        const value = types.Value{ .integer = @intCast(index) };
        db.put(key, &value) catch unreachable;
    }
}

fn fill_batch_writes(
    values: *[batch_item_count]types.Value,
    writes: *[batch_item_count]types.PutWrite,
    key_storage: *[batch_item_count][batch_key_storage_bytes]u8,
    prefix: []const u8,
    value_base: usize,
    key_base: usize,
) void {
    for (0..batch_item_count) |index| {
        values[index] = .{ .integer = @intCast(value_base + index) };
        const key = std.fmt.bufPrint(&key_storage[index], "{s}:{d:0>8}", .{ prefix, key_base + index }) catch unreachable;
        writes[index] = .{
            .key = key,
            .value = &values[index],
        };
    }
}
