use std::path::PathBuf;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use logoot_plus::trace_bench::{
    generate_operations, load_trace_file, measure_merge_remote_cpu, measure_reload_from_disk_cpu,
    write_oplog,
};

fn trace_input_path() -> PathBuf {
    std::env::var("TRACE_INPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("traces/sample_trace.json"))
}

fn benchmark_merge_and_reload(c: &mut Criterion) {
    let input = trace_input_path();
    let trace = load_trace_file(&input).expect("trace json should parse");
    let generated = generate_operations(trace).expect("trace operations should generate");

    let oplog_path = PathBuf::from("results/criterion_trace_ops.json");
    write_oplog(&oplog_path, &generated.all_ops).expect("oplog should write");

    let mut merge_group = c.benchmark_group("trace_merge_remote");
    for target in 0..generated.trace.num_agents {
        merge_group.bench_with_input(BenchmarkId::new("target", target), &target, |b, &target| {
            b.iter(|| {
                let _ = measure_merge_remote_cpu(&generated, target, 1)
                    .expect("merge measurement should succeed");
            });
        });
    }
    merge_group.finish();

    let mut reload_group = c.benchmark_group("trace_reload_from_disk");
    for target in 0..generated.trace.num_agents {
        reload_group.bench_with_input(BenchmarkId::new("target", target), &target, |b, &target| {
            b.iter(|| {
                let _ = measure_reload_from_disk_cpu(&generated, &oplog_path, target, 1)
                    .expect("reload measurement should succeed");
            });
        });
    }
    reload_group.finish();
}

criterion_group!(benches, benchmark_merge_and_reload);
criterion_main!(benches);
