use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use logoot_plus::trace_bench::{
    ContentCheck, RssStats, TimingStats, TraceStats, generate_operations_with_checks, load_trace_file,
    measure_merge_rss, merge_remote_cpu_timed_once, reload_from_disk_cpu_once, write_oplog,
};

#[cfg(feature = "mem-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    MergeTime,
    ReloadTime,
    MemRss,
    MemHeap,
    All,
}

#[derive(Clone, Debug)]
struct Config {
    input: PathBuf,
    mode: Mode,
    iterations: usize,
    check_every: Option<usize>,
    target: Target,
    output: PathBuf,
    oplog: PathBuf,
}

#[derive(Clone, Debug)]
enum Target {
    All,
    One(usize),
}

#[derive(Clone, Debug, serde::Serialize)]
struct OutputFile {
    trace_path: String,
    mode: String,
    iterations: usize,
    check_every: Option<usize>,
    trace_stats: TraceStats,
    targets: Vec<TargetResult>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TargetResult {
    target: usize,
    merge_time: Option<TimingStats>,
    reload_time: Option<TimingStats>,
    mem_rss: Option<RssStats>,
    mem_heap: Option<HeapStats>,
    content_check: Option<ContentCheck>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct HeapStats {
    enabled: bool,
    artifact_path: String,
    notes: String,
}

fn main() {
    let config = match parse_args(env::args().collect::<Vec<_>>()) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{err}");
            print_usage();
            std::process::exit(2);
        }
    };

    eprintln!(
        "[setup] loading trace from {}",
        config.input.display()
    );
    let stage_started = Instant::now();
    let trace = match load_trace_file(&config.input) {
        Ok(trace) => trace,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "[setup] loaded trace in {:.2}s: kind={} agents={} txns={}",
        stage_started.elapsed().as_secs_f64(),
        trace.kind,
        trace.num_agents,
        trace.txns.len()
    );

    eprintln!("[setup] generating operation streams and parent-aware schedule");
    let stage_started = Instant::now();
    let generated = match generate_operations_with_checks(trace, config.check_every) {
        Ok(g) => g,
        Err(err) => {
            eprintln!("failed generating operations: {err}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "[setup] generated ops in {:.2}s: total_ops={} patches={}",
        stage_started.elapsed().as_secs_f64(),
        generated.stats.op_count,
        generated.stats.patch_count
    );

    if matches!(config.mode, Mode::ReloadTime | Mode::All | Mode::MemHeap) {
        eprintln!(
            "[setup] writing operation log to {}",
            config.oplog.display()
        );
        if let Err(err) = write_oplog(&config.oplog, &generated.all_ops) {
            eprintln!("failed writing operation log: {err}");
            std::process::exit(1);
        }
    }

    let targets = match collect_targets(&config.target, generated.trace.num_agents) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };
    let mut target_results = Vec::with_capacity(targets.len());
    eprintln!(
        "[run] mode={} targets={} iterations={} (per target)",
        mode_name(config.mode),
        targets.len(),
        config.iterations
    );

    let _heap_profiler = start_heap_profiler(config.mode);

    for target in targets {
        let mut result = TargetResult {
            target,
            merge_time: None,
            reload_time: None,
            mem_rss: None,
            mem_heap: None,
            content_check: None,
        };

        if matches!(config.mode, Mode::MergeTime | Mode::All) {
            match run_with_progress(
                "merge",
                target,
                config.iterations,
                || {
                    let (elapsed_ns, check) = merge_remote_cpu_timed_once(&generated, target)?;
                    Ok(IterationResult {
                        elapsed_ns,
                        content_check: check,
                    })
                },
            ) {
                Ok((stats, check)) => {
                    result.merge_time = Some(stats);
                    result.content_check = Some(check);
                }
                Err(err) => {
                    eprintln!("merge-time failed for target {target}: {err}");
                    std::process::exit(1);
                }
            }
        }

        if matches!(config.mode, Mode::ReloadTime | Mode::All) {
            match run_with_progress(
                "reload",
                target,
                config.iterations,
                || {
                    let (elapsed_ns, check) =
                        reload_from_disk_cpu_once(&generated, &config.oplog, target)?;
                    Ok(IterationResult {
                        elapsed_ns,
                        content_check: check,
                    })
                },
            ) {
                Ok((stats, check)) => {
                    result.reload_time = Some(stats);
                    if result.content_check.is_none() {
                        result.content_check = Some(check);
                    }
                }
                Err(err) => {
                    eprintln!("reload-time failed for target {target}: {err}");
                    std::process::exit(1);
                }
            }
        }

        if matches!(config.mode, Mode::MemRss | Mode::All) {
            match measure_merge_rss(&generated, target) {
                Ok((stats, check)) => {
                    result.mem_rss = Some(stats);
                    if result.content_check.is_none() {
                        result.content_check = Some(check);
                    }
                }
                Err(err) => {
                    eprintln!("mem-rss failed for target {target}: {err}");
                    std::process::exit(1);
                }
            }
        }

        if matches!(config.mode, Mode::MemHeap | Mode::All) {
            result.mem_heap = Some(heap_result(config.mode));
        }

        if let Some(check) = &result.content_check
            && !check.matches
        {
            eprintln!(
                "content mismatch for target {}: expected {:?} observed {:?}",
                target,
                check.expected_end_content,
                check.observed_content
            );
            std::process::exit(1);
        }

        target_results.push(result);
    }

    let output = OutputFile {
        trace_path: config.input.display().to_string(),
        mode: mode_name(config.mode).to_string(),
        iterations: config.iterations,
        check_every: config.check_every,
        trace_stats: generated.stats,
        targets: target_results,
    };

    if let Some(parent) = config.output.parent()
        && !parent.as_os_str().is_empty()
        && let Err(err) = fs::create_dir_all(parent)
    {
        eprintln!("failed to create output directory {}: {err}", parent.display());
        std::process::exit(1);
    }

    let json = match serde_json::to_vec_pretty(&output) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("failed to encode output json: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = fs::write(&config.output, json) {
        eprintln!("failed to write output {}: {err}", config.output.display());
        std::process::exit(1);
    }

    println!("wrote benchmark summary to {}", config.output.display());
}

fn parse_args(args: Vec<String>) -> Result<Config, String> {
    let mut input = None::<PathBuf>;
    let mut mode = Mode::All;
    let mut iterations = 20usize;
    let mut check_every = None::<usize>;
    let mut target = Target::All;
    let mut output = PathBuf::from("results/trace_bench.json");
    let mut oplog = PathBuf::from("results/trace_ops.json");

    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                let val = args.get(i).ok_or("--input requires a path")?;
                input = Some(PathBuf::from(val));
            }
            "--mode" => {
                i += 1;
                let val = args.get(i).ok_or("--mode requires a value")?;
                mode = parse_mode(val)?;
            }
            "--iterations" => {
                i += 1;
                let val = args.get(i).ok_or("--iterations requires an integer")?;
                iterations = val
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --iterations value {val}: {e}"))?;
            }
            "--check-every" => {
                i += 1;
                let val = args.get(i).ok_or("--check-every requires an integer")?;
                let parsed = val
                    .parse::<usize>()
                    .map_err(|e| format!("invalid --check-every value {val}: {e}"))?;
                if parsed == 0 {
                    return Err("--check-every must be > 0".to_string());
                }
                check_every = Some(parsed);
            }
            "--target" => {
                i += 1;
                let val = args.get(i).ok_or("--target requires all or an index")?;
                target = if val == "all" {
                    Target::All
                } else {
                    let idx = val
                        .parse::<usize>()
                        .map_err(|e| format!("invalid --target index {val}: {e}"))?;
                    Target::One(idx)
                };
            }
            "--output" => {
                i += 1;
                let val = args.get(i).ok_or("--output requires a path")?;
                output = PathBuf::from(val);
            }
            "--oplog" => {
                i += 1;
                let val = args.get(i).ok_or("--oplog requires a path")?;
                oplog = PathBuf::from(val);
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }

        i += 1;
    }

    let input = input.ok_or("--input is required")?;

    Ok(Config {
        input,
        mode,
        iterations,
        check_every,
        target,
        output,
        oplog,
    })
}

fn parse_mode(raw: &str) -> Result<Mode, String> {
    match raw {
        "merge-time" => Ok(Mode::MergeTime),
        "reload-time" => Ok(Mode::ReloadTime),
        "mem-rss" => Ok(Mode::MemRss),
        "mem-heap" => Ok(Mode::MemHeap),
        "all" => Ok(Mode::All),
        _ => Err(format!("invalid mode {raw}")),
    }
}

fn mode_name(mode: Mode) -> &'static str {
    match mode {
        Mode::MergeTime => "merge-time",
        Mode::ReloadTime => "reload-time",
        Mode::MemRss => "mem-rss",
        Mode::MemHeap => "mem-heap",
        Mode::All => "all",
    }
}

fn collect_targets(target: &Target, num_agents: usize) -> Result<Vec<usize>, String> {
    if num_agents == 0 {
        return Err("trace has no agents".to_string());
    }

    match target {
        Target::All => Ok((0..num_agents).collect::<Vec<_>>()),
        Target::One(idx) => {
            if *idx >= num_agents {
                Err(format!(
                    "target {} out of bounds for numAgents {}",
                    idx, num_agents
                ))
            } else {
                Ok(vec![*idx])
            }
        }
    }
}

fn print_usage() {
    eprintln!(
        "trace_bench \
  --input <trace.json> \
  [--mode merge-time|reload-time|mem-rss|mem-heap|all] \
  [--iterations N] \
    [--check-every N] \
  [--target all|INDEX] \
  [--oplog results/trace_ops.json] \
  [--output results/trace_bench.json]"
    );
}

#[derive(Clone, Debug)]
struct IterationResult {
    elapsed_ns: u128,
    content_check: ContentCheck,
}

fn run_with_progress<F>(
    label: &str,
    target: usize,
    iterations: usize,
    mut run_iteration: F,
) -> Result<(TimingStats, ContentCheck), String>
where
    F: FnMut() -> Result<IterationResult, String>,
{
    if iterations == 0 {
        return Err("iterations must be > 0".to_string());
    }

    let mut min_ns = u128::MAX;
    let mut max_ns = 0u128;
    let mut total_ns = 0u128;
    let mut first_check = None;

    let started = Instant::now();

    for i in 0..iterations {
        let result = run_iteration()?;

        if result.elapsed_ns < min_ns {
            min_ns = result.elapsed_ns;
        }
        if result.elapsed_ns > max_ns {
            max_ns = result.elapsed_ns;
        }
        total_ns += result.elapsed_ns;

        if first_check.is_none() {
            first_check = Some(result.content_check.clone());
        }

        let done = i + 1;
        let avg_ns = total_ns / (done as u128);
        let remaining = iterations.saturating_sub(done);
        let eta_ns = avg_ns.saturating_mul(remaining as u128);

        let elapsed_wall = started.elapsed().as_secs_f64();
        let eta_s = (eta_ns as f64) / 1_000_000_000.0;
        let avg_ms = (avg_ns as f64) / 1_000_000.0;
        let this_ms = (result.elapsed_ns as f64) / 1_000_000.0;

        println!(
            "[{label}][target={target}] iter {done}/{iterations} this={this_ms:.3}ms avg={avg_ms:.3}ms wall={elapsed_wall:.1}s eta={eta_s:.1}s"
        );
    }

    Ok((
        TimingStats {
            iterations,
            min_ns,
            max_ns,
            mean_ns: total_ns / (iterations as u128),
        },
        first_check.expect("content check should be set for at least one iteration"),
    ))
}

#[cfg(feature = "mem-heap")]
fn start_heap_profiler(mode: Mode) -> Option<dhat::Profiler> {
    if matches!(mode, Mode::MemHeap | Mode::All) {
        Some(dhat::Profiler::new_heap())
    } else {
        None
    }
}

#[cfg(not(feature = "mem-heap"))]
fn start_heap_profiler(_mode: Mode) -> Option<()> {
    None
}

fn heap_result(mode: Mode) -> HeapStats {
    if matches!(mode, Mode::MemHeap | Mode::All) {
        #[cfg(feature = "mem-heap")]
        {
            return HeapStats {
                enabled: true,
                artifact_path: "dhat-heap.json".to_string(),
                notes: "Heap profile is written when process exits.".to_string(),
            };
        }

        #[cfg(not(feature = "mem-heap"))]
        {
            return HeapStats {
                enabled: false,
                artifact_path: "".to_string(),
                notes: "Rebuild with --features mem-heap to enable DHAT output.".to_string(),
            };
        }
    }

    HeapStats {
        enabled: false,
        artifact_path: "".to_string(),
        notes: "Heap profiling disabled for this mode.".to_string(),
    }
}

#[allow(dead_code)]
fn _exists(path: &Path) -> bool {
    path.exists()
}
