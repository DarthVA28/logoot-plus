//! Local-side benchmarking for logoot-plus (choice-B semantics).
//!
//! All agents are timed in a single replay pass. N runs = N replays total
//! (not N × agents). Per-agent timings are bucketed separately.
//!
//! For each txn (in topological order):
//!   1. Apply patches on `docs[txn.agent]`, TIMED.
//!   2. Broadcast resulting WireDelta(s) to every other doc, UNTIMED.
//!
//! Untimed: ASCII sanitisation (once up front), bounds check, utf16 fallback,
//! `ins` payload clone, and broadcast to peers.
//!
//! Use `--target-agent N` to filter output to one agent (all are still timed).
//! Default: report all agents.

use std::collections::VecDeque;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use logoot_plus::document::Document;
use logoot_plus::trace_bench::{Patch, TraceFile, load_trace_file};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    PerOp,
    PerTxn,
}

#[derive(Clone, Debug)]
struct Config {
    input: PathBuf,
    mode: Mode,
    output: PathBuf,
    include_samples: bool,
    target_agent: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct SampleStats {
    count: usize,
    total_ms: f64,
    min_ms: f64,
    max_ms: f64,
    mean_ms: f64,
    median_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ContentCheck {
    #[serde(skip)]
    expected: String,
    #[serde(skip)]
    observed: String,
    matches: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
struct OutputRecord {
    trace_path: String,
    crdt: &'static str,
    mode: String,
    trace_kind: String,
    num_agents: usize,
    target_agent: usize,
    num_txns: usize,
    op_count: usize,
    total_replay_ms: f64,
    stats: SampleStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    samples_ns: Option<Vec<u128>>,
    content_check: ContentCheck,
}

// ---- ASCII sanitisation --------------------------------------------------

fn ascii_replace_preserving_utf16(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii() {
            out.push(c);
        } else {
            for _ in 0..c.len_utf16() {
                out.push('?');
            }
        }
    }
    out
}

fn sanitize_to_ascii(trace: &mut TraceFile) {
    for txn in &mut trace.txns {
        for patch in &mut txn.patches {
            if !patch.2.is_ascii() {
                patch.2 = ascii_replace_preserving_utf16(&patch.2);
            }
        }
    }
    if !trace.end_content.is_ascii() {
        trace.end_content = ascii_replace_preserving_utf16(&trace.end_content);
    }
}

// ---- Per-agent sample collector ------------------------------------------

struct AgentSamples {
    /// Per-op ns samples for this agent.
    samples: Vec<u128>,
    /// Running total ns for the current txn (used for per-txn aggregation).
    txn_ns: u128,
    /// Index into `samples` at the start of the current txn.
    txn_start: usize,
    /// Total ops attributed to this agent.
    op_count: usize,
}

impl AgentSamples {
    fn new() -> Self {
        AgentSamples { samples: Vec::new(), txn_ns: 0, txn_start: 0, op_count: 0 }
    }
    fn begin_txn(&mut self) {
        self.txn_start = self.samples.len();
        self.txn_ns = 0;
    }
    fn record(&mut self, ns: u128) {
        self.samples.push(ns);
        self.txn_ns += ns;
        self.op_count += 1;
    }
    fn end_txn_per_txn(&mut self) {
        // Collapse this txn's per-op samples into a single per-txn sample.
        self.samples.truncate(self.txn_start);
        if self.txn_ns > 0 {
            self.samples.push(self.txn_ns);
        }
    }
}

// -------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = match parse_args(args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            print_usage();
            std::process::exit(2);
        }
    };

    eprintln!("[setup] loading trace from {}", config.input.display());
    let load_started = Instant::now();
    let mut trace = match load_trace_file(&config.input) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    sanitize_to_ascii(&mut trace);
    eprintln!(
        "[setup] loaded in {:.2}s: kind={} agents={} txns={}",
        load_started.elapsed().as_secs_f64(),
        trace.kind,
        trace.num_agents,
        trace.txns.len()
    );

    if let Some(t) = config.target_agent {
        if t >= trace.num_agents {
            eprintln!("target agent {} out of range (trace has {} agents)", t, trace.num_agents);
            std::process::exit(1);
        }
    }

    eprintln!("[run] mode={} scheduling...", mode_name(config.mode));
    let sched_started = Instant::now();
    let order = match schedule_txns(&trace) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "[run] scheduled {} txns in {:.2}s",
        order.len(),
        sched_started.elapsed().as_secs_f64()
    );

    eprintln!("[run] replaying (all agents timed in one pass)...");
    let replay_started = Instant::now();
    let outcome = match replay(&trace, &order, config.mode) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let total_replay_ns = replay_started.elapsed().as_nanos();
    let total_replay_ms = (total_replay_ns as f64) / 1_000_000.0;

    eprintln!("[done] total_replay={:.3}s matches={}", total_replay_ms / 1000.0, outcome.content_check.matches);

    if !outcome.content_check.matches {
        eprintln!("[warn] did not converge to endContent (check skipped for now)");
    }

    // Build per-agent output records.
    let report_agents: Vec<usize> = match config.target_agent {
        Some(t) => vec![t],
        None => (0..trace.num_agents).collect(),
    };

    let mut records: Vec<OutputRecord> = Vec::with_capacity(report_agents.len());
    for agent in &report_agents {
        let agent = *agent;
        let agent_data = &outcome.agent_samples[agent];
        let stats = compute_stats(&agent_data.samples);

        eprintln!(
            "  agent={} ops={} total={:.4}ms median={:.4}ms",
            agent, agent_data.op_count, stats.total_ms, stats.median_ms,
        );

        let samples_ns = if config.include_samples {
            Some(agent_data.samples.clone())
        } else {
            None
        };

        records.push(OutputRecord {
            trace_path: config.input.display().to_string(),
            crdt: "dls",
            mode: mode_name(config.mode).to_string(),
            trace_kind: trace.kind.clone(),
            num_agents: trace.num_agents,
            target_agent: agent,
            num_txns: trace.txns.len(),
            op_count: agent_data.op_count,
            total_replay_ms,
            stats,
            samples_ns,
            content_check: outcome.content_check.clone(),
        });
    }

    if let Some(parent) = config.output.parent()
        && !parent.as_os_str().is_empty()
        && let Err(e) = fs::create_dir_all(parent)
    {
        eprintln!("failed to create output dir {}: {e}", parent.display());
        std::process::exit(1);
    }

    let bytes = if records.len() == 1 {
        serde_json::to_vec_pretty(&records[0])
    } else {
        serde_json::to_vec_pretty(&records)
    };
    let bytes = match bytes {
        Ok(b) => b,
        Err(e) => {
            eprintln!("failed to serialise output: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = fs::write(&config.output, bytes) {
        eprintln!("failed to write {}: {e}", config.output.display());
        std::process::exit(1);
    }
    eprintln!("[write] wrote {}", config.output.display());
}

struct ReplayOutcome {
    agent_samples: Vec<AgentSamples>,
    content_check: ContentCheck,
}

fn replay(
    trace: &TraceFile,
    order: &[usize],
    mode: Mode,
) -> Result<ReplayOutcome, String> {
    let n = trace.num_agents;
    if n == 0 {
        return Err("numAgents must be > 0".to_string());
    }

    let mut docs: Vec<Document> = (0..n)
        .map(|i| {
            let mut d = Document::new(i as u32);
            d.set_replica(i as u32);
            d
        })
        .collect();

    let mut agent_samples: Vec<AgentSamples> = (0..n).map(|_| AgentSamples::new()).collect();

    for &txn_idx in order {
        let txn = &trace.txns[txn_idx];
        if txn.agent >= n {
            return Err(format!(
                "txn {} agent {} out of bounds for numAgents {}",
                txn_idx, txn.agent, n
            ));
        }

        let acting = txn.agent;
        agent_samples[acting].begin_txn();

        for patch in &txn.patches {
            apply_patch_timed(
                &mut docs,
                acting,
                txn_idx,
                patch,
                &mut agent_samples[acting],
            )?;
        }

        if mode == Mode::PerTxn {
            agent_samples[acting].end_txn_per_txn();
        }
    }

    let observed = docs[0].read();
    let matches = observed == trace.end_content;
    Ok(ReplayOutcome {
        agent_samples,
        content_check: ContentCheck {
            expected: trace.end_content.clone(),
            observed,
            matches,
        },
    })
}

/// Apply a single patch on `acting`'s document, TIMED, and broadcast to
/// every other document (untimed).
fn apply_patch_timed(
    docs: &mut [Document],
    acting: usize,
    txn_idx: usize,
    patch: &Patch,
    samples: &mut AgentSamples,
) -> Result<(), String> {
    let pos_utf16 = patch.0;
    let del_len_utf16 = patch.1;
    let ins = &patch.2;

    let mut pos = pos_utf16;
    let mut del_len = del_len_utf16;
    let doc_size_before = docs[acting].blocks.tree_size();
    let mut to = pos.saturating_add(del_len);
    let mut converted_from_utf16 = false;
    if pos > doc_size_before || to > doc_size_before {
        let content = docs[acting].read();
        let from = utf16_to_char_index(&content, pos_utf16);
        let conv_to = utf16_to_char_index(&content, pos_utf16.saturating_add(del_len_utf16));
        pos = from;
        del_len = conv_to.saturating_sub(from);
        to = pos.saturating_add(del_len);
        converted_from_utf16 = true;
    }

    if del_len > 0 {
        if pos > doc_size_before || to > doc_size_before {
            return Err(format!(
                "invalid delete range in txn {}: {}..{} while doc size is {} (raw utf16 {}..{}, utf16_conversion_attempted={})",
                txn_idx, pos, to, doc_size_before,
                pos_utf16, pos_utf16.saturating_add(del_len_utf16), converted_from_utf16
            ));
        }
        let start = Instant::now();
        let op = docs[acting].del(pos, to);
        samples.record(start.elapsed().as_nanos());

        for (i, other) in docs.iter_mut().enumerate() {
            if i == acting { continue; }
            other.apply_remote_op(&op);
        }
    }

    if !ins.is_empty() {
        let text = ins.clone();
        let start = Instant::now();
        let op = docs[acting].ins(pos, text);
        samples.record(start.elapsed().as_nanos());

        if let Some(op) = op {
            for (i, other) in docs.iter_mut().enumerate() {
                if i == acting { continue; }
                other.apply_remote_op(&op);
            }
        }
    }

    Ok(())
}

fn utf16_to_char_index(text: &str, utf16_index: usize) -> usize {
    let mut utf16_count = 0usize;
    let mut char_count = 0usize;
    for ch in text.chars() {
        let next = utf16_count + ch.len_utf16();
        if next > utf16_index { break; }
        utf16_count = next;
        char_count += 1;
    }
    char_count
}

fn schedule_txns(trace: &TraceFile) -> Result<Vec<usize>, String> {
    let n = trace.txns.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![vec![]; n];
    for (idx, txn) in trace.txns.iter().enumerate() {
        for &parent in &txn.parents {
            if parent >= n {
                return Err(format!("txn {} references parent {} out of bounds (len={})", idx, parent, n));
            }
            in_degree[idx] += 1;
            dependents[parent].push(idx);
        }
    }
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);
    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        for &dep in &dependents[idx] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 { queue.push_back(dep); }
        }
    }
    if order.len() != n {
        return Err("cyclic or unsatisfied parents".to_string());
    }
    Ok(order)
}

fn compute_stats(samples: &[u128]) -> SampleStats {
    if samples.is_empty() {
        return SampleStats { count: 0, total_ms: 0.0, min_ms: 0.0, max_ms: 0.0,
            mean_ms: 0.0, median_ms: 0.0, p95_ms: 0.0, p99_ms: 0.0 };
    }
    let mut sorted: Vec<u128> = samples.to_vec();
    sorted.sort_unstable();
    let count = sorted.len();
    let total_ns: u128 = sorted.iter().sum();
    let ns_to_ms = |ns: u128| (ns as f64) / 1_000_000.0;
    let p95_idx = ((count as f64) * 0.95) as usize;
    let p99_idx = ((count as f64) * 0.99) as usize;
    SampleStats {
        count,
        total_ms: ns_to_ms(total_ns),
        min_ms: ns_to_ms(sorted[0]),
        max_ms: ns_to_ms(sorted[count - 1]),
        mean_ms: ns_to_ms(total_ns / (count as u128)),
        median_ms: ns_to_ms(sorted[count / 2]),
        p95_ms: ns_to_ms(sorted[p95_idx.min(count - 1)]),
        p99_ms: ns_to_ms(sorted[p99_idx.min(count - 1)]),
    }
}

fn parse_args(args: Vec<String>) -> Result<Config, String> {
    let mut input: Option<PathBuf> = None;
    let mut mode: Option<Mode> = None;
    let mut output: Option<PathBuf> = None;
    let mut include_samples = true;
    let mut target_agent: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "--input" => { i += 1; input = Some(PathBuf::from(args.get(i).ok_or("--input needs a value")?)); }
            "--mode" => { i += 1; let m = args.get(i).ok_or("--mode needs a value")?; mode = Some(parse_mode(m)?); }
            "--output" => { i += 1; output = Some(PathBuf::from(args.get(i).ok_or("--output needs a value")?)); }
            "--no-samples" => { include_samples = false; }
            "--samples" => { include_samples = true; }
            "--target-agent" => {
                i += 1;
                let raw = args.get(i).ok_or("--target-agent needs a value")?;
                target_agent = Some(raw.parse().map_err(|e| format!("invalid --target-agent {raw}: {e}"))?);
            }
            "-h" | "--help" => { print_usage(); std::process::exit(0); }
            _ => return Err(format!("unknown arg: {a}")),
        }
        i += 1;
    }
    Ok(Config {
        input: input.ok_or("--input is required")?,
        mode: mode.ok_or("--mode is required (per-op | per-txn)")?,
        output: output.unwrap_or_else(|| PathBuf::from("results/local_bench.json")),
        include_samples,
        target_agent,
    })
}

fn parse_mode(raw: &str) -> Result<Mode, String> {
    match raw { "per-op" => Ok(Mode::PerOp), "per-txn" => Ok(Mode::PerTxn),
        _ => Err(format!("invalid mode {raw} (expected per-op|per-txn)")) }
}
fn mode_name(m: Mode) -> &'static str {
    match m { Mode::PerOp => "per-op", Mode::PerTxn => "per-txn" }
}
fn print_usage() {
    eprintln!("local_bench --input <trace.json> --mode per-op|per-txn [--target-agent N] [--output path.json] [--no-samples]");
}