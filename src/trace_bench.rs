use std::fs;
use std::path::Path;
use std::time::Instant;
use std::collections::BTreeSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::document::Document;
use crate::operation::Operation;
use crate::LogootSplitSystem;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct TraceFile {
    pub kind: String,
    #[serde(rename = "endContent")]
    pub end_content: String,
    #[serde(rename = "numAgents")]
    pub num_agents: usize,
    pub txns: Vec<TraceTxn>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct TraceTxn {
    pub parents: Vec<usize>,
    #[serde(rename = "numChildren")]
    pub num_children: usize,
    pub agent: usize,
    pub time: String,
    pub patches: Vec<Patch>,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Patch(pub usize, pub usize, pub String);

#[derive(Clone, Debug, serde::Serialize)]
pub struct TraceStats {
    pub txn_count: usize,
    pub patch_count: usize,
    pub op_count: usize,
}

#[derive(Clone, Debug)]
pub struct GeneratedTrace {
    pub trace: TraceFile,
    pub local_ops: Vec<Vec<Operation>>,
    pub remote_ops: Vec<Vec<Operation>>,
    pub all_ops: Vec<Operation>,
    pub stats: TraceStats,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TimingStats {
    pub iterations: usize,
    pub min_ns: u128,
    pub max_ns: u128,
    pub mean_ns: u128,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ContentCheck {
    pub expected_end_content: String,
    pub observed_content: String,
    pub matches: bool,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RssStats {
    pub start_bytes: Option<u64>,
    pub peak_bytes: Option<u64>,
    pub end_bytes: Option<u64>,
}

pub fn load_trace_file(path: &Path) -> Result<TraceFile, String> {
    let bytes = fs::read(path).map_err(|e| format!("failed to read trace file {}: {e}", path.display()))?;
    serde_json::from_slice::<TraceFile>(&bytes)
        .map_err(|e| format!("failed to parse trace json {}: {e}", path.display()))
}

pub fn generate_operations(trace: TraceFile) -> Result<GeneratedTrace, String> {
    generate_operations_with_checks(trace, None)
}

pub fn generate_operations_for_targets_with_checks(
    trace: TraceFile,
    check_every_txns: Option<usize>,
    interested_targets: &[usize],
) -> Result<GeneratedTrace, String> {
    generate_operations_impl(trace, check_every_txns, Some(interested_targets))
}

pub fn generate_operations_with_checks(
    trace: TraceFile,
    check_every_txns: Option<usize>,
) -> Result<GeneratedTrace, String> {
    generate_operations_impl(trace, check_every_txns, None)
}

fn generate_operations_impl(
    trace: TraceFile,
    check_every_txns: Option<usize>,
    interested_targets: Option<&[usize]>,
) -> Result<GeneratedTrace, String> {
    if trace.num_agents == 0 {
        return Err("numAgents must be > 0".to_string());
    }

    if let Some(n) = check_every_txns
        && n == 0
    {
        return Err("check_every_txns must be > 0 when provided".to_string());
    }

    let mut target_mask = vec![true; trace.num_agents];
    if let Some(targets) = interested_targets {
        target_mask.fill(false);
        for target in targets {
            if *target >= trace.num_agents {
                return Err(format!(
                    "interested target {} out of bounds for numAgents {}",
                    target, trace.num_agents
                ));
            }
            target_mask[*target] = true;
        }
    }

    let mut system = LogootSplitSystem::new(trace.num_agents);

    let mut local_ops = vec![Vec::<Operation>::new(); trace.num_agents];
    let mut remote_ops = vec![Vec::<Operation>::new(); trace.num_agents];
    let mut all_ops = Vec::<Operation>::new();

    let mut patch_count = 0usize;
    let order = schedule_txns(&trace)?;
    let mut processed_txns = 0usize;

    let ancestor_map: Vec<Vec<usize>> = (0..trace.txns.len())
    .map(|i| ancestor_agents_for_txn(&trace, i))
    .collect();


    for txn_idx in order {
        let txn = &trace.txns[txn_idx];
        if txn.agent >= trace.num_agents {
            return Err(format!(
                "txn agent index {} out of bounds for numAgents {}",
                txn.agent, trace.num_agents
            ));
        }

        let sender = txn.agent;

        // Bring sender replica up to the full causal frontier (including transitive parent deps).
        // let ancestor_agents = ancestor_agents_for_txn(&trace, txn_idx);
        for agent in &ancestor_map[txn_idx] {
            if *agent != sender {
                safe_merge_from(
                    &mut system,
                    sender,
                    *agent,
                    format!("while preparing txn {}", txn_idx),
                )?;
            }
        }

        for patch in &txn.patches {
            patch_count += 1;
            apply_patch_to_sender(
                &mut system,
                txn_idx,
                sender,
                patch,
                &target_mask,
                &mut local_ops,
                &mut remote_ops,
                &mut all_ops,
            )?;
        }

        processed_txns += 1;
        if let Some(n) = check_every_txns
            && processed_txns % n == 0
        {
            run_periodic_sync_all_check(&system, processed_txns, txn_idx)?;
        }
    }

    let stats = TraceStats {
        txn_count: trace.txns.len(),
        patch_count,
        op_count: all_ops.len(),
    };

    // Final convergence check
    let num_agents = system.network.documents.len();
    system.network.sync_all();
    let expected = system.network.documents[0].read();
    for i in 1..num_agents {
        let content = system.network.documents[i].read();
        assert_eq!(content, expected, 
            "convergence check failed: replica {} diverged from replica 0", i);
    }

    println!("convergence check passed!");

    Ok(GeneratedTrace {
        trace,
        local_ops,
        remote_ops,
        all_ops,
        stats,
    })
}

fn run_periodic_sync_all_check(
    system: &LogootSplitSystem,
    processed_txns: usize,
    last_txn_idx: usize,
) -> Result<(), String> {
    let mut probe_network = system.network.clone();
    probe_network.sync_all();

    let mut expected = None::<String>;
    let mut expected_site = None::<u32>;

    for doc in &mut probe_network.documents {
        let site = doc.site_id();
        let content = doc.read();

        if let Some(ref exp) = expected {
            // use assert equal to get a diff on mismatch, but provide a custom error message with more context
            assert_eq!(&content, exp, "periodic sync_all check failed after {} txns (last scheduled txn {}): replica {} diverged from replica {}", processed_txns, last_txn_idx, site, expected_site.unwrap_or(site));
        } else {
            expected_site = Some(site);
            expected = Some(content);
        }
    }

    Ok(())
}

pub fn merge_remote_cpu_once(generated: &GeneratedTrace, target: usize) -> Result<ContentCheck, String> {
    let (_, check) = merge_remote_cpu_timed_once(generated, target)?;
    Ok(check)
}

pub fn merge_remote_cpu_timed_once(
    generated: &GeneratedTrace,
    target: usize,
) -> Result<(u128, ContentCheck), String> {
    let start = Instant::now();
    let mut doc = build_local_state(generated, target)?;
    for op in &generated.remote_ops[target] {
        doc.apply_op(op);
    }
    let elapsed = start.elapsed().as_nanos();

    let observed = doc.read();
    Ok((elapsed, ContentCheck {
        expected_end_content: generated.trace.end_content.clone(),
        matches: observed == generated.trace.end_content,
        observed_content: observed,
    }))
}

pub fn measure_merge_remote_cpu(
    generated: &GeneratedTrace,
    target: usize,
    iterations: usize,
) -> Result<(TimingStats, ContentCheck), String> {
    if iterations == 0 {
        return Err("iterations must be > 0".to_string());
    }

    validate_target(generated, target)?;

    let mut min_ns = u128::MAX;
    let mut max_ns = 0u128;
    let mut total_ns = 0u128;

    let mut content_check = None;

    for _ in 0..iterations {
        let (elapsed, check) = merge_remote_cpu_timed_once(generated, target)?;

        if elapsed < min_ns {
            min_ns = elapsed;
        }
        if elapsed > max_ns {
            max_ns = elapsed;
        }
        total_ns += elapsed;

        if content_check.is_none() {
            content_check = Some(check);
        }
    }

    Ok((
        TimingStats {
            iterations,
            min_ns,
            max_ns,
            mean_ns: total_ns / (iterations as u128),
        },
        content_check.expect("content check set"),
    ))
}

pub fn write_oplog(path: &Path, ops: &[Operation]) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create directory {}: {e}", parent.display()))?;
    }

    let bytes = serde_json::to_vec_pretty(ops)
        .map_err(|e| format!("failed to serialize operation log: {e}"))?;
    fs::write(path, bytes)
        .map_err(|e| format!("failed to write operation log {}: {e}", path.display()))
}

pub fn measure_reload_from_disk_cpu(
    generated: &GeneratedTrace,
    ops_path: &Path,
    target: usize,
    iterations: usize,
) -> Result<(TimingStats, ContentCheck), String> {
    if iterations == 0 {
        return Err("iterations must be > 0".to_string());
    }

    validate_target(generated, target)?;

    let mut min_ns = u128::MAX;
    let mut max_ns = 0u128;
    let mut total_ns = 0u128;

    let mut content_check = None;

    for _ in 0..iterations {
        let (elapsed, check) = reload_from_disk_cpu_once(generated, ops_path, target)?;

        if elapsed < min_ns {
            min_ns = elapsed;
        }
        if elapsed > max_ns {
            max_ns = elapsed;
        }
        total_ns += elapsed;

        if content_check.is_none() {
            content_check = Some(check);
        }
    }

    Ok((
        TimingStats {
            iterations,
            min_ns,
            max_ns,
            mean_ns: total_ns / (iterations as u128),
        },
        content_check.expect("content check set"),
    ))
}

pub fn reload_from_disk_cpu_once(
    generated: &GeneratedTrace,
    ops_path: &Path,
    target: usize,
) -> Result<(u128, ContentCheck), String> {
    validate_target(generated, target)?;

    let start = Instant::now();
    let bytes = fs::read(ops_path)
        .map_err(|e| format!("failed to read operation log {}: {e}", ops_path.display()))?;
    let ops = serde_json::from_slice::<Vec<Operation>>(&bytes)
        .map_err(|e| format!("failed to deserialize operation log {}: {e}", ops_path.display()))?;

    let mut doc = Document::new(target as u32);
    for op in &ops {
        doc.apply_op(op);
    }

    let elapsed = start.elapsed().as_nanos();
    let observed = doc.read();
    let check = ContentCheck {
        expected_end_content: generated.trace.end_content.clone(),
        matches: observed == generated.trace.end_content,
        observed_content: observed,
    };

    Ok((elapsed, check))
}

pub fn measure_merge_rss(generated: &GeneratedTrace, target: usize) -> Result<(RssStats, ContentCheck), String> {
    validate_target(generated, target)?;

    let mut doc = build_local_state(generated, target)?;

    let start = read_rss_bytes();
    let mut peak = start;

    for op in &generated.remote_ops[target] {
        doc.apply_op(op);
        let current = read_rss_bytes();
        if let (Some(cur), Some(p)) = (current, peak)
            && cur > p
        {
            peak = Some(cur);
        }
        if peak.is_none() {
            peak = current;
        }
    }

    let end = read_rss_bytes();

    let observed = doc.read();
    let check = ContentCheck {
        expected_end_content: generated.trace.end_content.clone(),
        matches: observed == generated.trace.end_content,
        observed_content: observed,
    };

    Ok((
        RssStats {
            start_bytes: start,
            peak_bytes: peak,
            end_bytes: end,
        },
        check,
    ))
}

fn apply_patch_to_sender(
    system: &mut LogootSplitSystem,
    txn_idx: usize,
    sender: usize,
    patch: &Patch,
    target_mask: &[bool],
    local_ops: &mut [Vec<Operation>],
    remote_ops: &mut [Vec<Operation>],
    all_ops: &mut Vec<Operation>,
) -> Result<(), String> {
    let sender_u32 = sender as u32;
    let sender_idx = system.network.index_of(sender_u32);

    let pos_utf16 = patch.0;
    let del_len_utf16 = patch.1;
    let ins = &patch.2;
    let mut pos = pos_utf16;
    let mut del_len = del_len_utf16;

    // Fast path: most traces already use Rust-char-compatible coordinates.
    // Fallback to UTF-16 conversion only when bounds are invalid.
    let doc_size_before = {
        let doc = &system.network.documents[sender_idx];
        doc.blocks.tree_size()
    };
    let mut to = pos.saturating_add(del_len);
    let mut converted_from_utf16 = false;
    if pos > doc_size_before || to > doc_size_before {
        let (from, conv_to) = {
            let doc = &mut system.network.documents[sender_idx];
            let content = doc.read();
            let from = utf16_to_char_index(&content, pos_utf16);
            let conv_to = utf16_to_char_index(&content, pos_utf16.saturating_add(del_len_utf16));
            (from, conv_to)
        };
        pos = from;
        del_len = conv_to.saturating_sub(from);
        to = pos.saturating_add(del_len);
        converted_from_utf16 = true;
    }

    if del_len > 0 {
        if pos > doc_size_before || to > doc_size_before {
            return Err(format!(
                "invalid delete range in txn {} (agent={}): {}..{} while doc size is {} (raw utf16 {}..{}, utf16_conversion_attempted={})",
                txn_idx,
                sender,
                pos,
                to,
                doc_size_before,
                pos_utf16,
                pos_utf16.saturating_add(del_len_utf16),
                converted_from_utf16
            ));
        }
        let op = {
            let doc = &mut system.network.documents[sender_idx];
            doc.del(pos, to)
        };
        system.network.broadcast(op.clone(), sender_u32);
        record_op(op, sender, target_mask, local_ops, remote_ops, all_ops);
    }

    if !ins.is_empty() {
        let maybe_op = {
            let doc = &mut system.network.documents[sender_idx];
            doc.ins(pos, ins.clone())
        };
        if let Some(op) = maybe_op {
            system.network.broadcast(op.clone(), sender_u32);
            record_op(op, sender, target_mask, local_ops, remote_ops, all_ops);
        }
    }

    Ok(())
}

fn utf16_to_char_index(text: &str, utf16_index: usize) -> usize {
    let mut utf16_count = 0usize;
    let mut char_count = 0usize;

    for ch in text.chars() {
        let next = utf16_count + ch.len_utf16();
        if next > utf16_index {
            break;
        }
        utf16_count = next;
        char_count += 1;
    }

    char_count
}

fn record_op(
    op: Operation,
    sender: usize,
    target_mask: &[bool],
    local_ops: &mut [Vec<Operation>],
    remote_ops: &mut [Vec<Operation>],
    all_ops: &mut Vec<Operation>,
) {
    if target_mask[sender] {
        local_ops[sender].push(op.clone());
    }
    for (target, ops) in remote_ops.iter_mut().enumerate() {
        if target != sender && target_mask[target] {
            ops.push(op.clone());
        }
    }
    all_ops.push(op);
}

fn build_local_state(generated: &GeneratedTrace, target: usize) -> Result<Document, String> {
    validate_target(generated, target)?;

    let mut doc = Document::new(target as u32);
    for op in &generated.local_ops[target] {
        doc.apply_op(op);
    }
    Ok(doc)
}

fn safe_merge_from(
    system: &mut LogootSplitSystem,
    into: usize,
    from: usize,
    context: String,
) -> Result<(), String> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        system.merge_from(into as u32, from as u32);
    }));

    if result.is_err() {
        return Err(format!(
            "merge panic: merge_from({}, {}) {}",
            into, from, context
        ));
    }
    Ok(())
}

// fn schedule_txns(trace: &TraceFile) -> Result<Vec<usize>, String> {
//     let n = trace.txns.len();
//     for (idx, txn) in trace.txns.iter().enumerate() {
//         for parent in &txn.parents {
//             if *parent >= n {
//                 return Err(format!(
//                     "txn {} has out-of-bounds parent {} for txn_count {}",
//                     idx, parent, n
//                 ));
//             }
//         }
//     }

//     let mut done = vec![false; n];
//     let mut order = Vec::with_capacity(n);

//     while order.len() < n {
//         let mut progressed = false;
//         for (idx, txn) in trace.txns.iter().enumerate() {
//             if done[idx] {
//                 continue;
//             }
//             if txn.parents.iter().all(|p| done[*p]) {
//                 done[idx] = true;
//                 order.push(idx);
//                 progressed = true;
//             }
//         }

//         if !progressed {
//             let blocked = trace
//                 .txns
//                 .iter()
//                 .enumerate()
//                 .filter(|(idx, _)| !done[*idx])
//                 .map(|(idx, _)| idx.to_string())
//                 .collect::<Vec<_>>()
//                 .join(",");
//             return Err(format!(
//                 "unable to schedule transactions due to cyclic or unsatisfied parents; blocked txns: [{}]",
//                 blocked
//             ));
//         }
//     }

//     Ok(order)
// }

fn schedule_txns(trace: &TraceFile) -> Result<Vec<usize>, String> {
    let n = trace.txns.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![vec![]; n];

    for (idx, txn) in trace.txns.iter().enumerate() {
        for &parent in &txn.parents {
            in_degree[idx] += 1;
            dependents[parent].push(idx);
        }
    }

    let mut queue: std::collections::VecDeque<usize> = 
        (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(n);

    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        for &dep in &dependents[idx] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.push_back(dep);
            }
        }
    }

    if order.len() != n {
        return Err("cyclic or unsatisfied parents".to_string());
    }
    Ok(order)
}

fn ancestor_agents_for_txn(trace: &TraceFile, txn_idx: usize) -> Vec<usize> {
    let mut seen_txns = vec![false; trace.txns.len()];
    let mut stack = trace.txns[txn_idx].parents.clone();
    let mut agents = BTreeSet::<usize>::new();

    while let Some(parent_idx) = stack.pop() {
        if seen_txns[parent_idx] {
            continue;
        }
        seen_txns[parent_idx] = true;

        let parent_txn = &trace.txns[parent_idx];
        agents.insert(parent_txn.agent);
        for ancestor in &parent_txn.parents {
            stack.push(*ancestor);
        }
    }

    agents.into_iter().collect::<Vec<_>>()
}

fn validate_target(generated: &GeneratedTrace, target: usize) -> Result<(), String> {
    if target >= generated.trace.num_agents {
        return Err(format!(
            "target {} out of bounds for numAgents {}",
            target, generated.trace.num_agents
        ));
    }
    Ok(())
}

fn read_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if !line.starts_with("VmRSS:") {
            continue;
        }

        let parts = line.split_whitespace().collect::<Vec<_>>();
        if parts.len() < 2 {
            return None;
        }

        let kb = parts[1].parse::<u64>().ok()?;
        return Some(kb.saturating_mul(1024));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{Patch, TraceFile, TraceTxn, schedule_txns, utf16_to_char_index};

    #[test]
    fn schedules_txns_by_parents() {
        let trace = TraceFile {
            kind: "concurrent".to_string(),
            end_content: "abc".to_string(),
            num_agents: 2,
            txns: vec![
                TraceTxn {
                    parents: vec![1],
                    num_children: 0,
                    agent: 1,
                    time: "t2".to_string(),
                    patches: vec![Patch(1, 0, "b".to_string())],
                },
                TraceTxn {
                    parents: vec![],
                    num_children: 1,
                    agent: 0,
                    time: "t1".to_string(),
                    patches: vec![Patch(0, 0, "a".to_string())],
                },
            ],
        };

        let order = schedule_txns(&trace).expect("scheduler should succeed");
        assert_eq!(order, vec![1, 0]);
    }

    #[test]
    fn utf16_index_conversion_handles_surrogates() {
        let s = "a😀b";
        assert_eq!(utf16_to_char_index(s, 0), 0);
        assert_eq!(utf16_to_char_index(s, 1), 1);
        assert_eq!(utf16_to_char_index(s, 2), 1);
        assert_eq!(utf16_to_char_index(s, 3), 2);
        assert_eq!(utf16_to_char_index(s, 4), 3);
    }
}
