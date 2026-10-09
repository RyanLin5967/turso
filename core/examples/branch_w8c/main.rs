//! r11-adversarial Q7 counter build (PREREG amendment 11): branch_w8x's chain arms against another lane's
//! store (r11-ever's F7), printing the store's work counters and this thread's CPU time split by call, to
//! attribute the growth of chainw's cost with d. Integer counts and CPU times only; it asserts nothing about
//! the store's policy.
//!
//!   cargo run -p turso_core --release --example branch_w8c -- --arm chainw --list 10000
//!
//! On this store (resolve-vol-bushy-ever, merging r11-adv-x-f7fix): `StoreBench` opens a VOLATILE store
//! in the F7 SPLICE ARM (off by default here; the lane's store spliced unconditionally), and a
//! branch write is a reservation plus a commit, as the pager makes it. The store's calls return
//! `Result`; every failure exits NOT A RESULT through `die`.

use turso_core::branch::bench::StoreBench;
use turso_core::branch::{BranchId, BranchStats, Reaped};

fn die(msg: &str) -> ! {
    eprintln!("NOT A RESULT: {msg}");
    std::process::exit(2)
}

fn thread_cpu_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid out-pointer for the duration of the call.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        die("clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    }
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

fn fork_trunk(s: &StoreBench) -> BranchId {
    s.fork_trunk().unwrap_or_else(|e| die(&format!("fork_trunk: {e}")))
}

fn fork_branch(s: &StoreBench, p: BranchId) -> BranchId {
    s.fork_branch(p).unwrap_or_else(|e| die(&format!("fork_branch: {e}")))
}

fn open(page_size: usize) -> StoreBench {
    StoreBench::new(page_size).unwrap_or_else(|e| die(&format!("open: {e}")))
}

fn reap(s: &StoreBench, id: BranchId) -> Reaped {
    s.reap(id).unwrap_or_else(|e| die(&format!("reap: {e}")))
}

fn stats(s: &StoreBench) -> BranchStats {
    s.stats().unwrap_or_else(|e| die(&format!("stats: {e}")))
}

/// W8 (`writes` false), W8 with distinct pages (`same` false) and W8b (every link overwrites page 0), as in
/// branch_w8x, with each call bracketed by the thread CPU clock. The clock reads sit outside the calls.
fn chain(d: usize, writes: bool, same: bool, page_size: usize) {
    let s = open(page_size);
    let mut prev = fork_trunk(&s);
    let mut deferred = 0usize;
    let (mut write_ns, mut fork_ns, mut reap_ns) = (0u64, 0u64, 0u64);
    let loop_start = thread_cpu_ns();
    for j in 0..d {
        let t0 = thread_cpu_ns();
        if writes {
            let page = if same { 0 } else { j as u32 };
            s.branch_write(prev, &[page])
                .unwrap_or_else(|e| die(&format!("write: {e}")));
        }
        let t1 = thread_cpu_ns();
        let next = fork_branch(&s, prev);
        let t2 = thread_cpu_ns();
        deferred += usize::from(reap(&s, prev).deferred);
        let t3 = thread_cpu_ns();
        write_ns += t1 - t0;
        fork_ns += t2 - t1;
        reap_ns += t3 - t2;
        prev = next;
    }
    let loop_ns = thread_cpu_ns() - loop_start;
    let st = stats(&s);
    let resolve_start = thread_cpu_ns();
    if writes {
        let mut buf = vec![0u8; page_size];
        let pages: Vec<u32> = if same {
            vec![0]
        } else {
            (0..d as u32).step_by((d / 1000).max(1)).collect()
        };
        for p in pages {
            if !s
                .resolve_into(prev, p, &mut buf)
                .unwrap_or_else(|e| die(&format!("resolve: {e}")))
            {
                die("the leaf does not see an ancestor's page");
            }
        }
    }
    let resolve_ns = thread_cpu_ns() - resolve_start;
    let r = reap(&s, prev);
    let after = stats(&s);
    let w = st.work;
    println!(
        "chain d={d} writes={writes} same_page={same} states_before={} held_before={} \
         releases_deferred={deferred} leaf_freed={} states_after={} held_after={} \
         splices={} splice_commits={} splice_entries={} view_build_entries={} resolve_calls={} \
         cpu_loop_us={} cpu_write_us={} cpu_fork_us={} cpu_reap_us={} cpu_resolve_us={}",
        st.live_branches,
        st.arena_slots_in_use,
        r.freed_pages,
        after.live_branches,
        after.arena_slots_in_use,
        w.splices,
        w.splice_commits,
        w.splice_entries,
        w.view_build_entries,
        after.work.resolve_calls - w.resolve_calls,
        loop_ns / 1000,
        write_ns / 1000,
        fork_ns / 1000,
        reap_ns / 1000,
        resolve_ns / 1000
    );
}

fn main() {
    let (mut arm, mut list) = (String::new(), Vec::<usize>::new());
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--arm" => arm = val,
            "--list" => {
                list = val
                    .split(',')
                    .map(|x| x.parse().unwrap_or_else(|_| die("bad --list")))
                    .collect()
            }
            _ => die(&format!("unknown flag {flag}")),
        }
    }
    println!("# branch_w8c arm={arm} list={list:?} page_size=64");
    for &n in &list {
        match arm.as_str() {
            "chain" => chain(n, false, false, 64),
            "chainw" => chain(n, true, false, 64),
            "chainow" => chain(n, true, true, 64),
            other => die(&format!("unknown arm {other}")),
        }
    }
    println!("# done");
}
