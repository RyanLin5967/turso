//! r11-adversarial cross-check (PREREG amendment 7): the W8 / W8b / W9 worst-case inputs, run
//! against another lane's store (r11-ever's F7, r11-bushy's F-rodeh) through the same store-level
//! wrapper as `branch_adv`. Prints integer counts only: branch states and arena slots held, the
//! reap's report, and a read check. It asserts nothing about the store's policy (whether a
//! release is deferred, whether a state is kept); that is what it measures.
//!
//! On the composed store (merged from turso r11-adv-x-f7fix 192ee35ad) `StoreBench` is a VOLATILE
//! store in the F7 splice arm, the composed store's port of r11-ever's F7 (see `branch::bench`),
//! and a write commits the pages it first-writes. `branch_adv` is not in this tree.
//!
//!   cargo run -p turso_core --release --example branch_w8x -- --arm chainow --list 100,1000

use turso_core::branch::bench::StoreBench;
use turso_core::branch::{BranchId, BranchStats, Reaped};

fn die(msg: &str) -> ! {
    eprintln!("NOT A RESULT: {msg}");
    std::process::exit(2)
}

fn fork_trunk(s: &StoreBench) -> BranchId {
    s.fork_trunk().unwrap_or_else(|e| die(&format!("fork_trunk: {e}")))
}

fn fork_branch(s: &StoreBench, p: BranchId) -> BranchId {
    s.fork_branch(p).unwrap_or_else(|e| die(&format!("fork_branch: {e}")))
}

fn reap(s: &StoreBench, id: BranchId) -> Reaped {
    s.reap(id).unwrap_or_else(|e| die(&format!("reap: {e}")))
}

fn stats(s: &StoreBench) -> BranchStats {
    s.stats().unwrap_or_else(|e| die(&format!("stats: {e}")))
}

/// W8 (`writes` false), W8 with distinct pages (`same` false) and W8b (every link overwrites page 0).
fn chain(d: usize, writes: bool, same: bool, page_size: usize) {
    let s = StoreBench::new(page_size);
    let mut prev = fork_trunk(&s);
    let mut deferred = 0usize;
    for j in 0..d {
        if writes {
            let page = if same { 0 } else { j as u32 };
            s.branch_write(prev, &[page])
                .unwrap_or_else(|e| die(&format!("write: {e}")));
        }
        let next = fork_branch(&s, prev);
        deferred += usize::from(reap(&s, prev).deferred);
        prev = next;
    }
    let st = stats(&s);
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
    let r = reap(&s, prev);
    let after = stats(&s);
    println!(
        "chain d={d} writes={writes} same_page={same} states_before={} held_before={} \
         releases_deferred={deferred} leaf_freed={} states_after={} held_after={}",
        st.live_branches,
        st.arena_slots_in_use,
        r.freed_pages,
        after.live_branches,
        after.arena_slots_in_use
    );
}

/// W9: k parents each fork two children and then (or, with `pre`, first) write 10 pages, then die.
fn deadfork(k: usize, pre: bool, page_size: usize) {
    let s = StoreBench::new(page_size);
    let pages: Vec<u32> = (1..=10).collect();
    let mut kids = Vec::with_capacity(2 * k);
    let mut deferred = 0usize;
    for _ in 0..k {
        let p = fork_trunk(&s);
        if pre {
            s.branch_write(p, &pages).unwrap_or_else(|e| die(&format!("write: {e}")));
        }
        kids.push(fork_branch(&s, p));
        kids.push(fork_branch(&s, p));
        if !pre {
            s.branch_write(p, &pages).unwrap_or_else(|e| die(&format!("write: {e}")));
        }
        deferred += usize::from(reap(&s, p).deferred);
    }
    let st = stats(&s);
    println!(
        "deadfork K={k} W=10 pre={pre} in_use={} live_branches={} written={} releases_deferred={deferred}",
        st.arena_slots_in_use,
        st.live_branches,
        k * 10
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
    println!("# branch_w8x arm={arm} list={list:?} page_size=64");
    for &n in &list {
        match arm.as_str() {
            "chain" => chain(n, false, false, 64),
            "chainw" => chain(n, true, false, 64),
            "chainow" => chain(n, true, true, 64),
            "deadfork" => deadfork(n, false, 64),
            "deadfork_pre" => deadfork(n, true, 64),
            other => die(&format!("unknown arm {other}")),
        }
    }
    println!("# done");
}
