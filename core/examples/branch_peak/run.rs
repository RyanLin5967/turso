//! Peak then shrink (lane r11-ever, PREREG amendment 11; sweep rows S4, S5 and H1): what a store keeps
//! after its branch count falls from a peak, and what the fall itself costs.
//!
//!   cargo run -p turso_core --release --example branch_peak -- --peak P --live L \
//!       --checkpoints a,b,... [--reap random|oldest] [--untimed]
//!
//! 1. grow: fork P branches from the trunk, each writing its own row;
//! 2. reap-down: reap P - L of them (a uniformly random live one, or the oldest), timing every reap;
//! 3. churn at L: every cycle forks one branch from the trunk, has it write its own row and reaps a
//!    random live one; the checkpoints count cycles of this phase.
//!
//! Resident structures are read at the end of each phase and at each checkpoint through
//! `Database::branch_resident`. `--peak L` is the control: the same churn with no peak.
//! The run exits NOT A RESULT if the engine does not hold exactly the live branches the harness
//! holds, if a zombie exists, or if a read returns the wrong value.
//!
//! Lane r12-f9-shrink (PREREG section 3) added: the per-structure byte fields of `BranchResident`
//! (so this source now needs a store that has them), the process's phys_footprint and resident size
//! (`proc_pid_rusage`) and the malloc zones' bytes in use and allocated at every checkpoint, a
//! fire-check of the footprint instrument at start, and `shrink_to_fit` on the harness's own list
//! of live branches after the reap-down, so its peak capacity is not counted as the store's. The
//! body lives here so that `branch_peak` (system allocator) and `branch_peak_mi` (mimalloc) run the
//! same code.

use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{Branch, BranchResident};
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, Value, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;

struct Args {
    peak: usize,
    live: usize,
    checkpoints: Vec<usize>,
    reap_oldest: bool,
    untimed: bool,
    window: usize,
    stall_us: f64,
    seed: u64,
    /// After each post-peak checkpoint, ask the allocator to return its free memory and read again
    /// (r12-f9-shrink amendment 3, D2).
    relief: bool,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_peak: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut a = Args {
        peak: 0,
        live: 1000,
        checkpoints: vec![],
        reap_oldest: false,
        untimed: false,
        relief: false,
        window: 5000,
        stall_us: 100.0,
        seed: 0x9E37_79B9_7F4A_7C15,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        match flag.as_str() {
            "--peak" => a.peak = val().parse().unwrap_or_else(|_| die("bad --peak")),
            "--live" => a.live = val().parse().unwrap_or_else(|_| die("bad --live")),
            "--checkpoints" => {
                a.checkpoints = val()
                    .split(',')
                    .map(|x| x.parse().unwrap_or_else(|_| die("bad --checkpoints")))
                    .collect()
            }
            "--reap" => {
                a.reap_oldest = match val().as_str() {
                    "random" => false,
                    "oldest" => true,
                    o => die(&format!("unknown --reap {o}")),
                }
            }
            "--window" => a.window = val().parse().unwrap_or_else(|_| die("bad --window")),
            "--stall-us" => a.stall_us = val().parse().unwrap_or_else(|_| die("bad --stall-us")),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--untimed" => a.untimed = true,
            "--relief" => a.relief = true,
            o => die(&format!("unknown argument {o}")),
        }
    }
    if a.live < 2 || a.peak < a.live || a.checkpoints.is_empty() {
        die("need --live >= 2, --peak >= --live and --checkpoints");
    }
    if a.checkpoints.windows(2).any(|w| w[0] >= w[1]) {
        die("--checkpoints must be strictly increasing");
    }
    a
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn row_for(n: usize) -> i64 {
    ((n as u64).wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

/// The `tag`-th branch's value: fixed length, so every write is in place.
fn branch_value(tag: u64) -> String {
    format!("b{tag:0>width$}", width = VALUE_LEN - 1)
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

/// `(phys_footprint, resident_size)` from `proc_pid_rusage`. The footprint counts compressed
/// pages, so the compressor does not move it; resident size does. `None` off macOS or on error.
#[cfg(target_os = "macos")]
fn footprint() -> Option<(u64, u64)> {
    // SAFETY: rusage_info_v4 is plain data, and proc_pid_rusage writes at most one of it.
    let mut info: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            &mut info as *mut libc::rusage_info_v4 as *mut libc::rusage_info_t,
        )
    };
    (rc == 0).then_some((info.ri_phys_footprint, info.ri_resident_size))
}

#[cfg(not(target_os = "macos"))]
fn footprint() -> Option<(u64, u64)> {
    None
}

/// `(bytes in use, bytes allocated)` summed over every malloc zone: what live objects take, and
/// what the zones hold from the OS. Under mimalloc as the global allocator Rust's heap is not in
/// any zone, so these then cover only the system zone.
#[cfg(target_os = "macos")]
fn malloc_zones() -> Option<(usize, usize)> {
    let mut s = libc::malloc_statistics_t {
        blocks_in_use: 0,
        size_in_use: 0,
        max_size_in_use: 0,
        size_allocated: 0,
    };
    // SAFETY: a null zone asks for the sum over all zones; `s` is a valid out-parameter.
    unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), &mut s) };
    Some((s.size_in_use, s.size_allocated))
}

#[cfg(not(target_os = "macos"))]
fn malloc_zones() -> Option<(usize, usize)> {
    None
}

fn mem_fields() -> String {
    let (fp, res) = footprint().unwrap_or((0, 0));
    let (in_use, allocated) = malloc_zones().unwrap_or((0, 0));
    format!(
        "footprint_bytes={fp} resident_bytes={res} rss_bytes={} malloc_in_use={in_use} \
         malloc_allocated={allocated}",
        rss_bytes()
    )
}

/// Fire-check of the footprint instrument (PREREG section 3c): map 64 MiB, touch every 4 KiB,
/// purge it the way the arena does, touch it again, unmap it, and print the footprint and resident
/// deltas after each step. A footprint that does not move by ~64 MiB on touch and back on purge
/// would make every footprint number below NOT A RESULT.
#[cfg(unix)]
fn instrument_fire_check() -> String {
    const N: usize = 64 << 20;
    let read = || footprint().unwrap_or((0, 0));
    let d = |a: (u64, u64), b: (u64, u64)| (b.0 as i64 - a.0 as i64, b.1 as i64 - a.1 as i64);
    let touch = |p: *mut u8| {
        for off in (0..N).step_by(4096) {
            // SAFETY: `p` maps N writable bytes.
            unsafe { p.add(off).write_volatile(1) };
        }
    };
    let base = read();
    // SAFETY: an anonymous private mapping, unmapped below; nothing else refers to it.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            N,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return "# instrument fire-check: mmap failed".into();
    }
    let p = p as *mut u8;
    touch(p);
    let touched = read();
    #[cfg(target_os = "macos")]
    let (purge, reuse) = (libc::MADV_FREE_REUSABLE, Some(libc::MADV_FREE_REUSE));
    #[cfg(not(target_os = "macos"))]
    let (purge, reuse) = (libc::MADV_DONTNEED, None::<libc::c_int>);
    // SAFETY: the range is the mapping above.
    let rc_purge = unsafe { libc::madvise(p as *mut libc::c_void, N, purge) };
    let purged = read();
    let rc_reuse = reuse.map_or(0, |r| unsafe { libc::madvise(p as *mut libc::c_void, N, r) });
    touch(p);
    let retouched = read();
    // SAFETY: unmaps exactly the mapping above.
    let rc_unmap = unsafe { libc::munmap(p as *mut libc::c_void, N) };
    let unmapped = read();
    format!(
        "# instrument fire-check (64 MiB = 67108864 B; deltas from before the map, footprint/resident): \
         touched={:?} purged={:?} retouched={:?} unmapped={:?} rc_purge={rc_purge} rc_reuse={rc_reuse} \
         rc_unmap={rc_unmap}",
        d(base, touched),
        d(base, purged),
        d(base, retouched),
        d(base, unmapped)
    )
}

#[cfg(not(unix))]
fn instrument_fire_check() -> String {
    "# instrument fire-check: not on this platform".into()
}

struct Live {
    branch: Branch,
    row: i64,
    tag: u64,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[((p / 100.0) * (sorted.len() - 1) as f64).round() as usize]
}

fn summary(name: &str, v: &mut Vec<f64>) -> String {
    if v.is_empty() {
        return format!("{name}_n=0");
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    format!(
        "{name}_n={} {name}_p50={:.2} {name}_p99={:.2} {name}_max={:.1}",
        v.len(),
        pct(v, 50.0),
        pct(v, 99.0),
        v[v.len() - 1]
    )
}

fn resident_line(label: &str, n_ever: usize, live: usize, r: &BranchResident) -> String {
    format!(
        "# ckpt phase={label} n_ever={n_ever} live={live} states={} zombies={} table_capacity={} \
         next_id={} arena_high_water={} arena_in_use={} arena_free_list_len={} \
         arena_free_list_capacity={} arena_free_bits_words={} arena_chunks={} page_map_nodes={} \
         arena_chunks_mapped={} arena_resident_bytes={} arena_meta_bytes={} arena_purges={} \
         arena_reuses={} arena_chunk_maps={} arena_chunk_unmaps={} table_value_bytes={} \
         table_index_bytes={} table_entry_bytes={} table_value_chunks={} {}",
        r.states,
        r.zombies,
        r.table_capacity,
        r.next_id,
        r.arena_high_water,
        r.arena_in_use,
        r.arena_free_list_len,
        r.arena_free_list_capacity,
        r.arena_free_bits_words,
        r.arena_chunks,
        r.page_map_nodes,
        r.arena_chunks_mapped,
        r.arena_resident_bytes,
        r.arena_meta_bytes,
        r.arena_purges,
        r.arena_reuses,
        r.arena_chunk_maps,
        r.arena_chunk_unmaps,
        r.table_value_bytes,
        r.table_index_bytes,
        r.table_entry_bytes,
        r.table_value_chunks,
        mem_fields()
    )
}

/// `relieve` asks the global allocator to hand its free memory back to the OS and returns what it
/// reports (bytes, where it reports any).
pub fn main(allocator: &str, relieve: fn() -> usize) {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_peak.db");
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let trunk = db.connect().unwrap();
    trunk.execute("PRAGMA synchronous = NORMAL").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!(
                "INSERT INTO t VALUES ({id}, '{:0>width$}')",
                id,
                width = VALUE_LEN
            ))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    println!("# branch_peak — Turso fork, r11-ever PREREG amendment 11; r12-f9-shrink PREREG section 3");
    println!(
        "# peak={} live={} checkpoints={:?} reap_oldest={} untimed={} window={} stall_us={} seed={:#x} \
         build={} allocator={allocator} rss_base_bytes={}",
        args.peak,
        args.live,
        args.checkpoints,
        args.reap_oldest,
        args.untimed,
        args.window,
        args.stall_us,
        args.seed,
        if cfg!(debug_assertions) { "DEBUG" } else { "release" },
        rss_bytes()
    );
    println!("{}", instrument_fire_check());
    println!("# base {}", mem_fields());
    let mut rng = Rng(args.seed);
    let mut created = 0usize;
    let fork_one = |created: usize| -> Live {
        let branch = trunk.fork_branch().unwrap();
        let row = row_for(created);
        let tag = created as u64 + 1;
        let conn = branch.connect().unwrap();
        conn.execute(format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(tag)))
            .unwrap();
        drop(conn);
        Live { branch, row, tag }
    };
    let timed = |untimed: bool, v: &mut Vec<f64>, f: &mut dyn FnMut()| {
        if untimed {
            f();
        } else {
            let t = Instant::now();
            f();
            v.push(t.elapsed().as_secs_f64() * 1e6);
        }
    };

    // 1. grow to the peak, timing every fork.
    let mut live: std::collections::VecDeque<Live> = std::collections::VecDeque::new();
    let mut grow_fork = Vec::new();
    let mut grow_stalls = Vec::new();
    let t0 = Instant::now();
    while live.len() < args.peak {
        let mut l = None;
        let n = created;
        timed(args.untimed, &mut grow_fork, &mut || l = Some(fork_one(n)));
        created += 1;
        if let Some(&us) = grow_fork.last() {
            if us >= args.stall_us && grow_stalls.len() < 2000 {
                grow_stalls.push((created, us));
            }
        }
        live.push_back(l.unwrap());
    }
    let grow_s = t0.elapsed().as_secs_f64();
    let r = db.branch_resident();
    if r.states != args.peak || r.zombies != 0 {
        not_a_result(&format!("after growth the engine holds {} states, {} zombies", r.states, r.zombies));
    }
    println!("{}", resident_line("grown", created, live.len(), &r));

    // 2. reap down to L, timing every reap. The i-th reap (1-based) pushes the free list to length
    //    i + (free slots before): a Vec doubling copies it at every power of two.
    let mut reap_down = Vec::new();
    let mut reap_stalls = Vec::new();
    let t1 = Instant::now();
    let mut reaped = 0usize;
    while live.len() > args.live {
        let victim = if args.reap_oldest {
            live.pop_front().unwrap()
        } else {
            let k = rng.below(live.len());
            live.swap_remove_back(k).unwrap()
        };
        let mut res = None;
        let mut vb = Some(victim.branch);
        timed(args.untimed, &mut reap_down, &mut || {
            res = Some(vb.take().unwrap().reap().unwrap())
        });
        reaped += 1;
        let res = res.unwrap();
        if res.freed_pages != 1 || res.deferred {
            not_a_result(&format!("a reap-down reap freed {res:?}"));
        }
        if let Some(&us) = reap_down.last() {
            if us >= args.stall_us && reap_stalls.len() < 2000 {
                reap_stalls.push((reaped, us));
            }
        }
    }
    let reap_s = t1.elapsed().as_secs_f64();
    // The harness's own peak-sized buffers are not the store's: give them back before measuring.
    live.shrink_to_fit();
    let grow_summary = summary("grow_branch", &mut grow_fork);
    drop(grow_fork);
    let reap_summary = summary("reap", &mut reap_down);
    drop(reap_down);
    let r = db.branch_resident();
    if r.states != args.live || r.zombies != 0 || r.arena_in_use != args.live {
        not_a_result(&format!("after the reap-down: {r:?}"));
    }
    println!("{}", resident_line("reaped", created, live.len(), &r));
    if args.relief {
        let returned = relieve();
        let r = db.branch_resident();
        println!("{} relief_returned={returned}", resident_line("reaped_relief", created, live.len(), &r));
    }
    println!(
        "# phase grow seconds={grow_s:.1} {grow_summary} (fork+connect+write per branch) stalls_ge={} \
         listed={:?}",
        args.stall_us, grow_stalls
    );
    println!(
        "# phase reapdown seconds={reap_s:.2} reaped={reaped} {reap_summary} stalls_ge={} listed={:?}",
        args.stall_us, reap_stalls
    );

    // 3. churn at L.
    let names = ["fork", "open", "write", "reap", "read_own"];
    let mut ops: [Vec<f64>; 5] = Default::default();
    let mut max_all = [0f64; 5];
    let mut cycles = 0usize;
    let mut buf_reads = 0u64;
    for &ckpt in &args.checkpoints {
        let window_start = ckpt.saturating_sub(args.window);
        while cycles < ckpt {
            let in_window = cycles >= window_start;
            let mut samples: [Option<f64>; 5] = [None; 5];
            let time = |i: usize, f: &mut dyn FnMut(), s: &mut [Option<f64>; 5]| {
                if args.untimed {
                    f();
                } else {
                    let t = Instant::now();
                    f();
                    s[i] = Some(t.elapsed().as_secs_f64() * 1e6);
                }
            };
            let mut branch = None;
            time(0, &mut || branch = Some(trunk.fork_branch().unwrap()), &mut samples);
            let branch = branch.unwrap();
            let row = row_for(created);
            let tag = created as u64 + 1;
            created += 1;
            let mut conn = None;
            time(1, &mut || conn = Some(branch.connect().unwrap()), &mut samples);
            let conn = conn.unwrap();
            let sql = format!("UPDATE t SET v = '{}' WHERE id = {row}", branch_value(tag));
            time(2, &mut || conn.execute(&sql).map(|_| ()).unwrap(), &mut samples);
            drop(conn);
            let k = rng.below(live.len());
            let victim = live.swap_remove_back(k).unwrap();
            live.push_back(Live { branch, row, tag });
            let mut vb = Some(victim.branch);
            time(
                3,
                &mut || {
                    vb.take().unwrap().reap().unwrap();
                },
                &mut samples,
            );
            if cycles % 10 == 0 {
                let t = &live[rng.below(live.len())];
                let conn = t.branch.connect().unwrap();
                let mut got = String::new();
                time(4, &mut || {
                    let mut stmt = conn
                        .prepare(format!("SELECT v FROM t WHERE id = {}", t.row))
                        .unwrap();
                    let rows = stmt.run_collect_rows().unwrap();
                    got = match rows.as_slice() {
                        [row] => match &row[0] {
                            Value::Text(s) => s.as_str().to_string(),
                            o => not_a_result(&format!("expected text, got {o:?}")),
                        },
                        _ => not_a_result("a read returned no single row"),
                    };
                }, &mut samples);
                if got != branch_value(t.tag) {
                    not_a_result(&format!("a branch misread its own row {}", t.row));
                }
                buf_reads += 1;
            }
            for (i, s) in samples.iter().enumerate() {
                if let Some(us) = *s {
                    max_all[i] = max_all[i].max(us);
                    if in_window {
                        ops[i].push(us);
                    }
                }
            }
            cycles += 1;
        }
        let r = db.branch_resident();
        if r.states != args.live || r.zombies != 0 || r.arena_in_use != args.live {
            not_a_result(&format!("churn checkpoint {ckpt}: {r:?}"));
        }
        println!("{}", resident_line("churn", created, live.len(), &r));
        if args.relief {
            let returned = relieve();
            let r = db.branch_resident();
            println!("{} relief_returned={returned}", resident_line("churn_relief", created, live.len(), &r));
        }
        if !args.untimed {
            for (i, v) in ops.iter_mut().enumerate() {
                if v.is_empty() {
                    continue;
                }
                v.sort_by(|a, b| a.partial_cmp(b).unwrap());
                println!(
                    "{created}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{}",
                    names[i],
                    v.len(),
                    pct(v, 50.0),
                    pct(v, 90.0),
                    pct(v, 99.0),
                    v[v.len() - 1],
                    max_all[i],
                    cycles
                );
                v.clear();
            }
        }
    }
    println!("# churn cycles={cycles} reads_checked={buf_reads}");
    let t2 = Instant::now();
    drop(live);
    let end = db.branch_stats();
    println!(
        "# teardown seconds={:.3} end {end:?}",
        t2.elapsed().as_secs_f64()
    );
    println!("# end {}", mem_fields());
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
}
