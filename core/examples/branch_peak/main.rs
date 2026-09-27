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
//! `Database::branch_resident`, using only the fields the unfixed store (11b61bf44) already has, so
//! one source builds against every store. `--peak L` is the control: the same churn with no peak.
//! The run exits NOT A RESULT if the engine does not hold exactly the live branches the harness
//! holds, if a zombie exists, or if a read returns the wrong value.
//!
//! r11-ever amendment 34 (instrument I and its arms; every flag off reproduces the run above):
//!   --phases       time each reap-down reap's phases in the store, with getrusage around every reap
//!                  (outside the timed window); list each reap >= stall_us with its split
//!   --null         after every reap-down reap, time a null op (256 xorshift steps, no memory)
//!   --mem          malloc_zone_statistics at each phase end and churn checkpoint (macOS)
//!   --entry-pages  the distinct pages holding the live states' table entries and `current` maps
//!   --arm B|G|M|R|P|T  G: park the reap-down's states and leak them; R: reserve the free lists to
//!                  --peak first; P: touch each churn victim's entry (untimed) before its timed reap;
//!                  T: shrink the table after the reap-down; M: a label for a DYLD_INSERT_LIBRARIES run.

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
    phases: bool,
    null: bool,
    mem: bool,
    entry_pages: bool,
    arm: char,
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
        window: 5000,
        stall_us: 100.0,
        seed: 0x9E37_79B9_7F4A_7C15,
        phases: false,
        null: false,
        mem: false,
        entry_pages: false,
        arm: 'B',
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
            "--phases" => a.phases = true,
            "--null" => a.null = true,
            "--mem" => a.mem = true,
            "--entry-pages" => a.entry_pages = true,
            "--arm" => {
                a.arm = match val().as_str() {
                    "B" => 'B',
                    "G" => 'G',
                    "M" => 'M',
                    "R" => 'R',
                    "P" => 'P',
                    "T" => 'T',
                    o => die(&format!("unknown --arm {o}")),
                }
            }
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

/// Process-wide (minflt, majflt, nvcsw, nivcsw) from getrusage (amendment 34).
fn rusage() -> [i64; 4] {
    // SAFETY: getrusage fills one plain struct that it is handed.
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) };
    [
        u.ru_minflt as i64,
        u.ru_majflt as i64,
        u.ru_nvcsw as i64,
        u.ru_nivcsw as i64,
    ]
}

fn delta(a: [i64; 4], b: [i64; 4]) -> [i64; 4] {
    [b[0] - a[0], b[1] - a[1], b[2] - a[2], b[3] - a[3]]
}

fn utc_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

fn os_page_size() -> usize {
    // SAFETY: sysconf reads a constant.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p <= 0 {
        die("sysconf(_SC_PAGESIZE) failed");
    }
    p as usize
}

/// Amendment 34's null op: 256 xorshift steps on a register, no memory.
fn null_op(x: u64) -> u64 {
    let mut x = x | 1;
    for _ in 0..256 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
    }
    std::hint::black_box(x)
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Default)]
struct MallocStatistics {
    blocks_in_use: u32,
    size_in_use: usize,
    max_size_in_use: usize,
    size_allocated: usize,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn malloc_zone_statistics(zone: *mut std::ffi::c_void, stats: *mut MallocStatistics);
}

/// malloc_zone_statistics over every zone (amendment 34, --mem).
fn zone_line(label: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        let mut st = MallocStatistics::default();
        // SAFETY: a null zone asks for every zone's totals; the struct matches malloc/malloc.h.
        unsafe { malloc_zone_statistics(std::ptr::null_mut(), &mut st) };
        format!(
            "# zone phase={label} blocks_in_use={} size_in_use={} max_size_in_use={} size_allocated={}",
            st.blocks_in_use, st.size_in_use, st.max_size_in_use, st.size_allocated
        )
    }
    #[cfg(not(target_os = "macos"))]
    format!("# zone phase={label} unavailable")
}

fn entry_pages_line(db: &Database, label: &str, page: usize) -> String {
    let (entries, maps) = db.branch_live_entry_pages(page);
    format!("# entry_pages phase={label} page_size={page} entries={entries} maps={maps}")
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
         rss_bytes={}",
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
        rss_bytes()
    )
}

fn main() {
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
    println!("# branch_peak — Turso fork, r11-ever PREREG amendment 11");
    println!(
        "# peak={} live={} checkpoints={:?} reap_oldest={} untimed={} window={} stall_us={} seed={:#x} \
         build={} rss_base_bytes={}",
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
    let page = os_page_size();
    println!(
        "# amendment34 arm={} phases={} null={} mem={} entry_pages={} page_size={page} \
         dyld_insert={:?}",
        args.arm,
        args.phases,
        args.null,
        args.mem,
        args.entry_pages,
        std::env::var("DYLD_INSERT_LIBRARIES").ok()
    );
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
    if args.mem {
        println!("{}", zone_line("grown"));
    }
    if args.entry_pages {
        println!("{}", entry_pages_line(&db, "grown", page));
    }
    println!(
        "# phase grow seconds={grow_s:.1} {} (fork+connect+write per branch) stalls_ge={} listed={:?}",
        summary("grow_branch", &mut grow_fork),
        args.stall_us,
        grow_stalls
    );

    // 2. reap down to L, timing every reap. The i-th reap (1-based) pushes the free list to length
    //    i + (free slots before): a Vec doubling copies it at every power of two.
    let mut reap_down = Vec::new();
    let mut reap_stalls = Vec::new();
    match args.arm {
        'G' => {
            db.branch_set_graveyard(Some(args.peak));
        }
        'R' => db.branch_reserve_free(args.peak),
        _ => {}
    }
    if args.phases {
        db.branch_set_reap_phases(true);
    }
    // Amendment 34: each reap >= stall_us with its phase split and rusage deltas; the null op's.
    let mut split_stalls: Vec<String> = Vec::new();
    let mut null_times = Vec::new();
    let mut null_stalls: Vec<String> = Vec::new();
    let mut phase_sums = [0u64; 5];
    let mut ru_sum = [0i64; 4];
    let mut null_seed = args.seed;
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
        let ru_a = args.phases.then(rusage);
        timed(args.untimed, &mut reap_down, &mut || {
            res = Some(vb.take().unwrap().reap().unwrap())
        });
        let ru_d = ru_a.map(|a| delta(a, rusage()));
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
        if let Some(d) = ru_d {
            let ph = db.branch_last_reap_phases();
            for (s, p) in phase_sums.iter_mut().zip(ph) {
                *s += p;
            }
            for (s, x) in ru_sum.iter_mut().zip(d) {
                *s += x;
            }
            if let Some(&us) = reap_down.last() {
                if us >= args.stall_us && split_stalls.len() < 2000 {
                    let rest = us * 1e3 - ph.iter().sum::<u64>() as f64;
                    split_stalls.push(format!(
                        "# stall reap idx={reaped} us={us:.3} lock_ns={} remove_ns={} release_ns={} \
                         child_gone_ns={} drop_ns={} rest_ns={rest:.0} minflt={} majflt={} nvcsw={} \
                         nivcsw={} utc_ms={}",
                        ph[0], ph[1], ph[2], ph[3], ph[4], d[0], d[1], d[2], d[3], utc_ms()
                    ));
                }
            }
        }
        if args.null {
            let ru_a = rusage();
            let t = Instant::now();
            null_seed = null_op(null_seed);
            let us = t.elapsed().as_secs_f64() * 1e6;
            let d = delta(ru_a, rusage());
            null_times.push(us);
            if us >= args.stall_us && null_stalls.len() < 2000 {
                null_stalls.push(format!(
                    "# stall null idx={reaped} us={us:.3} minflt={} majflt={} nvcsw={} nivcsw={} utc_ms={}",
                    d[0], d[1], d[2], d[3], utc_ms()
                ));
            }
        }
    }
    let reap_s = t1.elapsed().as_secs_f64();
    if args.phases {
        db.branch_set_reap_phases(false);
    }
    let leaked = if args.arm == 'G' { db.branch_leak_graveyard() } else { 0 };
    if args.arm == 'T' && !db.branch_shrink_table() {
        not_a_result("arm T: this store cannot rebuild its branch table");
    }
    let r = db.branch_resident();
    if r.states != args.live || r.zombies != 0 || r.arena_in_use != args.live {
        not_a_result(&format!("after the reap-down: {r:?}"));
    }
    println!("{}", resident_line("reaped", created, live.len(), &r));
    if args.phases {
        println!(
            "# reapdown phases_sum_ns lock={} remove={} release={} child_gone={} drop={} \
             rusage minflt={} majflt={} nvcsw={} nivcsw={} graveyard_leaked={leaked}",
            phase_sums[0], phase_sums[1], phase_sums[2], phase_sums[3], phase_sums[4],
            ru_sum[0], ru_sum[1], ru_sum[2], ru_sum[3]
        );
        for l in &split_stalls {
            println!("{l}");
        }
    } else if args.arm == 'G' {
        println!("# reapdown graveyard_leaked={leaked}");
    }
    if args.null {
        println!("# reapdown null {} listed={}", summary("null", &mut null_times), null_stalls.len());
        for l in &null_stalls {
            println!("{l}");
        }
    }
    if args.mem {
        println!("{}", zone_line("reaped"));
    }
    if args.entry_pages {
        println!("{}", entry_pages_line(&db, "reaped", page));
    }
    println!(
        "# phase reapdown seconds={reap_s:.2} reaped={reaped} {} stalls_ge={} listed={:?}",
        summary("reap", &mut reap_down),
        args.stall_us,
        reap_stalls
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
            if args.arm == 'P' {
                std::hint::black_box(db.branch_touch(&victim.branch));
            }
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
        if args.mem {
            println!("{}", zone_line("churn"));
        }
        if args.entry_pages {
            println!("{}", entry_pages_line(&db, "churn", page));
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
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
}
