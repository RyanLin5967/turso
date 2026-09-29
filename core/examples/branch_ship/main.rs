//! r11-ship harness: moving a branch store that holds N live branches.
//! Specification: artie-research frontier/round11/r11-ship/PREREG.md.
//!
//!   cargo run -p turso_core --release --example branch_ship -- --arm flat|bushy|chain --n N [options]
//!
//! One process per N. Grow to N live branches -> T0: full sends (every mode counted, the fixed one
//! piped into an in-process replica) -> window W1 (fixed size) -> T1: incremental sends -> window
//! W2 (N/10 cycles) -> T2: incremental sends, a full send, and exports. `chain` grows one chain of
//! depth N and stops after T0.
//!
//! Instruments, none of which is the send code itself:
//! - the incremental MINIMUM is the diff of two full-state dumps (content hashes, metadata entries);
//! - the replica must equal the source by a digest of the whole observable state, at every point;
//! - sampled branches are exported from the source AND the replica, must be byte-identical, and a
//!   few are opened with real SQLite (rusqlite): integrity_check, and every row checked against
//!   this harness's own model of what that branch sees.
//! Any mismatch prints `NOT A RESULT` and exits 1.
//!
//! `--plant flip|tomb|ref|inherit` plants a defect (a flipped payload byte, a dropped tombstone, a
//! corrupted reference hash, a replica that skips deriving page maps); the run must then print
//! `NOT A RESULT`. That is the fire-check of the checks above.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::time::Instant;

use turso_core::branch::{
    Branch, Digest, Plant, Replica, SendMode, SendReport, ShipDump, ShipSnap, TrunkImage,
    CURRENT_ENTRY_BYTES, RETAINED_ENTRY_BYTES, STATE_HEADER_BYTES, WRITTEN_ENTRY_BYTES,
};
use turso_core::{Connection, Database, DatabaseOpts, OpenFlags, PlatformIO, SqliteDialect, IO};

const TRUNK_ROWS: i64 = 20_000;
const VALUE_LEN: usize = 100;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arm {
    Flat,
    Bushy,
    Chain,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PlantArg {
    None,
    Flip,
    Tomb,
    Ref,
    Inherit,
}

#[derive(Clone)]
struct Args {
    arm: Arm,
    n: usize,
    w1: usize,
    w2: Option<usize>,
    trunk_every: usize,
    seed: u64,
    plant: PlantArg,
    exports: usize,
    samples: usize,
    delta: bool,
    out_dir: Option<PathBuf>,
    /// Time the fixed sends (under lockrun): no counted-only modes, no counted alternatives, no
    /// minimum dumps; the digests still run, outside the timed calls.
    timing: bool,
    /// Cycles the main thread runs WHILE each piped send streams from its snapshot (F-S1).
    mid_cycles: usize,
}

fn die(msg: &str) -> ! {
    eprintln!("branch_ship: {msg}");
    std::process::exit(2)
}

fn not_a_result(msg: &str) -> ! {
    println!("NOT A RESULT: {msg}");
    std::process::exit(1)
}

fn parse_args() -> Args {
    let mut a = Args {
        arm: Arm::Flat,
        n: 1000,
        w1: 1000,
        w2: None,
        trunk_every: 16,
        seed: 0x9E37_79B9_7F4A_7C15,
        plant: PlantArg::None,
        exports: 8,
        samples: 64,
        delta: true,
        out_dir: None,
        timing: false,
        mid_cycles: 200,
    };
    let mut arm = None;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().unwrap_or_else(|| die(&format!("{flag} needs a value")));
        let num = |s: String| s.parse::<usize>().unwrap_or_else(|_| die("bad number"));
        match flag.as_str() {
            "--arm" => {
                arm = Some(match val().as_str() {
                    "flat" => Arm::Flat,
                    "bushy" => Arm::Bushy,
                    "chain" => Arm::Chain,
                    other => die(&format!("unknown arm {other}")),
                })
            }
            "--n" => a.n = num(val()),
            "--w1" => a.w1 = num(val()),
            "--w2" => a.w2 = Some(num(val())),
            "--trunk-every" => a.trunk_every = num(val()),
            "--seed" => a.seed = val().parse().unwrap_or_else(|_| die("bad --seed")),
            "--exports" => a.exports = num(val()),
            "--samples" => a.samples = num(val()),
            "--no-delta" => a.delta = false,
            "--timing" => a.timing = true,
            "--mid-cycles" => a.mid_cycles = num(val()),
            "--out-dir" => a.out_dir = Some(PathBuf::from(val())),
            "--plant" => {
                a.plant = match val().as_str() {
                    "none" => PlantArg::None,
                    "flip" => PlantArg::Flip,
                    "tomb" => PlantArg::Tomb,
                    "ref" => PlantArg::Ref,
                    "inherit" => PlantArg::Inherit,
                    other => die(&format!("unknown plant {other}")),
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    a.arm = arm.unwrap_or_else(|| die("--arm is required"));
    if a.n == 0 || a.trunk_every == 0 {
        die("--n and --trunk-every must be positive");
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

fn trunk_value(id: i64) -> String {
    format!("{:0>width$}", id, width = VALUE_LEN)
}

fn trunk_gen_value(generation: u64) -> String {
    format!("t{:0>width$}", generation, width = VALUE_LEN - 1)
}

/// A value no other write produces: the writer's serial and its write count, same length as every
/// other value so an UPDATE rewrites the row in place.
fn unique_value(serial: u64, write: u64) -> String {
    let body = format!("b{serial}w{write}");
    format!("{body}{}", "x".repeat(VALUE_LEN - body.len()))
}

fn row_for(n: u64) -> i64 {
    (n.wrapping_mul(2_654_435_761) % TRUNK_ROWS as u64) as i64 + 1
}

/// The spread walk: 37 is coprime with 20,000, so each trunk write lands one leaf further on.
fn spread_row(g: u64) -> i64 {
    ((g * 37) % TRUNK_ROWS as u64) as i64 + 1
}

/// The trunk's write history, kept by the harness (never read from the engine).
#[derive(Default)]
struct TrunkModel {
    writes: u64,
    history: HashMap<i64, Vec<(u64, u64)>>,
}

impl TrunkModel {
    fn record(&mut self, row: i64) -> u64 {
        let g = self.writes;
        self.writes += 1;
        self.history.entry(row).or_default().push((g, g));
        g
    }
    fn value_at(&self, row: i64, writes_at_fork: u64) -> String {
        let Some(h) = self.history.get(&row) else {
            return trunk_value(row);
        };
        let n = h.partition_point(|&(seq, _)| seq < writes_at_fork);
        if n == 0 {
            trunk_value(row)
        } else {
            trunk_gen_value(h[n - 1].1)
        }
    }
}

struct Live {
    branch: Branch,
    serial: u64,
    own_row: i64,
    writes: u64,
    /// Rows this branch or an ancestor wrote, as this branch sees them.
    sees: HashMap<i64, String>,
    /// Trunk writes committed when this branch's ancestry left the trunk.
    trunk_at: u64,
}

struct Bench {
    db: Arc<Database>,
    db_path: PathBuf,
    trunk: Arc<Connection>,
    page_size: usize,
    model: TrunkModel,
    rng: Rng,
    live: Vec<Live>,
    serial: u64,
    trunk_every: usize,
    grown: u64,
}

impl Bench {
    fn update(conn: &Arc<Connection>, row: i64, value: &str) {
        conn.execute(format!("UPDATE t SET v = '{value}' WHERE id = {row}"))
            .unwrap();
    }

    fn trunk_write(&mut self) {
        let row = spread_row(self.model.writes);
        let g = self.model.record(row);
        Self::update(&self.trunk, row, &trunk_gen_value(g));
    }

    /// Fork one branch (from the trunk, or from live branch `parent`), and write its own row. A
    /// branch parent then rewrites ITS own row, retaining its old version for the new child.
    fn grow(&mut self, parent: Option<usize>) {
        let serial = self.serial;
        self.serial += 1;
        let (branch, mut sees, trunk_at) = match parent {
            None => (
                self.trunk.fork_branch().unwrap(),
                HashMap::new(),
                self.model.writes,
            ),
            Some(p) => {
                let p = &self.live[p];
                (p.branch.fork().unwrap(), p.sees.clone(), p.trunk_at)
            }
        };
        let own_row = row_for(serial);
        let value = unique_value(serial, 0);
        let conn = branch.connect().unwrap();
        Self::update(&conn, own_row, &value);
        drop(conn);
        sees.insert(own_row, value);
        self.live.push(Live {
            branch,
            serial,
            own_row,
            writes: 1,
            sees,
            trunk_at,
        });
        if let Some(p) = parent {
            self.rewrite(p);
        }
        self.grown += 1;
        if self.grown % self.trunk_every as u64 == 0 {
            self.trunk_write();
        }
    }

    fn rewrite(&mut self, i: usize) {
        let l = &mut self.live[i];
        let value = unique_value(l.serial, l.writes);
        l.writes += 1;
        let conn = l.branch.connect().unwrap();
        Self::update(&conn, l.own_row, &value);
        drop(conn);
        l.sees.insert(l.own_row, value);
    }

    fn fork_parent(&mut self, arm: Arm) -> Option<usize> {
        match arm {
            Arm::Bushy if !self.live.is_empty() && self.rng.below(4) == 0 => {
                Some(self.rng.below(self.live.len()))
            }
            _ => None,
        }
    }

    /// One churn cycle: fork + write, reap a random live branch, rewrite a random live branch.
    fn cycle(&mut self, arm: Arm) {
        let parent = self.fork_parent(arm);
        self.grow(parent);
        let v = self.rng.below(self.live.len());
        let victim = self.live.swap_remove(v);
        victim.branch.reap().unwrap();
        let r = self.rng.below(self.live.len());
        self.rewrite(r);
    }

    fn checkpoint_image(&self) -> TrunkImage {
        let rows = self
            .trunk
            .prepare("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap()
            .run_collect_rows()
            .unwrap();
        if rows[0][0].as_int() != Some(0) {
            not_a_result(&format!("trunk checkpoint did not complete: {rows:?}"));
        }
        let bytes = std::fs::read(&self.db_path).unwrap();
        let wal = PathBuf::from(format!("{}-wal", self.db_path.display()));
        if std::fs::metadata(&wal).map_or(0, |m| m.len()) != 0 {
            not_a_result("the WAL is not empty after TRUNCATE; the image would miss commits");
        }
        TrunkImage::new(self.page_size, bytes)
    }
}

/// The write half of an in-process pipe.
struct PipeWriter(SyncSender<Vec<u8>>);
impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .send(buf.to_vec())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "receiver gone"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct PipeReader {
    rx: Receiver<Vec<u8>>,
    cur: Vec<u8>,
    pos: usize,
}
impl Read for PipeReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.pos == self.cur.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.cur = chunk;
                    self.pos = 0;
                }
                Err(_) => return Ok(0),
            }
        }
        let n = buf.len().min(self.cur.len() - self.pos);
        buf[..n].copy_from_slice(&self.cur[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

const STREAM_HEADER: &str = "stream\tpoint\tlive\tmode\ttotal\ttotal_raw\ttotal_delta\ttotal_delta_fork\ttotal_dedup\tpage_bytes\tref_bytes\tmeta_bytes\tslot_rec\ttrunk_rec\tref_rec\tstate_rec\tdead_rec\tcur_ent\tret_ent\ttret_ent\twritten_ent\tlive_list\tmaps\tdup\tstates_visited\tentries_visited\tslots_visited\tindex_visited\tnodes_visited\titems_checked\tlocked_ops\tlocked_ns\tkids_checked\tbase_lookups\tbase_lookup_nodes\tchange_index_nodes";

fn print_stream(point: &str, live: usize, mode: &str, r: &SendReport) {
    let non_payload = r.total_bytes - r.payload_shipped;
    println!(
        "stream\t{point}\t{live}\t{mode}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        r.total_bytes,
        non_payload + r.payload_raw,
        non_payload + r.payload_delta,
        non_payload + r.payload_delta_fork,
        non_payload + r.payload_dedup,
        r.page_bytes,
        r.ref_bytes,
        r.meta_bytes,
        r.slot_records,
        r.trunk_page_records,
        r.ref_records,
        r.state_records,
        r.dead_records,
        r.current_entries,
        r.retained_entries,
        r.trunk_retained_entries,
        r.written_entries,
        r.live_list_bytes,
        r.maps_bytes,
        r.dup_payloads,
        r.states_visited,
        r.entries_visited,
        r.slots_visited,
        r.index_visited,
        r.nodes_visited,
        r.items_checked,
        r.locked_ops,
        r.locked_ns,
        r.kids_checked,
        r.base_lookups,
        r.base_lookup_nodes,
        r.change_index_nodes,
    );
}

fn plant_of(p: PlantArg) -> Plant {
    match p {
        PlantArg::Flip => Plant::FlipPayload,
        PlantArg::Tomb => Plant::DropTombstone,
        PlantArg::Ref => Plant::BadRefHash,
        PlantArg::None | PlantArg::Inherit => Plant::None,
    }
}

/// Take F-S1's snapshot, then send it into `replica` through a bounded pipe from a second thread
/// while this thread runs `b`'s workload for `args.mid_cycles` cycles: the send holds no store lock,
/// so the store keeps working, and the replica must still equal the store AT the snapshot.
#[allow(clippy::too_many_arguments)]
fn pipe(
    b: &mut Bench,
    replica: &mut Replica,
    args: &Args,
    mode: SendMode,
    base: Option<u64>,
    image: &TrunkImage,
    plant: Plant,
    point: &str,
) -> (SendReport, u64) {
    let count = !args.timing;
    let (snap, snap_ns): (ShipSnap, u64) = b
        .db
        .branch_snapshot()
        .unwrap_or_else(|e| not_a_result(&format!("{point}: snapshot refused: {e}")));
    let (view_ops, touched, copied, write_base_lookups, write_base_work) = b.db.branch_view_work();
    let (h_states, h_slots, h_tret, h_written) = snap.heights();
    println!(
        "snap\t{point}\tseq={}\tsnapshot_locked_ns={snap_ns}\tview_ops={view_ops}\tview_nodes_touched={touched}\tview_nodes_copied={copied}\theight_states={h_states}\theight_slots={h_slots}\theight_trunk_retained={h_tret}\theight_written={h_written}\twrite_base_lookups={write_base_lookups}\twrite_base_work={write_base_work}\theight_changes={}",
        snap.seq(),
        snap.change_index_height()
    );
    if args.timing {
        // Serialisation alone, into a sink that discards.
        let t = Instant::now();
        Database::branch_send_snapshot(&snap, mode, base, image, args.delta, plant, false, Some(&mut std::io::sink()))
            .unwrap();
        println!("time\t{point}\t{mode:?}\tnull_sink_send_s={:.4}", t.elapsed().as_secs_f64());
    }
    let (tx, rx) = sync_channel::<Vec<u8>>(16);
    let t = Instant::now();
    let arm = args.arm;
    let (sent, received, during, send_s) = std::thread::scope(|s| {
        let hr = s.spawn(|| {
            let mut reader = PipeReader {
                rx,
                cur: Vec::new(),
                pos: 0,
            };
            replica.receive(&mut reader)
        });
        let snap = &snap;
        let hs = s.spawn(move || {
            let mut writer = PipeWriter(tx);
            let ts = Instant::now();
            let sent = Database::branch_send_snapshot(
                snap,
                mode,
                base,
                image,
                args.delta,
                plant,
                count,
                Some(&mut writer),
            );
            drop(writer);
            (sent, ts.elapsed().as_secs_f64())
        });
        // The store keeps working while the send streams.
        let mut during = 0u64;
        if arm != Arm::Chain {
            for _ in 0..args.mid_cycles {
                if !hs.is_finished() {
                    during += 1;
                }
                b.cycle(arm);
            }
        }
        let (sent, send_s) = hs.join().unwrap();
        (sent, hr.join().unwrap(), during, send_s)
    });
    let secs = t.elapsed().as_secs_f64();
    let work = match received {
        Ok(w) => w,
        Err(e) => not_a_result(&format!("{point}: the replica refused the stream: {e}")),
    };
    let mut sent = sent.unwrap_or_else(|e| not_a_result(&format!("{point}: send failed: {e}")));
    // The send's time under the store mutex was the snapshot's (F-S1): `branch_send_snapshot` takes
    // no store, so its report's locked_ops is 0 and stays so (PREREG A6.2); only its locked_ns is
    // the snapshot's measured hold.
    sent.locked_ns = snap_ns;
    println!(
        "recv\t{point}\trecords={}\tslots_written={}\tslots_claimed={}\trefs={}\ttrunk_pages={}\tdeaths={}\tgc_freed={}\tstates_new={}\tstates_updated={}\tentries={}\tretained_inserted={}\ttrie_inserts={}\tslots_released={}\tfork_deltas={}\tmid_cycles={}\tmid_cycles_while_sending={during}(interleaving-decided)\tsend_s={send_s:.3}\twall_s={secs:.3}(unlocked)",
        work.records,
        work.slots_written,
        work.slots_claimed,
        work.refs,
        work.trunk_pages,
        work.deaths,
        work.gc_freed,
        work.states_new,
        work.states_updated,
        work.entries,
        work.retained_inserted,
        work.trie_inserts,
        work.slots_released,
        work.fork_deltas,
        if arm == Arm::Chain { 0 } else { args.mid_cycles },
    );
    (sent, snap.seq())
}

/// The replica against the store's digest taken at the snapshot it received.
fn check_digest(want: &Digest, replica: &Replica, point: &str) {
    let rep = replica.digest();
    println!(
        "digest\t{point}\tsource={:016x}{:016x}/{}/{}/{}\treplica={:016x}{:016x}/{}/{}/{}\tequal={}",
        want.hash.0,
        want.hash.1,
        want.states,
        want.slots,
        want.entries,
        rep.hash.0,
        rep.hash.1,
        rep.states,
        rep.slots,
        rep.entries,
        *want == rep
    );
    if *want != rep {
        not_a_result(&format!("{point}: replica digest differs from the source's at the snapshot"));
    }
}

/// The incremental minimum from two dumps: new content, and new metadata entries in fixed width.
fn print_minimum(point: &str, d0: &ShipDump, d1: &ShipDump, page_size: usize) {
    let new = |a: &HashSet<u64>, b: &HashSet<u64>| b.difference(a).count() as u64;
    let pages = new(&d0.content, &d1.content);
    let headers = new(&d0.state_headers, &d1.state_headers);
    let cur = new(&d0.current, &d1.current);
    let ret = new(&d0.retained, &d1.retained);
    let tw = new(&d0.trunk_written, &d1.trunk_written);
    let tr = new(&d0.trunk_retained, &d1.trunk_retained);
    let gone = d0.states.difference(&d1.states).count() as u64;
    let meta = headers * STATE_HEADER_BYTES
        + cur * CURRENT_ENTRY_BYTES
        + ret * RETAINED_ENTRY_BYTES
        + tw * WRITTEN_ENTRY_BYTES
        + tr * RETAINED_ENTRY_BYTES
        + gone * 8
        + 8;
    println!(
        "min\t{point}\tnew_contents={pages}\tpage_bytes={}\tstate_headers={headers}\tcurrent={cur}\tretained={ret}\ttrunk_written={tw}\ttrunk_retained={tr}\tstates_gone={gone}\tmeta_bytes={meta}\ttotal={}",
        pages * page_size as u64,
        pages * page_size as u64 + meta
    );
}

fn full_minimum(point: &str, d: &ShipDump, b: &Bench, image: &TrunkImage) {
    let s = b.db.branch_stats();
    let meta = d.state_headers.len() as u64 * STATE_HEADER_BYTES
        + d.current.len() as u64 * CURRENT_ENTRY_BYTES
        + d.retained.len() as u64 * RETAINED_ENTRY_BYTES
        + d.trunk_written.len() as u64 * WRITTEN_ENTRY_BYTES
        + d.trunk_retained.len() as u64 * RETAINED_ENTRY_BYTES
        + 8;
    let pages = s.arena_slots_in_use as u64 + u64::from(image.pages());
    println!(
        "fullmin\t{point}\tstates={}\tarena_in_use={}\ttrunk_pages={}\tpage_records_min={pages}\tdistinct_contents={}\tpage_bytes={}\tmeta_bytes={meta}\ttotal={}\tfull_img_bytes={}",
        s.live_branches,
        s.arena_slots_in_use,
        image.pages(),
        d.content.len(),
        pages * b.page_size as u64,
        pages * b.page_size as u64 + meta,
        s.live_branches as u64 * u64::from(image.pages()) * b.page_size as u64,
    );
}

/// Export `samples` random live branches from the source and the replica; they must match. The
/// first `files` go to disk and through real SQLite against the model.
fn exports(b: &mut Bench, replica: &Replica, image: &TrunkImage, samples: usize, files: usize, dir: &Path) {
    let mut image_bytes = 0u64;
    let mut delta_pages = 0u64;
    let mut checked_rows = 0u64;
    for k in 0..samples.min(b.live.len()) {
        let i = b.rng.below(b.live.len());
        let id = b.live[i].branch.id();
        let mut src = Vec::new();
        let (pages, differ) = b
            .db
            .branch_export(id, image, &mut src)
            .unwrap_or_else(|e| not_a_result(&format!("export of branch {} failed: {e}", id.0)));
        let mut rep = Vec::new();
        replica
            .export(id, &mut rep)
            .unwrap_or_else(|e| not_a_result(&format!("replica export of {} failed: {e}", id.0)));
        if src != rep {
            not_a_result(&format!("branch {}: source and replica exports differ", id.0));
        }
        if pages != image.pages() {
            not_a_result(&format!(
                "branch {} exports {pages} pages, the trunk has {}: the full_img count assumes equal",
                id.0,
                image.pages()
            ));
        }
        image_bytes += src.len() as u64;
        delta_pages += u64::from(differ);
        if k < files {
            let path = dir.join(format!("export_{}.db", id.0));
            std::fs::write(&path, &src).unwrap();
            checked_rows += sqlite_check(&path, &b.live[i], &b.model, &mut b.rng);
        }
    }
    println!(
        "export\tsamples={}\tfiles_checked_by_sqlite={}\trows_checked={checked_rows}\timage_bytes_per_branch={}\tpages_differing_from_trunk_per_branch={:.2}",
        samples.min(b.live.len()),
        files.min(samples).min(b.live.len()),
        image_bytes / samples.min(b.live.len()).max(1) as u64,
        delta_pages as f64 / samples.min(b.live.len()).max(1) as f64
    );
}

fn sqlite_check(path: &Path, l: &Live, model: &TrunkModel, rng: &mut Rng) -> u64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    let ok: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap_or_else(|e| not_a_result(&format!("sqlite integrity_check failed: {e}")));
    if ok != "ok" {
        not_a_result(&format!("{}: sqlite integrity_check: {ok}", path.display()));
    }
    let count: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    if count != TRUNK_ROWS {
        not_a_result(&format!("{}: {count} rows", path.display()));
    }
    let mut rows: Vec<i64> = l.sees.keys().copied().collect();
    for _ in 0..64 {
        rows.push(rng.below(TRUNK_ROWS as usize) as i64 + 1);
    }
    for &row in &rows {
        let got: String = conn
            .query_row("SELECT v FROM t WHERE id = ?1", [row], |r| r.get(0))
            .unwrap();
        let want = l
            .sees
            .get(&row)
            .cloned()
            .unwrap_or_else(|| model.value_at(row, l.trunk_at));
        if got != want {
            not_a_result(&format!(
                "{}: row {row} reads {got:?}, the model says {want:?}",
                path.display()
            ));
        }
    }
    drop(conn);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    rows.len() as u64
}

fn rss_bytes() -> u64 {
    memory_stats::memory_stats().map_or(0, |m| m.physical_mem as u64)
}

fn state_line(b: &Bench, point: &str) {
    let s = b.db.branch_stats();
    println!(
        "# {point}: live_states={} harness_live={} arena_in_use={} arena_free={} trunk_writes={} seq={} log_bytes={} tombstones={} rss_bytes={}",
        s.live_branches,
        b.live.len(),
        s.arena_slots_in_use,
        s.arena_slots_free,
        b.model.writes,
        b.db.branch_ship_seq(),
        b.db.branch_log_bytes(),
        b.db.branch_tombstones(),
        rss_bytes()
    );
}

/// The incremental point: counted modes, the fixed stream into the replica, the minimum.
#[allow(clippy::too_many_arguments)]
/// The incremental point: the minimum's dump and the digest at this moment, the counted modes, then
/// F-S1's snapshot piped into the replica while the workload continues.
#[allow(clippy::too_many_arguments)]
fn incremental(
    b: &mut Bench,
    replica: &mut Replica,
    args: &Args,
    point: &str,
    base: u64,
    dump0: &ShipDump,
    log0: u64,
) -> (u64, ShipDump, u64) {
    let image = b.checkpoint_image();
    state_line(b, point);
    let live = b.db.branch_stats().live_branches;
    let want = b.db.branch_digest(&image);
    let log1 = b.db.branch_log_bytes();
    let dump1 = if args.timing {
        ShipDump::default()
    } else {
        b.db.branch_dump(&image)
    };
    for (mode, name) in [(SendMode::IncrRoot, "incr_root"), (SendMode::IncrAlloc, "incr_alloc")] {
        if args.timing {
            break;
        }
        let r = b
            .db
            .branch_send(mode, Some(base), &image, false, Plant::None, true, None)
            .unwrap();
        print_stream(point, live, name, &r);
    }
    let (r, seq) = pipe(b, replica, args, SendMode::IncrFix, Some(base), &image, plant_of(args.plant), point);
    print_stream(point, live, "incr_fix", &r);
    println!("log\t{point}\tlog_bytes={}", log1 - log0);
    if !args.timing {
        print_minimum(point, dump0, &dump1, b.page_size);
    }
    check_digest(&want, replica, point);
    // The replica acknowledged `seq`: holes at or before it can go.
    let dropped = b.db.branch_forget_tombstones(seq);
    println!("# {point}: holes dropped after the replica's ack: {dropped}");
    (seq, dump1, log1)
}

fn main() {
    let args = parse_args();
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("branch_ship.db");
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
    trunk.execute("PRAGMA synchronous = OFF").unwrap();
    trunk
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    trunk.execute("BEGIN").unwrap();
    for id in 1..=TRUNK_ROWS {
        trunk
            .execute(format!("INSERT INTO t VALUES ({id}, '{}')", trunk_value(id)))
            .unwrap();
    }
    trunk.execute("COMMIT").unwrap();
    trunk.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let page_size = trunk.prepare("PRAGMA page_size").unwrap().run_collect_rows().unwrap()[0][0]
        .as_int()
        .unwrap() as usize;
    db.branch_enable_shipping().unwrap();

    let w2 = args.w2.unwrap_or(args.n / 10);
    println!("# branch_ship — r11-ship PREREG; Turso fork F1+F2+F4 + shipping");
    println!(
        "# arm={:?} n={} w1={} w2={w2} trunk_every={} seed={:#x} plant={:?} delta={} exports={} samples={} timing={} mid_cycles={} page_size={page_size} build={}",
        args.arm,
        args.n,
        args.w1,
        args.trunk_every,
        args.seed,
        args.plant,
        args.delta,
        args.exports,
        args.samples,
        args.timing,
        args.mid_cycles,
        if cfg!(debug_assertions) { "DEBUG" } else { "release" }
    );
    println!("{STREAM_HEADER}");

    let mut b = Bench {
        db: db.clone(),
        db_path: path.clone(),
        trunk,
        page_size,
        model: TrunkModel::default(),
        rng: Rng(args.seed),
        live: Vec::new(),
        serial: 0,
        trunk_every: args.trunk_every,
        grown: 0,
    };
    let t = Instant::now();
    match args.arm {
        Arm::Chain => {
            for level in 0..args.n {
                let parent = if level == 0 { None } else { Some(b.live.len() - 1) };
                // A chain level keeps its parent's rows and adds its own; no parent rewrite.
                let serial = b.serial;
                b.serial += 1;
                let (branch, mut sees, trunk_at) = match parent {
                    None => (b.trunk.fork_branch().unwrap(), HashMap::new(), b.model.writes),
                    Some(p) => {
                        let p = &b.live[p];
                        (p.branch.fork().unwrap(), p.sees.clone(), p.trunk_at)
                    }
                };
                let own_row = row_for(serial);
                let value = unique_value(serial, 0);
                let conn = branch.connect().unwrap();
                Bench::update(&conn, own_row, &value);
                drop(conn);
                sees.insert(own_row, value);
                b.live.push(Live {
                    branch,
                    serial,
                    own_row,
                    writes: 1,
                    sees,
                    trunk_at,
                });
            }
        }
        arm => {
            while b.live.len() < args.n {
                let parent = b.fork_parent(arm);
                b.grow(parent);
            }
        }
    }
    println!("# grow_s={:.1}(unlocked)", t.elapsed().as_secs_f64());

    // T0: full sends.
    let image0 = b.checkpoint_image();
    state_line(&b, "T0");
    let live0 = b.db.branch_stats().live_branches;
    let want0 = b.db.branch_digest(&image0);
    let log0 = b.db.branch_log_bytes();
    let dump0 = if args.timing {
        ShipDump::default()
    } else {
        b.db.branch_dump(&image0)
    };
    if !args.timing {
        full_minimum("T0", &dump0, &b, &image0);
    }
    if !args.timing {
        let r = b
            .db
            .branch_send(SendMode::FullMaps, None, &image0, false, Plant::None, true, None)
            .unwrap();
        print_stream("T0", live0, "full_maps", &r);
    }
    let mut replica = Replica::new(page_size);
    replica.skip_inherit = args.plant == PlantArg::Inherit;
    let full_plant = if args.plant == PlantArg::Flip { Plant::FlipPayload } else { Plant::None };
    let (r, seq0) = pipe(&mut b, &mut replica, &args, SendMode::FullFix, None, &image0, full_plant, "T0");
    print_stream("T0", live0, "full_fix", &r);
    check_digest(&want0, &replica, "T0");

    let out_dir = args
        .out_dir
        .clone()
        .unwrap_or_else(|| dir.path().to_path_buf());
    std::fs::create_dir_all(&out_dir).unwrap();

    if args.arm != Arm::Chain {
        let t = Instant::now();
        for _ in 0..args.w1 {
            b.cycle(args.arm);
        }
        println!("# w1_s={:.1}(unlocked)", t.elapsed().as_secs_f64());
        let (seq1, dump1, log1) = incremental(&mut b, &mut replica, &args, "T1", seq0, &dump0, log0);
        let t = Instant::now();
        for _ in 0..w2 {
            b.cycle(args.arm);
        }
        println!("# w2_s={:.1}(unlocked)", t.elapsed().as_secs_f64());
        let (seq2, dump2, log2) = incremental(&mut b, &mut replica, &args, "T2", seq1, &dump1, log1);
        // T3: catch the replica up on T2's mid-send cycles with nothing running, so that the source
        // and the replica hold the same state for the full-send count and the exports.
        let quiet = Args {
            mid_cycles: 0,
            ..args.clone()
        };
        let (_seq3, dump3, _log3) = incremental(&mut b, &mut replica, &quiet, "T3", seq2, &dump2, log2);
        let image3 = b.checkpoint_image();
        let live3 = b.db.branch_stats().live_branches;
        if !args.timing {
            let r = b
                .db
                .branch_send(SendMode::FullFix, None, &image3, false, Plant::None, true, None)
                .unwrap();
            print_stream("T3", live3, "full_fix", &r);
            full_minimum("T3", &dump3, &b, &image3);
        }
        exports(&mut b, &replica, &image3, args.samples, args.exports, &out_dir);
    } else {
        exports(&mut b, &replica, &image0, args.samples, args.exports, &out_dir);
    }
    let t = Instant::now();
    let live = std::mem::take(&mut b.live);
    drop(live);
    let end = db.branch_stats();
    if end.live_branches != 0 || end.arena_slots_in_use != 0 {
        not_a_result(&format!("teardown leaked: {end:?}"));
    }
    println!(
        "# teardown_s={:.1}: every branch freed, arena empty; replica stats {:?}",
        t.elapsed().as_secs_f64(),
        replica.stats()
    );
    println!("# DONE");
}
