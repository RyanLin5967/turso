//! FIRE-CHECK of turso_pg_server::counters::foreign_thread_cpu_ns, the foreign-thread detector the
//! word-held FULL verdict and the re-judgment's off-thread guard rest on, in a test binary of its
//! own: in the library's test binary every other test's threads are foreign to these.
#![cfg(target_vendor = "apple")]

use std::time::{Duration, Instant};
use turso_pg_server::counters::foreign_thread_cpu_ns;

/// The two tests' threads are foreign to each other: run one at a time (in parallel the control
/// read the other test's spinner, 285 us).
static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Both ways: a planted thread that spins 20 ms inside the window is seen (at least 10 ms of foreign
/// CPU), and a window with every other thread parked reads exactly zero.
#[test]
fn a_foreign_thread_that_runs_is_seen_and_a_parked_one_is_not() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (go, wait) = std::sync::mpsc::channel::<()>();
    let (done_tx, done) = std::sync::mpsc::channel::<()>();
    let spinner = std::thread::spawn(move || {
        wait.recv().unwrap();
        let t = Instant::now();
        let mut x = 0u64;
        while t.elapsed() < Duration::from_millis(20) {
            x = x.wrapping_add(std::hint::black_box(1));
        }
        std::hint::black_box(x);
        done_tx.send(()).unwrap();
        // Parked from here until the end of the test.
        wait.recv().ok();
    });
    // Let the spinner reach its first wait.
    std::thread::sleep(Duration::from_millis(50));
    let before = foreign_thread_cpu_ns().expect("foreign CPU readable");
    go.send(()).unwrap();
    done.recv().unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let after = foreign_thread_cpu_ns().unwrap();
    assert!(
        after - before >= 10_000_000,
        "a 20 ms spin read as {} ns of foreign CPU",
        after - before
    );
    // The control: nothing else runs (the spinner is parked again).
    let quiet0 = foreign_thread_cpu_ns().unwrap();
    let mut x = 0u64;
    let t = Instant::now();
    while t.elapsed() < Duration::from_millis(20) {
        x = x.wrapping_add(std::hint::black_box(1));
    }
    std::hint::black_box(x);
    let quiet1 = foreign_thread_cpu_ns().unwrap();
    assert_eq!(quiet1 - quiet0, 0, "foreign CPU with every other thread parked");
    drop(go);
    spinner.join().unwrap();
}

/// The burst the guard exists for: a foreign thread woken for a few thousand instructions (a
/// condition-variable wake that finds nothing to do, then parks) is seen in each of 100 windows.
#[test]
fn a_microsecond_foreign_wake_is_seen_in_every_window() {
    let _one = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let (wake, woken) = std::sync::mpsc::channel::<()>();
    let (ack, acked) = std::sync::mpsc::channel::<()>();
    let waker = std::thread::spawn(move || {
        while woken.recv().is_ok() {
            let mut x = 0u64;
            for i in 0..1000u64 {
                x = x.wrapping_add(std::hint::black_box(i));
            }
            std::hint::black_box(x);
            ack.send(()).unwrap();
        }
    });
    std::thread::sleep(Duration::from_millis(20));
    let mut seen = 0;
    let mut deltas = Vec::new();
    for _ in 0..100 {
        let before = foreign_thread_cpu_ns().unwrap();
        wake.send(()).unwrap();
        acked.recv().unwrap();
        // The waker parks again in recv right after its send; give it the moment to.
        std::thread::sleep(Duration::from_millis(1));
        let after = foreign_thread_cpu_ns().unwrap();
        seen += usize::from(after > before);
        deltas.push(after - before);
    }
    drop(wake);
    waker.join().unwrap();
    deltas.sort_unstable();
    // Measured: each wake reads 7-72 us (the kernel accounts in whole microseconds).
    assert_eq!(
        seen, 100,
        "a woken foreign thread was seen in {seen} of 100 windows; deltas {deltas:?}"
    );
}
