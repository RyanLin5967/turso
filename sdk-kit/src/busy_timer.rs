//! The busy timer: wakes a task once a busy handler's backoff is over (engine review 11 MED 4).
//!
//! An async-mode step reports the backoff (`StepResult::Sleep`) as `TursoStatusCode::Io`. A
//! caller that stepped with its task's waker (the Rust `turso` crate) answers that Io with
//! `run_io` and `Poll::Pending`. Waking the task at once polls it again at once and spins for the
//! whole busy timeout; waiting the backoff out in `run_io` holds the executor thread inside the
//! poll. So the step hands the waker to this timer instead: one thread for the process, started
//! on first use, that wakes each waker when its backoff is over.
//!
//! The deadline is read on the timer thread, so a caller never reads a clock here (wasm has none).
//! Where no thread can be started (wasm), the waker is woken at once, the behaviour before this
//! timer existed.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    sync::{mpsc, OnceLock},
    task::Waker,
    time::{Duration, Instant},
};

/// A request to wake `waker` once `duration` has passed.
struct Request {
    duration: Duration,
    waker: Waker,
}

/// A request with its deadline on the timer thread's clock.
struct Due {
    at: Instant,
    waker: Waker,
}

impl PartialEq for Due {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Due {}

impl PartialOrd for Due {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Due {
    /// `BinaryHeap` pops its greatest element first, so the earliest deadline compares greatest.
    fn cmp(&self, other: &Self) -> Ordering {
        other.at.cmp(&self.at)
    }
}

/// Wakes `waker` once `duration` has passed, from the busy timer thread.
pub(crate) fn wake_after(duration: Duration, waker: Waker) {
    static TIMER: OnceLock<Option<mpsc::Sender<Request>>> = OnceLock::new();
    let timer = TIMER.get_or_init(|| {
        let (sender, receiver) = mpsc::channel();
        std::thread::Builder::new()
            .name("turso-busy-timer".to_string())
            .spawn(move || run(receiver))
            .ok()
            .map(|_| sender)
    });
    let request = Request { duration, waker };
    match timer {
        Some(sender) => {
            if let Err(mpsc::SendError(request)) = sender.send(request) {
                request.waker.wake();
            }
        }
        None => request.waker.wake(),
    }
}

/// The timer thread: wakes every due waker, then waits for the next deadline or request.
fn run(receiver: mpsc::Receiver<Request>) {
    let mut due = BinaryHeap::<Due>::new();
    loop {
        let now = Instant::now();
        while due.peek().is_some_and(|next| next.at <= now) {
            if let Some(next) = due.pop() {
                next.waker.wake();
            }
        }
        let request = match due.peek() {
            Some(next) => receiver.recv_timeout(next.at - now),
            None => receiver
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
        };
        match request {
            Ok(Request { duration, waker }) => due.push(Due {
                at: Instant::now() + duration,
                waker,
            }),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}
