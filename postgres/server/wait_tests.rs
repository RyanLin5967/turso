use super::{Backoff, Interrupts, LimboError};
use std::time::{Duration, Instant};

#[test]
fn an_ordinary_backoff_ends_at_the_statement_deadline() {
    let interrupts = Interrupts::new(std::sync::Arc::default());
    let _armed = interrupts.begin(20);
    let mut backoff = Backoff::new(Duration::from_secs(1), &interrupts);
    backoff.attempt = 11; // The existing schedule's 100 ms delay.
    let start = Instant::now();
    assert!(matches!(backoff.wait(), Err(LimboError::Interrupt)));
    assert!(
        start.elapsed() < Duration::from_millis(70),
        "a 20 ms statement deadline waited through a 100 ms backoff: {:?}",
        start.elapsed()
    );
}

#[test]
fn cancel_wakes_an_ordinary_backoff() {
    let interrupts = Interrupts::new(std::sync::Arc::default());
    let _armed = interrupts.begin(0);
    let mut backoff = Backoff::new(Duration::from_secs(1), &interrupts);
    backoff.attempt = 11;
    std::thread::scope(|scope| {
        let cancelling = scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(20));
            let sent = Instant::now();
            interrupts.request_cancel();
            sent
        });
        assert!(matches!(backoff.wait(), Err(LimboError::Interrupt)));
        let answered = Instant::now();
        let sent = cancelling.join().unwrap();
        assert!(
            answered.saturating_duration_since(sent) < Duration::from_millis(50),
            "cancel waited through the rest of a 100 ms backoff: {:?}",
            answered.saturating_duration_since(sent)
        );
    });
}
