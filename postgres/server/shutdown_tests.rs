use super::*;
use std::io::Read;
use std::time::{Duration, Instant};

#[test]
fn shutdown_reaches_an_admitted_session_before_its_worker_runs() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *server.shared.before_session.lock().unwrap() = Some(Box::new(move || {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    }));
    let mut client = admit(&server);
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let registered = server.shared.sessions.open.lock().unwrap().len();
    let (closed, result, elapsed) = std::thread::scope(|scope| {
        let start = Instant::now();
        let stopping = scope.spawn(|| server.end_sessions());
        let closed = matches!(client.read(&mut [0]), Ok(0));
        // Always release the test worker and socket, including on the red baseline.
        let _ = client.shutdown(std::net::Shutdown::Both);
        release_tx.send(()).unwrap();
        (closed, stopping.join().unwrap(), start.elapsed())
    });
    assert_eq!((registered, closed, result.2), (1, true, 0));
    assert!(
        elapsed < Duration::from_secs(2),
        "shutdown took {elapsed:?}"
    );
    assert!(server.shared.sessions.open.lock().unwrap().is_empty());
    assert_eq!(server.shared.live.load(Ordering::Relaxed), 0);
}

#[test]
fn a_panicking_admitted_worker_releases_the_socket_and_registration() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    let shared = server.shared.clone();
    let (registered_tx, registered_rx) = std::sync::mpsc::channel();
    *server.shared.before_session.lock().unwrap() = Some(Box::new(move || {
        registered_tx
            .send(shared.sessions.open.lock().unwrap().len())
            .unwrap();
        panic!("test session unwind");
    }));
    let mut client = admit(&server);
    let registered = registered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let worker = server.threads.lock().unwrap().pop().unwrap();
    assert!(worker.join().is_err(), "the test worker must unwind");
    assert_eq!(
        registered, 1,
        "admission owns the registration before execution"
    );
    assert_eq!(client.read(&mut [0]).unwrap(), 0);
    assert!(server.shared.sessions.open.lock().unwrap().is_empty());
    assert_eq!(server.shared.live.load(Ordering::Relaxed), 0);
}

#[test]
fn a_socket_duplication_failure_refuses_the_session() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    server
        .shared
        .fail_socket_clone
        .store(true, Ordering::Relaxed);
    let mut client = admit(&server);
    let closed = matches!(client.read(&mut [0]), Ok(0));
    let _ = client.shutdown(std::net::Shutdown::Both);
    let (_, _, left) = server.end_sessions();
    assert!(closed, "a session without a shutdown handle must not start");
    assert_eq!(left, 0);
    assert!(server.shared.sessions.open.lock().unwrap().is_empty());
    assert_eq!(server.shared.live.load(Ordering::Relaxed), 0);
}

#[test]
fn beginning_another_statement_cannot_clear_shutdown() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    let session = Session::new(server.shared.clone());
    let armed = session.interrupts.begin(0);
    assert!(!session.interrupts.should_stop());
    server.end_sessions();
    assert!(session.interrupts.should_stop());
    drop(armed);
    let _next = session.interrupts.begin(0);
    assert!(
        session.interrupts.should_stop(),
        "statement begin cleared shutdown"
    );
}

#[test]
fn a_session_created_after_shutdown_starts_stopped() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    server.end_sessions();
    let session = Session::new(server.shared.clone());
    let _armed = session.interrupts.begin(0);
    assert!(
        session.interrupts.should_stop(),
        "a late session missed shutdown"
    );
}

#[test]
fn admission_after_shutdown_closes_without_starting_a_worker() {
    let dir = tempfile::TempDir::new().unwrap();
    let server = server(&dir);
    server.end_sessions();
    let mut client = admit(&server);
    let closed = matches!(client.read(&mut [0]), Ok(0));
    let started = server.threads.lock().unwrap().len();
    let _ = client.shutdown(std::net::Shutdown::Both);
    server.end_sessions();
    assert_eq!((closed, started), (true, 0));
    assert_eq!(server.shared.live.load(Ordering::Relaxed), 0);
}

fn server(dir: &tempfile::TempDir) -> TursoPgServer {
    let path = dir.path().join("c43.db").to_string_lossy().into_owned();
    let opts = database_opts(turso_core::branch::BranchDurability::Catalog {
        sync: turso_core::branch::SyncClass::Off,
    });
    let (_, db) =
        turso_pg::open_database(&path, None, turso_core::OpenFlags::default(), opts).unwrap();
    TursoPgServer::new(
        "127.0.0.1:0".into(),
        path,
        db,
        4,
        Duration::from_millis(100),
        Arc::new(AtomicUsize::new(0)),
    )
}

fn admit(server: &TursoPgServer) -> std::net::TcpStream {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_millis(250)))
        .unwrap();
    let (socket, addr) = listener.accept().unwrap();
    socket.set_nonblocking(true).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = rt.enter();
    server.admit(tokio::net::TcpStream::from_std(socket).unwrap(), addr);
    client
}
