//! Prewarm of a durable branch store's files at open (r12-catload lane; a measurement arm, opt-in by
//! `R12_PREWARM`, off when unset). ⚠ Unreviewed.
//!
//! A store opened after a reboot, a restore or on a new replica finds its files cold in the OS page
//! cache, so each first touch of a branch pays device reads: a catalog load's B-tree leaves, a
//! branch's arena pages. The published answer is to read the files ahead at open:
//!
//! * PostgreSQL's pg_prewarm (contrib) has three modes: `prefetch` "issues asynchronous prefetch
//!   requests to the operating system", `read` "reads the requested range of blocks" synchronously,
//!   and `buffer` "reads the requested range of blocks into the database buffer cache"; autoprewarm
//!   reloads the blocks shared buffers held before a restart.
//! * InnoDB's buffer pool dump and load saves the page ids of the most recently used pages at
//!   shutdown and reads them back at startup, in the background.
//! * PostgreSQL 15's `recovery_prefetch` prefetches the blocks the WAL names, with `posix_fadvise`.
//!
//! (Quoted from their manuals in artie-research frontier/round12/r12-catload/raw/priorart_fetch.txt.)
//!
//! Modes here:
//! * `read` and `prefetch` warm whole FILES in the OS page cache: the catalog and its WAL, and the
//!   arena. `read` reads them sequentially; `prefetch` asks the kernel to read them ahead and
//!   returns (`fcntl(F_RDADVISE)` on macOS, `posix_fadvise(POSIX_FADV_WILLNEED)` on Linux; refused
//!   elsewhere, as pg_prewarm's `prefetch` is).
//! * `interior` and `buffer` warm the catalog's own page cache (see `Catalog::prewarm`): its B-trees'
//!   interior pages, or every page.
//!
//! `R12_PREWARM_FILES` picks the files for `read` and `prefetch`: `catalog`, `arena`, or both (the
//! default, `catalog,arena`). A value either variable does not know refuses the open: a misspelt arm
//! must not run as the control.
//!
//! Blind spots, stated: the whole file is warmed, so the cost is linear in the file (pg_prewarm's
//! whole-relation form, not autoprewarm's hot set); the OS may evict what was warmed before it is
//! used; `prefetch` returns before the reads finish, so a touch right after the open can still wait.

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::{LimboError, Result};

/// What an open prewarms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Prewarm {
    #[default]
    Off,
    /// The catalog's interior B-tree pages into its page cache.
    Interior,
    /// Every catalog page into its page cache.
    Buffer,
    /// The selected files read sequentially into the OS page cache.
    Read,
    /// Read-ahead of the selected files requested from the OS.
    Prefetch,
}

impl Prewarm {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Prewarm::Off => "off",
            Prewarm::Interior => "interior",
            Prewarm::Buffer => "buffer",
            Prewarm::Read => "read",
            Prewarm::Prefetch => "prefetch",
        }
    }

    /// A mode that warms files in the OS page cache.
    pub(crate) fn warms_files(self) -> bool {
        matches!(self, Prewarm::Read | Prewarm::Prefetch)
    }
}

/// The files `read` and `prefetch` warm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Targets {
    pub(crate) catalog: bool,
    pub(crate) arena: bool,
}

/// `R12_PREWARM` and `R12_PREWARM_FILES`.
pub(crate) fn from_env() -> Result<(Prewarm, Targets)> {
    let bad = |what: String| Err(LimboError::InvalidArgument(what));
    let mode = match std::env::var("R12_PREWARM") {
        Err(std::env::VarError::NotPresent) => Prewarm::Off,
        Ok(v) => match v.as_str() {
            "" | "off" => Prewarm::Off,
            "interior" => Prewarm::Interior,
            "buffer" => Prewarm::Buffer,
            "read" => Prewarm::Read,
            "prefetch" => Prewarm::Prefetch,
            other => {
                return bad(format!(
                    "R12_PREWARM={other:?}: expected off, interior, buffer, read or prefetch"
                ))
            }
        },
        Err(e) => return bad(format!("R12_PREWARM: {e}")),
    };
    let mut targets = Targets {
        catalog: true,
        arena: true,
    };
    match std::env::var("R12_PREWARM_FILES") {
        Err(std::env::VarError::NotPresent) => {}
        Ok(v) => {
            targets = Targets {
                catalog: false,
                arena: false,
            };
            for part in v.split(',') {
                match part {
                    "catalog" => targets.catalog = true,
                    "arena" => targets.arena = true,
                    other => {
                        return bad(format!(
                            "R12_PREWARM_FILES={v:?}: {other:?} is not catalog or arena"
                        ))
                    }
                }
            }
        }
        Err(e) => return bad(format!("R12_PREWARM_FILES: {e}")),
    }
    Ok((mode, targets))
}

/// What a prewarm did (r12-catload instrument, observing only).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PrewarmStats {
    pub(crate) mode: Prewarm,
    /// Files warmed in the OS page cache (`read`, `prefetch`).
    pub(crate) files: u64,
    /// Bytes read from them (`read`).
    pub(crate) bytes: u64,
    /// Bytes whose read-ahead was requested (`prefetch`).
    pub(crate) advised: u64,
    /// Catalog pages requested through its page cache (`interior`, `buffer`).
    pub(crate) pages: u64,
    /// Interior pages among them (`interior`).
    pub(crate) interior: u64,
    /// The catalog page cache's capacity after the prewarm, in pages (0: no catalog).
    pub(crate) cache_pages: u64,
    pub(crate) ns: u64,
}

impl PrewarmStats {
    /// Two parts of one open's prewarm (the store's files and the catalog's pages) as one.
    pub(crate) fn merged(self, other: PrewarmStats) -> PrewarmStats {
        PrewarmStats {
            mode: if self.mode == Prewarm::Off { other.mode } else { self.mode },
            files: self.files + other.files,
            bytes: self.bytes + other.bytes,
            advised: self.advised + other.advised,
            pages: self.pages + other.pages,
            interior: self.interior + other.interior,
            cache_pages: self.cache_pages.max(other.cache_pages),
            ns: self.ns + other.ns,
        }
    }
}

/// A database file's WAL, named as Turso names it.
pub(crate) fn wal_of(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", path.display()))
}

/// Warm `paths` in the OS page cache as `mode` says (`read` or `prefetch`; any other mode does
/// nothing). A path that does not exist is skipped.
pub(crate) fn warm_files(paths: &[&Path], mode: Prewarm, st: &mut PrewarmStats) -> Result<()> {
    if !mode.warms_files() {
        return Ok(());
    }
    let t = std::time::Instant::now();
    let mut buf = if mode == Prewarm::Read {
        vec![0u8; 1 << 20]
    } else {
        Vec::new()
    };
    for &path in paths {
        let fail = |e: std::io::Error| {
            LimboError::InternalError(format!("branch prewarm: {}: {e}", path.display()))
        };
        let mut f = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(fail(e)),
        };
        st.files += 1;
        if mode == Prewarm::Read {
            loop {
                let n = f.read(&mut buf).map_err(fail)?;
                if n == 0 {
                    break;
                }
                st.bytes += n as u64;
            }
        } else {
            let len = f.metadata().map_err(fail)?.len();
            advise(&f, len).map_err(fail)?;
            st.advised += len;
        }
    }
    st.ns += u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX);
    Ok(())
}

/// Ask the OS to read `len` bytes of `f` ahead, and return without waiting.
#[cfg(target_os = "macos")]
fn advise(f: &std::fs::File, len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // `ra_count` is an int: the range goes in chunks.
    const CHUNK: u64 = 1 << 30;
    let mut off = 0u64;
    while off < len {
        let count = (len - off).min(CHUNK);
        let ra = libc::radvisory {
            ra_offset: off as libc::off_t,
            ra_count: count as libc::c_int,
        };
        // SAFETY: F_RDADVISE reads one radvisory, which `ra` is, for the duration of the call.
        if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_RDADVISE, &ra as *const libc::radvisory) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
        off += count;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise(f: &std::fs::File, _len: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: posix_fadvise reads nothing through pointers; offset 0 and length 0 mean the whole file.
    match unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED) } {
        0 => Ok(()),
        e => Err(std::io::Error::from_raw_os_error(e)),
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn advise(_f: &std::fs::File, _len: u64) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "R12_PREWARM=prefetch has no read-ahead request on this platform",
    ))
}
