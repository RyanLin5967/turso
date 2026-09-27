//! `branch_peak` under mimalloc as the global allocator (lane r12-f9-shrink, PREREG section 2 item 7:
//! allocator decay for the process heap). The body is `branch_peak/run.rs`, unchanged.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[path = "../branch_peak/run.rs"]
mod run;

extern "C" {
    /// mimalloc's own purge on demand; linked through the `mimalloc` crate above.
    fn mi_collect(force: bool);
}

fn relieve() -> usize {
    // SAFETY: mi_collect only frees memory mimalloc holds and no object uses.
    unsafe { mi_collect(true) };
    0
}

fn main() {
    run::main("mimalloc", relieve);
}
