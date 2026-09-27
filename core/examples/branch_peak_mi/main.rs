//! `branch_peak` under mimalloc as the global allocator (lane r12-f9-shrink, PREREG section 2 item 7:
//! allocator decay for the process heap). The body is `branch_peak/run.rs`, unchanged.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[path = "../branch_peak/run.rs"]
mod run;

fn main() {
    run::main("mimalloc");
}
