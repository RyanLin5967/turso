//! Peak then shrink (lane r11-ever, PREREG amendment 11; lane r12-f9-shrink): see `run.rs`. This
//! binary runs under the system allocator; `branch_peak_mi` runs the same body under mimalloc.

mod run;

fn main() {
    run::main("system");
}
