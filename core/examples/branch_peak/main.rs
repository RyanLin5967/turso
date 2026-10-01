//! Peak then shrink (lane r11-ever, PREREG amendment 11; lane r12-f9-shrink): see `run.rs`. This
//! binary runs under the system allocator; `branch_peak_mi` runs the same body under mimalloc.

mod run;

/// The system allocator, counted (amendment 13: live heap bytes beside the footprint).
#[global_allocator]
static GLOBAL: run::Counting<std::alloc::System> = run::Counting(std::alloc::System);

/// libmalloc's purge on demand over every zone (`malloc_zone_pressure_relief`, goal 0: as much as
/// it can). Off macOS there is nothing to call.
fn relieve() -> usize {
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        // SAFETY: a null zone means every zone; the call only returns free memory to the OS.
        return unsafe { malloc_zone_pressure_relief(std::ptr::null_mut(), 0) };
    }
    #[allow(unreachable_code)]
    0
}

fn main() {
    run::main("system", relieve);
}
