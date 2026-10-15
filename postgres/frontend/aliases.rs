use chrono::Utc;
use turso_ext::{scalar, ExtensionApi, Value as ExtValue};

/// How many times the PostgreSQL time aliases were registered in this process (each a set of
/// symbol-table inserts, 13 allocations): observation for the once-per-Database registration test.
static REGISTRATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[doc(hidden)]
pub fn registrations() -> u64 {
    REGISTRATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Registered once per Database by `PostgresDialect::static_extensions`.
pub(crate) fn register_pg_functions(ext_api: &mut ExtensionApi) {
    REGISTRATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    unsafe {
        register_alias(ext_api, c"now".as_ptr());
        register_alias(ext_api, c"clock_timestamp".as_ptr());
        register_alias(ext_api, c"transaction_timestamp".as_ptr());
        register_alias(ext_api, c"statement_timestamp".as_ptr());
    }
}

unsafe fn register_alias(ext_api: &mut ExtensionApi, name: *const std::ffi::c_char) {
    (ext_api.register_scalar_function)(
        ext_api.ctx,
        name,
        -1,
        false,
        0,
        postgres_frontend_now,
        None,
        None,
    );
}

#[scalar(name = "now")]
fn postgres_frontend_now(_args: &[ExtValue]) -> ExtValue {
    let now = Utc::now();
    let formatted = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    ExtValue::from_text(formatted)
}
