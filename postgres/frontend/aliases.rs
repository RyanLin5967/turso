use chrono::Utc;
use turso_core::Connection;
use turso_ext::{scalar, ExtensionApi, Value as ExtValue};

pub fn install(conn: &Connection) {
    conn.register_static_extension(register_pg_functions);
}

fn register_pg_functions(ext_api: &mut ExtensionApi) {
    unsafe {
        register_alias(ext_api, c"now".as_ptr());
        register_alias(ext_api, c"clock_timestamp".as_ptr());
        register_alias(ext_api, c"transaction_timestamp".as_ptr());
        register_alias(ext_api, c"statement_timestamp".as_ptr());
        // PostgreSQL's make_date / make_timestamp refuse a date past the range (22008) where the
        // SQLite dialect's answer NULL; registered last, they replace the engine's.
        register_scalar(ext_api, c"make_date".as_ptr(), turso_core::make_date_or_refuse);
        register_scalar(ext_api, c"make_timestamp".as_ptr(), turso_core::make_timestamp_or_refuse);
    }
}

unsafe fn register_scalar(
    ext_api: &mut ExtensionApi,
    name: *const std::ffi::c_char,
    func: turso_ext::ScalarFunction,
) {
    (ext_api.register_scalar_function)(ext_api.ctx, name, -1, false, 0, func, None, None);
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
