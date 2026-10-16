//! Rewording a real producer's diagnostic must preserve its SQL condition and engine behavior.
use super::*;

#[test]
fn c44_rewording_a_parser_error_preserves_its_condition() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("wording.db").to_string_lossy().into_owned();
    let (_io, db) = turso_pg::open_database(
        &path,
        None,
        turso_core::OpenFlags::default(),
        database_opts(turso_core::branch::BranchDurability::Catalog {
            sync: turso_core::branch::SyncClass::Off,
        }),
    )
    .unwrap();
    let session = Session::new(Arc::new(Shared::new(
        db,
        path,
        1,
        std::time::Duration::from_millis(DEFAULT_LOCK_WAIT_MS),
    )));
    let conn = session.current(&mut session.state()).unwrap();
    let literal = conn
        .prepare("SELECT 'not-a-number'::float8")
        .err()
        .expect("the invalid float literal must be refused");
    assert_rewording(literal, "22P02");
}

#[test]
fn c44_rewording_a_date_error_preserves_its_condition() {
    let error = turso_core::make_date_or_refuse(&[
        Value::from_i64(i32::MAX as i64),
        Value::from_i64(1),
        Value::from_i64(1),
    ])
    .unwrap_err();
    assert_rewording(error, "22008");
}

fn assert_rewording(mut error: LimboError, code: &str) {
    assert_eq!(sqlstate(&error), code, "the producer's original condition");
    let sqlite_code = error.sqlite_result_code();
    let catchable = error.is_catchable_value_error();
    match &mut error {
        LimboError::ParseError(message) | LimboError::ExtensionError(message) => {
            message.clear();
            message.push_str("stack depth limit exceeded: revised diagnostic");
        }
        other => panic!("unexpected producer variant: {other:?}"),
    }
    assert_eq!(
        sqlstate(&error),
        code,
        "rewording must preserve the condition"
    );
    assert_eq!(error.sqlite_result_code(), sqlite_code);
    assert_eq!(error.is_catchable_value_error(), catchable);
}

#[test]
fn c44_protocol_text_preserves_engine_behavior() {
    use turso_parser::sql_error::SqlErrorMessage;
    let errors = [
        LimboError::ParseError(SqlErrorMessage::classified(
            SqlErrorKind::DuplicateTable,
            "table t already exists",
        )),
        LimboError::Raise(
            turso_parser::ast::ResolveType::Abort,
            SqlErrorMessage::classified(
                SqlErrorKind::StringDataRightTruncation,
                "value too long for type character",
            ),
        ),
    ];
    for mut error in errors {
        let display = error.to_string();
        let sqlite_code = error.sqlite_result_code();
        let catchable = error.is_catchable_value_error();
        let code = sqlstate(&error);
        match &mut error {
            LimboError::ParseError(message) | LimboError::Raise(_, message) => {
                *message = message
                    .clone()
                    .with_protocol_text("producer-selected protocol text");
            }
            _ => unreachable!(),
        }
        assert_eq!(
            engine_info(&error).message,
            "producer-selected protocol text"
        );
        assert_eq!(sqlstate(&error), code);
        assert_eq!(error.to_string(), display);
        assert_eq!(error.sqlite_result_code(), sqlite_code);
        assert_eq!(error.is_catchable_value_error(), catchable);
    }
}
