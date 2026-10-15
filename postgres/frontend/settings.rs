use turso_parser::ast;

/// Taken from the translated statement, so quoted savepoint names and commented commands use
/// exactly the transaction operation the engine executes.
#[derive(Debug, Clone)]
pub enum SettingsTransaction {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    Release(String),
    RollbackTo(String),
}

impl SettingsTransaction {
    pub(crate) fn from_stmt(stmt: &ast::Stmt) -> Option<Self> {
        Some(match stmt {
            ast::Stmt::Begin { .. } => Self::Begin,
            ast::Stmt::Commit { .. } => Self::Commit,
            ast::Stmt::Rollback {
                savepoint_name: None,
                ..
            } => Self::Rollback,
            ast::Stmt::Rollback {
                savepoint_name: Some(name),
                ..
            } => Self::RollbackTo(name.as_str().to_owned()),
            ast::Stmt::Savepoint { name } => Self::Savepoint(name.as_str().to_owned()),
            ast::Stmt::Release { name } => Self::Release(name.as_str().to_owned()),
            _ => return None,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Values {
    pub search_path: Vec<String>,
    pub foreign_keys: bool,
    pub lock_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub min_messages: u8,
}

impl Default for Values {
    fn default() -> Self {
        Self {
            search_path: Vec::new(),
            foreign_keys: true,
            lock_timeout_ms: 0,
            statement_timeout_ms: 0,
            min_messages: 7,
        }
    }
}

pub(crate) enum Setting {
    SearchPath(Vec<String>),
    ForeignKeys(bool),
    LockTimeout(u64),
    StatementTimeout(u64),
    MinMessages(u8),
}

impl Setting {
    fn apply(&self, values: &mut Values) {
        match self {
            Self::SearchPath(path) => values.search_path.clone_from(path),
            Self::ForeignKeys(on) => values.foreign_keys = *on,
            Self::LockTimeout(ms) => values.lock_timeout_ms = *ms,
            Self::StatementTimeout(ms) => values.statement_timeout_ms = *ms,
            Self::MinMessages(rank) => values.min_messages = *rank,
        }
    }
}

struct Frame {
    name: Option<String>,
    before: Values,
    persistent_before: Values,
    persistent: Values,
}

#[derive(Default)]
pub(crate) struct Settings {
    pub current: Values,
    frames: Vec<Frame>,
}

impl Settings {
    pub fn adopt(&mut self, values: Values) {
        self.current = values;
        self.frames.clear();
    }

    /// All mutable settings pass here. SET changes both the visible and committed value; LOCAL
    /// changes only the visible value. A later SET therefore supersedes an earlier LOCAL too.
    pub fn assign(&mut self, setting: Setting, local: bool, in_transaction: bool) -> bool {
        if local && !in_transaction {
            return false;
        }
        if in_transaction && self.frames.is_empty() {
            self.push(None);
        }
        setting.apply(&mut self.current);
        if !local {
            if let Some(frame) = self.frames.last_mut() {
                setting.apply(&mut frame.persistent);
            }
        }
        matches!(setting, Setting::SearchPath(_))
    }

    /// Called only after the engine successfully completes the operation. Returns whether the
    /// search path changed, including restoration at a block or savepoint boundary.
    pub fn transaction(&mut self, operation: &SettingsTransaction) -> bool {
        match operation {
            SettingsTransaction::Begin => {
                if self.frames.is_empty() {
                    self.push(None);
                }
                false
            }
            SettingsTransaction::Commit | SettingsTransaction::Rollback => {
                if self.frames.is_empty() {
                    return false;
                }
                let persistent = self.frames.last().unwrap().persistent.clone();
                let first = self.frames.remove(0);
                self.frames.clear();
                self.restore(if matches!(operation, SettingsTransaction::Commit) {
                    persistent
                } else {
                    first.before
                })
            }
            SettingsTransaction::Savepoint(name) => {
                if self.frames.is_empty() {
                    self.push(None);
                }
                self.push(Some(name.clone()));
                false
            }
            SettingsTransaction::Release(name) => {
                if let Some(at) = self
                    .frames
                    .iter()
                    .rposition(|f| f.name.as_ref() == Some(name))
                {
                    let persistent = self.frames.last().unwrap().persistent.clone();
                    self.frames.truncate(at);
                    if let Some(parent) = self.frames.last_mut() {
                        parent.persistent = persistent;
                    }
                }
                false
            }
            SettingsTransaction::RollbackTo(name) => {
                if let Some(at) = self
                    .frames
                    .iter()
                    .rposition(|f| f.name.as_ref() == Some(name))
                {
                    self.frames.truncate(at + 1);
                    let frame = &mut self.frames[at];
                    frame.persistent.clone_from(&frame.persistent_before);
                    let before = frame.before.clone();
                    self.restore(before)
                } else {
                    false
                }
            }
        }
    }

    fn push(&mut self, name: Option<String>) {
        let persistent = self
            .frames
            .last()
            .map_or(&self.current, |f| &f.persistent)
            .clone();
        self.frames.push(Frame {
            name,
            before: self.current.clone(),
            persistent_before: persistent.clone(),
            persistent,
        });
    }

    fn restore(&mut self, values: Values) -> bool {
        let path_changed = self.current.search_path != values.search_path;
        self.current = values;
        path_changed
    }
}
