use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatabasePrivilege {
    Read,
    Write,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUser {
    pub password_hash: String,
    pub admin: bool,
    pub created_at: String,
    #[serde(default)]
    pub privileges: HashMap<String, DatabasePrivilege>,
}

impl StoredUser {
    pub fn can_read(&self, db: &str) -> bool {
        if self.admin {
            return true;
        }
        matches!(
            self.privileges.get(db),
            Some(DatabasePrivilege::Read | DatabasePrivilege::All)
        )
    }

    pub fn can_write(&self, db: &str) -> bool {
        if self.admin {
            return true;
        }
        matches!(
            self.privileges.get(db),
            Some(DatabasePrivilege::Write | DatabasePrivilege::All)
        )
    }
}
