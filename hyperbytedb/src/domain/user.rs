use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DatabasePrivilege {
    Read,
    Write,
    /// The default so a Raft entry from before `GRANT READ | WRITE` (which
    /// had no privilege, and always meant ALL) still decodes as it did.
    #[default]
    All,
}

impl DatabasePrivilege {
    /// What is left after `REVOKE <revoked>`, as InfluxDB v1 does it: ALL
    /// less READ is WRITE, and revoking what isn't held changes nothing.
    #[must_use]
    pub fn without(self, revoked: Self) -> Option<Self> {
        use DatabasePrivilege::{All, Read, Write};
        match (self, revoked) {
            (_, All) | (Read, Read) | (Write, Write) => None,
            (All, Read) => Some(Write),
            (All, Write) => Some(Read),
            (held, _) => Some(held),
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            DatabasePrivilege::Read => "READ",
            DatabasePrivilege::Write => "WRITE",
            DatabasePrivilege::All => "ALL PRIVILEGES",
        }
    }
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

#[cfg(test)]
mod tests {
    use super::DatabasePrivilege::{self, All, Read, Write};

    #[test]
    fn revoke_clears_only_the_revoked_privilege() {
        let cases: [(
            DatabasePrivilege,
            DatabasePrivilege,
            Option<DatabasePrivilege>,
        ); 9] = [
            (All, Read, Some(Write)),
            (All, Write, Some(Read)),
            (All, All, None),
            (Read, Read, None),
            (Read, Write, Some(Read)),
            (Read, All, None),
            (Write, Write, None),
            (Write, Read, Some(Write)),
            (Write, All, None),
        ];
        for (held, revoked, left) in cases {
            assert_eq!(held.without(revoked), left, "{held:?} - {revoked:?}");
        }
    }

    /// A Raft entry written before GRANT took a privilege meant ALL.
    #[test]
    fn an_old_grant_entry_decodes_as_all() {
        let old = r#"{"Grant":{"username":"u","database":"db"}}"#;
        match serde_json::from_str(old).unwrap() {
            crate::domain::cluster::types::MutationRequest::Grant { privilege, .. } => {
                assert_eq!(privilege, All);
            }
            other => panic!("{other:?}"),
        }
    }
}
