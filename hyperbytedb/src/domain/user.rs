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

/// A salted argon2 hash of `password`, as stored for users.
///
/// # Errors
/// `Internal` if hashing fails.
pub fn hash_password(password: &str) -> Result<String, crate::error::HyperbytedbError> {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| {
            crate::error::HyperbytedbError::Internal(format!("password hash failed: {e}").into())
        })?;
    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {

    #[test]
    fn hash_password_is_a_salted_argon2_hash() {
        let a = super::hash_password("secret").unwrap();
        let b = super::hash_password("secret").unwrap();
        assert!(a.starts_with("$argon2"), "{a}");
        assert_ne!(a, b, "each hash has its own salt");
        assert!(!a.contains("secret"));
    }

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
