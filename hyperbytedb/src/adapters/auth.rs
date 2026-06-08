use async_trait::async_trait;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::domain::user::StoredUser;
use crate::error::HyperbytedbError;
use crate::ports::auth::AuthPort;
use crate::ports::metadata::MetadataPort;

const CREDENTIAL_CACHE_TTL_SECS: u64 = 60;

/// Fast non-crypto hash of the input password for cache keying.
fn password_fingerprint(password: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    password.hash(&mut h);
    h.finish()
}

/// Implements [`AuthPort`] by looking up users via [`MetadataPort`] and
/// verifying passwords with Argon2. Caches successful verifications
/// for `CREDENTIAL_CACHE_TTL_SECS` to avoid redundant Argon2 CPU work.
pub struct MetadataAuthAdapter {
    metadata: Arc<dyn MetadataPort>,
    /// `(username, password_fingerprint) → (stored_hash, verified_at)`.
    /// Keyed on a hash of the input password so different passwords produce
    /// different cache entries. The stored hash is retained to auto-invalidate
    /// if the user's password is changed in the DB.
    verified_cache: RwLock<HashMap<(String, u64), (String, Instant)>>,
}

impl MetadataAuthAdapter {
    pub fn new(metadata: Arc<dyn MetadataPort>) -> Self {
        Self {
            metadata,
            verified_cache: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AuthPort for MetadataAuthAdapter {
    async fn authenticate(&self, username: &str, password: &str) -> Result<bool, HyperbytedbError> {
        Ok(self.authenticate_user(username, password).await?.is_some())
    }

    async fn authenticate_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<StoredUser>, HyperbytedbError> {
        let stored = match self.metadata.get_user(username).await? {
            Some(s) => s,
            None => return Ok(None),
        };

        let pw_fp = password_fingerprint(password);
        let cache_key = (username.to_string(), pw_fp);
        if let Ok(cache) = self.verified_cache.read()
            && let Some((cached_hash, ts)) = cache.get(&cache_key)
            && *cached_hash == stored.password_hash
            && ts.elapsed().as_secs() < CREDENTIAL_CACHE_TTL_SECS
        {
            return Ok(Some(stored));
        }

        if !verify_password(password, &stored.password_hash) {
            return Ok(None);
        }

        if let Ok(mut cache) = self.verified_cache.write() {
            cache.insert(cache_key, (stored.password_hash.clone(), Instant::now()));
            if cache.len() > 1000 {
                let cutoff =
                    Instant::now() - std::time::Duration::from_secs(CREDENTIAL_CACHE_TTL_SECS);
                cache.retain(|_, (_, ts)| *ts > cutoff);
            }
        }

        Ok(Some(stored))
    }
}

fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::password_hash::PasswordVerifier;
    let parsed = match argon2::password_hash::PasswordHash::new(hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    argon2::Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}
