use async_trait::async_trait;

use crate::domain::user::StoredUser;
use crate::error::HyperbytedbError;

#[async_trait]
pub trait AuthPort: Send + Sync {
    async fn authenticate(&self, username: &str, password: &str) -> Result<bool, HyperbytedbError>;

    async fn authenticate_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<StoredUser>, HyperbytedbError>;
}
