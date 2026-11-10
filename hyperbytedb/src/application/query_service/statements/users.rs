//! User and privilege statements.

use super::super::*;

pub(in crate::application::query_service) async fn create_user(
    svc: &QueryServiceImpl,
    username: &str,
    password: &str,
    admin: &bool,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let password_hash = crate::domain::user::hash_password(password)
        .map_err(|e| HyperbytedbError::Internal(crate::error::ChainedError::from_error(e)))?;
    crate::application::schema_mutation_apply::create_user(
        svc.metadata.as_ref(),
        username,
        &password_hash,
        *admin,
    )
    .await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn grant(
    svc: &QueryServiceImpl,
    username: &str,
    database: &Option<String>,
    privilege: &crate::domain::user::DatabasePrivilege,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    match database {
        Some(db) => {
            svc.metadata
                .grant_privilege(username, db, *privilege)
                .await?;
        }
        None => {
            if svc.metadata.get_user(username).await?.is_none() {
                return Err(HyperbytedbError::QueryParse(format!(
                    "user '{username}' does not exist"
                )));
            }
            crate::application::schema_mutation_apply::set_admin(
                svc.metadata.as_ref(),
                username,
                true,
            )
            .await?;
        }
    }
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn revoke(
    svc: &QueryServiceImpl,
    username: &str,
    database: &Option<String>,
    privilege: &crate::domain::user::DatabasePrivilege,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    match database {
        Some(db) => {
            crate::application::schema_mutation_apply::revoke_privilege(
                svc.metadata.as_ref(),
                username,
                db,
                *privilege,
            )
            .await?;
        }
        None => {
            if svc.metadata.get_user(username).await?.is_none() {
                return Err(HyperbytedbError::QueryParse(format!(
                    "user '{username}' does not exist"
                )));
            }
            crate::application::schema_mutation_apply::set_admin(
                svc.metadata.as_ref(),
                username,
                false,
            )
            .await?;
        }
    }
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn set_password(
    svc: &QueryServiceImpl,
    username: &str,
    password: &str,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let password_hash = crate::domain::user::hash_password(password)
        .map_err(|e| HyperbytedbError::Internal(crate::error::ChainedError::from_error(e)))?;
    crate::application::schema_mutation_apply::set_password(
        svc.metadata.as_ref(),
        username,
        &password_hash,
    )
    .await?;
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}
