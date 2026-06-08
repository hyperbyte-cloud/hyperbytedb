//! Cold-start catalog recovery for persisted chDB data directories.
//!
//! libchdb-local keeps the `default` database in an Overlay engine and writes
//! table metadata under `metadata/default/*.sql` without persisting the database
//! attach file or Atomic symlink layout. A fresh process therefore generates a
//! new database UUID and fails to attach restored MergeTree parts.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use chdb_rust::format::OutputFormat;

use crate::adapters::chdb::session::SharedSession;
use crate::domain::chdb_naming::quote_backticks;
use crate::error::HyperbytedbError;

/// Prepare on-disk metadata so a fresh libchdb process can attach restored tables.
/// Must run before the first [`SharedSession`] open.
pub fn prepare_cold_start_metadata(session_path: &Path) -> Result<(), HyperbytedbError> {
    repair_atomic_default_symlink(session_path)
}

/// Persist the `default` database attach file after the first native table is
/// materialized so backups include the metadata libchdb needs on cold start.
pub async fn persist_default_database_metadata(
    session: &SharedSession,
) -> Result<bool, HyperbytedbError> {
    let session_path = Path::new(session.data_path());
    let default_sql = session_path.join("metadata/default.sql");
    if default_sql.exists() {
        return Ok(false);
    }

    let session = session.get()?;
    let raw = tokio::task::spawn_blocking(move || {
        let sql = "SELECT uuid FROM system.databases WHERE name = 'default' FORMAT TabSeparated";
        let result = session.0.execute(
            sql,
            Some(&[chdb_rust::arg::Arg::OutputFormat(
                OutputFormat::TabSeparated,
            )]),
        );
        match result {
            Ok(qr) => qr
                .data_utf8()
                .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
            Err(e) => Err(HyperbytedbError::Chdb(e.to_string())),
        }
    })
    .await
    .map_err(|e| {
        HyperbytedbError::Internal(format!("chDB default database query join error: {e}"))
    })??;

    let uuid = raw.lines().next().unwrap_or_default().trim();
    if uuid.is_empty() {
        return Err(HyperbytedbError::Chdb(
            "default database uuid missing from system.databases".into(),
        ));
    }

    write_default_database_sql(session_path, uuid)?;
    Ok(true)
}

/// Attach any tables declared in `metadata/default/*.sql` that are not yet visible
/// in `system.tables`. Runs after the session is open as a fallback.
pub async fn reload_persisted_tables(session: &SharedSession) -> Result<usize, HyperbytedbError> {
    let meta_dir = Path::new(session.data_path()).join("metadata/default");
    if !meta_dir.is_dir() {
        return Ok(0);
    }

    let existing = list_default_tables(session).await?;
    let mut attached = 0usize;

    for entry in fs::read_dir(&meta_dir).map_err(|e| {
        HyperbytedbError::Chdb(format!(
            "failed to read chDB metadata dir {}: {e}",
            meta_dir.display()
        ))
    })? {
        let entry = entry.map_err(|e| {
            HyperbytedbError::Chdb(format!(
                "failed to read chDB metadata entry in {}: {e}",
                meta_dir.display()
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sql") {
            continue;
        }
        let Some(table_name) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if existing.contains(table_name) {
            continue;
        }

        let body = fs::read_to_string(&path).map_err(|e| {
            HyperbytedbError::Chdb(format!(
                "failed to read chDB table metadata {}: {e}",
                path.display()
            ))
        })?;
        let sql = attach_sql_for_table(table_name, &body);
        tracing::info!(table = %table_name, "attaching restored chDB table from on-disk metadata");
        execute_statement(session, &sql).await?;
        attached += 1;
    }

    Ok(attached)
}

fn repair_atomic_default_symlink(session_path: &Path) -> Result<(), HyperbytedbError> {
    let default_sql_path = session_path.join("metadata/default.sql");
    let default_meta = session_path.join("metadata/default");
    if default_meta.is_symlink() || !default_meta.is_dir() {
        return Ok(());
    }

    let default_sql = fs::read_to_string(&default_sql_path).map_err(|e| {
        HyperbytedbError::Chdb(format!(
            "failed to read chDB database metadata {}: {e}",
            default_sql_path.display()
        ))
    })?;
    let Some(uuid) = parse_default_database_uuid(&default_sql) else {
        return Ok(());
    };

    let store_dir = store_path_for_uuid(session_path, uuid);
    if !store_dir.is_dir() {
        return Err(HyperbytedbError::Chdb(format!(
            "expected chDB store directory for default database uuid {uuid}: {}",
            store_dir.display()
        )));
    }

    fs::remove_dir_all(&default_meta).map_err(|e| {
        HyperbytedbError::Chdb(format!(
            "failed to remove chDB metadata dir {} before symlink repair: {e}",
            default_meta.display()
        ))
    })?;

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&store_dir, &default_meta).map_err(|e| {
            HyperbytedbError::Chdb(format!(
                "failed to create chDB metadata symlink {} -> {}: {e}",
                default_meta.display(),
                store_dir.display()
            ))
        })?;
    }
    #[cfg(not(unix))]
    {
        return Err(HyperbytedbError::Chdb(
            "chDB cold start metadata repair requires Unix symlinks".into(),
        ));
    }

    tracing::info!(
        uuid = %uuid,
        symlink = %default_meta.display(),
        "repaired chDB default database metadata symlink for cold start"
    );
    Ok(())
}

fn write_default_database_sql(session_path: &Path, uuid: &str) -> Result<(), HyperbytedbError> {
    let default_sql = session_path.join("metadata/default.sql");
    let Some(parent) = default_sql.parent() else {
        return Err(HyperbytedbError::Chdb(
            "default.sql path has no parent directory".to_string(),
        ));
    };
    fs::create_dir_all(parent).map_err(|e| {
        HyperbytedbError::Chdb(format!(
            "failed to create chDB metadata dir {}: {e}",
            parent.display()
        ))
    })?;
    let statement = format!("ATTACH DATABASE default ENGINE=Atomic UUID '{uuid}'\n");
    fs::write(&default_sql, statement).map_err(|e| {
        HyperbytedbError::Chdb(format!(
            "failed to write chDB database metadata {}: {e}",
            default_sql.display()
        ))
    })?;
    tracing::info!(
        path = %default_sql.display(),
        uuid = %uuid,
        "persisted chDB default database metadata for cold start"
    );
    Ok(())
}

fn parse_default_database_uuid(default_sql: &str) -> Option<&str> {
    let marker = "UUID '";
    let start = default_sql.find(marker)? + marker.len();
    let rest = &default_sql[start..];
    let end = rest.find('\'')?;
    Some(&rest[..end])
}

fn store_path_for_uuid(session_path: &Path, uuid: &str) -> PathBuf {
    let prefix = uuid.get(..3).unwrap_or(uuid);
    session_path.join("store").join(prefix).join(uuid)
}

fn attach_sql_for_table(table: &str, body: &str) -> String {
    let quoted = quote_backticks(table);
    if body.contains("ATTACH TABLE _") {
        body.replacen("ATTACH TABLE _", &format!("ATTACH TABLE {quoted}"), 1)
    } else {
        body.to_string()
    }
}

async fn list_default_tables(session: &SharedSession) -> Result<HashSet<String>, HyperbytedbError> {
    let raw = query_tab_separated(
        session,
        "SELECT name FROM system.tables WHERE database = 'default'",
    )
    .await?;
    Ok(raw
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

async fn query_tab_separated(
    session: &SharedSession,
    sql: &str,
) -> Result<String, HyperbytedbError> {
    let session = session.get()?;
    let sql = format!("{sql} FORMAT TabSeparated");
    tokio::task::spawn_blocking(move || {
        let result = session.0.execute(
            &sql,
            Some(&[chdb_rust::arg::Arg::OutputFormat(
                OutputFormat::TabSeparated,
            )]),
        );
        match result {
            Ok(qr) => qr
                .data_utf8()
                .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
            Err(e) => Err(HyperbytedbError::Chdb(e.to_string())),
        }
    })
    .await
    .map_err(|e| HyperbytedbError::Internal(format!("chDB catalog query join error: {e}")))?
}

async fn execute_statement(session: &SharedSession, sql: &str) -> Result<(), HyperbytedbError> {
    let session = session.get()?;
    let sql = sql.to_string();
    tokio::task::spawn_blocking(move || {
        session
            .0
            .execute(&sql, None)
            .map(|_| ())
            .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
    })
    .await
    .map_err(|e| HyperbytedbError::Internal(format!("chDB catalog attach join error: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_path_uses_uuid_prefix() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = store_path_for_uuid(tmp.path(), "722d7b65-10de-4d87-80a0-62f649fe492d");
        assert_eq!(
            path,
            tmp.path()
                .join("store/722/722d7b65-10de-4d87-80a0-62f649fe492d")
        );
    }

    #[test]
    fn parses_default_database_uuid() {
        let sql =
            "ATTACH DATABASE default ENGINE=Atomic UUID '722d7b65-10de-4d87-80a0-62f649fe492d'\n";
        assert_eq!(
            parse_default_database_uuid(sql),
            Some("722d7b65-10de-4d87-80a0-62f649fe492d")
        );
    }
}
