use crate::config::HyperbytedbConfig;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct BackupManifest {
    timestamp: String,
    wal_last_seq: u64,
    #[serde(alias = "parquet_files")]
    engine_data_paths: Vec<String>,
}

pub async fn backup(config: HyperbytedbConfig, output_path: &str) -> anyhow::Result<()> {
    let output_path = output_path.to_string();
    tokio::task::spawn_blocking(move || {
        tracing::info!(output = %output_path, "starting backup");

        std::fs::create_dir_all(&output_path)?;

        // Checkpoint WAL RocksDB
        let wal_backup = format!("{}/wal", output_path);
        let wal_last_seq;
        {
            let mut wal_opts = rocksdb::Options::default();
            wal_opts.create_missing_column_families(true);
            let wal_cfs = vec![
                rocksdb::ColumnFamilyDescriptor::new("default", rocksdb::Options::default()),
                rocksdb::ColumnFamilyDescriptor::new("wal_meta", rocksdb::Options::default()),
            ];
            let wal_db = rocksdb::DB::open_cf_descriptors_read_only(
                &wal_opts,
                &config.storage.wal_dir,
                wal_cfs,
                false,
            )
            .map_err(|e| anyhow::anyhow!("failed to open WAL for backup: {e}"))?;

            wal_last_seq = wal_db
                .cf_handle("wal_meta")
                .and_then(|cf| wal_db.get_cf(&cf, b"last_seq").ok().flatten())
                .and_then(|v| {
                    let arr: [u8; 8] = v.as_slice().try_into().ok()?;
                    Some(u64::from_le_bytes(arr))
                })
                .unwrap_or(0);

            let checkpoint = rocksdb::checkpoint::Checkpoint::new(&wal_db)
                .map_err(|e| anyhow::anyhow!("failed to create WAL checkpoint: {e}"))?;
            checkpoint
                .create_checkpoint(&wal_backup)
                .map_err(|e| anyhow::anyhow!("WAL checkpoint failed: {e}"))?;
        }
        tracing::info!(wal_last_seq, "WAL checkpoint created");

        // Checkpoint metadata RocksDB
        let meta_backup = format!("{}/meta", output_path);
        {
            let meta_opts = {
                let mut opts = rocksdb::Options::default();
                opts.create_missing_column_families(true);
                opts
            };
            let cfs = vec![rocksdb::ColumnFamilyDescriptor::new(
                "metadata",
                rocksdb::Options::default(),
            )];
            let meta_db = rocksdb::DB::open_cf_descriptors_read_only(
                &meta_opts,
                &config.storage.meta_dir,
                cfs,
                false,
            )
            .map_err(|e| anyhow::anyhow!("failed to open metadata for backup: {e}"))?;
            let checkpoint = rocksdb::checkpoint::Checkpoint::new(&meta_db)
                .map_err(|e| anyhow::anyhow!("failed to create metadata checkpoint: {e}"))?;
            checkpoint
                .create_checkpoint(&meta_backup)
                .map_err(|e| anyhow::anyhow!("metadata checkpoint failed: {e}"))?;
        }
        tracing::info!("metadata checkpoint created");

        // Embedded chDB session directory (historically stored beside Parquet under data/)
        let chdb_backup = format!("{}/data", output_path);
        let mut engine_paths = Vec::new();
        if std::path::Path::new(&config.chdb.session_data_path).exists() {
            copy_dir_recursive(
                &config.chdb.session_data_path,
                &chdb_backup,
                &mut engine_paths,
            )?;
        }
        tracing::info!(count = engine_paths.len(), "chDB session data backed up");

        // Write manifest
        let manifest = BackupManifest {
            timestamp: chrono::Utc::now().to_rfc3339(),
            wal_last_seq,
            engine_data_paths: engine_paths,
        };
        let manifest_path = format!("{}/manifest.json", output_path);
        std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

        tracing::info!(path = %output_path, "backup complete");
        Ok(())
    })
    .await?
}

pub async fn restore(config: HyperbytedbConfig, input_path: &str) -> anyhow::Result<()> {
    let input_path = input_path.to_string();
    tokio::task::spawn_blocking(move || {
        tracing::info!(input = %input_path, "starting restore");

        let manifest_path = format!("{}/manifest.json", input_path);
        if !std::path::Path::new(&manifest_path).exists() {
            anyhow::bail!("manifest.json not found in backup directory");
        }
        let manifest: BackupManifest =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;

        tracing::info!(
            timestamp = %manifest.timestamp,
            wal_last_seq = manifest.wal_last_seq,
            session_file_count = manifest.engine_data_paths.len(),
            "loaded backup manifest"
        );

        let wal_backup = format!("{}/wal", input_path);
        let meta_backup = format!("{}/meta", input_path);
        let data_backup = format!("{}/data", input_path);

        if !std::path::Path::new(&wal_backup).exists()
            && !std::path::Path::new(&meta_backup).exists()
            && !std::path::Path::new(&data_backup).exists()
        {
            anyhow::bail!("backup directory contains no wal/, meta/, or data/ subdirectories");
        }

        // Restore WAL
        if std::path::Path::new(&wal_backup).exists() {
            if std::path::Path::new(&config.storage.wal_dir).exists() {
                std::fs::remove_dir_all(&config.storage.wal_dir)?;
            }
            copy_dir_recursive(&wal_backup, &config.storage.wal_dir, &mut Vec::new())?;
            tracing::info!("WAL restored");
        }

        // Restore metadata
        if std::path::Path::new(&meta_backup).exists() {
            if std::path::Path::new(&config.storage.meta_dir).exists() {
                std::fs::remove_dir_all(&config.storage.meta_dir)?;
            }
            copy_dir_recursive(&meta_backup, &config.storage.meta_dir, &mut Vec::new())?;
            tracing::info!("metadata restored");
        }

        // Restore chDB session directory
        if std::path::Path::new(&data_backup).exists() {
            if std::path::Path::new(&config.chdb.session_data_path).exists() {
                std::fs::remove_dir_all(&config.chdb.session_data_path)?;
            }
            let mut restored_files = Vec::new();
            copy_dir_recursive(
                &data_backup,
                &config.chdb.session_data_path,
                &mut restored_files,
            )?;
            tracing::info!(count = restored_files.len(), "chDB session data restored");
        }

        tracing::info!("restore complete");
        Ok(())
    })
    .await?
}

fn copy_dir_recursive(src: &str, dst: &str, files: &mut Vec<String>) -> anyhow::Result<()> {
    let mut queue = vec![(src.to_string(), dst.to_string())];
    while let Some((src_dir, dst_dir)) = queue.pop() {
        std::fs::create_dir_all(&dst_dir)?;
        for entry in std::fs::read_dir(&src_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let dst_path = format!("{}/{}", dst_dir, name.to_string_lossy());
            if path.is_dir() {
                queue.push((path.to_string_lossy().into_owned(), dst_path));
            } else {
                std::fs::copy(&path, &dst_path)?;
                files.push(path.to_string_lossy().into_owned());
            }
        }
    }
    Ok(())
}
