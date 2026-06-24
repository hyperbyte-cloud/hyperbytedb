//! Versioned on-disk WAL payload for prepared Arrow slots.

use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::domain::prepared_wal::{PreparedMeasurementBatch, PreparedWalSlot};
use crate::domain::wal::WalEntry;
use crate::error::HyperbytedbError;
use crate::ports::wal::WalFormat;

const MAGIC: &[u8; 4] = b"HBWA";
const VERSION: u8 = 1;

fn write_string(w: &mut Vec<u8>, s: &str) -> Result<(), HyperbytedbError> {
    let bytes = s.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(HyperbytedbError::Wal("WAL string too long".into()));
    }
    w.write_all(&(bytes.len() as u16).to_le_bytes())
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    w.write_all(bytes)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    Ok(())
}

fn read_string(r: &mut Cursor<&[u8]>) -> Result<String, HyperbytedbError> {
    let mut len_buf = [0u8; 2];
    r.read_exact(&mut len_buf)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    let len = u16::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    String::from_utf8(buf).map_err(|e| HyperbytedbError::Wal(e.to_string()))
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, HyperbytedbError> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| HyperbytedbError::Wal(format!("IPC encode: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| HyperbytedbError::Wal(format!("IPC encode write: {e}")))?;
        writer
            .finish()
            .map_err(|e| HyperbytedbError::Wal(format!("IPC encode finish: {e}")))?;
    }
    Ok(buf)
}

fn decode_record_batch(bytes: &[u8]) -> Result<Arc<RecordBatch>, HyperbytedbError> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| HyperbytedbError::Wal(format!("IPC decode: {e}")))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|e| HyperbytedbError::Wal(format!("IPC decode batch: {e}")))?
        .ok_or_else(|| HyperbytedbError::Wal("IPC stream empty".into()))?;
    Ok(Arc::new(batch))
}

/// Encode a prepared slot (and optional legacy WalEntry for peer sync) to bytes.
pub fn encode_prepared_slot(
    slot: &PreparedWalSlot,
    legacy_entry: Option<&WalEntry>,
) -> Result<Vec<u8>, HyperbytedbError> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    write_string(&mut out, &slot.database)?;
    write_string(&mut out, &slot.retention_policy)?;
    out.extend_from_slice(&slot.origin_node_id.to_le_bytes());
    out.extend_from_slice(&(slot.measurements.len() as u32).to_le_bytes());

    for m in &slot.measurements {
        write_string(&mut out, &m.measurement)?;
        write_string(&mut out, &m.table_name)?;
        write_string(&mut out, &m.series_table_name)?;
        out.extend_from_slice(&(m.row_count as u32).to_le_bytes());
        out.extend_from_slice(&m.min_time.to_le_bytes());
        out.extend_from_slice(&m.max_time.to_le_bytes());

        let fact_ipc = encode_record_batch(&m.batch)?;
        out.extend_from_slice(&(fact_ipc.len() as u32).to_le_bytes());
        out.extend_from_slice(&fact_ipc);

        let series_ipc = if let Some(ref sb) = m.new_series_batch {
            encode_record_batch(sb)?
        } else {
            Vec::new()
        };
        out.extend_from_slice(&(series_ipc.len() as u32).to_le_bytes());
        if !series_ipc.is_empty() {
            out.extend_from_slice(&series_ipc);
        }
    }

    let legacy = legacy_entry
        .map(bincode::serialize)
        .transpose()
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?
        .unwrap_or_default();
    out.extend_from_slice(&(legacy.len() as u32).to_le_bytes());
    if !legacy.is_empty() {
        out.extend_from_slice(&legacy);
    }

    Ok(out)
}

pub fn decode_prepared_slot(
    bytes: &[u8],
) -> Result<(PreparedWalSlot, Option<WalEntry>), HyperbytedbError> {
    if bytes.len() < MAGIC.len() + 1 || &bytes[..4] != MAGIC {
        return Err(HyperbytedbError::Wal("invalid prepared WAL magic".into()));
    }
    let version = bytes[4];
    if version != VERSION {
        return Err(HyperbytedbError::Wal(format!(
            "unsupported prepared WAL version {version}"
        )));
    }

    let mut cursor = Cursor::new(&bytes[5..]);
    let database = read_string(&mut cursor)?;
    let retention_policy = read_string(&mut cursor)?;
    let mut origin_buf = [0u8; 8];
    cursor
        .read_exact(&mut origin_buf)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    let origin_node_id = u64::from_le_bytes(origin_buf);

    let mut mc_buf = [0u8; 4];
    cursor
        .read_exact(&mut mc_buf)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    let measurement_count = u32::from_le_bytes(mc_buf) as usize;

    let mut measurements = Vec::with_capacity(measurement_count);
    for _ in 0..measurement_count {
        let measurement = read_string(&mut cursor)?;
        let table_name = read_string(&mut cursor)?;
        let series_table_name = read_string(&mut cursor)?;

        let mut rc_buf = [0u8; 4];
        cursor
            .read_exact(&mut rc_buf)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let row_count = u32::from_le_bytes(rc_buf) as usize;

        let mut time_buf = [0u8; 8];
        cursor
            .read_exact(&mut time_buf)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let min_time = i64::from_le_bytes(time_buf);
        cursor
            .read_exact(&mut time_buf)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let max_time = i64::from_le_bytes(time_buf);

        let mut len_buf = [0u8; 4];
        cursor
            .read_exact(&mut len_buf)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let fact_len = u32::from_le_bytes(len_buf) as usize;
        let mut fact_ipc = vec![0u8; fact_len];
        cursor
            .read_exact(&mut fact_ipc)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let batch = decode_record_batch(&fact_ipc)?;

        cursor
            .read_exact(&mut len_buf)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        let series_len = u32::from_le_bytes(len_buf) as usize;
        let new_series_batch = if series_len > 0 {
            let mut series_ipc = vec![0u8; series_len];
            cursor
                .read_exact(&mut series_ipc)
                .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
            Some(decode_record_batch(&series_ipc)?)
        } else {
            None
        };

        measurements.push(PreparedMeasurementBatch {
            measurement,
            table_name,
            series_table_name,
            batch,
            row_count,
            min_time,
            max_time,
            new_series_batch,
        });
    }

    cursor
        .read_exact(&mut mc_buf)
        .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
    let legacy_len = u32::from_le_bytes(mc_buf) as usize;
    let legacy_entry = if legacy_len > 0 {
        let mut legacy = vec![0u8; legacy_len];
        cursor
            .read_exact(&mut legacy)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
        Some(bincode::deserialize(&legacy).map_err(|e| HyperbytedbError::Wal(e.to_string()))?)
    } else {
        None
    };

    Ok((
        PreparedWalSlot {
            database,
            retention_policy,
            origin_node_id,
            measurements,
        },
        legacy_entry,
    ))
}

pub fn encode_wal_value(
    format: WalFormat,
    slot: Option<&PreparedWalSlot>,
    entry: &WalEntry,
) -> Result<Vec<u8>, HyperbytedbError> {
    match format {
        WalFormat::Bincode => {
            bincode::serialize(entry).map_err(|e| HyperbytedbError::Wal(e.to_string()))
        }
        WalFormat::ArrowIpc => {
            let slot = slot.ok_or_else(|| {
                HyperbytedbError::Wal("arrow IPC WAL requires prepared slot".into())
            })?;
            encode_prepared_slot(slot, Some(entry))
        }
    }
}

pub fn decode_wal_value(
    format: WalFormat,
    bytes: &[u8],
) -> Result<(Option<PreparedWalSlot>, WalEntry), HyperbytedbError> {
    match format {
        WalFormat::Bincode => {
            let entry: WalEntry =
                bincode::deserialize(bytes).map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
            Ok((None, entry))
        }
        WalFormat::ArrowIpc => {
            let (slot, legacy) = decode_prepared_slot(bytes)?;
            let entry = legacy.ok_or_else(|| {
                HyperbytedbError::Wal("arrow IPC WAL missing legacy WalEntry".into())
            })?;
            Ok((Some(slot), entry))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::point::{FieldValue, Point};
    use arrow::array::{Float64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::collections::BTreeMap;

    fn sample_slot() -> PreparedWalSlot {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("origin_node_id", DataType::UInt64, false),
            Field::new("ingest_seq", DataType::UInt64, false),
            Field::new("series_id", DataType::UInt64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let batch = Arc::new(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![1_i64]).with_timezone("UTC")),
                    Arc::new(UInt64Array::from(vec![0_u64])),
                    Arc::new(UInt64Array::from(vec![10_u64])),
                    Arc::new(UInt64Array::from(vec![99_u64])),
                    Arc::new(Float64Array::from(vec![Some(1.0)])),
                ],
            )
            .unwrap(),
        );
        PreparedWalSlot {
            database: "db".into(),
            retention_policy: "autogen".into(),
            origin_node_id: 7,
            measurements: vec![PreparedMeasurementBatch {
                measurement: "cpu".into(),
                table_name: "`db_autogen_cpu`".into(),
                series_table_name: "`db_autogen_cpu_series`".into(),
                batch,
                row_count: 1,
                min_time: 1,
                max_time: 1,
                new_series_batch: None,
            }],
        }
    }

    fn sample_entry() -> WalEntry {
        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "h1".into());
        let mut fields = BTreeMap::new();
        fields.insert("v".into(), FieldValue::Float(1.0));
        WalEntry {
            database: "db".into(),
            retention_policy: "autogen".into(),
            points: vec![Point {
                measurement: "cpu".into(),
                tags,
                fields,
                timestamp: 1,
            }],
            origin_node_id: 7,
        }
    }

    #[test]
    fn ipc_round_trip() {
        let slot = sample_slot();
        let entry = sample_entry();
        let bytes = encode_prepared_slot(&slot, Some(&entry)).unwrap();
        let (decoded_slot, decoded_entry) = decode_prepared_slot(&bytes).unwrap();
        assert_eq!(decoded_slot.database, "db");
        assert_eq!(decoded_entry.expect("legacy entry").points.len(), 1);
        assert_eq!(decoded_slot.measurements[0].row_count, 1);
    }
}
