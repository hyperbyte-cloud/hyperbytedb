//! HTTP headers and hinted-handoff binary format for write replication (Influx line protocol).

/// `Content-Type` for replicated writes: same body encoding as `POST /write`.
pub const LINE_PROTOCOL_MEDIA_TYPE_V1: &str = "application/vnd.hyperbytedb.replicate+line.v1";

pub const HTTP_HEADER_DATABASE: &str = "X-Hyperbytedb-DB";
pub const HTTP_HEADER_RETENTION_POLICY: &str = "X-Hyperbytedb-RP";
pub const HTTP_HEADER_PRECISION: &str = "X-Hyperbytedb-Precision";
pub const HTTP_HEADER_ORIGIN_NODE: &str = "X-Hyperbytedb-Origin-Node";
pub const HTTP_HEADER_REPLICATED: &str = "X-Hyperbytedb-Replicated";
/// When the coordinator wants to wait for the receiver to durably append the
/// batch to the local WAL before responding (used by `sync_quorum`). Receiver
/// returns `200 OK` with `application/json` body `{"ok": true, "ack_seq": <u64>}`.
/// With the header absent or set to `false`, the receiver keeps the existing
/// fire-and-forget `204 No Content` behavior — wire-compatible with older peers
/// that ignore the header.
pub const HTTP_HEADER_SYNC: &str = "X-Hyperbytedb-Sync";

const HH_MAGIC: &[u8; 4] = b"CFh1";

/// Metadata + line-protocol bytes (hinted-handoff on disk or logical replicate batch).
#[derive(Debug, Clone)]
pub struct ReplicationHintPayload {
    pub database: String,
    pub retention_policy: String,
    pub precision: Option<String>,
    pub line_body: Vec<u8>,
}

impl ReplicationHintPayload {
    /// Encode for RocksDB hinted-handoff values.
    pub fn encode_hint_value(&self) -> Result<Vec<u8>, crate::error::HyperbytedbError> {
        let db = self.database.as_bytes();
        let rp = self.retention_policy.as_bytes();
        if db.len() > u32::MAX as usize || rp.len() > u32::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: db/rp name too long".into(),
            ));
        }
        let prec_bytes = self
            .precision
            .as_ref()
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_default();
        if prec_bytes.len() > u16::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: precision too long".into(),
            ));
        }
        if self.line_body.len() > u64::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: body too large".into(),
            ));
        }

        let mut out = Vec::with_capacity(
            4 + 4 + db.len() + 4 + rp.len() + 1 + 2 + prec_bytes.len() + 8 + self.line_body.len(),
        );
        out.extend_from_slice(HH_MAGIC);
        out.extend_from_slice(&(db.len() as u32).to_le_bytes());
        out.extend_from_slice(db);
        out.extend_from_slice(&(rp.len() as u32).to_le_bytes());
        out.extend_from_slice(rp);
        if prec_bytes.is_empty() {
            out.push(0u8);
        } else {
            out.push(1u8);
            out.extend_from_slice(&(prec_bytes.len() as u16).to_le_bytes());
            out.extend_from_slice(&prec_bytes);
        }
        out.extend_from_slice(&(self.line_body.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.line_body);
        Ok(out)
    }

    /// Decode hinted-handoff value (`CFh1` only).
    pub fn decode_hint_value(data: &[u8]) -> Result<Self, crate::error::HyperbytedbError> {
        if data.len() < 4 || &data[0..4] != HH_MAGIC {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: bad magic or unsupported legacy format".into(),
            ));
        }
        let mut i = 4;
        let read_u32 = |buf: &[u8], i: &mut usize| -> Result<u32, crate::error::HyperbytedbError> {
            if *i + 4 > buf.len() {
                return Err(crate::error::HyperbytedbError::Internal(
                    "replication hint: truncated".into(),
                ));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&buf[*i..*i + 4]);
            *i += 4;
            Ok(u32::from_le_bytes(arr))
        };
        let read_u16 = |buf: &[u8], i: &mut usize| -> Result<u16, crate::error::HyperbytedbError> {
            if *i + 2 > buf.len() {
                return Err(crate::error::HyperbytedbError::Internal(
                    "replication hint: truncated".into(),
                ));
            }
            let mut arr = [0u8; 2];
            arr.copy_from_slice(&buf[*i..*i + 2]);
            *i += 2;
            Ok(u16::from_le_bytes(arr))
        };
        let read_u64 = |buf: &[u8], i: &mut usize| -> Result<u64, crate::error::HyperbytedbError> {
            if *i + 8 > buf.len() {
                return Err(crate::error::HyperbytedbError::Internal(
                    "replication hint: truncated".into(),
                ));
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&buf[*i..*i + 8]);
            *i += 8;
            Ok(u64::from_le_bytes(arr))
        };

        let dlen = read_u32(data, &mut i)? as usize;
        if i + dlen > data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: truncated db".into(),
            ));
        }
        let database = std::str::from_utf8(&data[i..i + dlen])
            .map_err(|e| crate::error::HyperbytedbError::Internal(e.to_string()))?
            .to_string();
        i += dlen;

        let rlen = read_u32(data, &mut i)? as usize;
        if i + rlen > data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: truncated rp".into(),
            ));
        }
        let retention_policy = std::str::from_utf8(&data[i..i + rlen])
            .map_err(|e| crate::error::HyperbytedbError::Internal(e.to_string()))?
            .to_string();
        i += rlen;

        if i >= data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: missing flags".into(),
            ));
        }
        let prec_flag = data[i];
        i += 1;
        let precision = match prec_flag {
            0 => None,
            1 => {
                let plen = read_u16(data, &mut i)? as usize;
                if i + plen > data.len() {
                    return Err(crate::error::HyperbytedbError::Internal(
                        "replication hint: truncated precision".into(),
                    ));
                }
                let p = std::str::from_utf8(&data[i..i + plen])
                    .map_err(|e| crate::error::HyperbytedbError::Internal(e.to_string()))?
                    .to_string();
                i += plen;
                Some(p)
            }
            _ => {
                return Err(crate::error::HyperbytedbError::Internal(
                    "replication hint: bad precision flag".into(),
                ));
            }
        };

        let blen = read_u64(data, &mut i)? as usize;
        if i + blen > data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                "replication hint: truncated body".into(),
            ));
        }
        let line_body = data[i..i + blen].to_vec();
        Ok(Self {
            database,
            retention_policy,
            precision,
            line_body,
        })
    }
}
