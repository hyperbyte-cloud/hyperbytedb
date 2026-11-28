//! HTTP headers and hinted-handoff binary format for write replication (Influx line protocol).

/// Header carrying the origin's row version for a replicated body
/// ([`crate::domain::wal::WalEntry::version`]). Older peers neither send nor
/// read it.
pub const HTTP_HEADER_VERSION: &str = "X-Hyperbytedb-Version";

/// `Content-Type` for replicated writes: same body encoding as `POST /write`.
pub const LINE_PROTOCOL_MEDIA_TYPE_V1: &str = "application/vnd.hyperbytedb.replicate+line.v1";

pub const HTTP_HEADER_DATABASE: &str = "X-Hyperbytedb-DB";
pub const HTTP_HEADER_RETENTION_POLICY: &str = "X-Hyperbytedb-RP";
pub const HTTP_HEADER_PRECISION: &str = "X-Hyperbytedb-Precision";
pub const HTTP_HEADER_ORIGIN_NODE: &str = "X-Hyperbytedb-Origin-Node";
/// Shard region the replicated body belongs to. Absent on replayed
/// hinted-handoff records, which predate the region being recorded.
pub const HTTP_HEADER_REGION: &str = "X-Hyperbytedb-Region";
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
    /// The origin's row version; 0 for none.
    pub version: u64,
}

impl ReplicationHintPayload {
    /// Encode for RocksDB hinted-handoff values.
    pub fn encode_hint_value(&self) -> Result<Vec<u8>, crate::error::HyperbytedbError> {
        let db = self.database.as_bytes();
        let rp = self.retention_policy.as_bytes();
        if db.len() > u32::MAX as usize || rp.len() > u32::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: db/rp name too long"),
            ));
        }
        let prec_bytes = self
            .precision
            .as_ref()
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_default();
        if prec_bytes.len() > u16::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: precision too long"),
            ));
        }
        if self.line_body.len() > u64::MAX as usize {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: body too large"),
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
        // After the body, where older decoders do not look.
        out.extend_from_slice(&self.version.to_le_bytes());
        Ok(out)
    }

    /// Decode hinted-handoff value (`CFh1` only).
    pub fn decode_hint_value(data: &[u8]) -> Result<Self, crate::error::HyperbytedbError> {
        if data.len() < 4 || &data[0..4] != HH_MAGIC {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new(
                    "replication hint: bad magic or unsupported legacy format",
                ),
            ));
        }
        let mut i = 4;
        let read_u32 = |buf: &[u8], i: &mut usize| -> Result<u32, crate::error::HyperbytedbError> {
            if *i + 4 > buf.len() {
                return Err(crate::error::HyperbytedbError::Internal(
                    crate::error::ChainedError::new("replication hint: truncated"),
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
                    crate::error::ChainedError::new("replication hint: truncated"),
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
                    crate::error::ChainedError::new("replication hint: truncated"),
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
                crate::error::ChainedError::new("replication hint: truncated db"),
            ));
        }
        let database = std::str::from_utf8(&data[i..i + dlen])
            .map_err(|e| {
                crate::error::HyperbytedbError::Internal(crate::error::ChainedError::from_error(e))
            })?
            .to_string();
        i += dlen;

        let rlen = read_u32(data, &mut i)? as usize;
        if i + rlen > data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: truncated rp"),
            ));
        }
        let retention_policy = std::str::from_utf8(&data[i..i + rlen])
            .map_err(|e| {
                crate::error::HyperbytedbError::Internal(crate::error::ChainedError::from_error(e))
            })?
            .to_string();
        i += rlen;

        if i >= data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: missing flags"),
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
                        crate::error::ChainedError::new("replication hint: truncated precision"),
                    ));
                }
                let p = std::str::from_utf8(&data[i..i + plen])
                    .map_err(|e| {
                        crate::error::HyperbytedbError::Internal(
                            crate::error::ChainedError::from_error(e),
                        )
                    })?
                    .to_string();
                i += plen;
                Some(p)
            }
            _ => {
                return Err(crate::error::HyperbytedbError::Internal(
                    crate::error::ChainedError::new("replication hint: bad precision flag"),
                ));
            }
        };

        let blen = read_u64(data, &mut i)? as usize;
        if i + blen > data.len() {
            return Err(crate::error::HyperbytedbError::Internal(
                crate::error::ChainedError::new("replication hint: truncated body"),
            ));
        }
        let line_body = data[i..i + blen].to_vec();
        i += blen;
        // Absent from hints queued before versions.
        let version = data[i..]
            .first_chunk::<8>()
            .map_or(0, |v| u64::from_le_bytes(*v));
        Ok(Self {
            version,
            database,
            retention_policy,
            precision,
            line_body,
        })
    }
}

/// Content type of `/internal/replicate-batch` bodies: a bincode
/// [`ReplicateBatchRequest`].
pub const REPLICATE_BATCH_MEDIA_TYPE_V1: &str = "application/vnd.hyperbytedb.replicate-batch.v1";

/// One region's rows inside a [`ReplicateBatchRequest`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicateBatchPart {
    pub database: String,
    pub retention_policy: String,
    pub precision: Option<String>,
    /// Checked by the receiver exactly as the single-batch region header is.
    pub region_id: Option<u64>,
    /// Line protocol or binary points frames.
    pub body: Vec<u8>,
    /// The origin's row version; 0 for none. Not part of the struct's bincode
    /// layout, so older peers still decode it: see
    /// [`encode_replicate_batch`].
    #[serde(skip)]
    pub version: u64,
}

/// Every queued replication part bound for one peer, in one request.
///
/// Replication used to send one request per region part: under sharding a
/// 1000-point client batch splits into one part per region, so at N=12 on n3
/// requests carried ~14 points (1.3 KB) each, 8 at a time per node. That
/// capped a node at ~1,600 requests/s; the rest of the traffic spilled into
/// hinted handoff (9.9M hints, 3.5M dropped at the hint cap) and the node
/// spent its CPU on hint writes, compaction and replay.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicateBatchRequest {
    pub origin_node: u64,
    pub parts: Vec<ReplicateBatchPart>,
}

/// Body of `/internal/replicate-batch`: a bincode [`ReplicateBatchRequest`]
/// followed by one version per part. A receiver without versions ignores the
/// trailing bytes.
pub fn encode_replicate_batch(
    origin_node: u64,
    parts: &[ReplicateBatchPart],
) -> Result<Vec<u8>, bincode::Error> {
    // Wire-identical to `ReplicateBatchRequest` (bincode writes a struct as
    // its fields in order) without cloning every body.
    let mut out = bincode::serialize(&(origin_node, parts))?;
    let versions: Vec<u64> = parts.iter().map(|p| p.version).collect();
    bincode::serialize_into(&mut out, &versions)?;
    Ok(out)
}

/// Inverse of [`encode_replicate_batch`]; a request from a sender without
/// versions leaves them 0.
pub fn decode_replicate_batch(body: &[u8]) -> Result<ReplicateBatchRequest, bincode::Error> {
    let mut rest = body;
    let mut req: ReplicateBatchRequest = bincode::deserialize_from(&mut rest)?;
    if !rest.is_empty() {
        let versions: Vec<u64> = bincode::deserialize(rest)?;
        for (part, version) in req.parts.iter_mut().zip(versions) {
            part.version = version;
        }
    }
    Ok(req)
}

/// What the receiver did with one part.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReplicatePartOutcome {
    /// Queued for apply (the async replication contract).
    Accepted,
    /// The receiver is not a peer of the part's region.
    NotARegionPeer,
    /// The receiver's apply queue is full; retry later.
    QueueFull,
    Failed(String),
}

/// One outcome per request part, in order.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicateBatchResponse {
    pub results: Vec<ReplicatePartOutcome>,
}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn replicate_batch_versions_are_invisible_to_a_peer_without_them() {
        let part = ReplicateBatchPart {
            database: "db".into(),
            retention_policy: "autogen".into(),
            precision: None,
            region_id: Some(3),
            body: b"cpu v=1 1".to_vec(),
            version: 99,
        };
        let body = encode_replicate_batch(1, std::slice::from_ref(&part)).unwrap();
        assert_eq!(decode_replicate_batch(&body).unwrap().parts, vec![part]);

        // An older receiver reads the struct and ignores the rest.
        let old: ReplicateBatchRequest = bincode::deserialize(&body).unwrap();
        assert_eq!(old.parts[0].version, 0);
        // An older sender sends the struct alone.
        let from_old = bincode::serialize(&old).unwrap();
        assert_eq!(decode_replicate_batch(&from_old).unwrap(), old);
    }

    #[test]
    fn a_hint_keeps_its_version_and_one_without_still_decodes() {
        let hint = ReplicationHintPayload {
            database: "db".into(),
            retention_policy: "autogen".into(),
            precision: None,
            line_body: b"cpu v=1 1".to_vec(),
            version: 99,
        };
        let bytes = hint.encode_hint_value().unwrap();
        assert_eq!(
            ReplicationHintPayload::decode_hint_value(&bytes)
                .unwrap()
                .version,
            99
        );
        let old = ReplicationHintPayload::decode_hint_value(&bytes[..bytes.len() - 8]).unwrap();
        assert_eq!((old.version, old.line_body), (0, hint.line_body));
    }
}
