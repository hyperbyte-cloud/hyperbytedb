//! MessagePack size guards before full deserialization.

use std::io::Cursor;

use rmp::Marker;
use rmp::decode::{self, RmpRead};

use crate::error::HyperbytedbError;

fn msgpack_parse_err<E: std::fmt::Debug>(e: E) -> HyperbytedbError {
    HyperbytedbError::MsgpackParse {
        reason: format!("{e:?}"),
    }
}

fn skip_msgpack_value<R: RmpRead>(rd: &mut R) -> Result<(), HyperbytedbError> {
    match decode::read_marker(rd).map_err(msgpack_parse_err)? {
        Marker::Null | Marker::True | Marker::False => Ok(()),
        Marker::FixPos(_) | Marker::FixNeg(_) => Ok(()),
        Marker::U8 | Marker::I8 => skip_bytes(rd, 1),
        Marker::U16 | Marker::I16 => skip_bytes(rd, 2),
        Marker::U32 | Marker::I32 | Marker::F32 => skip_bytes(rd, 4),
        Marker::U64 | Marker::I64 | Marker::F64 => skip_bytes(rd, 8),
        Marker::FixStr(len) => skip_bytes(rd, u32::from(len)),
        Marker::Str8 => {
            let len = u32::from(decode::read_u8(rd).map_err(msgpack_parse_err)?);
            skip_bytes(rd, len)
        }
        Marker::Str16 => {
            let len = u32::from(decode::read_u16(rd).map_err(msgpack_parse_err)?);
            skip_bytes(rd, len)
        }
        Marker::Str32 => {
            let len = decode::read_u32(rd).map_err(msgpack_parse_err)?;
            skip_bytes(rd, len)
        }
        Marker::Bin8 => {
            let len = u32::from(decode::read_u8(rd).map_err(msgpack_parse_err)?);
            skip_bytes(rd, len)
        }
        Marker::Bin16 => {
            let len = u32::from(decode::read_u16(rd).map_err(msgpack_parse_err)?);
            skip_bytes(rd, len)
        }
        Marker::Bin32 => {
            let len = decode::read_u32(rd).map_err(msgpack_parse_err)?;
            skip_bytes(rd, len)
        }
        Marker::FixArray(len) => skip_array_elements(rd, u32::from(len)),
        Marker::Array16 => {
            let len = decode::read_u16(rd).map_err(msgpack_parse_err)?;
            skip_array_elements(rd, u32::from(len))
        }
        Marker::Array32 => {
            let len = decode::read_u32(rd).map_err(msgpack_parse_err)?;
            skip_array_elements(rd, len)
        }
        Marker::FixMap(len) => skip_map_entries(rd, u32::from(len)),
        Marker::Map16 => {
            let len = decode::read_u16(rd).map_err(msgpack_parse_err)?;
            skip_map_entries(rd, u32::from(len))
        }
        Marker::Map32 => {
            let len = decode::read_u32(rd).map_err(msgpack_parse_err)?;
            skip_map_entries(rd, len)
        }
        Marker::FixExt1 => skip_bytes(rd, 2),
        Marker::FixExt2 => skip_bytes(rd, 3),
        Marker::FixExt4 => skip_bytes(rd, 5),
        Marker::FixExt8 => skip_bytes(rd, 9),
        Marker::FixExt16 => skip_bytes(rd, 17),
        Marker::Ext8 => {
            let len = decode::read_u8(rd).map_err(msgpack_parse_err)?;
            skip_bytes(rd, u32::from(len) + 1)
        }
        Marker::Ext16 => {
            let len = decode::read_u16(rd).map_err(msgpack_parse_err)?;
            skip_bytes(rd, u32::from(len) + 1)
        }
        Marker::Ext32 => {
            let len = decode::read_u32(rd).map_err(msgpack_parse_err)?;
            skip_bytes(rd, len.saturating_add(1))
        }
        Marker::Reserved => Err(HyperbytedbError::MsgpackParse {
            reason: "reserved msgpack marker".into(),
        }),
    }
}

fn skip_bytes<R: RmpRead>(rd: &mut R, n: u32) -> Result<(), HyperbytedbError> {
    let mut buf = [0u8; 4096];
    let mut remaining = n as u64;
    while remaining > 0 {
        let chunk = remaining.min(buf.len() as u64) as usize;
        rd.read_exact_buf(&mut buf[..chunk])
            .map_err(msgpack_parse_err)?;
        remaining -= chunk as u64;
    }
    Ok(())
}

fn skip_array_elements<R: RmpRead>(rd: &mut R, len: u32) -> Result<(), HyperbytedbError> {
    for _ in 0..len {
        skip_msgpack_value(rd)?;
    }
    Ok(())
}

fn skip_map_entries<R: RmpRead>(rd: &mut R, len: u32) -> Result<(), HyperbytedbError> {
    for _ in 0..len {
        skip_msgpack_value(rd)?;
        skip_msgpack_value(rd)?;
    }
    Ok(())
}

fn read_msgpack_str<R: RmpRead>(rd: &mut R) -> Result<String, HyperbytedbError> {
    let len = match decode::read_marker(rd).map_err(msgpack_parse_err)? {
        Marker::FixStr(len) => u32::from(len),
        Marker::Str8 => u32::from(decode::read_u8(rd).map_err(msgpack_parse_err)?),
        Marker::Str16 => u32::from(decode::read_u16(rd).map_err(msgpack_parse_err)?),
        Marker::Str32 => decode::read_u32(rd).map_err(msgpack_parse_err)?,
        marker => {
            return Err(HyperbytedbError::MsgpackParse {
                reason: format!("expected msgpack string, got {marker:?}"),
            });
        }
    };
    let mut buf = vec![0u8; len as usize];
    rd.read_exact_buf(&mut buf).map_err(msgpack_parse_err)?;
    String::from_utf8(buf).map_err(msgpack_parse_err)
}

/// Return the element count of a top-level msgpack array without deserializing elements.
pub fn peek_top_level_array_len(body: &[u8]) -> Result<usize, HyperbytedbError> {
    if body.is_empty() {
        return Ok(0);
    }
    let mut cur = Cursor::new(body);
    let len = decode::read_array_len(&mut cur).map_err(msgpack_parse_err)?;
    Ok(len as usize)
}

/// Return the `values` array length from a columnar msgpack map without full deserialization.
pub fn peek_columnar_values_len(body: &[u8]) -> Result<Option<usize>, HyperbytedbError> {
    if body.is_empty() {
        return Ok(None);
    }
    let mut cur = Cursor::new(body);
    let map_len = decode::read_map_len(&mut cur).map_err(msgpack_parse_err)?;
    for _ in 0..map_len {
        let key = read_msgpack_str(&mut cur)?;
        if key == "values" {
            let len = decode::read_array_len(&mut cur).map_err(msgpack_parse_err)?;
            return Ok(Some(len as usize));
        }
        skip_msgpack_value(&mut cur)?;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::columnar_msgpack::ColumnarMsgpackBatch;
    use crate::domain::point::FieldValue;
    use std::collections::BTreeMap;

    #[test]
    fn peek_top_level_array_len_matches_decode() {
        #[derive(serde::Serialize)]
        struct Wire {
            measurement: String,
            #[serde(default)]
            tags: BTreeMap<String, String>,
            fields: BTreeMap<String, FieldValue>,
            timestamp: Option<i64>,
        }
        let mut fields = BTreeMap::new();
        fields.insert("idle".into(), FieldValue::Float(0.5));
        let wire = vec![Wire {
            measurement: "cpu".into(),
            tags: BTreeMap::new(),
            fields,
            timestamp: Some(1),
        }];
        let body = rmp_serde::to_vec_named(&wire).unwrap();
        assert_eq!(peek_top_level_array_len(&body).unwrap(), 1);
    }

    #[test]
    fn peek_columnar_values_len_matches_wire() {
        let batch = ColumnarMsgpackBatch {
            measurement: "cpu".into(),
            tags: BTreeMap::new(),
            field: "idle".into(),
            values: vec![1.0, 2.0, 3.0],
            timestamps: None,
        };
        let body = rmp_serde::to_vec_named(&batch).unwrap();
        assert_eq!(peek_columnar_values_len(&body).unwrap(), Some(3));
    }
}
