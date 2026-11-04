//! The binary point frame used node to node: `magic | u32 LE length |
//! bincode(Vec<Point>)`. A wire format, so it lives with the point model;
//! the WAL batcher and the peer client use it as well as ingestion.

/// Start of a binary points frame. Line protocol never starts with NUL, so a
/// body is binary exactly when it starts with this.
pub const POINTS_FRAME_MAGIC: &[u8; 4] = b"\0HBP";

/// Whether `body` is binary points frames rather than line protocol.
#[must_use]
pub fn is_binary_points(body: &[u8]) -> bool {
    body.starts_with(POINTS_FRAME_MAGIC)
}

/// Starting buffer size per point for one-pass bincode encoding: a point with
/// a few short tags and one field. Only a hint; the buffer grows as needed.
pub const POINT_ENCODED_SIZE_HINT: usize = 96;

/// Append `value`'s bincode encoding to `out` in one pass. Same bytes as
/// `bincode::serialize`, without its separate sizing pass.
pub fn encode_bincode_into<T: serde::Serialize + ?Sized>(
    out: &mut Vec<u8>,
    value: &T,
) -> Result<(), bincode::Error> {
    bincode::serialize_into(out, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_recognised_by_their_magic() {
        assert!(is_binary_points(b"\0HBP\0\0\0\0"));
        assert!(!is_binary_points(b"cpu v=1 1"));
        let mut out = Vec::new();
        encode_bincode_into(&mut out, &vec![1u32, 2]).unwrap();
        assert_eq!(out, bincode::serialize(&vec![1u32, 2]).unwrap());
    }
}
