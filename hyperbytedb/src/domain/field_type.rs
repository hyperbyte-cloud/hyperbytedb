//! Field-type compatibility and widening for line-protocol ingest.
//!
//! Telegraf and other writers may send the same semantic field as integer (`…i`)
//! or unsigned (`…u`). We widen compatible numeric types rather than rejecting
//! batches or silently NULLing values at insert time.

use std::collections::HashMap;

/// Return the canonical discriminant when `a` and `b` can coexist, or `None`.
#[must_use]
pub fn widen_field_disc(a: u8, b: u8) -> Option<u8> {
    if a == b {
        return Some(a);
    }
    // Float absorbs integer (mirror native_adapter / parquet behaviour).
    if (a == 0 && b == 1) || (a == 1 && b == 0) {
        return Some(0);
    }
    // Integer and unsigned widen to unsigned (Telegraf `…u` counters).
    if (a == 1 && b == 2) || (a == 2 && b == 1) {
        return Some(2);
    }
    None
}

/// Whether an incoming field discriminant is acceptable given stored metadata.
#[must_use]
pub fn field_types_compatible(stored: u8, incoming: u8) -> bool {
    widen_field_disc(stored, incoming).is_some()
}

/// Union `batch` into `existing`, widening on conflict and never removing keys.
#[must_use]
pub fn merge_field_type_map(
    existing: &HashMap<String, u8>,
    batch: &HashMap<String, u8>,
) -> HashMap<String, u8> {
    let mut out = existing.clone();
    for (k, &incoming) in batch {
        match out.get_mut(k) {
            Some(stored) => {
                if let Some(w) = widen_field_disc(*stored, incoming) {
                    *stored = w;
                }
            }
            None => {
                out.insert(k.clone(), incoming);
            }
        }
    }
    out
}

/// Widen a single incoming discriminant into `stored`, returning the new value.
#[must_use]
pub fn merge_field_disc(stored: u8, incoming: u8) -> u8 {
    widen_field_disc(stored, incoming).unwrap_or(stored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widen_float_integer() {
        assert_eq!(widen_field_disc(0, 1), Some(0));
        assert_eq!(widen_field_disc(1, 0), Some(0));
    }

    #[test]
    fn widen_integer_unsigned_to_unsigned() {
        assert_eq!(widen_field_disc(1, 2), Some(2));
        assert_eq!(widen_field_disc(2, 1), Some(2));
    }

    #[test]
    fn incompatible_types() {
        assert_eq!(widen_field_disc(0, 3), None);
        assert_eq!(widen_field_disc(3, 1), None);
    }

    #[test]
    fn merge_map_widens_uptime() {
        let mut existing = HashMap::new();
        existing.insert("uptime".to_string(), 1);
        let mut batch = HashMap::new();
        batch.insert("uptime".to_string(), 2);
        let merged = merge_field_type_map(&existing, &batch);
        assert_eq!(merged.get("uptime"), Some(&2));
    }

    #[test]
    fn merge_map_never_shrinks() {
        let mut existing = HashMap::new();
        existing.insert("load1".to_string(), 0);
        existing.insert("uptime".to_string(), 2);
        let batch = HashMap::from([("uptime_format".to_string(), 3)]);
        let merged = merge_field_type_map(&existing, &batch);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged.get("load1"), Some(&0));
    }
}
