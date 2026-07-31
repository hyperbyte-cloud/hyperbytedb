//! Identifier sanitisation for chDB-native tables.
//!
//! Influx / line-protocol identifiers are far more permissive than the
//! ClickHouse identifier grammar (any UTF-8, embedded backticks, leading
//! digits, ...). When we synthesise table and column names from
//! `(database, retention_policy, measurement, tag/field key)` tuples we
//! therefore replace anything that isn't `[A-Za-z0-9_]` with `_` and
//! prefix a leading `_` if the first byte is a digit.
//!
//! Tag / field column collisions reuse
//! [`crate::domain::column_mapping::tag_column_name`] so the
//! native adapter and the query translator share the same physical
//! column names for each measurement.

use std::collections::HashSet;
use std::fmt;

use crate::domain::column_mapping::tag_column_name as map_tag_column_name;
use crate::error::HyperbytedbError;

/// Sanitized unquoted ClickHouse identifier (`[A-Za-z0-9_]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SanitizedIdent(String);

impl SanitizedIdent {
    /// Sanitize arbitrary input into a valid ClickHouse identifier.
    #[must_use]
    pub fn sanitize(input: &str) -> Self {
        Self(sanitise_ident(input))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Backtick-quoted form suitable for splicing into SQL.
    #[must_use]
    pub fn quoted(&self) -> QuotedTableName {
        QuotedTableName::new_quoted(quote_backticks(self.as_str()))
    }
}

impl fmt::Display for SanitizedIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Backtick-quoted table or object name safe to splice into generated SQL.
///
/// Construct only via [`quoted_table_name`], [`quoted_series_table_name`], or
/// related helpers in this module — do not build from raw user input elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuotedTableName(String);

impl QuotedTableName {
    pub(crate) fn new_quoted(s: String) -> Self {
        Self(s)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QuotedTableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Reject control characters in identifiers before quoting.
fn reject_control_chars(ident: &str) -> Result<(), HyperbytedbError> {
    if ident.chars().any(char::is_control) {
        return Err(HyperbytedbError::QueryParse(format!(
            "identifier contains control characters: {ident:?}"
        )));
    }
    Ok(())
}

/// Replace every byte that isn't `[A-Za-z0-9_]` with `_` and prefix
/// `_` if the result starts with a digit. Empty input becomes `_`.
fn sanitise_ident(input: &str) -> String {
    if input.is_empty() {
        return "_".to_string();
    }
    let mut out = String::with_capacity(input.len() + 1);
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// Build the unquoted table identifier for a `(db, rp, measurement)`
/// tuple, using `db_rp_measurement` after sanitisation.
#[must_use]
pub fn unquoted_table_name(db: &str, rp: &str, measurement: &str) -> SanitizedIdent {
    SanitizedIdent(format!(
        "{}_{}_{}",
        sanitise_ident(db),
        sanitise_ident(rp),
        sanitise_ident(measurement)
    ))
}

/// Quote an identifier with backticks, escaping embedded backticks.
/// Used for table names, column names, and database references in
/// generated SQL.
#[must_use]
pub fn quote_backticks(ident: &str) -> String {
    let escaped = ident.replace('\\', "\\\\").replace('`', "``");
    format!("`{escaped}`")
}

/// Like [`quote_backticks`], but rejects control characters first.
pub fn quote_backticks_validated(ident: &str) -> Result<String, HyperbytedbError> {
    reject_control_chars(ident)?;
    Ok(quote_backticks(ident))
}

/// Backtick-quoted, sanitised table name suitable for splicing into
/// `CREATE TABLE`, `INSERT INTO`, `DROP TABLE`, and `FROM` clauses.
#[must_use]
pub fn quoted_table_name(db: &str, rp: &str, measurement: &str) -> QuotedTableName {
    unquoted_table_name(db, rp, measurement).quoted()
}

/// Unquoted name of the per-measurement series (tag dimension) table:
/// `<db>_<rp>_<measurement>_series`. The `_series` suffix is appended after
/// sanitisation (it is already valid `[A-Za-z0-9_]`).
#[must_use]
pub fn unquoted_series_table_name(db: &str, rp: &str, measurement: &str) -> SanitizedIdent {
    SanitizedIdent(format!(
        "{}_series",
        unquoted_table_name(db, rp, measurement).0
    ))
}

/// Backtick-quoted series (tag dimension) table name. See
/// [`unquoted_series_table_name`].
#[must_use]
pub fn quoted_series_table_name(db: &str, rp: &str, measurement: &str) -> QuotedTableName {
    unquoted_series_table_name(db, rp, measurement).quoted()
}

/// Unquoted ClickHouse object name for a fact-table materialized view:
/// `<db>_<rp>_<mv_name>_mv`.
#[must_use]
pub fn unquoted_fact_mv_name(db: &str, rp: &str, mv_name: &str) -> SanitizedIdent {
    SanitizedIdent(format!("{}_mv", unquoted_table_name(db, rp, mv_name).0))
}

/// Backtick-quoted fact MV object name.
#[must_use]
pub fn quoted_fact_mv_name(db: &str, rp: &str, mv_name: &str) -> QuotedTableName {
    unquoted_fact_mv_name(db, rp, mv_name).quoted()
}

/// Unquoted ClickHouse object name for a series-dimension MV:
/// `<db>_<rp>_<mv_name>_series_mv`.
#[must_use]
pub fn unquoted_series_mv_name(db: &str, rp: &str, mv_name: &str) -> SanitizedIdent {
    SanitizedIdent(format!(
        "{}_series_mv",
        unquoted_table_name(db, rp, mv_name).0
    ))
}

/// Backtick-quoted series MV object name.
#[must_use]
pub fn quoted_series_mv_name(db: &str, rp: &str, mv_name: &str) -> QuotedTableName {
    unquoted_series_mv_name(db, rp, mv_name).quoted()
}

/// Resolve the physical column name for a tag key, taking field-name
/// collisions into account (tag wins the prefix `__tag__`). Mirrors
/// the Parquet writer's behaviour exactly so logical identifiers
/// remain stable across storage formats.
#[must_use]
pub fn tag_column_name(tag_key: &str, field_names: &HashSet<&str>) -> String {
    sanitise_ident(&map_tag_column_name(tag_key, field_names))
}

/// Field columns never collide (per-measurement uniqueness is checked
/// upstream), so we just sanitise.
#[must_use]
pub fn field_column_name(field_key: &str) -> String {
    sanitise_ident(field_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitises_punctuation_and_unicode() {
        assert_eq!(sanitise_ident("foo.bar"), "foo_bar");
        assert_eq!(sanitise_ident("a-b c"), "a_b_c");
        assert_eq!(sanitise_ident("naïve"), "na_ve");
    }

    #[test]
    fn sanitises_leading_digit() {
        assert_eq!(sanitise_ident("1day"), "_1day");
    }

    #[test]
    fn sanitises_empty() {
        assert_eq!(sanitise_ident(""), "_");
    }

    #[test]
    fn quoted_table_name_matches_db_rp_measurement() {
        assert_eq!(
            quoted_table_name("mydb", "autogen", "cpu").as_str(),
            "`mydb_autogen_cpu`"
        );
        assert_eq!(
            quoted_table_name("my-db", "autogen", "cpu.load").as_str(),
            "`my_db_autogen_cpu_load`"
        );
    }

    #[test]
    fn quote_backticks_escapes_embedded_backticks() {
        assert_eq!(quote_backticks("a`b"), "`a``b`");
    }

    #[test]
    fn quote_backticks_validated_rejects_control_chars() {
        assert!(quote_backticks_validated("a\nb").is_err());
        assert!(quote_backticks_validated("ok_name").is_ok());
    }

    #[test]
    fn series_table_name_appends_suffix() {
        assert_eq!(
            quoted_series_table_name("mydb", "autogen", "cpu").as_str(),
            "`mydb_autogen_cpu_series`"
        );
        assert_eq!(
            quoted_series_table_name("my-db", "autogen", "cpu.load").as_str(),
            "`my_db_autogen_cpu_load_series`"
        );
    }

    #[test]
    fn tag_column_collision_uses_tag_prefix() {
        let fields: HashSet<&str> = ["cpu"].into_iter().collect();
        assert_eq!(tag_column_name("cpu", &fields), "__tag__cpu");
        assert_eq!(tag_column_name("host", &fields), "host");
    }
}
