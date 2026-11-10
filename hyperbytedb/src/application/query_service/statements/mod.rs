//! `execute_statement`'s larger arms, one function per statement kind.

pub(super) mod ddl;
pub(super) mod delete;
pub(super) mod select;
pub(super) mod show_catalog;
pub(super) mod show_schema;
pub(super) mod users;
