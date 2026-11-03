# Query Analysis Module Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Domain code stops importing the ClickHouse SQL translator (`timeseriesql::to_clickhouse`). The pure InfluxQL AST analysis it uses moves to a translator-independent module.

**Architecture:** Add `timeseriesql/analysis.rs` for AST facts that hold whatever the backend:
- output column names;
- whether a SELECT aggregates;
- the TOP/BOTTOM/SAMPLE selector descriptor;
- the shared merge-column names.

`to_clickhouse` re-exports what it used to define, so the translator and its 1,500+ lines of tests are unchanged. Domain imports from `analysis`.

**Tech Stack:** Rust 2024.

**Spec:** the deferred item "the domain using the SQL translator" in `docs/superpowers/plans/2026-11-03-code-health-fixes.md`, validated at `7ea9643`.

**Independent** of the other plans.

## What domain imports today (validated)

| Domain file | Imports | Defined at |
|---|---|---|
| `domain/rollup.rs` | `to_clickhouse::select_output_field_name` | `to_clickhouse/select.rs:859` |
| `domain/sharding/query_merge.rs:17` | `select_has_true_aggregate`, `select_output_field_name` | `select.rs:1058`, `:859` |
| `domain/sharding/multi_selector.rs:21` | `BUCKET_COL`, `MultiSelector`, `SelectorKind`, `TIME_NS_COL` | `to_clickhouse/selectors.rs:25`, `:38`, `:30`, `:27` |

Their dependency closure, all pure AST code:
- `select_output_field_name` uses `default_field_alias` (`select.rs:872`), `influxql_name` (`:879`) and `expr_name_parts` (`:885`).
- `select_has_true_aggregate` uses `expr_contains_aggregate` (`aggregates.rs:24`), which uses `is_window_transform_call` (`aggregates.rs:9`) and `expr_contains_raw_transform` (`aggregates.rs:40`).
- `MultiSelector` and `SelectorKind` are plain data.

`multi_selector()` (`selectors.rs:80`) uses translator helpers (`with_output_names`), and domain does not call it, so it **stays** in `to_clickhouse`.

## Global Constraints

- No behaviour change, and the generated SQL is byte-identical. The existing `to_clickhouse/tests.rs` must pass unchanged.
- `Cargo.lock` keeps chdb-rust 2.0.0; commits carry no Claude trailers.
- The gate: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --no-fail-fast --test '*'`.

## Review Focus

1. **Output names of unaliased expressions** (`mean("v") * 2` named `mean`, `"a" + "b"` named `a_b`). These must be identical through the re-export. The existing translator tests cover this; Task 1 also adds direct `analysis` tests.
2. **Window transforms** (`difference`, `derivative`) are not aggregates. `select_has_true_aggregate` must still say `false` for them; Task 1 tests it.
3. **The sharded TOP/BOTTOM merge** reads `BUCKET_COL` / `TIME_NS_COL` columns the translator emits. Both must keep the same string values; Task 1 asserts the literals.

---

### Task 1: Move the AST analysis into `timeseriesql/analysis.rs`

**Files:**
- Create: `hyperbytedb/src/timeseriesql/analysis.rs`, and add `pub mod analysis;` in `timeseriesql/mod.rs`
- Modify: `to_clickhouse/select.rs`, `to_clickhouse/aggregates.rs`, `to_clickhouse/selectors.rs`, `to_clickhouse/mod.rs` (re-exports)
- Modify: `domain/rollup.rs`, `domain/sharding/query_merge.rs`, `domain/sharding/multi_selector.rs`

**Interfaces:** `analysis.rs` exports:
- `pub fn select_output_field_name(field: &Field) -> Option<String>`
- `pub fn default_field_alias(expr: &Expr) -> Option<String>`
- `pub fn select_has_true_aggregate(stmt: &SelectStatement) -> bool`
- `pub fn expr_contains_aggregate(expr: &Expr) -> bool`
- `pub fn is_window_transform_call(name: &str) -> bool`
- `pub fn expr_contains_raw_transform(expr: &Expr) -> bool`
- `pub enum SelectorKind`, `pub struct MultiSelector`
- `pub const BUCKET_COL: &str = "__bucket"`, `pub const TIME_NS_COL: &str = "__tns"`

`to_clickhouse` keeps the old paths with `pub use crate::timeseriesql::analysis::{...}` (`pub(super)` items become `pub(crate)` re-exports).

- [ ] **Step 1: Write the failing tests** in `analysis.rs`:
  - `output_names_follow_influxql`: `mean("v") * 2` gives `Some("mean")`; `"a" + "b"` gives `Some("a_b")`; a bare `"v"` gives `None`; an alias wins.
  - `window_transforms_are_not_aggregates`: `SELECT difference("v") FROM m` gives `false`; `SELECT mean("v") FROM m` gives `true`.
  - `merge_column_names_are_stable`: `BUCKET_COL == "__bucket"` and `TIME_NS_COL == "__tns"`.
- [ ] **Step 2: Run the tests** with `cargo test --lib timeseriesql::analysis`. Expected: FAIL, because the module doesn't exist yet.
- [ ] **Step 3: Move the items** listed above (with their private helpers `influxql_name`, `expr_name_parts`), add the re-exports, and switch the three domain files to `crate::timeseriesql::analysis`. Then run `grep -rn "to_clickhouse" src/domain`. Expected: no output.
- [ ] **Step 4: Run the gate,** expecting `to_clickhouse/tests.rs` to pass unchanged, **then commit** with the message "Domain uses AST analysis, not the SQL translator".
