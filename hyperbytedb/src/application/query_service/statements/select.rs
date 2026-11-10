//! SELECT.

use super::super::*;

pub(in crate::application::query_service) async fn select(
    svc: &QueryServiceImpl,
    db: &str,
    select_stmt: &crate::timeseriesql::ast::SelectStatement,
    epoch: Option<&str>,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    if db.is_empty() {
        return Ok(StatementResult {
            statement_id,
            series: None,
            error: Some("database is required".to_string()),
        });
    }

    if select_stmt.into.is_some() {
        let count = execute_select_into(svc, db, select_stmt, epoch).await?;
        return Ok(StatementResult {
            statement_id,
            series: Some(vec![SeriesResult {
                name: "result".to_string(),
                tags: None,
                columns: vec!["time".to_string(), "written".to_string()],
                values: vec![vec![
                    serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                    serde_json::json!(count),
                ]],
                partial: None,
            }]),
            error: None,
        });
    }

    let group_by_tags: Vec<String> = select_stmt
        .group_by
        .as_ref()
        .map(|gb| {
            gb.tag_dimensions()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    let (time_min, time_max) = to_clickhouse::extract_time_bounds(select_stmt.condition.as_ref());

    if select_stmt.from.is_empty() {
        return Err(HyperbytedbError::QueryParse(
            "SELECT requires FROM clause".to_string(),
        ));
    }

    let mut all_series = Vec::new();
    for source in &select_stmt.from {
        let mut series = execute_select_from_source(
            svc,
            db,
            select_stmt,
            source,
            time_min,
            time_max,
            epoch,
            &group_by_tags,
            query_rp,
        )
        .await?;
        all_series.append(&mut series);
    }

    stamp_aggregate_time(select_stmt, &mut all_series, epoch, time_min);
    order_columns_like_select(select_stmt, &mut all_series);

    // Apply SLIMIT/SOFFSET (series-level pagination)
    if select_stmt.slimit.is_some() || select_stmt.soffset.is_some() {
        let soffset = select_stmt.soffset.unwrap_or(0) as usize;
        let slimit = select_stmt.slimit.unwrap_or(u64::MAX) as usize;
        let len = all_series.len();
        let start = soffset.min(len);
        let end = (start + slimit).min(len);
        all_series = all_series[start..end].to_vec();
    }

    Ok(StatementResult {
        statement_id,
        series: Some(all_series),
        error: None,
    })
}
