//! DELETE and DROP SERIES.

use super::super::*;

pub(in crate::application::query_service) async fn delete(
    svc: &QueryServiceImpl,
    db: &str,
    del: &crate::timeseriesql::ast::DeleteStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let del_rp = if let Some(rp) = query_rp {
        rp.to_string()
    } else {
        svc.metadata
            .get_default_rp(db)
            .await
            .unwrap_or_else(|_| "autogen".to_string())
    };
    // The rows are deleted, as in InfluxDB: points written after the
    // DELETE stay. It used to store a tombstone that every later query
    // applied, hiding later points too.
    let predicate_sql = crate::application::predicate_sql::build_delete_predicate(
        &svc.metadata,
        db,
        &del_rp,
        &del.from,
        del.condition.as_ref(),
    )
    .await?;

    if let Some(ctx) = svc.shard_routing.as_ref() {
        scatter_delete_to_regions(
            ctx,
            svc.metadata.as_ref(),
            Some(crate::application::predicate_sql::LocalRows {
                wal: Some(svc.wal.as_ref()),
                sink: svc.points_sink.as_ref(),
            }),
            db,
            &del_rp,
            &del.from,
            &predicate_sql,
        )
        .await?;
    } else {
        crate::application::predicate_sql::delete_rows(
            Some(svc.wal.as_ref()),
            svc.points_sink.as_ref(),
            db,
            &del_rp,
            &del.from,
            &predicate_sql,
        )
        .await?;
    }

    tracing::debug!(
        db = db,
        measurement = %del.from,
        predicate = %predicate_sql,
        "DELETE applied"
    );

    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}

pub(in crate::application::query_service) async fn drop_series(
    svc: &QueryServiceImpl,
    db: &str,
    s: &crate::timeseriesql::ast::DropSeriesStatement,
    query_rp: Option<&str>,
    statement_id: u32,
) -> Result<StatementResult, HyperbytedbError> {
    let target_db = s.database.as_deref().unwrap_or(db);
    let measurement = s.from.as_ref().and_then(|n| match n {
        MeasurementName::Name(n) => Some(n.clone()),
        MeasurementName::Regex(_) => None,
    });
    let rp = if let Some(rp) = query_rp {
        rp.to_string()
    } else {
        svc.metadata.get_default_rp(target_db).await?
    };
    let predicate_sql = if let Some(ref cond) = s.condition {
        let meas = measurement.as_deref().unwrap_or("");
        crate::application::predicate_sql::build_predicate_sql(
            &svc.metadata,
            target_db,
            &rp,
            meas,
            cond,
        )
        .await?
    } else {
        String::new()
    };
    if measurement.is_some() || !predicate_sql.is_empty() {
        svc.metadata
            .delete_series_matching(target_db, &rp, measurement.as_deref(), &predicate_sql)
            .await?;
        // The series' rows are deleted; points written to them later
        // stay. A tombstone used to hide those too.
        let mut targets: Vec<(String, String)> = Vec::new();
        if let Some(ref meas) = measurement {
            let pred = crate::application::predicate_sql::build_delete_predicate(
                &svc.metadata,
                target_db,
                &rp,
                meas,
                s.condition.as_ref(),
            )
            .await?;
            targets.push((meas.clone(), pred));
        } else if svc.shard_routing.is_some() {
            // Every measurement that has the condition's tags.
            for meas in svc.metadata.list_measurements(target_db).await? {
                if let Ok(pred) = crate::application::predicate_sql::build_delete_predicate(
                    &svc.metadata,
                    target_db,
                    &rp,
                    &meas,
                    s.condition.as_ref(),
                )
                .await
                {
                    targets.push((meas, pred));
                }
            }
        }
        for (meas, pred) in targets {
            if let Some(ctx) = svc.shard_routing.as_ref() {
                scatter_delete_to_regions(
                    ctx,
                    svc.metadata.as_ref(),
                    Some(crate::application::predicate_sql::LocalRows {
                        wal: Some(svc.wal.as_ref()),
                        sink: svc.points_sink.as_ref(),
                    }),
                    target_db,
                    &rp,
                    &meas,
                    &pred,
                )
                .await?;
            } else {
                crate::application::predicate_sql::delete_rows(
                    Some(svc.wal.as_ref()),
                    svc.points_sink.as_ref(),
                    target_db,
                    &rp,
                    &meas,
                    &pred,
                )
                .await?;
            }
        }
    }
    Ok(StatementResult {
        statement_id,
        series: Some(vec![]),
        error: None,
    })
}
