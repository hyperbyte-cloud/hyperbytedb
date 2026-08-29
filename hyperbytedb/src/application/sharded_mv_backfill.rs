//! Scatter materialized-view historical backfill across shard regions.

use std::sync::Arc;

use metrics::counter;

use crate::application::shard_peer_resolution::{RegionTargetRole, ScatterKind};
use crate::application::shard_query::inject_region_series_id_predicate_with_alias;
use crate::application::shard_routing::{ShardRoutingContext, scatter_to_region_peers};
use crate::domain::sharding::{MvBackfillPhase, ShardMvBackfillRequest, ShardRegion};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::ports::sharding::ShardMapPort;

/// Borrowed measurement identity within a database and retention policy.
#[derive(Clone, Copy)]
pub struct MeasurementShardRef<'a> {
    pub db: &'a str,
    pub rp: &'a str,
    pub measurement: &'a str,
}

/// Ports required to execute MV backfill SQL on a shard peer.
#[derive(Clone, Copy)]
pub struct ShardedMvBackfillPorts<'a> {
    pub query_port: &'a Arc<dyn QueryPort>,
    pub points_sink: &'a Arc<dyn PointsSinkPort>,
    pub metadata: &'a Arc<dyn MetadataPort>,
}

/// Region-scoped fact and series backfill statements for one MV create.
#[derive(Clone, Copy)]
pub struct ShardedMvBackfillPlan<'a> {
    pub source: MeasurementShardRef<'a>,
    pub dest: MeasurementShardRef<'a>,
    pub fact_sql: &'a str,
    pub series_sql: &'a str,
}

struct RegionBackfillJob<'a> {
    ports: ShardedMvBackfillPorts<'a>,
    source: MeasurementShardRef<'a>,
    dest: MeasurementShardRef<'a>,
    region: &'a ShardRegion,
    phase: MvBackfillPhase,
    sql: String,
}

pub async fn scatter_mv_backfill(
    ctx: &ShardRoutingContext,
    ports: ShardedMvBackfillPorts<'_>,
    plan: ShardedMvBackfillPlan<'_>,
) -> Result<(), HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let source = plan.source;
    let space = map
        .space(source.db, source.rp, source.measurement)
        .ok_or_else(|| {
            HyperbytedbError::ShardMap(
                format!(
                    "source measurement {}/{}/{} has no shard space",
                    source.db, source.rp, source.measurement
                )
                .into(),
            )
        })?;

    for region in &space.regions {
        let fact_sql = inject_region_series_id_predicate_with_alias(
            plan.fact_sql.to_string(),
            region.start,
            region.end,
            Some("t"),
        );
        execute_region_backfill(
            ctx,
            RegionBackfillJob {
                ports,
                source,
                dest: plan.dest,
                region,
                phase: MvBackfillPhase::Fact,
                sql: fact_sql,
            },
        )
        .await?;

        let series_sql = inject_region_series_id_predicate_with_alias(
            plan.series_sql.to_string(),
            region.start,
            region.end,
            Some("s"),
        );
        execute_region_backfill(
            ctx,
            RegionBackfillJob {
                ports,
                source,
                dest: plan.dest,
                region,
                phase: MvBackfillPhase::Series,
                sql: series_sql,
            },
        )
        .await?;

        counter!("hyperbytedb_shard_mv_backfill_regions_total").increment(1);
    }

    Ok(())
}

async fn execute_region_backfill(
    ctx: &ShardRoutingContext,
    job: RegionBackfillJob<'_>,
) -> Result<(), HyperbytedbError> {
    let RegionBackfillJob {
        ports,
        source,
        dest,
        region,
        phase,
        sql,
    } = job;

    let req = ShardMvBackfillRequest {
        db: source.db.to_string(),
        rp: source.rp.to_string(),
        epoch: region.epoch,
        region_id: region.region_id,
        phase,
        sql: sql.clone(),
        dest_db: dest.db.to_string(),
        dest_rp: dest.rp.to_string(),
        dest_measurement: dest.measurement.to_string(),
    };

    if region.peers.contains(&ctx.node_id) {
        apply_mv_backfill_sql(ports.points_sink, ports.metadata, ports.query_port, &req).await?;
        return Ok(());
    }

    let region_id = region.region_id;
    scatter_to_region_peers(ctx, region, RegionTargetRole::Read, ScatterKind::Query, {
        let req = req.clone();
        move |peer_id, addr, timeout| {
            let req = req.clone();
            let addr = addr.to_string();
            async move {
                let url = format!("http://{addr}/internal/shard/mv-backfill");
                let resp = ctx
                    .peer_client
                    .http_client()
                    .post(&url)
                    .json(&req)
                    .timeout(timeout)
                    .send()
                    .await
                    .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
                if resp.status() == reqwest::StatusCode::CONFLICT {
                    return Err(HyperbytedbError::StaleShardEpoch { region_id });
                }
                if !resp.status().is_success() {
                    return Err(HyperbytedbError::PeerUnreachable(format!(
                        "shard mv-backfill to peer {peer_id} failed: {}",
                        resp.status()
                    )));
                }
                Ok(())
            }
        }
    })
    .await
}

pub async fn apply_mv_backfill_sql(
    points_sink: &Arc<dyn PointsSinkPort>,
    metadata: &Arc<dyn MetadataPort>,
    query_port: &Arc<dyn QueryPort>,
    req: &ShardMvBackfillRequest,
) -> Result<(), HyperbytedbError> {
    if let Some(dest_meta) = metadata
        .get_measurement(&req.dest_db, &req.dest_rp, &req.dest_measurement)
        .await?
    {
        points_sink
            .ensure_measurement_schema(&req.dest_db, &req.dest_rp, &dest_meta)
            .await?;
    }

    query_port.execute_sql(&req.sql).await?;
    Ok(())
}
