//! Binary entry point for `hyperbytedb-proxy`.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    hyperbytedb_proxy::run().await
}
