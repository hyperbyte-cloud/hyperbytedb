//! Async client for HyperbyteDB's InfluxDB v1 API (`/write`, `/query`).
//!
//! ```no_run
//! use hyperbytedb_client::{Client, Options, Point, QueryOptions, WriteOptions};
//!
//! # async fn run() -> hyperbytedb_client::Result<()> {
//! let db = Client::with_options("http://hyperbytedb:8086", Options {
//!     username: Some("writer".into()),
//!     password: Some("...".into()),
//!     ..Default::default()
//! })?;
//! db.create_database("app").await?;
//! db.write("app", &[Point::new("cpu").tag("host", "web1").field("usage", 12.5)], &WriteOptions::default())
//!     .await?;
//! let results = db
//!     .query(r#"SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host""#, &QueryOptions::database("app"))
//!     .await?;
//! for row in results[0].rows() {
//!     println!("{row:?}");
//! }
//! # Ok(())
//! # }
//! ```

mod client;
mod error;
mod line_protocol;

pub use client::{
    Client, Epoch, Options, QueryOptions, QueryResult, Series, WriteOptions, VERSION,
};
pub use error::{Error, Result};
pub use line_protocol::{encode, FieldValue, Point, Precision};
