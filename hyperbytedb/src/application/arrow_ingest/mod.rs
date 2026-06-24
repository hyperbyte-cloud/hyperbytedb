//! Build prepared WAL slots at ingest time.

mod columnar;
mod line_protocol;
mod msgpack;
mod points;

pub use columnar::columnar_to_prepared_slot;
pub use line_protocol::line_body_to_prepared_slot;
pub use msgpack::msgpack_body_to_prepared_slot;
pub use points::points_to_prepared_slot;
