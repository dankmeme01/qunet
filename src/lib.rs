#![feature(
    arbitrary_self_types,
    associated_type_defaults,
    generic_const_exprs,
    sync_unsafe_cell,
    never_type,
    io_slice_as_bytes
)]
#![allow(clippy::new_without_default, clippy::collapsible_if)]

pub mod buffers;
#[cfg(feature = "client")]
pub mod client;
pub mod database;
pub mod message;
pub mod protocol;
pub mod server;
pub mod transport;
