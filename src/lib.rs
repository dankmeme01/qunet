#![feature(
    arbitrary_self_types,
    associated_type_defaults,
    generic_const_exprs,
    sync_unsafe_cell,
    never_type
)]

pub mod buffers;
pub mod client;
pub mod database;
pub mod message;
pub mod protocol;
pub mod server;
pub mod transport;
