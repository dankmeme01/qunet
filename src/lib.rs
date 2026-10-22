#![feature(arbitrary_self_types, associated_type_defaults, impl_trait_in_bindings)]
#![allow(clippy::new_without_default, clippy::collapsible_if)]

pub mod buffers;
#[cfg(feature = "client")]
pub mod client;
pub mod database;
pub mod message;
pub mod protocol;
pub mod server;
pub mod transport;
