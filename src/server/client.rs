use std::net::SocketAddr;

use crate::server::app_handler::AppHandler;

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
}

impl<H: AppHandler> ClientState<H> {}
