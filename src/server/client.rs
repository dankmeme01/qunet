use std::net::SocketAddr;

use crate::server::{
    app_handler::AppHandler,
    message::{BufferKind, channel},
};

pub enum ClientNotification {
    DataMessage(BufferKind),
}

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
    pub notif_tx: channel::Sender<ClientNotification>,
}

impl<H: AppHandler> ClientState<H> {
    pub fn send_message(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage(msg))
    }
}
