use std::net::SocketAddr;

use crate::server::{
    app_handler::AppHandler,
    message::{BufferKind, channel},
};

pub enum ClientNotification {
    DataMessage { buf: BufferKind, reliable: bool },
}

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
    pub notif_tx: channel::Sender<ClientNotification>,
}

impl<H: AppHandler> ClientState<H> {
    pub fn send_data(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage {
            buf: msg,
            reliable: true,
        })
    }

    pub fn send_unreliable_data(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage {
            buf: msg,
            reliable: false,
        })
    }
}
