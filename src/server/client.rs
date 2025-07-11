use std::net::SocketAddr;

use crate::{
    message::{BufferKind, channel},
    server::app_handler::AppHandler,
    transport::{ClientTransport, TransportType},
};

pub enum ClientNotification {
    DataMessage {
        buf: BufferKind,
        reliable: bool,
    },
    RetransmitHandshake,
}

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
    pub notif_tx: channel::Sender<ClientNotification>,
    transport_type: TransportType,
}

impl<H: AppHandler> ClientState<H> {
    pub(crate) fn new(app_data: H::ClientData, transport: &ClientTransport<H>) -> Self {
        Self {
            app_data,
            connection_id: transport.connection_id(),
            address: transport.address(),
            notif_tx: transport.notif_chan.0.clone(),
            transport_type: transport.transport_type(),
        }
    }

    pub fn data(&self) -> &H::ClientData {
        &self.app_data
    }

    pub fn send_data(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage { buf: msg, reliable: true })
    }

    pub fn send_unreliable_data(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage { buf: msg, reliable: false })
    }

    pub fn retransmit_handshake(&self) -> bool {
        self.notif_tx.send(ClientNotification::RetransmitHandshake)
    }

    pub fn transport_type(&self) -> TransportType {
        self.transport_type
    }
}
