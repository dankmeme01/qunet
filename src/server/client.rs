use std::{borrow::Cow, hash::Hash, net::SocketAddr, ops::Deref};

use crate::{
    message::{BufferKind, channel},
    server::{Server, app_handler::AppHandler},
    transport::{QunetTransport, TransportType},
};

pub enum ClientNotification {
    DataMessage {
        buf: BufferKind,
        reliable: bool,
    },
    RetransmitHandshake,

    /// Terminate the client connection without sending any data.
    Terminate,
    /// Disconnect the client gracefully, sending a `ServerClose` message with the given reason
    Disconnect(Cow<'static, str>),
}

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
    pub notif_tx: channel::Sender<ClientNotification>,
    transport_type: TransportType,
}

impl<H: AppHandler> ClientState<H> {
    pub(crate) fn new(app_data: H::ClientData, transport: &QunetTransport) -> Self {
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

    pub async fn send_data(&self, data: &[u8], server: &Server<H>) -> bool {
        let mut buf = server.request_buffer(data.len()).await;
        buf.append_bytes(data);
        self.send_data_bufkind(buf)
    }

    pub fn send_data_bufkind(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage { buf: msg, reliable: true })
    }

    pub async fn send_unreliable_data(&self, data: &[u8], server: &Server<H>) -> bool {
        let mut buf = server.request_buffer(data.len()).await;
        buf.append_bytes(data);
        self.send_data_bufkind(buf)
    }

    pub fn send_unreliable_data_bufkind(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage { buf: msg, reliable: false })
    }

    pub fn retransmit_handshake(&self) -> bool {
        self.notif_tx.send(ClientNotification::RetransmitHandshake)
    }

    pub fn transport_type(&self) -> TransportType {
        self.transport_type
    }

    /// Terminates the client connection
    pub fn terminate(&self) -> bool {
        self.notif_tx.send(ClientNotification::Terminate)
    }

    /// Disconnects the client gracefully, sending a `ServerClose` message with the given reason.
    pub fn disconnect(&self, reason: Cow<'static, str>) -> bool {
        self.notif_tx.send(ClientNotification::Disconnect(reason))
    }
}

impl<H: AppHandler> Deref for ClientState<H> {
    type Target = H::ClientData;

    fn deref(&self) -> &Self::Target {
        &self.app_data
    }
}

impl<H: AppHandler> PartialEq for ClientState<H> {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id
    }
}

impl<H: AppHandler> Eq for ClientState<H> {}

impl<H: AppHandler> Hash for ClientState<H> {
    fn hash<Hs: std::hash::Hasher>(&self, state: &mut Hs) {
        self.connection_id.hash(state);
    }
}
