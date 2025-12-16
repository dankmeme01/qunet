use std::{
    borrow::Cow,
    hash::Hash,
    net::SocketAddr,
    ops::Deref,
    sync::atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

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

    /// Terminate the client and suspend the connection
    TerminateSuspend,

    /// Disconnect the client gracefully, sending a `ServerClose` message with the given reason
    Disconnect(Cow<'static, str>),
}

pub struct ClientState<H: AppHandler> {
    pub(crate) app_data: H::ClientData,
    pub connection_id: u64,
    pub address: SocketAddr,
    pub notif_tx: channel::Sender<ClientNotification>,
    pub suspended: AtomicBool,
    pub(crate) suspended_notify: Notify, // to notify anyone waiting for suspension
    pub(crate) terminate_notify: Notify,
    transport_type: TransportType,
}

impl<H: AppHandler> ClientState<H> {
    pub(crate) fn new(app_data: H::ClientData, transport: &QunetTransport) -> Self {
        Self {
            app_data,
            connection_id: transport.connection_id(),
            address: transport.address(),
            notif_tx: transport.notif_chan.0.clone(),
            suspended: AtomicBool::new(false),
            suspended_notify: Notify::new(),
            terminate_notify: Notify::new(),
            transport_type: transport.transport_type(),
        }
    }

    pub fn data(&self) -> &H::ClientData {
        &self.app_data
    }

    pub async fn send_data(&self, data: &[u8], server: &Server<H>) -> bool {
        let mut buf = server.request_buffer(data.len());
        buf.append_bytes(data);
        self.send_data_bufkind(buf)
    }

    pub fn send_data_bufkind(&self, msg: BufferKind) -> bool {
        self.notif_tx.send(ClientNotification::DataMessage { buf: msg, reliable: true })
    }

    pub async fn send_unreliable_data(&self, data: &[u8], server: &Server<H>) -> bool {
        let mut buf = server.request_buffer(data.len());
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

    pub(crate) fn set_suspended(&self, suspended: bool) {
        self.suspended.store(suspended, Ordering::Relaxed);
        if suspended {
            self.suspended_notify.notify_one();
        }
    }

    /// Terminates the client connection
    pub fn terminate(&self) -> bool {
        self.terminate_notify.notify_one();
        self.notif_tx.send(ClientNotification::Terminate)
    }

    /// Disconnects the client gracefully, sending a `ServerClose` message with the given reason.
    pub fn disconnect(&self, reason: impl Into<Cow<'static, str>>) -> bool {
        if self.suspended.load(Ordering::Relaxed) {
            self.terminate()
        } else {
            self.notif_tx.send(ClientNotification::Disconnect(reason.into()))
        }
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
