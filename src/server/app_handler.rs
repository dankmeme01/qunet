use std::net::SocketAddr;

use tracing::info;

use crate::{
    buffers::byte_writer::{ByteWriter, ByteWriterError},
    server::{Server, ServerHandle, client::ClientState},
};

pub use crate::server::msg_data::MsgData;

pub type AppError = Box<dyn std::error::Error + Send + Sync>;
pub type AppResult<T> = Result<T, AppError>;

#[allow(unused_variables)]
pub trait AppHandler: Send + Sync + Sized + 'static {
    // Server control: setup, startup, shutdown

    /// This callback is invoked before the initial setup phase of the server, right as `build()` or `run()` is called.
    /// Returning an error from this callback shuts down the entire server with `ServerOutcome::CustomError`
    fn pre_setup(&self, server: &Server<Self>) -> impl Future<Output = AppResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked after the initial setup phase of the server, by then all listeners are bound, and some other stuff is initialized.
    /// Returning an error from this callback shuts down the entire server with `ServerOutcome::CustomError`
    fn post_setup(&self, server: &Server<Self>) -> impl Future<Output = AppResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked after all listeners have been successfully started up.
    /// Returning an error from this callback shuts down the entire server with `ServerOutcome::CustomError`.
    /// Unlike most of other callbacks, this callback gives you a `ServerHandle` which can be cloned and stored for later use.
    fn on_launch(&self, server: ServerHandle<Self>) -> impl Future<Output = AppResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked when the server enters the graceful shutdown phase.
    /// No listeners have been shut down by this time.
    /// Returning an error from this callback aborts graceful shutdown, and the server will fail with `ServerOutcome::CleanupFailure`.
    /// Note: time spent in this callback is subject to the graceful shutdown timeout. Adjust it if you don't want this callback to be abruptly stopped.
    fn pre_shutdown(&self, server: &Server<Self>) -> impl Future<Output = AppResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked after all the listeners and connections have been shut down by the server.
    /// Returning an error from this callback causes the server to fail with `ServerOutcome::CleanupFailure`.
    /// Note: time spent in this callback is subject to the graceful shutdown timeout. Adjust it if you don't want this callback to be abruptly stopped.
    fn post_shutdown(&self, server: &Server<Self>) -> impl Future<Output = AppResult<()>> + Send {
        async move { Ok(()) }
    }

    // Client handling (connection, disconnection, data packets)

    /// Client data structure that is stored with every client.
    type ClientData: Send + Sync = ();

    /// This callback is invoked when a new client connects to the server. It must return `ClientData`,
    /// which will be stored for later use. `on_client_disconnect` will eventually be called for this connection.
    /// The only exception is if this callback returns an error, in that case the client connection will be silently dropped.
    fn on_client_connect(
        &self,
        server: &Server<Self>,
        connection_id: u64,
        address: SocketAddr,
        kind_str: &str,
    ) -> impl Future<Output = AppResult<Self::ClientData>> + Send;

    /// This callback is invoked when a client fully disconnects from the server. It is not invoked when a connection break happens,
    /// for that see `on_client_suspend`. After this callback is called, all the data related to the client data is freed.
    fn on_client_disconnect(
        &self,
        server: &Server<Self>,
        client: &ClientState<Self>,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// This callback is invoked when a client connection is "suspended".
    /// Suspensions happen when an unexpected network error occurs, or when a client intentionally tells us not to disconnect them.
    /// After being suspended, the client has a limited (configurable) amount of time to reconnect.
    /// If that happens, `on_client_resume` is called. Otherwise, after the timeout expires, `on_client_disconnect` is called and client state is erased.
    fn on_client_suspend(
        &self,
        server: &Server<Self>,
        client: &ClientState<Self>,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// This callback is invoked when a suspended client connection is resumed, due to the client reconnecting.
    fn on_client_resume(
        &self,
        server: &Server<Self>,
        client: &ClientState<Self>,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// This callback is invoked when a client sends a data packet to the server.
    fn on_client_data(
        &self,
        server: &Server<Self>,
        client: &ClientState<Self>,
        data: MsgData<'_>,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }

    // Custom ping/keepalive data

    /// This callback is invoked when an unconnected client sends a ping packet to the server.
    /// The application can use this callback to write custom data into the ping response packet.
    fn on_ping(
        &self,
        server: &Server<Self>,
        writer: &mut ByteWriter,
    ) -> Result<(), ByteWriterError> {
        Ok(())
    }

    /// This callback is invoked when a connected client sends a keepalive packet to the server.
    /// The application can use this callback to write custom data into the keepalive response packet.
    fn on_keepalive(
        &self,
        server: &Server<Self>,
        writer: &mut ByteWriter,
    ) -> Result<(), ByteWriterError> {
        Ok(())
    }
}

#[derive(Default, Debug, Clone)]
pub struct DefaultAppHandler;

impl AppHandler for DefaultAppHandler {
    type ClientData = ();

    async fn on_client_connect(
        &self,
        _server: &Server<Self>,
        connection_id: u64,
        address: SocketAddr,
        kind: &str,
    ) -> AppResult<Self::ClientData> {
        info!("[{}] Accepted {} connection (ID: {})", address, kind, connection_id);

        Ok(())
    }
}
