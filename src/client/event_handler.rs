use crate::{
    client::{Client, ClientHandle},
    message::MsgData,
};

pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;
pub type HandlerResult<T> = Result<T, HandlerError>;

#[allow(unused_variables)]
pub trait EventHandler: Send + Sync + Sized + 'static {
    /// This callback is invoked before the initial setup phase of the client, right as `build()` or `run()` is called.
    /// Returning an error from this callback shuts down the entire client with `ClientOutcome::CustomError`
    fn pre_setup(&self, client: &Client<Self>) -> impl Future<Output = HandlerResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked after the initial setup phase of the client.
    /// Returning an error from this callback shuts down the entire client with `ClientOutcome::CustomError`
    fn post_setup(&self, client: &Client<Self>) -> impl Future<Output = HandlerResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked when the client successfully connects to a server.
    fn on_connected(&self, client: &ClientHandle<Self>) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// This callback is invoked when the client disconnects from a server, due to either a graceful shutdown or an error.
    fn on_disconnected(&self, client: &ClientHandle<Self>) -> impl Future<Output = ()> + Send {
        async move {}
    }

    /// This callback is invoked when the client receives data from the server.
    fn on_recv_data(
        &self,
        client: &Client<Self>,
        data: MsgData<'_>,
    ) -> impl Future<Output = ()> + Send {
        async move {}
    }
}

#[derive(Default, Debug, Clone)]
pub struct DefaultEventHandler;

impl EventHandler for DefaultEventHandler {}
