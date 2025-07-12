use crate::client::{Client, ClientHandle};

pub type HandlerError = Box<dyn std::error::Error + Send + Sync>;
pub type HandlerResult<T> = Result<T, HandlerError>;

#[allow(unused_variables)]
pub trait EventHandler: Send + Sync + Sized + 'static {
    /// This callback is invoked before the initial setup phase of the client, right as `build()` or `run()` is called.
    /// Returning an error from this callback shuts down the entire client with `ClientOutcome::CustomError`
    fn pre_setup(&self, server: &Client<Self>) -> impl Future<Output = HandlerResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked after the initial setup phase of the client.
    /// Returning an error from this callback shuts down the entire client with `ClientOutcome::CustomError`
    fn post_setup(&self, server: &Client<Self>) -> impl Future<Output = HandlerResult<()>> + Send {
        async move { Ok(()) }
    }

    /// This callback is invoked once the client is fully launched.
    /// Returning an error from this callback shuts down the entire client with `ServerOutcome::CustomError`.
    /// Unlike most of other callbacks, this callback gives you a `ServerHandle` which can be cloned and stored for later use.
    /// This is where you can start establishing a connection.
    fn on_launch(
        &self,
        server: ClientHandle<Self>,
    ) -> impl Future<Output = HandlerResult<()>> + Send {
        async move { Ok(()) }
    }
}

#[derive(Default, Debug, Clone)]
pub struct DefaultEventHandler;

impl EventHandler for DefaultEventHandler {}
