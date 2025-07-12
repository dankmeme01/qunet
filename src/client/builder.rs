use std::sync::Arc;

use crate::client::{
    Client, ClientHandle, ClientOutcome,
    event_handler::{DefaultEventHandler, EventHandler},
};

#[derive(Default, Debug)]
pub struct ClientBuilder<H: EventHandler = DefaultEventHandler> {
    pub(crate) event_handler: Option<H>,
}

impl<H: EventHandler> ClientBuilder<H> {
    pub fn with_event_handler<E: EventHandler>(self, event_handler: E) -> ClientBuilder<E> {
        ClientBuilder {
            event_handler: Some(event_handler),
        }
    }

    pub fn build_raw(self) -> Client<H> {
        Client::<H>::from_builder(self)
    }

    pub async fn build(self) -> Result<ClientHandle<H>, ClientOutcome> {
        let mut client = self.build_raw();
        client.setup().await?;

        Ok(ClientHandle { inner: Arc::new(client) })
    }
}
