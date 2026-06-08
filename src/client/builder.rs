use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::client::{
    Client, ClientHandle, ClientOutcome,
    event_handler::{DefaultEventHandler, EventHandler},
};

#[derive(Default, Debug)]
pub struct ClientBuilder<H: EventHandler = DefaultEventHandler> {
    pub(crate) event_handler: Option<H>,
    pub(crate) quic_cert_path: Option<PathBuf>,
    pub(crate) keepalive_interval: Option<Duration>,
}

impl<H: EventHandler> ClientBuilder<H> {
    pub fn with_event_handler<E: EventHandler>(self, event_handler: E) -> ClientBuilder<E> {
        ClientBuilder {
            event_handler: Some(event_handler),
            quic_cert_path: self.quic_cert_path,
            keepalive_interval: self.keepalive_interval,
        }
    }

    pub fn with_quic_cert_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.quic_cert_path = Some(path.into());
        self
    }

    pub fn with_keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = Some(interval);
        self
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
