use crate::message::QunetRawMessage;

pub struct Sender<T> {
    inner: flume::Sender<T>,
}

pub struct Receiver<T> {
    inner: flume::Receiver<T>,
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> bool {
        self.inner.try_send(msg).is_ok()
    }
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Option<T> {
        self.inner.recv_async().await.ok()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

pub type RawMessageSender = Sender<QunetRawMessage>;
pub type RawMessageReceiver = Receiver<QunetRawMessage>;

pub fn new_channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = flume::bounded(16);

    (Sender { inner: tx }, Receiver { inner: rx })
}
