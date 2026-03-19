use crate::message::QunetRawMessage;

pub struct Sender<T> {
    inner: kanal::AsyncSender<T>,
}

pub struct Receiver<T> {
    inner: kanal::AsyncReceiver<T>,
}

impl<T> Sender<T> {
    pub fn send(&self, msg: T) -> bool {
        self.inner.try_send(msg).is_ok()
    }

    pub async fn send_async(&self, msg: T) -> bool {
        self.inner.send(msg).await.is_ok()
    }
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Option<T> {
        self.inner.recv().await.ok()
    }

    pub fn drain(&self) -> Result<Vec<T>, kanal::ReceiveError> {
        let mut vec = Vec::new();
        self.inner.drain_into(&mut vec)?;
        Ok(vec)
    }

    pub fn drain_into(&self, vec: &mut Vec<T>) -> Result<usize, kanal::ReceiveError> {
        self.inner.drain_into(vec)
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

pub fn new_channel<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = kanal::bounded_async(cap);

    (Sender { inner: tx }, Receiver { inner: rx })
}

pub fn new_channel_unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = kanal::unbounded_async();

    (Sender { inner: tx }, Receiver { inner: rx })
}
