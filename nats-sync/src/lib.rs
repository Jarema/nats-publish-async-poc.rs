use tokio::runtime::Runtime;

pub struct Client {
    inner: nats_async::Client,
    rt: Runtime,
}

impl Client {
    pub fn connect() -> Client {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let inner = rt.block_on(nats_async::Client::connect());

        Client { inner, rt }
    }

    pub fn publish(&mut self, subject: &str, payload: &[u8]) {
        self.rt.block_on(self.inner.publish(subject, payload))
    }

    pub fn flush(&mut self) {
        self.rt.block_on(self.inner.flush())
    }

    pub fn shutdown(&mut self) {
        self.rt.block_on(self.inner.shutdown())
    }
}
