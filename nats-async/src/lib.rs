use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub struct Connection {
    writer: Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
}

impl Connection {
    pub async fn connect() -> Connection {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        let addr = "127.0.0.1:4222".parse().unwrap();
        let con = socket.connect(addr).await.unwrap();
        con.set_nodelay(false).unwrap();
        let (read, writer) = con.into_split();
        let mut read = BufReader::new(read);
        let writer = BufWriter::new(writer);

        let writer = Arc::new(Mutex::new(writer));
        writer.lock().await.write_all(b"CONNECT { \"no_responders\": true, \"headers\": true, \"verbose\": false, \"pedantic\": false }\r\n").await.unwrap();

        {
            let writer = writer.clone();
            tokio::task::spawn(async move {
                loop {
                    let mut buf = String::new();
                    let n = read.read_line(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    if buf == "PING\r\n" {
                        writer.lock().await.write_all(b"PONG\r\n").await.unwrap();
                    }
                }
            });
        }

        Connection { writer }
    }

    pub async fn flush(&mut self) {
        self.writer.lock().await.flush().await.unwrap();
    }
    pub async fn publish(&mut self, subject: &str, payload: &[u8]) {
        self.encode(Op::Publish(subject, payload)).await;
    }
    pub async fn encode(&mut self, op: Op<'_>) {
        match op {
            Op::Pong => self
                .writer
                .lock()
                .await
                .write_all(b"PONG\r\n")
                .await
                .unwrap(),
            Op::Publish(subject, payload) => {
                let mut writer = self.writer.lock().await;
                writer.write_all(b"PUB ").await.unwrap();
                writer.write_all(subject.as_bytes()).await.unwrap();
                writer.write_all(b" ").await.unwrap();

                let mut bufi = itoa::Buffer::new();
                writer
                    .write_all(bufi.format(payload.len()).as_bytes())
                    .await
                    .unwrap();
                writer.write_all(b"\r\n").await.unwrap();
                writer.write_all(payload).await.unwrap();
                writer.write_all(b"\r\n").await.unwrap();
            }
        }
    }
    pub async fn shutdown(&self) {
        self.writer.lock().await.shutdown().await.unwrap();
    }
}

#[derive(Debug)]
pub enum Op<'a> {
    Pong,
    Publish(&'a str, &'a [u8]),
}
