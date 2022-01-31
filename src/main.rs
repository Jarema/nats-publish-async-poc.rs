use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::time::Instant;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut con = Connection::connect().await;
        println!("conncted");
        std::thread::sleep(Duration::from_secs(2));
        let now = Instant::now();
        // let mut self.writer = tokio_stream::iter(0..10_000_000);

        // while let Some(_value) = self.writer.next().await {
        //     con.write(b"pub events.data 2\r\nhi\r\n").await;
        // }

        for _ in 0..10_000_000 {
            con.publish("events.data", b"foo").await;
        }
        con.flush().await;
        println!("elapsed: {:?}", now.elapsed());
    })
}

struct Connection {
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl Connection {
    async fn connect() -> Connection {
        let con = TcpStream::connect("localhost:4222").await.unwrap();
        let (read, mut writer) = con.into_split();
        let mut read = BufReader::new(read);

        let (tx, mut rx): (UnboundedSender<Op>, UnboundedReceiver<Op>) =
            tokio::sync::mpsc::unbounded_channel();

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

    async fn flush(&mut self) {
        self.writer.lock().await.flush().await.unwrap();
    }
    async fn publish<'a>(&mut self, subject: &'a str, payload: &[u8]) {
        self.encode(Op::Publish(subject, payload)).await;
    }
    async fn encode<'a>(&mut self, op: Op<'a>) {
        match op {
            Op::Pong => self
                .writer
                .lock()
                .await
                .write_all(b"PONG\r\n")
                .await
                .unwrap(),
            Op::Raw(data) => self.writer.lock().await.write_all(&data).await.unwrap(),
            Op::Flush => self.writer.lock().await.flush().await.unwrap(),
            Op::Publish(subject, payload) => {
                let mut buf = bytes::BytesMut::with_capacity(payload.len() + subject.len());
                buf.put(&b"PUB "[..]);
                buf.put(subject.as_bytes());
                buf.put(&b" "[..]);

                let mut buflen = itoa::Buffer::new();
                buf.put(buflen.format(payload.len()).as_bytes());
                buf.put(&b"\r\n"[..]);

                buf.put(&*payload);
                buf.put(&b"\r\n"[..]);

                self.writer
                    .lock()
                    .await
                    .write_all_buf(&mut buf)
                    .await
                    .unwrap();
            }
        }
    }
}

#[derive(Debug)]
enum Op<'a> {
    Raw(Vec<u8>),
    Pong,
    Flush,
    Publish(&'a str, &'a [u8]),
}
