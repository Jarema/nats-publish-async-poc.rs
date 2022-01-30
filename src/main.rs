use std::time::Duration;

use bytes::BufMut;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_stream::StreamExt;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut con = Connection::connect().await;
        println!("conncted");
        std::thread::sleep(Duration::from_secs(2));
        let now = Instant::now();
        // let mut stream = tokio_stream::iter(0..10_000_000);

        // while let Some(_value) = stream.next().await {
        //     con.write(b"pub events.data 2\r\nhi\r\n").await;
        // }

        for i in 0..10_000_000 {
            // con.write(format!("pub events.{} 2\r\nhi\r\n", i).as_bytes())
            //     .await;
            con.publish(format!("events.{}", i).as_str(), b"some data")
                .await;
        }
        con.flush().await;
        println!("elapsed: {:?}", now.elapsed());
    })
}

struct Connection {
    tx: UnboundedSender<Op>,
}

impl Connection {
    async fn connect() -> Connection {
        let con = TcpStream::connect("localhost:4222").await.unwrap();
        let (read, mut write) = con.into_split();
        let mut read = BufReader::new(read);

        let (tx, mut rx): (UnboundedSender<Op>, UnboundedReceiver<Op>) =
            tokio::sync::mpsc::unbounded_channel();
        write.write_all(b"CONNECT { \"no_responders\": true, \"headers\": true, \"verbose\": false, \"pedantic\": false }\r\n").await.unwrap();

        tokio::task::spawn(async move {
            loop {
                let data = rx.recv().await.unwrap();
                match data {
                    Op::Pong => write.write_all(b"PONG\r\n").await.unwrap(),
                    Op::Raw(data) => write.write_all(&data).await.unwrap(),
                    Op::Flush => write.flush().await.unwrap(),
                    Op::Publish(subject, payload, resp) => {
                        let mut buf = bytes::BytesMut::with_capacity(payload.len());

                        buf.put(&b"PUB "[..]);
                        buf.put(subject.as_bytes());
                        buf.put(&b" "[..]);
                        let mut ibuf = itoa::Buffer::new();
                        buf.put(ibuf.format(payload.len()).as_bytes());
                        buf.put(&b"\r\n"[..]);
                        buf.put(&*payload);
                        buf.put(&b"\r\n"[..]);

                        write.write_all(&buf).await.unwrap();
                        resp.send(()).unwrap();
                    }
                }
            }
        });

        {
            let tx = tx.clone();
            tokio::task::spawn(async move {
                loop {
                    let mut buf = String::new();
                    let n = read.read_line(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    println!("MESSAGE FROM SERVER: {:?}", buf);
                    if buf == "PING\r\n" {
                        tx.send(Op::Pong).unwrap();
                    }
                }
            });
        }

        Connection { tx }
    }

    async fn write(&mut self, data: &[u8]) {
        self.tx.send(Op::Raw(data.to_vec())).unwrap();
    }
    async fn flush(&mut self) {
        self.tx.send(Op::Flush).unwrap();
    }
    async fn publish(&self, subject: &str, payload: &[u8]) {
        let (otx, orx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Op::Publish(subject.to_string(), payload.to_vec(), otx))
            .unwrap();
        orx.await.unwrap();
    }
}

#[derive(Debug)]
enum Op {
    Raw(Vec<u8>),
    Pong,
    Flush,
    Publish(String, Vec<u8>, tokio::sync::oneshot::Sender<()>),
}
