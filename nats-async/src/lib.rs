use std::str::{self, FromStr};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub struct Client {
    writer: Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
    recv: tokio::sync::mpsc::UnboundedReceiver<Message>,
}

impl Client {
    pub async fn connect(conn_str: &str) -> Client {
        let con = tokio::net::TcpStream::connect(conn_str).await.unwrap();
        con.set_nodelay(false).unwrap();
        let (read, writer) = con.into_split();
        let mut read = BufReader::new(read);
        let writer = BufWriter::new(writer);
        // let (tx, rx) = flume::unbounded();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let writer = Arc::new(Mutex::new(writer));
        writer.lock().await.write_all(b"CONNECT { \"no_responders\": true, \"headers\": true, \"verbose\": false, \"pedantic\": false }\r\n").await.unwrap();

        {
            let writer = writer.clone();
            tokio::task::spawn(async move {
                loop {
                    let mut line = String::new();
                    let n = read.read_line(&mut line).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    let op = line
                        .split_ascii_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_ascii_uppercase();

                    if op == "PING" {
                        writer.lock().await.write_all(b"PONG\r\n").await.unwrap();
                    }
                    if op == "MSG" {
                        // Extract whitespace-delimited arguments that come after "MSG".
                        let args = line["MSG".len()..]
                            .split_whitespace()
                            .filter(|s| !s.is_empty());
                        let args = args.collect::<Vec<_>>();

                        // Parse the operation syntax: MSG <subject> <sid> [reply-to] <#bytes>
                        let (subject, sid, reply_to, num_bytes) = match args[..] {
                            [subject, sid, num_bytes] => (subject, sid, None, num_bytes),
                            [subject, sid, reply_to, num_bytes] => {
                                (subject, sid, Some(reply_to), num_bytes)
                            }
                            _ => {
                                panic!("could not parse proto");
                            }
                        };

                        // Convert the slice into an owned string.
                        let subject = subject.to_string();

                        // Parse the subject ID.
                        let sid = u64::from_str(sid)
                            .map_err(|_| {
                                panic!("could not parse sid");
                            })
                            .unwrap();

                        // Convert the slice into an owned string.
                        let reply_to = reply_to.map(ToString::to_string);

                        // Parse the number of payload bytes.
                        let num_bytes = u32::from_str(num_bytes)
                            .map_err(|_| {
                                panic!("cannot parse the number of bytes argument after MSG");
                            })
                            .unwrap();

                        // Read the payload.
                        let mut payload = Vec::new();
                        payload.resize(num_bytes as usize, 0_u8);
                        read.read_exact(&mut payload[..]).await.unwrap();
                        // Read "\r\n".
                        read.read_exact(&mut [0_u8; 2]).await.unwrap();
                        let msg = Message { payload };
                        tx.send(msg).unwrap();
                    }
                }
            });
        }

        Client { writer, recv: rx }
    }

    pub async fn flush(&mut self) {
        self.writer.lock().await.flush().await.unwrap();
    }
    pub async fn publish(&mut self, subject: &str, payload: &[u8]) {
        self.encode(Op::Publish(subject, payload)).await;
    }
    pub async fn subscribe(&mut self, subject: &str) -> Subscription<'_> {
        println!("subscribing");
        self.encode(Op::Subscribe(subject)).await;
        self.flush().await;
        Subscription {
            recv: &mut self.recv,
        }
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
            Op::Subscribe(subject) => self
                .writer
                .lock()
                .await
                .write_all(format!("SUB {} {}\r\n", subject, 1).as_bytes())
                .await
                .unwrap(),
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
    Subscribe(&'a str),
}

pub struct Subscription<'a> {
    pub recv: &'a mut tokio::sync::mpsc::UnboundedReceiver<Message>,
}

#[derive(Debug)]
pub struct Message {
    pub payload: Vec<u8>,
}
