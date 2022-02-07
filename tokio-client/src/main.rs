use nats_async::Client;
use tokio::time::Instant;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut con = Client::connect("127.0.0.1:4222").await;
        println!("conncted");
        let now = Instant::now();

        for _ in 0..1_000_000_000 {
            con.publish("events.data", b"foo").await;
        }

        let sub = con.subscribe("events.>").await;

        con.flush().await;
        println!("elapsed: {:?}", now.elapsed());
        con.shutdown().await;
    })
}
