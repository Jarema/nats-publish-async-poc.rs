use nats_async::Client;
use tokio::time::Instant;

static ITERATIONS: i32 = 10_000_000;

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .build()
        .unwrap();

    rt.block_on(async move {
        let con = Client::connect("127.0.0.1:4222").await;
        println!("conncted");

        let cmd = std::env::args().nth(1).unwrap();

        match cmd.as_str() {
            "sub" => subscribe(con).await,
            "pub" => publish(con).await,
            _ => panic!("unknown command"),
        }
    })
}

async fn publish(mut nc: nats_async::Client) {
    let now = Instant::now();

    for _i in 0..ITERATIONS {
        nc.publish("events.data", b"foo").await;
    }
    nc.flush().await;
    println!("published in {:?}", now.elapsed());
}

async fn subscribe(mut nc: nats_async::Client) {
    println!("starting subscription");
    let sub = nc.subscribe("events.>").await;
    let mut now = Instant::now();
    for i in 0..ITERATIONS {
        if i == 0 {
            now = Instant::now();
        }
        sub.recv.recv_async().await.unwrap();
    }
    println!("consumed messages in {:?}", now.elapsed());
}
