use nats_sync::Client;
use std::time::Instant;

fn main() {
    let mut con = Client::connect();
    println!("conncted");
    let now = Instant::now();

    for _ in 0..1_000_000_000 {
        con.publish("events.data", b"foo");
    }
    con.flush();
    println!("elapsed: {:?}", now.elapsed());
    con.shutdown();
}
