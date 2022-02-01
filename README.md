# nats-publish-async-poc.rs

POC for performance capabilities of `tokio` `TCPStream` in context of `NATS` publisher.

Tokio client:  
`cargo run --bin tokio-client --release`

async-std client:  
`cargo run --bin async-std-client --release`

sync client:  
`cargo run --bin sync-client --release`

Crate `nats-async` contains async tokio client.  
Crate `nats-sync` is a sync wrapper around tokio.

Async-std leverages `async-std` feature `tokio1` being a compatibility mode.
