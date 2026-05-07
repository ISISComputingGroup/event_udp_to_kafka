# Rust Data Stream Processor

This rust project takes streamed UDP data, processes it (based on a wiring config), flatbuffers it and sends it to the Kafka Events topic.

This code is intended to be run from a docker container, reading the UDP traffic from a Kafka topic of the forwarded traffic.
This forwarded traffic is JSON'ed using the stream forwarder, with both the src_ip and packet data.

For detailed documentation, run `cargo doc --no-deps --open`.
