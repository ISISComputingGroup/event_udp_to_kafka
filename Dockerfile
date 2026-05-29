FROM rust:latest AS builder
RUN apt-get update && apt-get install -y cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/event_udp_to_kafka
COPY . .
RUN cargo install --path .

FROM debian:stable-slim
RUN apt-get update && apt-get install -y libcurl4-openssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/event-udp-to-kafka /usr/local/bin/event-udp-to-kafka
COPY ./src/config/* .
CMD ["event-udp-to-kafka"]
