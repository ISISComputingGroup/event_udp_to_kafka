use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct EventUdpToKafkaConfig {
    /// UDP port to bind to
    pub port: u32,

    /// Ip address of the host (the IP address on which to bind a UDP port)
    pub host_ip: String,

    /// Kafka topic to send the data to
    pub dest_kafka_topic: String,

    /// Kafka topic to get data from
    pub src_kafka_topic: String,

    /// Script operating mode
    /// 0 - Consume UDP packets from Kafka SRC topic, process and send back to kafka
    /// 1 - Gets UDP packets from a local socket binding, processes and kafkas
    /// This script is mainly designed to function in the Kafka-> Kafka configuration
    /// With the UDP->Kafka rust buffering via kafka. This gives some failover, and potential throughput options
    ///
    mode: Option<u32>,

    /// Filepath to the wiring configuration file (csv)
    pub wiring_csv_path: String,

    /// IP and port on which to bind the metrics server.
    /// Example: `127.0.0.1:8484`
    pub metrics_bind_addr: String,

    /// Map of Kafka producer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    pub kafka_producer: HashMap<String, String>,

    /// Map of Kafka consumer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    pub kafka_consumer: HashMap<String, String>,
}

impl EventUdpToKafkaConfig {
    pub fn mode(&self) -> u32 {
        self.mode.unwrap_or(0)
    }
}
