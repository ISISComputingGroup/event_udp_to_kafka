use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct EventUdpToKafkaConfig {
    /// Ip address and port to bind UDP socket to
    /// e.g. 192.168.1.1:12345
    udp_bind_addr: String,

    /// UDP recieve buffer size. Should be at least as large as the largest
    /// single UDP datagram which will be received.
    udp_buffer_size: Option<usize>,

    /// Kafka topic to send the data to
    dest_kafka_topic: String,

    /// Filepath to the wiring configuration file (csv)
    wiring_csv_path: String,

    /// IP and port on which to bind the metrics server.
    /// Example: `127.0.0.1:8484`
    metrics_bind_addr: String,

    /// Map of Kafka producer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    kafka_producer: HashMap<String, String>,
}

impl EventUdpToKafkaConfig {
    pub fn udp_bind_addr(&self) -> &str {
        &self.udp_bind_addr
    }

    pub fn udp_buffer_size(&self) -> usize {
        self.udp_buffer_size.unwrap_or(9000)
    }

    pub fn dest_kafka_topic(&self) -> &str {
        &self.dest_kafka_topic
    }

    pub fn wiring_csv_path(&self) -> &str {
        &self.wiring_csv_path
    }

    pub fn metrics_bind_addr(&self) -> &str {
        &self.metrics_bind_addr
    }

    pub fn kafka_producer_settings(&self) -> &HashMap<String, String> {
        &self.kafka_producer
    }
}
