use serde::Deserialize;
use std::collections::HashMap;
use std::net::IpAddr;
#[cfg(test)]
use std::net::Ipv4Addr;

#[derive(Debug, Deserialize)]
pub struct EventUdpToKafkaConfig {
    /// Ip address and port to bind UDP socket to
    /// e.g. 192.168.1.1:12345
    pub udp_bind_addr: String,

    /// UDP recieve buffer size. Should be at least as large as the largest
    /// single UDP datagram which will be received.
    pub udp_buffer_size: Option<usize>,

    /// Scaling factor to convert the 8-bit 'raw' PPP signal
    /// into uAh per frame
    pub raw_to_uah_scaling: Option<f64>,

    /// Kafka topic to send the data to
    pub dest_kafka_topic: String,

    /// IP and port on which to bind the metrics server.
    /// Example: `127.0.0.1:8484`
    pub metrics_bind_addr: String,

    /// IP of the streaming control board.
    pub streaming_control_board_ip: IpAddr,

    /// Map of Kafka producer configuration properties. Values should be provided as strings.
    /// All properties are passed through to `librdkafka`.
    pub kafka_producer: HashMap<String, String>,
}

impl EventUdpToKafkaConfig {
    pub fn udp_buffer_size(&self) -> usize {
        self.udp_buffer_size.unwrap_or(9000)
    }

    pub fn raw_to_uah_scaling(&self) -> f64 {
        self.raw_to_uah_scaling.unwrap_or(1.738e-6)
    }

    #[cfg(test)]
    pub fn make_default_config() -> EventUdpToKafkaConfig {
        EventUdpToKafkaConfig {
            udp_bind_addr: "127.0.0.1:1234".to_string(),
            udp_buffer_size: None,
            raw_to_uah_scaling: None,
            dest_kafka_topic: "unittest_events".to_string(),
            metrics_bind_addr: "127.0.0.1:2345".to_string(),
            streaming_control_board_ip: Ipv4Addr::new(127, 0, 0, 1).into(),
            kafka_producer: HashMap::new(),
        }
    }
}
