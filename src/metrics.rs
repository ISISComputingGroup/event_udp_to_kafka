//! Utilities for exposing Prometheus-compatible metrics

use crate::config::EventUdpToKafkaConfig;
use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

pub const INCOMING_UDP_PACKETS: &str = "udp2kafka_incoming_udp_packets";
pub const INCOMING_UDP_PACKETS_BY_IP: &str = "udp2kafka_incoming_udp_packets_by_ip";
pub const INCOMING_UDP_PACKET_SIZE: &str = "udp2kafka_incoming_udp_packet_size";
pub const INCOMING_UDP_PACKET_SIZE_BY_IP: &str = "udp2kafka_incoming_udp_packet_size_by_ip";

pub const INCOMING_UDP_HEADERS: &str = "udp2kafka_incoming_udp_headers";
pub const INCOMING_UDP_PACKET_ERRORS: &str = "udp2kafka_incoming_udp_packet_errors";
pub const INCOMING_UDP_NO_HEADER_FOUND: &str = "udp2kafka_incoming_udp_no_header_found";
pub const INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT: &str =
    "udp2kafka_incoming_udp_invalid_header_length_too_short";
pub const INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG: &str =
    "udp2kafka_incoming_udp_invalid_header_length_too_long";

pub const PROCESSING_ERRORS: &str = "udp2kafka_processing_errors";
pub const PROCESSING_ERRORS_BY_IP: &str = "udp2kafka_processing_errors_by_ip";
pub const PROCESSING_TIME: &str = "udp2kafka_processing_time";

pub const STREAMING_CONTROL_BOARD_FRAMES: &str = "udp2kafka_streaming_control_board_frames";
pub const NEUTRON_EVENTS: &str = "udp2kafka_neutron_events";
pub const INVALID_NEUTRON_EVENTS: &str = "udp2kafka_invalid_neutron_events";

pub const OUTGOING_KAFKA_PRODUCE_ERRORS: &str = "udp2kafka_outgoing_kafka_production_errors";
pub const OUTGOING_KAFKA_MESSAGES: &str = "udp2kafka_outgoing_kafka_messages";
pub const OUTGOING_KAFKA_MESSAGE_SIZE: &str = "udp2kafka_outgoing_kafka_message_size";

pub const KAFKA_REPLYQ: &str = "udp2kafka_kafka_replyq";
pub const KAFKA_MSG_CNT: &str = "udp2kafka_kafka_msg_cnt";
pub const KAFKA_MSG_MAX: &str = "udp2kafka_kafka_msg_max";
pub const KAFKA_MSG_SIZE: &str = "udp2kafka_kafka_msg_size";
pub const KAFKA_MSG_SIZE_MAX: &str = "udp2kafka_kafka_msg_size_max";
pub const KAFKA_TX: &str = "udp2kafka_kafka_tx";
pub const KAFKA_TX_BYTES: &str = "udp2kafka_kafka_tx_bytes";
pub const KAFKA_RX: &str = "udp2kafka_kafka_rx";
pub const KAFKA_RX_BYTES: &str = "udp2kafka_kafka_rx_bytes";
pub const KAFKA_FAILED_DELIVERIES: &str = "udp2kafka_kafka_failed_deliveries";

pub fn initialize_metrics(config: &EventUdpToKafkaConfig) -> Result<(), String> {
    let builder = PrometheusBuilder::new()
        .with_recommended_naming(true)
        .with_http_listener(
            config
                .metrics_bind_addr
                .parse::<SocketAddr>()
                .map_err(|e| e.to_string())?,
        );

    builder.install().map_err(|e| e.to_string())?;

    describe_counter!(
        INCOMING_UDP_PACKETS,
        Unit::Count,
        "Total UDP packets received"
    );
    counter!(INCOMING_UDP_PACKETS).absolute(0);

    describe_counter!(
        INCOMING_UDP_PACKETS_BY_IP,
        Unit::Count,
        "Total UDP packets received from a specific IP address"
    );

    describe_counter!(
        INCOMING_UDP_PACKET_SIZE,
        Unit::Bytes,
        "Size of total UDP packets received"
    );
    counter!(INCOMING_UDP_PACKET_SIZE).absolute(0);

    describe_counter!(
        INCOMING_UDP_PACKET_SIZE_BY_IP,
        Unit::Bytes,
        "Size of total UDP packets received from a specific IP address"
    );

    describe_counter!(INCOMING_UDP_HEADERS, Unit::Count, "Incoming UDP headers.");

    describe_counter!(
        INCOMING_UDP_PACKET_ERRORS,
        Unit::Count,
        "Number of errors returned from UDP socket recvfrom()"
    );
    counter!(INCOMING_UDP_PACKET_ERRORS).absolute(0);

    describe_counter!(
        INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT,
        Unit::Count,
        "Number of UDP headers that contained an length declaration shorter than a header"
    );
    counter!(INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT).absolute(0);

    describe_counter!(
        INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG,
        Unit::Count,
        "Number of UDP headers that contained an length declaration longer than the remaining content"
    );
    counter!(INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG).absolute(0);

    describe_counter!(
        STREAMING_CONTROL_BOARD_FRAMES,
        Unit::Count,
        "Number of frame headers received from streaming control board."
    );
    counter!(STREAMING_CONTROL_BOARD_FRAMES).absolute(0);

    describe_counter!(PROCESSING_ERRORS, Unit::Count, "Message processing errors.");
    counter!(PROCESSING_ERRORS).absolute(0);

    describe_counter!(
        PROCESSING_ERRORS_BY_IP,
        Unit::Count,
        "Message processing errors from a specific IP address."
    );

    describe_histogram!(
        PROCESSING_TIME,
        Unit::Seconds,
        "Processing time for each UDP packet."
    );

    describe_counter!(NEUTRON_EVENTS, Unit::Count, "Number of neutron events");
    counter!(NEUTRON_EVENTS).absolute(0);

    describe_counter!(
        INVALID_NEUTRON_EVENTS,
        Unit::Count,
        "Number of invalid (dropped) neutron events which were part of a valid packet"
    );
    counter!(INVALID_NEUTRON_EVENTS).absolute(0);

    describe_counter!(
        OUTGOING_KAFKA_PRODUCE_ERRORS,
        Unit::Count,
        "Number of errors producing a message to Kafka"
    );
    counter!(OUTGOING_KAFKA_PRODUCE_ERRORS).absolute(0);

    describe_counter!(
        OUTGOING_KAFKA_MESSAGES,
        Unit::Count,
        "Number of messages successfully enqueued for sending to Kafka"
    );
    counter!(OUTGOING_KAFKA_MESSAGES).absolute(0);

    describe_counter!(
        OUTGOING_KAFKA_MESSAGE_SIZE,
        Unit::Bytes,
        "Size of messages successfully enqueued for sending to Kafka"
    );
    counter!(OUTGOING_KAFKA_MESSAGE_SIZE).absolute(0);

    describe_gauge!(
        KAFKA_REPLYQ,
        Unit::Count,
        "Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()"
    );
    gauge!(KAFKA_REPLYQ).set(0);

    describe_gauge!(
        KAFKA_MSG_CNT,
        Unit::Count,
        "Current number of messages in producer queues"
    );
    gauge!(KAFKA_MSG_CNT).set(0);

    describe_gauge!(
        KAFKA_MSG_MAX,
        Unit::Count,
        "Threshold: maximum number of messages allowed allowed on the producer queues"
    );
    gauge!(KAFKA_MSG_MAX).set(0);

    describe_gauge!(
        KAFKA_MSG_SIZE_MAX,
        Unit::Bytes,
        "Threshold: maximum total size of messages allowed on the producer queues"
    );
    gauge!(KAFKA_MSG_SIZE_MAX).set(0);

    describe_gauge!(
        KAFKA_TX,
        Unit::Count,
        "Total number of requests sent to Kafka brokers"
    );
    gauge!(KAFKA_TX).set(0);

    describe_gauge!(
        KAFKA_TX_BYTES,
        Unit::Bytes,
        "Total number of bytes transmitted to Kafka brokers"
    );
    gauge!(KAFKA_TX_BYTES).set(0);

    describe_gauge!(
        KAFKA_RX,
        Unit::Count,
        "Total number of responses received from Kafka brokers"
    );
    gauge!(KAFKA_RX).set(0);

    describe_gauge!(
        KAFKA_RX_BYTES,
        Unit::Bytes,
        "Total number of bytes received from Kafka brokers"
    );
    gauge!(KAFKA_RX_BYTES).set(0);

    describe_counter!(
        KAFKA_FAILED_DELIVERIES,
        Unit::Count,
        "Number of deliveries to Kafka broker which failed"
    );
    counter!(KAFKA_FAILED_DELIVERIES).absolute(0);

    Ok(())
}
