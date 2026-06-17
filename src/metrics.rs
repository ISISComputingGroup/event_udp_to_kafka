//! Utilities for exposing Prometheus-compatible metrics

use crate::config::EventUdpToKafkaConfig;
use crate::udp_message::UdpPacketType;
use metrics::{Unit, counter, describe_counter, describe_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use strum::IntoEnumIterator;

pub const INCOMING_UDP_PACKETS: &str = "udp2kafka_incoming_udp_packets";
pub const INCOMING_UDP_PACKET_SIZE: &str = "udp2kafka_incoming_udp_packet_size";

pub const INCOMING_UDP_HEADERS: &str = "udp2kafka_incoming_udp_headers";
pub const INCOMING_UDP_PACKET_ERRORS: &str = "udp2kafka_incoming_udp_packet_errors";
pub const INCOMING_UDP_NO_HEADER_FOUND: &str = "udp2kafka_incoming_udp_no_header_found";
pub const INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT: &str =
    "udp2kafka_incoming_udp_invalid_header_length_too_short";
pub const INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG: &str =
    "udp2kafka_incoming_udp_invalid_header_length_too_long";

pub const PROCESSING_ERRORS: &str = "udp2kafka_processing_errors";
pub const PROCESSING_TIME: &str = "udp2kafka_processing_time";

pub const NEUTRON_EVENTS: &str = "udp2kafka_neutron_events";

pub const OUTGOING_KAFKA_PRODUCE_ERRORS: &str = "udp2kafka_outgoing_kafka_production_errors";
pub const OUTGOING_KAFKA_MESSAGES: &str = "udp2kafka_outgoing_kafka_messages";
pub const OUTGOING_KAFKA_MESSAGE_SIZE: &str = "udp2kafka_outgoing_kafka_message_size";

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
        INCOMING_UDP_PACKET_SIZE,
        Unit::Bytes,
        "Size of total UDP packets received"
    );
    counter!(INCOMING_UDP_PACKET_SIZE).absolute(0);

    describe_counter!(INCOMING_UDP_HEADERS, Unit::Count, "Incoming UDP headers.");

    for typ in UdpPacketType::iter() {
        counter!(INCOMING_UDP_HEADERS, "type" => typ.as_prometheus_label()).absolute(0);
    }

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

    describe_counter!(PROCESSING_ERRORS, Unit::Count, "Message processing errors.");

    for typ in UdpPacketType::iter() {
        counter!(PROCESSING_ERRORS, "type" => typ.as_prometheus_label()).absolute(0);
    }

    describe_histogram!(
        PROCESSING_TIME,
        Unit::Seconds,
        "Processing time for each UDP packet."
    );

    describe_counter!(NEUTRON_EVENTS, Unit::Count, "Number of neutron events");
    counter!(NEUTRON_EVENTS).absolute(0);

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

    Ok(())
}
