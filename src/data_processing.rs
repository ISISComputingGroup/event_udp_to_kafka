//! Utilities for converting UDP bytes to flatbuffers-encoded messages.

use anyhow::{Context, anyhow};
use std::net::IpAddr;

use crate::config::EventUdpToKafkaConfig;
use crate::metrics::{
    INCOMING_UDP_HEADERS, INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG,
    INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT, INCOMING_UDP_PACKET_SIZE,
    INCOMING_UDP_PACKETS, NEUTRON_EVENTS, PROCESSING_ERRORS,
};
use crate::packet_formats::parse_board_data;
use crate::udp_message::{InvalidMessageReason, UdpMessageView, UdpPacketType};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use log::warn;
use metrics::counter;

/// Process a byte-slice of UDP data to the corresponding flatbuffers messages.
///
/// Input: binary data from a UDP packet, which may contain multiple event packets
pub fn process_udp_bytes_to_kafka<F>(
    fbb: &mut FlatBufferBuilder,
    udp_packet: &[u8],
    src_ip: &IpAddr,
    config: &EventUdpToKafkaConfig,
    mut sink: F,
) where
    F: FnMut(&[u8]),
{
    counter!(INCOMING_UDP_PACKETS).increment(1);
    counter!(INCOMING_UDP_PACKET_SIZE).increment(udp_packet.len() as u64);

    let frames = packet_to_frames(udp_packet);

    for frame in frames {
        let packet_type = frame.packet_type();

        counter!(INCOMING_UDP_HEADERS, "type" => packet_type.as_prometheus_label()).increment(1);

        let result = match packet_type {
            UdpPacketType::NeutronData => {
                process_neutron_frame(fbb, frame, src_ip, config, &mut sink)
            }
            _ => Err(anyhow!("unimplemented packet type")),
        };

        if let Err(e) = result {
            warn!(
                "Error processing {} packet: {}",
                packet_type.as_prometheus_label(),
                e
            );
            counter!(PROCESSING_ERRORS, "type" => packet_type.as_prometheus_label()).increment(1);
        }
    }
}

/// Extract individual UDP messages from UDP data which may contain multiple messages.
///
/// Input: a slice of binary UDP data
///
/// Output: a vector of UDP messages (views onto the underlying byte-slice)
fn packet_to_frames(udp: &[u8]) -> Vec<UdpMessageView<'_>> {
    let mut result = vec![];
    let mut offset = 0;

    while offset < udp.len() {
        if let Some(content) = udp.get(offset..) {
            match UdpMessageView::new(content) {
                Ok(view) => {
                    offset += view.total_length_bytes();
                    result.push(view);
                }
                Err(InvalidMessageReason::ContentTooShort)
                | Err(InvalidMessageReason::MissingHeaderMarker) => {
                    // Likely trailing zero padding. This is not an *error*, but indicates
                    // that there are no more messages in this UDP packet.
                    break;
                }
                Err(InvalidMessageReason::DeclaredLengthTooShort(length)) => {
                    warn!(
                        "Packet with invalid declared length {} (shorter than length of a header)",
                        length
                    );
                    counter!(INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_SHORT).increment(1);
                    break;
                }
                Err(InvalidMessageReason::DeclaredLengthTooLong(length)) => {
                    warn!(
                        "Packet with invalid declared length {} on content buffer of length {}",
                        length,
                        content.len()
                    );
                    counter!(INCOMING_UDP_INVALID_HEADER_DECLARED_LENGTH_TOO_LONG).increment(1);
                    break;
                }
            }
        } else {
            break;
        }
    }
    result
}

/// Convert a neutron data packet to flatbuffers messages.
fn process_neutron_frame<F>(
    fbb: &mut FlatBufferBuilder,
    message: UdpMessageView,
    src_ip: &IpAddr,
    config: &EventUdpToKafkaConfig,
    mut sink: F,
) -> anyhow::Result<()>
where
    F: FnMut(&[u8]),
{
    let nanoseconds_since_epoch =
        message
            .gps_time()
            .nanoseconds_since_epoch()
            .with_context(|| {
                format!(
                    "Invalid frame header; timestamp {:?} is invalid",
                    message.gps_time()
                )
            })?;

    let events = parse_board_data(
        message.packet_format_code(),
        message.board_specific_parameters(),
        message.data_bytes(),
    )?;

    let is_streaming_control_board = src_ip == &config.streaming_control_board_ip;

    // The period number and PPP only ever come from the streaming control board packets.
    //
    // This is to prevent duplicate frames appearing in downstream consumers, such as
    // `kafka_event_aggregator`, if one of these messages was delayed - either at the
    // streaming/UDP level, or during transmission via the `_rawEvents` Kafka stream.
    //
    // Vetoes are always transmitted, as some vetoes could be generated locally on each
    // detector board. Downstream consumers, such as `kafka_event_aggregator`, will OR
    // together vetoes from the streaming control board and from each individual detector
    // module, when assembling a frame.
    send_pu00(
        fbb,
        "event_udp_to_kafka",
        0,
        nanoseconds_since_epoch,
        Some(message.vetoes()),
        is_streaming_control_board.then_some(message.period_number().into()),
        is_streaming_control_board.then_some(message.ppp_per_frame(config)),
        &mut sink,
    );

    if !events.is_empty() {
        counter!(NEUTRON_EVENTS).increment(events.len() as u64);

        send_ev44(
            fbb,
            "event_udp_to_kafka",
            0,
            nanoseconds_since_epoch,
            events.time_of_flight(),
            events.pixel_id(),
            &mut sink,
        );
    }
    Ok(())
}

/// Encode data to ev44 format
fn send_ev44<F>(
    bldr: &mut FlatBufferBuilder,
    source_name: &str,
    message_id: u64,
    pulse_time: u64,
    tofs: &[i32],
    det_ids: &[i32],
    mut sink: F,
) where
    F: FnMut(&[u8]),
{
    bldr.reset();

    let args = Event44MessageArgs {
        source_name: Some(bldr.create_string(source_name)),
        message_id: message_id as i64,
        reference_time: Some(bldr.create_vector(&[pulse_time as i64])),
        reference_time_index: Some(bldr.create_vector(&[0])),
        time_of_flight: Some(bldr.create_vector(tofs)),
        pixel_id: Some(bldr.create_vector(det_ids)),
    };

    let ev44_offset = Event44Message::create(bldr, &args);
    finish_event_44_message_buffer(bldr, ev44_offset);
    sink(bldr.finished_data());
}

#[allow(clippy::too_many_arguments)]
fn send_pu00<F>(
    bldr: &mut FlatBufferBuilder,
    source_name: &str,
    message_id: u64,
    pulse_time: u64,
    vetos: Option<u32>,
    period_number: Option<u32>,
    proton_charge: Option<f32>,
    mut sink: F,
) where
    F: FnMut(&[u8]),
{
    bldr.reset();

    let args = Pu00MessageArgs {
        source_name: Some(bldr.create_string(source_name)),
        message_id: message_id as i64,
        reference_time: pulse_time as i64,
        vetos,
        period_number,
        proton_charge,
    };

    let pu00_offset = Pu00Message::create(bldr, &args);
    finish_pu_00_message_buffer(bldr, pu00_offset);
    sink(bldr.finished_data());
}

#[cfg(test)]
mod tests {
    use crate::config::EventUdpToKafkaConfig;
    use crate::data_processing::process_udp_bytes_to_kafka;
    use crate::packet_formats::TestablePacketFormat;
    use crate::packet_formats::packet_format_1::PacketFormat1;
    use crate::testing::{make_udp_header, make_udp_packet};
    use flatbuffers::FlatBufferBuilder;
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};
    use std::net::Ipv4Addr;

    #[test]
    fn test_process_empty_events() {
        let raw_data = make_udp_header::<PacketFormat1>(0, 123);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            &Ipv4Addr::new(192, 168, 1, 1).into(),
            &EventUdpToKafkaConfig::make_testing_config(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        // pu00 only, no ev44
        assert_eq!(msgs.len(), 1);

        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::PulseMetadataPu00(_)) => {}
            _ => panic!("Incorrect message type"),
        }

    }

    #[test]
    fn test_full_process_events() {
        let num_events = 2;
        let raw_data = make_udp_packet::<PacketFormat1>(num_events, 123);

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let ip = Ipv4Addr::new(192, 168, 1, 1);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            &ip.into(),
            &EventUdpToKafkaConfig::make_testing_config(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 2);  // pu00, ev44
        match deserialize_message(&msgs[1]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(
                    msg.time_of_flight().unwrap().get(0),
                    PacketFormat1::FAKE_EVENT_TOF
                );
                assert_eq!(
                    msg.time_of_flight().unwrap().get(1),
                    PacketFormat1::FAKE_EVENT_TOF
                );

                assert_eq!(
                    msg.pixel_id().unwrap().get(0),
                    PacketFormat1::FAKE_EVENT_PIXEL
                );
                assert_eq!(
                    msg.pixel_id().unwrap().get(1),
                    PacketFormat1::FAKE_EVENT_PIXEL
                );
            }
            _ => panic!("Could not deserialize"),
        }
    }

    #[test]
    fn test_full_process_events_with_trailing_padding_zeros() {
        let mut raw_data = make_udp_packet::<PacketFormat1>(2, 123);

        // Trailing padding zeros
        raw_data.extend_from_slice(&[0; 1001]);

        let ip = Ipv4Addr::new(192, 168, 1, 1);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            &ip.into(),
            &EventUdpToKafkaConfig::make_testing_config(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 2);  // one pu00, one ev44
        match deserialize_message(&msgs[1]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(
                    msg.time_of_flight().unwrap().get(0),
                    PacketFormat1::FAKE_EVENT_TOF
                );
                assert_eq!(
                    msg.time_of_flight().unwrap().get(1),
                    PacketFormat1::FAKE_EVENT_TOF
                );

                assert_eq!(
                    msg.pixel_id().unwrap().get(0),
                    PacketFormat1::FAKE_EVENT_PIXEL
                );
                assert_eq!(
                    msg.pixel_id().unwrap().get(1),
                    PacketFormat1::FAKE_EVENT_PIXEL
                );
            }
            _ => panic!("Could not deserialize"),
        }
    }

    #[test]
    fn test_full_process_multiple_events() {
        let mut raw_data = make_udp_packet::<PacketFormat1>(2, 12);
        raw_data.extend_from_slice(&make_udp_packet::<PacketFormat1>(2, 34));

        let ip = Ipv4Addr::new(192, 168, 1, 1);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            &ip.into(),
            &EventUdpToKafkaConfig::make_testing_config(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 4); // pu00, ev44, pu00, ev44
        for msg in msgs {
            match deserialize_message(&msg) {
                Ok(DeserializedMessage::EventDataEv44(msg)) => {
                    assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                    assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                    assert_eq!(
                        msg.time_of_flight().unwrap().get(0),
                        PacketFormat1::FAKE_EVENT_TOF
                    );
                    assert_eq!(
                        msg.time_of_flight().unwrap().get(1),
                        PacketFormat1::FAKE_EVENT_TOF
                    );

                    assert_eq!(
                        msg.pixel_id().unwrap().get(0),
                        PacketFormat1::FAKE_EVENT_PIXEL
                    );
                    assert_eq!(
                        msg.pixel_id().unwrap().get(1),
                        PacketFormat1::FAKE_EVENT_PIXEL
                    );
                }
                Ok(DeserializedMessage::PulseMetadataPu00(_)) => {}
                _ => panic!("Could not deserialize msg"),
            }
        }
    }
}
