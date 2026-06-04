//! Utilities for converting UDP bytes to flatbuffers-encoded messages.

use crate::WiringConfigRecord;

use crate::metrics::{
    INCOMING_UDP_HEADERS, INCOMING_UDP_NO_HEADER_FOUND, INCOMING_UDP_PACKET_SIZE,
    INCOMING_UDP_PACKETS, NEUTRON_EVENTS, PROCESSING_ERRORS,
};
use crate::udp_message::{UdpMessageView, UdpPacketType};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use log::{error, warn};
use metrics::counter;

/// Process a byte-slice of UDP data to the corresponding flatbuffers messages.
///
/// Input: binary data from a UDP packet (which may contain multiple event packets)
pub fn process_udp_bytes_to_kafka<F>(
    fbb: &mut FlatBufferBuilder,
    udp_packet: &[u8],
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
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
                process_neutron_frame(fbb, frame, src_ip, wiring_config, &mut sink)
            }
            _ => Err("unimplemented packet type".to_owned()),
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
        if let Some(header_view) = udp.get(offset..).and_then(UdpMessageView::new) {
            offset += header_view.total_length_bytes();
            result.push(header_view);
        } else {
            // We didn't find a valid header where we were expecting one.
            // This should not happen in normal operation.
            error!("No header found at offset {}", offset);
            counter!(INCOMING_UDP_NO_HEADER_FOUND).increment(1);
            break;
        }
    }
    result
}

/// Convert a neutron data packet to flatbuffers messages.
fn process_neutron_frame<F>(
    fbb: &mut FlatBufferBuilder,
    message: UdpMessageView,
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    sink: F,
) -> Result<(), String>
where
    F: FnMut(&[u8]),
{
    let nanoseconds_since_epoch =
        message
            .gps_time()
            .nanoseconds_since_epoch()
            .ok_or_else(|| {
                format!(
                    "Invalid frame header; timestamp {:?} is invalid",
                    message.gps_time()
                )
            })?;

    let event_data = message.data_bytes();

    if !event_data.len().is_multiple_of(8) {
        return Err("Event data is not a multiple of pairs of 4-byte words".to_owned());
    }

    let packet_config = wiring_config
        .iter()
        .filter(|line| line.streaming_ip == src_ip)
        .collect::<Vec<&WiringConfigRecord>>();

    let first_packet_config = packet_config.first().ok_or("no packet config")?;

    // do we want this for LVDS or have if 1, else if greater than 1?
    let (tofs, det_ids) = match first_packet_config.brd_type.as_str() {
        "PC3634M1S" => process_pc3634m1s_events(event_data, first_packet_config), // 128CH LVDS Card
        "PC3544MS" => process_pc3544ms_events(event_data, &packet_config),        // MADC PB
        "PC3877MS" => process_pc3877ms_events(event_data, first_packet_config), // WLSF Streaming Electronics
        _ => {
            return Err("Unknown board type".to_owned());
        }
    };

    if tofs.is_empty() {
        // An empty frame is ok; we don't need to emit an ev44 for it.
        return Ok(());
    }

    counter!(NEUTRON_EVENTS).increment(tofs.len() as u64);

    send_ev44(
        fbb,
        "rust_proc",
        0,
        nanoseconds_since_epoch,
        &tofs,
        &det_ids,
        sink,
    );
    Ok(())
}

/// Extract vectors of (time_of_flight, pixel_id) from pc3544ms event data.
fn process_pc3544ms_events(
    event_data: &[u8],
    packet_config: &[&WiringConfigRecord],
) -> (Vec<i32>, Vec<i32>) {
    match packet_config[0].packet_type.as_str() {
        "Position" => {
            event_data
                .as_chunks::<8>()
                .0
                .iter()
                .filter_map(|event| {
                    let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                    let event_position =
                        u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFF;

                    if let Some(channel_config) = packet_config.iter().find(|c| c.ch == channel) {
                        let detector_id = (event_position
                            / (4096 / channel_config.mantid_detector_id_length))
                            + channel_config.mantid_detector_id_start;

                        let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;

                        Some((tof as i32, detector_id as i32))
                    } else {
                        None
                    }
                })
                .unzip()
        }
        "PulseHeight" => {
            event_data
                .as_chunks::<8>()
                .0
                .iter()
                .filter_map(|event| {
                    let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                    let pulse_height =
                        (u32::from_be_bytes(event[4..8].try_into().ok()?) >> 12) & 0xFFF;

                    if let Some(channel_config) = packet_config.iter().find(|c| c.ch == channel) {
                        let detector_id = (pulse_height
                            / (4096 / channel_config.mantid_detector_id_length))
                            + channel_config.mantid_detector_id_start;
                        let event_tof = u32::from_be_bytes(event[0..4].try_into().ok()?) & 0xFFFFFF;

                        Some((event_tof as i32, detector_id as i32))
                    } else {
                        None
                    }
                })
                .unzip()
        }
        _ => {
            error!("Unable to process events: unknown stream type in config");
            (vec![], vec![])
        }
    }
}

/// Extract vectors of (time_of_flight, pixel_id) from pc3634m1s event data.
fn process_pc3634m1s_events(
    event_data: &[u8],
    packet_config: &WiringConfigRecord,
) -> (Vec<i32>, Vec<i32>) {
    match packet_config.packet_type.as_str() {
        "DIM_OUT" => event_data
            .as_chunks::<8>()
            .0
            .iter()
            .map(|event| {
                let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap());
                val += packet_config.mantid_detector_id_start;
                (tof as i32, val as i32)
            })
            .unzip(),
        _ => {
            error!("Unable to process events: unknown stream type in config");
            (vec![], vec![])
        }
    }
}

/// Extract vectors of (time_of_flight, pixel_id) from pc3877ms event data.
fn process_pc3877ms_events(
    event_data: &[u8],
    packet_config: &WiringConfigRecord,
) -> (Vec<i32>, Vec<i32>) {
    const CLOCK_TICKS_TO_NS: u32 = 20;

    match packet_config.packet_type.as_str() {
        "Position" => event_data
            .as_chunks::<8>()
            .0
            .iter()
            .map(|event| {
                let mut tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                tof *= CLOCK_TICKS_TO_NS;

                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFFF;
                val += packet_config.mantid_detector_id_start;

                (tof as i32, val as i32)
            })
            .unzip(),
        "PulseHeight" => event_data
            .as_chunks::<8>()
            .0
            .iter()
            .map(|event| {
                let mut val = (u32::from_be_bytes(event[4..8].try_into().unwrap()) >> 16) & 0xFFF;
                val += packet_config.mantid_detector_id_start;

                let tof = (u32::from_be_bytes(event[0..4].try_into().unwrap())) & 0xFFFFFF;

                (tof as i32, val as i32)
            })
            .unzip(),
        _ => {
            error!("Unable to process events: unknown stream type in config");
            (vec![], vec![])
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{TESTING_TIMESTAMP_NS_SINCE_EPOCH, make_raw_neutron_udp_header};
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};

    /// tof = 456000 ns
    /// val = 123
    fn make_pc3877ms_event() -> Vec<u8> {
        vec![
            0xFF, 0, 89, 16, // 20ns (scaling) * (89 * 256 + 16) = 456000ns
            0xFF, 0xFF, 0, 123, // Position 123
        ]
    }

    fn pc3877ms_wiring() -> Vec<WiringConfigRecord> {
        vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            brd_type: "PC3877MS".to_owned(),
            packet_type: "Position".to_owned(),
            sw_pos: 0,
            streaming_ip: "192.168.1.1".to_owned(),
            ch: 0,
            mantid_detector_id_start: 0,
            mantid_detector_id_length: 1,
            comment: "".to_owned(),
        }]
    }

    #[test]
    fn test_process_pc3877ms_events() {
        let num_events = 2;
        let mut raw_data = make_raw_neutron_udp_header(num_events, 123);

        raw_data.extend_from_slice(&make_pc3877ms_event());
        raw_data.extend_from_slice(&make_pc3877ms_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &pc3877ms_wiring(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(msg.time_of_flight().unwrap().get(0), 456000);
                assert_eq!(msg.time_of_flight().unwrap().get(1), 456000);

                assert_eq!(msg.pixel_id().unwrap().get(0), 123);
                assert_eq!(msg.pixel_id().unwrap().get(1), 123);
            }
            _ => panic!("Could not deserialize"),
        }
    }

    #[test]
    fn test_process_pc3877ms_events_with_trailing_padding_zeros() {
        let mut raw_data = make_raw_neutron_udp_header(2, 123);

        raw_data.extend_from_slice(&make_pc3877ms_event());
        raw_data.extend_from_slice(&make_pc3877ms_event());

        // Trailing padding zeros
        raw_data.extend_from_slice(&[0; 1001]);

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &pc3877ms_wiring(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(msg.time_of_flight().unwrap().get(0), 456000);
                assert_eq!(msg.time_of_flight().unwrap().get(1), 456000);

                assert_eq!(msg.pixel_id().unwrap().get(0), 123);
                assert_eq!(msg.pixel_id().unwrap().get(1), 123);
            }
            _ => panic!("Could not deserialize"),
        }
    }

    #[test]
    fn test_process_multiple_pc3877ms_events() {
        let mut raw_data = make_raw_neutron_udp_header(2, 12);

        raw_data.extend_from_slice(&make_pc3877ms_event());
        raw_data.extend_from_slice(&make_pc3877ms_event());

        raw_data.extend_from_slice(&make_raw_neutron_udp_header(2, 34));

        raw_data.extend_from_slice(&make_pc3877ms_event());
        raw_data.extend_from_slice(&make_pc3877ms_event());

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &pc3877ms_wiring(),
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 2);
        for msg in msgs {
            match deserialize_message(&msg) {
                Ok(DeserializedMessage::EventDataEv44(msg)) => {
                    assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                    assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                    assert_eq!(msg.time_of_flight().unwrap().get(0), 456000);
                    assert_eq!(msg.time_of_flight().unwrap().get(1), 456000);

                    assert_eq!(msg.pixel_id().unwrap().get(0), 123);
                    assert_eq!(msg.pixel_id().unwrap().get(1), 123);
                }
                _ => panic!("Could not deserialize msg 1"),
            }
        }
    }

    /// tof = 456000 ns
    /// channel 2, position 1234
    fn make_pc3544ms_event() -> Vec<u8> {
        vec![
            0xFF, 0x06, 0xF5, 0x40, // 456000ns
            0b11101011, 0xFF, 0xF4, 0xD2, // Channel 2 (b010), 0x4D2 = position 1234
        ]
    }

    #[test]
    fn test_process_pc3544ms_events() {
        let num_events = 2;
        let mut raw_data = make_raw_neutron_udp_header(num_events, 123);

        raw_data.extend_from_slice(&make_pc3544ms_event());
        raw_data.extend_from_slice(&make_pc3544ms_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let wiring_config = vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            brd_type: "PC3544MS".to_owned(),
            packet_type: "Position".to_owned(),
            sw_pos: 0,
            streaming_ip: "192.168.1.1".to_owned(),
            ch: 2,
            mantid_detector_id_start: 11103001,
            mantid_detector_id_length: 256,
            comment: "".to_owned(),
        }];
        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &wiring_config,
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(msg.time_of_flight().unwrap().get(0), 456000);
                assert_eq!(msg.time_of_flight().unwrap().get(1), 456000);

                assert_eq!(msg.pixel_id().unwrap().get(0), 11103001 + 77);
                assert_eq!(msg.pixel_id().unwrap().get(1), 11103001 + 77);
            }
            _ => panic!("Could not deserialize"),
        }
    }

    /// tof = 456000 ns
    /// detector ID = 123456789
    fn make_pc3634m1s_event() -> Vec<u8> {
        vec![
            0xFF, 0x06, 0xF5, 0x40, // 456000ns
            0x07, 0x5B, 0xCD, 0x15, // Detector ID = 123456789
        ]
    }

    #[test]
    fn test_process_pc3634m1s_events() {
        let num_events = 2;
        let mut raw_data = make_raw_neutron_udp_header(num_events, 123);

        raw_data.extend_from_slice(&make_pc3634m1s_event());
        raw_data.extend_from_slice(&make_pc3634m1s_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let wiring_config = vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            brd_type: "PC3634M1S".to_owned(),
            packet_type: "DIM_OUT".to_owned(),
            sw_pos: 0,
            streaming_ip: "192.168.1.1".to_owned(),
            ch: 0,
            mantid_detector_id_start: 0,
            mantid_detector_id_length: 1,
            comment: "".to_owned(),
        }];

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &wiring_config,
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(
                    msg.reference_time().get(0) as u64,
                    TESTING_TIMESTAMP_NS_SINCE_EPOCH
                );
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);

                assert_eq!(msg.time_of_flight().unwrap().get(0), 456000);
                assert_eq!(msg.time_of_flight().unwrap().get(1), 456000);

                assert_eq!(msg.pixel_id().unwrap().get(0), 123456789);
                assert_eq!(msg.pixel_id().unwrap().get(1), 123456789);
            }
            _ => panic!("Could not deserialize"),
        }
    }

    #[test]
    fn test_process_empty_events() {
        let raw_data = make_raw_neutron_udp_header(0, 123);
        let wiring_config = vec![];

        let mut msgs = vec![];
        process_udp_bytes_to_kafka(
            &mut FlatBufferBuilder::new(),
            &raw_data,
            "192.168.1.1",
            &wiring_config,
            |msg| {
                msgs.push(msg.to_vec());
            },
        );

        // No ev44s should have been emitted - no events to emit
        assert_eq!(msgs.len(), 0);
    }
}
