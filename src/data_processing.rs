//! Utilities for converting UDP bytes to flatbuffers-encoded messages.

use crate::WiringConfigRecord;

use crate::udp_message::{HEADER_LEN_BYTES, HEADER_MARKER, UdpMessageView, UdpPacketType};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use itertools::Itertools;
use log::{error, warn};

/// Process a hexed string of UDP data to the corresponding flatbuffers messages.
///
/// Input: hexed string containing the data from a UDP packet (which may contain multiple
/// event packets)
///
/// Output: Vector of flatbuffers-encoded messages to send to Kafka
pub fn process_udp_to_kafka<F>(
    fbb: &mut FlatBufferBuilder,
    udp_hex: &str,
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    sink: F,
) where
    F: FnMut(&[u8]),
{
    process_udp_bytes_to_kafka(
        fbb,
        &hex::decode(udp_hex).expect("Invalid hex"),
        src_ip,
        wiring_config,
        sink,
    )
}

/// Process a byte-slice of UDP data to the corresponding flatbuffers messages.
///
/// Input: binary data from a UDP packet (which may contain multiple event packets)
///
/// Output: Vector of flatbuffers-encoded messages to send to Kafka
pub fn process_udp_bytes_to_kafka<F>(
    fbb: &mut FlatBufferBuilder,
    udp_packet: &[u8],
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    mut sink: F,
) where
    F: FnMut(&[u8]),
{
    // Split into the different frames in the packet
    // Filters any empty frames each time
    let frames = packet_to_frames(udp_packet);

    for frame in frames {
        match frame.packet_type() {
            Some(UdpPacketType::NeutronData) => {
                let result = process_neutron_frame(fbb, frame, src_ip, wiring_config, &mut sink);
                if let Err(e) = result {
                    warn!("Error processing neutron data: {}", e);
                    // todo: metrics
                }
            }
            Some(UdpPacketType::SampleEnvironment) => {
                warn!("Received unimplemented sample environment packet");
            }
            Some(UdpPacketType::VetoFrame) => {
                warn!("Received unimplemented veto packet");
            }
            None => {
                warn!("Received unimplemented packet type");
            }
        }
    }
}

/// Extract individual UDP messages from UDP data which may contain multiple messages.
///
/// Input: a slice of binary UDP data
///
/// Output: a vector of UDP packets; Each UDP message will be complete,
/// i.e. containing the full UDP data for that message including all header bytes
/// Vector will be empty if no frames found
fn packet_to_frames(udp: &[u8]) -> Vec<UdpMessageView<'_>> {
    // Find the byte-offsets of the beginning of frame markers.
    let mut marker_offsets = vec![];
    let mut offset = 0;

    while offset < udp.len() {
        if udp.get(offset..offset + 4) == Some(HEADER_MARKER) {
            marker_offsets.push(offset);
            offset += HEADER_LEN_BYTES;
        }
        offset += 4; // Advance by a 4-byte word each time.
    }

    // Last message goes up to end of data.
    marker_offsets.push(udp.len());

    marker_offsets
        .iter()
        .tuple_windows()
        .filter_map(|(&start, &end)| udp.get(start..end).and_then(UdpMessageView::new))
        .collect()
}

/// Convert a neutron data packet to flatbuffers messages.
///
/// Input: a neutron event UDP packet, header, events, and possibly padding zeros.
/// Output: Vec of Flatbuffers-encoded messages to send to Kafka
fn process_neutron_frame<F>(
    fbb: &mut FlatBufferBuilder,
    message: UdpMessageView,
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    sink: F,
) -> Result<(), &'static str>
where
    F: FnMut(&[u8]),
{
    let nanoseconds_since_epoch = message
        .gps_time()
        .nanoseconds_since_epoch()
        .ok_or("Invalid frame header; timestamp is invalid")?;

    let event_data = message.data_bytes();

    if !event_data.len().is_multiple_of(8) {
        return Err("Event data is not a multiple of pairs of 4-byte words");
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
            return Err("Unknown board type");
        }
    };

    if tofs.is_empty() {
        return Err("No events within frame");
    }

    // Trying with EV44 Packets
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
                .chunks_exact(8)
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
                .chunks_exact(8)
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
            .chunks_exact(8)
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
            .chunks_exact(8)
            .map(|event| {
                let mut tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                tof *= CLOCK_TICKS_TO_NS;

                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFFF;
                val += packet_config.mantid_detector_id_start;

                (tof as i32, val as i32)
            })
            .unzip(),
        "PulseHeight" => event_data
            .chunks_exact(8)
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
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};

    /// A valid timestamp, encoded in the UDP packed format.
    const VALID_TIMESTAMP: u64 = (26 << (32 + 24))  // 2026
        + (106 << (32 + 15))  // April 16th
        + (17 << (32 + 10))  // hour 17
        + (9 << (32 + 4))  // minute 9
        + (35 << 30)  // second 35
        + (123 << 20)  // millisecond 123
        + (456 << 10)  // microsecond 456
        + (789); // nanosecond 789

    fn make_raw_udp_header(num_events: usize) -> Vec<u8> {
        // Note: 4-byte words
        // Total header length: 64 bytes (16 words)
        [255_u8; 4] // Header word 0: 'running' header marker
            .iter()
            .chain(&[255_u8; 4]) // Header word 1: neutron data header marker
            .chain(&[0_u8; 4]) // Header word 2: information
            .chain(&[0_u8; 4]) // Header word 3: frame number
            .chain(&VALID_TIMESTAMP.to_be_bytes()) // Header words 4 & 5: GPS timestamp
            .chain(&[0_u8; 2]) // Header word 6: period number
            .chain(&[0_u8; 2]) // Header word 6: unused
            .chain(&(num_events as u32).to_be_bytes()) // Header word 7: events in frame
            .chain(&[0_u8; 2]) // Header word 8: ppp_in_frame
            .chain(&[0_u8; 2]) // Header word 8: unused
            .chain(&[0_u8; 4]) // Header word 9: vetoes
            .chain(&[0_u8; 4]) // Header word 10: address of next frame
            .chain(&[0_u8; 4]) // Header word 11: address of next frame (word address)
            .chain(&[0_u8; 4]) // Header word 12: streamed frame number
            .chain(&[0_u8; 4]) // Header word 13: not used
            .chain(&[0_u8; 4]) // Header word 14: not used
            .chain(&[0_u8; 4]) // Header word 15: not used
            .copied()
            .collect()
    }

    /// tof = 456000 ns
    /// val = 123
    fn make_pc3877ms_event() -> Vec<u8> {
        vec![
            0xFF, 0, 89, 16, // 20ns (scaling) * (89 * 256 + 16) = 456000ns
            0xFF, 0xFF, 0, 123, // Position 123
        ]
    }

    #[test]
    fn test_process_pc3877ms_events() {
        let num_events = 2;
        let mut raw_data = make_raw_udp_header(num_events);

        raw_data.extend_from_slice(&make_pc3877ms_event());
        raw_data.extend_from_slice(&make_pc3877ms_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let data = hex::encode(raw_data);

        let wiring_config = vec![WiringConfigRecord {
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
        }];

        let mut msgs = vec![];
        process_udp_to_kafka(
            &mut FlatBufferBuilder::new(),
            &data,
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

                assert_eq!(msg.pixel_id().unwrap().get(0), 123);
                assert_eq!(msg.pixel_id().unwrap().get(1), 123);
            }
            _ => panic!("Could not deserialize"),
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
        let mut raw_data = make_raw_udp_header(num_events);

        raw_data.extend_from_slice(&make_pc3544ms_event());
        raw_data.extend_from_slice(&make_pc3544ms_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let data = hex::encode(raw_data);

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
        process_udp_to_kafka(
            &mut FlatBufferBuilder::new(),
            &data,
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
        let mut raw_data = make_raw_udp_header(num_events);

        raw_data.extend_from_slice(&make_pc3634m1s_event());
        raw_data.extend_from_slice(&make_pc3634m1s_event());

        let n_bytes = raw_data.len();

        assert_eq!(n_bytes, 64 + num_events * 8);

        let data = hex::encode(raw_data);

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
        process_udp_to_kafka(
            &mut FlatBufferBuilder::new(),
            &data,
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

                assert_eq!(msg.pixel_id().unwrap().get(0), 123456789);
                assert_eq!(msg.pixel_id().unwrap().get(1), 123456789);
            }
            _ => panic!("Could not deserialize"),
        }
    }
}
