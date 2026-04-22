use crate::WiringConfigRecord;

use crate::header::{HEADER_LEN_BYTES, UdpHeaderView};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use itertools::Itertools;
use log::{error, warn};

/// Input: hexed string containing the data from a UDP packet (which may contain multiple
/// event packets)
///
/// Output: Vector of flatbuffers-encoded messages to send to Kafka
pub fn process_udp_to_kafka(
    udp_hex: &str,
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
) -> Vec<Vec<u8>> {
    process_udp_bytes_to_kafka(
        &hex::decode(udp_hex).expect("Invalid hex"),
        src_ip,
        wiring_config,
    )
}

/// Input: binary data from a UDP packet (which may contain multiple event packets)
///
/// Output: Vector of flatbuffers-encoded messages to send to Kafka
pub fn process_udp_bytes_to_kafka(
    udp_packet: &[u8],
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
) -> Vec<Vec<u8>> {
    // make the vector for the product now
    let mut kafka_bytes: Vec<Vec<u8>> = vec![];

    // Split into the different frames in the packet
    // Filters any empty frames each time
    let frames = packet_to_frames(udp_packet);

    for frame in frames {
        match frame.packet_type {
            UdpPacketType::NeutronData => {
                let result =
                    process_neutron_frame(frame.packet, src_ip, wiring_config, &mut kafka_bytes);
                if let Err(e) = result {
                    warn!("Error processing neutron data: {}", e);
                    // todo: metrics
                }
            }
            UdpPacketType::SampleEnvironment => {
                todo!("Implement sample environment")
            }
            UdpPacketType::VetoFrame => {
                todo!("Implement veto frame")
            }
        }
    }

    kafka_bytes
}

/// Types of packets we may receive over UDP.
enum UdpPacketType {
    VetoFrame,
    SampleEnvironment,
    NeutronData,
}

/// A reference to a decoded UDP packet of a particular type.
/// The slice referenced by `packet` contains a header, event data if applicable, and potentially
/// trailing zeros.
struct UdpPacket<'a> {
    packet_type: UdpPacketType,
    packet: &'a [u8],
}

/// Input: a slice of binary UDP data
///
/// Output: a vector of UDP packets; Each UDP message will be complete,
/// i.e. containing the full UDP data for that message including all header bytes
/// Vector will be empty if no frames found
fn packet_to_frames(udp: &[u8]) -> Vec<UdpPacket<'_>> {
    const MARKER: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];
    const VETO_FRAME_HEADER: &[u8] = &[0xFC, 0xFF, 0xFF, 0xFF];
    const SE_FRAME_HEADER: &[u8] = &[0xFD, 0xFF, 0xFF, 0xFF];
    const NEUTRON_HEADER: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

    // Find the byte-offsets of the beginning of frame markers.
    let mut marker_offsets = vec![];
    let mut offset = 0;

    while offset < udp.len() {
        if udp.get(offset..offset + 4) == Some(&MARKER) {
            marker_offsets.push(offset);
            offset += HEADER_LEN_BYTES;
        }
        offset += 4; // Advance by a 4-byte word each time.
    }

    // Last message can go up to end of data.
    marker_offsets.push(udp.len());

    let mut packets = vec![];

    for (&start_inclusive, &end_exclusive) in marker_offsets.iter().tuple_windows() {
        if let Some(msg) = udp.get(start_inclusive..end_exclusive)
            && msg.len() >= HEADER_LEN_BYTES
        {
            match msg.get(4..8) {
                Some(NEUTRON_HEADER) => {
                    packets.push(UdpPacket {
                        packet_type: UdpPacketType::NeutronData,
                        packet: msg,
                    });
                }
                Some(VETO_FRAME_HEADER) => {
                    packets.push(UdpPacket {
                        packet_type: UdpPacketType::VetoFrame,
                        packet: msg,
                    });
                }
                Some(SE_FRAME_HEADER) => {
                    packets.push(UdpPacket {
                        packet_type: UdpPacketType::SampleEnvironment,
                        packet: msg,
                    });
                }
                _ => {
                    // Unknown packet type
                }
            }
        }
    }

    packets
}

/// Input: a neutron event UDP packet, header, events, and possibly padding zeros.
/// Output: Vec of Flatbuffers-encoded messages to send to Kafka
fn process_neutron_frame(
    frame_udp: &[u8],
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    ev44_fb_packets: &mut Vec<Vec<u8>>,
) -> Result<(), &'static str> {
    let header = UdpHeaderView::new(frame_udp).ok_or("Invalid frame header; not long enough")?;

    let nanoseconds_since_epoch = header
        .gps_time()
        .nanoseconds_since_epoch()
        .ok_or("Invalid frame header; timestamp is invalid")?;

    let event_data = &frame_udp[HEADER_LEN_BYTES..];

    if event_data.len() % 8 != 0 {
        return Err("Event data is not a multiple of pairs of 4-byte words");
    }

    let events_to_proc = event_data.len() / 8;

    let mut bldr = FlatBufferBuilder::new();

    // find IP address within the wiring config - get config line
    let mut packet_config_single: &WiringConfigRecord =
        wiring_config.first().ok_or("Invalid wiring table")?;

    let mut packet_config_multi: Vec<&WiringConfigRecord> = vec![];

    let mut num_matches = 0;
    for line in wiring_config {
        if src_ip == line.streaming_ip {
            num_matches += 1;
            packet_config_single = line;
            packet_config_multi.push(line);
        }
    }

    if num_matches >= 1 {
        // do we want this for LVDS or have if 1, else if greater than 1?
        let (tofs, det_ids) = match packet_config_single.brd_type.as_str() {
            "PC3634M1S" => {
                process_pc3634m1s_events(event_data, packet_config_single, events_to_proc)
            } // 128CH LVDS Card
            "PC3544MS" => process_pc3544ms_events(event_data, &packet_config_multi, events_to_proc), // MADC PB
            "PC3877MS" => process_pc3877ms_events(event_data, packet_config_single, events_to_proc), // WLSF Streaming Electronics
            _ => {
                return Err("Unknown board type");
            }
        };

        if tofs.is_empty() {
            return Err("No events within frame");
        } else {
            // Trying with EV44 Packets
            let fb_bytes = encode_ev44(
                &mut bldr,
                "rust_proc",
                0,
                nanoseconds_since_epoch,
                &tofs,
                &det_ids,
            );
            ev44_fb_packets.push(fb_bytes);
        }
    }
    Ok(())
}

fn process_pc3544ms_events(
    event_data: &[u8],
    packet_config: &[&WiringConfigRecord],
    events_to_proc: usize,
) -> (Vec<u32>, Vec<u32>) {
    match packet_config[0].packet_type.as_str() {
        "Position" => {
            event_data
                .chunks_exact(8)
                .take(events_to_proc)
                .filter_map(|event| {
                    let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                    let event_position =
                        u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFF;

                    if let Some(channel_config) = packet_config.iter().find(|c| c.ch == channel) {
                        let detector_id = (event_position
                            / (4096 / channel_config.mantid_detector_id_length))
                            + channel_config.mantid_detector_id_start;

                        let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;

                        Some((tof, detector_id))
                    } else {
                        None
                    }
                })
                .unzip()
        }
        "PulseHeight" => {
            event_data
                .chunks_exact(8)
                .take(events_to_proc)
                .filter_map(|event| {
                    let channel = (event[4] >> 2) & 0b111; // Bits 26..=28
                    let pulse_height =
                        (u32::from_be_bytes(event[4..8].try_into().unwrap()) >> 12) & 0xFFF;

                    if let Some(channel_config) = packet_config.iter().find(|c| c.ch == channel) {
                        let detector_id = (pulse_height
                            / (4096 / channel_config.mantid_detector_id_length))
                            + channel_config.mantid_detector_id_start;
                        let event_tof =
                            u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;

                        Some((event_tof, detector_id))
                    } else {
                        None
                    }
                })
                .unzip()
        }
        _ => {
            error!("Unable to PROC -> Unknown stream type in config");
            (vec![], vec![])
        }
    }
}

fn process_pc3634m1s_events(
    event_data: &[u8],
    packet_config: &WiringConfigRecord,
    events_to_proc: usize,
) -> (Vec<u32>, Vec<u32>) {
    match packet_config.packet_type.as_str() {
        "DIM_OUT" => event_data
            .chunks_exact(8)
            .take(events_to_proc)
            .map(|event| {
                let tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap());
                val += packet_config.mantid_detector_id_start;
                (tof, val)
            })
            .unzip(),
        _ => {
            error!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (vec![], vec![])
        }
    }
}

fn process_pc3877ms_events(
    event_data: &[u8],
    packet_config: &WiringConfigRecord,
    events_to_proc: usize,
) -> (Vec<u32>, Vec<u32>) {
    const CLOCK_TICKS_TO_NS: u32 = 20;

    match packet_config.packet_type.as_str() {
        "Position" => event_data
            .chunks_exact(8)
            .take(events_to_proc)
            .map(|event| {
                let mut tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                tof *= CLOCK_TICKS_TO_NS;

                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFFF;
                val += packet_config.mantid_detector_id_start;

                (tof, val)
            })
            .unzip(),
        "PulseHeight" => event_data
            .chunks_exact(8)
            .take(events_to_proc as usize)
            .map(|event| {
                let mut val = (u32::from_be_bytes(event[4..8].try_into().unwrap()) >> 16) & 0xFFF;
                val += packet_config.mantid_detector_id_start;

                let tof = (u32::from_be_bytes(event[0..4].try_into().unwrap())) & 0xFFFFFF;

                (tof, val)
            })
            .unzip(),
        _ => {
            error!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (vec![], vec![])
        }
    }
}

fn encode_ev44(
    bldr: &mut FlatBufferBuilder,
    source_name: &str,
    message_id: u64,
    pulse_time: u64,
    tofs: &[u32],
    det_ids: &[u32],
) -> Vec<u8> {
    bldr.reset();

    let reference_time = vec![pulse_time as i64];
    let reference_time_index = vec![0];

    let tofs_i32 = tofs.iter().map(|t| *t as i32).collect::<Vec<_>>();
    let det_ids_i32 = det_ids.iter().map(|d| *d as i32).collect::<Vec<_>>();

    let args = Event44MessageArgs {
        source_name: Some(bldr.create_string(source_name)),
        message_id: message_id as i64,
        reference_time: Some(bldr.create_vector(&reference_time)),
        reference_time_index: Some(bldr.create_vector(&reference_time_index)),
        time_of_flight: Some(bldr.create_vector(&tofs_i32)),
        pixel_id: Some(bldr.create_vector(&det_ids_i32)),
    };

    let ev44_offset = Event44Message::create(bldr, &args);
    finish_event_44_message_buffer(bldr, ev44_offset);
    bldr.finished_data().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use isis_streaming_data_types::{DeserializedMessage, deserialize_message};

    /// A valid timestamp, encoded in the UDP packed format.
    const VALID_TIMESTAMP: u64 = (26 << (32 + 24))
        + (106 << (32 + 15))
        + (17 << (32 + 10))
        + (9 << (32 + 4))
        + (35 << 30)
        + (123 << 20)
        + (456 << 10)
        + (789);

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
            0xFF, 0, 89, 16,  // 20ns (scaling) * (89 * 256 + 16) = 456000ns
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

        let msgs = process_udp_to_kafka(&data, "192.168.1.1", &wiring_config);

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
            0xFF, 0x06, 0xF5, 0x40,  // 456000ns
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

        let msgs = process_udp_to_kafka(&data, "192.168.1.1", &wiring_config);

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

    #[test]
    fn test_process_pc3634m1s_events() {
        let num_events = 2;
        let mut raw_data = make_raw_udp_header(num_events);

        raw_data.extend_from_slice(&vec![0_u8; num_events * 8]); // 8-byte event messages

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

        let msgs = process_udp_to_kafka(&data, "192.168.1.1", &wiring_config);

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 2);
            }
            _ => panic!("Could not deserialize"),
        }
    }
}
