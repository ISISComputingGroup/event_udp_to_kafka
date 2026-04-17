use crate::WiringConfigRecord;

use crate::header::{HEADER_LEN_BYTES, UdpHeaderView};
use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use log::{error, trace, warn};

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
                process_neutron_frame(frame.packet, src_ip, wiring_config, &mut kafka_bytes);
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

    let mut packets = vec![];

    let mut offset = 0;

    while offset < udp.len() {
        if udp.get(offset..offset + 4) != Some(MARKER) {
            // Not a valid header start byte
            offset += 1;
        }
        if let Some(header_bytes) = udp.get(offset..offset + HEADER_LEN_BYTES)
            && let Some(header) = UdpHeaderView::new(header_bytes)
        {
            match udp.get(offset + 4..offset + 8) {
                Some(NEUTRON_HEADER) => {
                    let events_length_bytes = header.events_in_frame() as usize * 8; // 8 bytes per neutron event
                    if let Some(packet) =
                        udp.get(offset..offset + HEADER_LEN_BYTES + events_length_bytes)
                    {
                        packets.push(UdpPacket {
                            packet_type: UdpPacketType::NeutronData,
                            packet,
                        });
                        offset += HEADER_LEN_BYTES + events_length_bytes;
                    } else {
                        break; // Not enough UDP data compared to what header said there would be
                    }
                }
                Some(VETO_FRAME_HEADER) => {
                    packets.push(UdpPacket {
                        packet_type: UdpPacketType::VetoFrame,
                        packet: header_bytes,
                    });
                    offset += HEADER_LEN_BYTES;
                }
                Some(SE_FRAME_HEADER) => {
                    packets.push(UdpPacket {
                        packet_type: UdpPacketType::SampleEnvironment,
                        packet: header_bytes,
                    });
                    offset += HEADER_LEN_BYTES;
                }
                _ => {
                    // Unknown packet type
                    offset += 1;
                }
            }
        } else {
            break; // Not enough bytes to be a valid UDP header
        }
    }

    packets
}

/// Metadata retrieved from a frame header
pub struct FrameHeader {
    pub events_in_frame: u32,
    pub frame_number: u32,
    pub period_num: u16,
    pub ppp_in_frame: u16,
    pub total_ns: u64,
}

/// Input: a neutron event UDP packet, with header, events, and possibly padding zeros.
/// Output: Vec of Flatbuffers-encoded messages to send to Kafka
fn process_neutron_frame(
    frame_udp: &[u8],
    src_ip: &str,
    wiring_config: &[WiringConfigRecord],
    ev44_fb_packets: &mut Vec<Vec<u8>>,
) {
    let num_words = frame_udp.len() / 4;
    let exp_events = (num_words - 16) / 2; // could be less if PCB has added padding Zeros

    let header = header_decoder(frame_udp);

    let events_to_proc = exp_events.min(header.events_in_frame as usize);

    let event_data = &frame_udp[HEADER_LEN_BYTES..];

    let mut tofs: Vec<u32> = Vec::with_capacity(exp_events);
    let mut det_ids: Vec<u32> = Vec::with_capacity(exp_events);

    let mut bldr = FlatBufferBuilder::new();

    // find IP address within the wiring config - get config line
    let mut packet_config_single: &WiringConfigRecord = wiring_config.first().unwrap();
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
        match packet_config_single.brd_type.as_str() {
            "PC3634M1S" => {
                (tofs, det_ids) =
                    process_pc3634m1_events(event_data, packet_config_single, events_to_proc);
            } // 128CH LVDS Card
            "PC3544MS" => {
                (tofs, det_ids) =
                    process_pc3544ms_events(event_data, &packet_config_multi, events_to_proc);
            } // MADC PB
            "PC3877MS" => {
                (tofs, det_ids) =
                    process_pc3877ms_events(event_data, packet_config_single, events_to_proc);
            } // WLSF Streaming Electronics
            _ => {
                warn!("MELON'ed -> Unable to PROC -> Unknown BRD Type");
            }
        }
        trace!("Dets: {:?}", det_ids);
        trace!("TOFs: {:?}", tofs);

        if tofs.is_empty() {
            warn!("MELON'ed -> no Events found within frame");
        } else {
            // Trying with EV44 Packets
            let fb_bytes = encode_ev44(&mut bldr, "rust_proc", 0, header.total_ns, &tofs, &det_ids);
            ev44_fb_packets.push(fb_bytes);
        }
    }
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

fn process_pc3634m1_events(
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
            .take(events_to_proc as usize)
            .map(|event| {
                let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFFF;
                val += packet_config.mantid_detector_id_start;

                let mut tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                tof *= CLOCK_TICKS_TO_NS;

                (tof, val)
            })
            .unzip(),
        "PulseHeight" => event_data
            .chunks_exact(8)
            .take(events_to_proc as usize)
            .map(|event| {
                let mut val = (u32::from_be_bytes(event[4..8].try_into().unwrap()) >> 12) & 0xFFF;
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

/// Decode a frame header
pub fn header_decoder(bytes: &[u8]) -> FrameHeader {
    let header = UdpHeaderView::new(bytes).expect("Invalid UDP header");

    FrameHeader {
        events_in_frame: header.events_in_frame(),
        frame_number: header.frame_number(),
        period_num: header.period_number(),
        ppp_in_frame: header.ppp_in_frame(),
        total_ns: header
            .gps_time()
            .nanoseconds_since_epoch()
            .expect("Invalid timestamp"),
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

    fn make_raw_udp_message(num_events: usize) -> Vec<u8> {
        // Note: 4-byte words
        // Total header length: 60 bytes (15 words)
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
            .chain(&vec![0_u8; num_events * 8]) // 8-byte event messages
            .copied()
            .collect()
    }

    #[test]
    fn test_decode_pc3877ms() {
        let num_events = 100;
        let raw_data = make_raw_udp_message(num_events);
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
            comment: "WLSF Module".to_owned(),
        }];

        let msgs = process_udp_to_kafka(&data, "192.168.1.1", &wiring_config);

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 100);
            }
            _ => panic!("Could not deserialize"),
        }
    }


    #[test]
    fn test_decode_pc3877ms_with_padding_zeros() {
        let num_events = 100;
        // A valid message followed by 5000 meaningless padding zeros
        let raw_data = make_raw_udp_message(num_events).into_iter().chain([0_u8; 5000].into_iter()).collect::<Vec<_>>();

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
            comment: "WLSF Module".to_owned(),
        }];

        let msgs = process_udp_to_kafka(&data, "192.168.1.1", &wiring_config);

        assert_eq!(msgs.len(), 1);
        match deserialize_message(&msgs[0]) {
            Ok(DeserializedMessage::EventDataEv44(msg)) => {
                assert_eq!(msg.reference_time().get(0), 1776359375123456789);
                assert_eq!(msg.time_of_flight().unwrap().len(), 100);
            }
            _ => panic!("Could not deserialize"),
        }
    }
}
