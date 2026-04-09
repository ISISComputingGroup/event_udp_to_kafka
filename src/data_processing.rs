use crate::WiringConfigRecord;
use chrono::prelude::*;
extern crate flatbuffers;

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use log::{debug, error, trace, warn};

pub fn process_udp_to_kafka<'a>(
    udp_hex: &'a str,
    src_ip: &'a str,
    wiring_config: &'a Vec<WiringConfigRecord>,
) -> Vec<Vec<u8>> {
    // make the vector for the product now
    let mut kafka_bytes: Vec<Vec<u8>> = Vec::new();

    // Split into the different frames in the packet
    // Filters any empty frames each time
    let (frames_udp, frames_types) = packet_to_frames(udp_hex);
    if frames_types.is_empty() {
        kafka_bytes
    } else {
        // If there are valid frames to deal with
        debug!("NFilt: {} - Types: {:?}", frames_udp.len(), frames_types);
        for frame_i in 0..frames_udp.len() {
            trace!("processing frame: {frame_i}");
            match frames_types[frame_i] {
                1 => {
                    trace!("PROC For Neutron Frame Header - {:?}", frames_udp[frame_i]);
                    process_neutron_frame(
                        frames_udp[frame_i],
                        src_ip,
                        wiring_config,
                        &mut kafka_bytes,
                    );
                }
                2 => {
                    trace!("PROC For Veto Frame Header");
                    process_neutron_frame(
                        frames_udp[frame_i],
                        src_ip,
                        wiring_config,
                        &mut kafka_bytes,
                    );
                }
                3 => trace!("PROC For SE Frame Header"),
                _ => warn!("Undefined frame type"),
            }
        }

        kafka_bytes
    }
}

fn packet_to_frames(udp_hex: &str) -> (Vec<&str>, Vec<u8>) {
    // Takes in a reference to a hex string containing UDP data
    // Returns two vectors, first of each frame second of the frame type
    // Vectors will have a len of 0 if no frames found

    const VETO_FRAME_HEADER: &str = "fcffffff";
    const SE_FRAME_HEADER: &str = "fdffffff";
    const NEUTRON_HEADER: &str = "ffffffff";

    // Convert the packet into the words
    let words = udp_hex
        .as_bytes()
        .chunks(8)
        .map(str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap();

    // Make a vector of the addresses for each frame header found
    let mut frame_index: Vec<u32> = Vec::new();
    // Vector to hold a number representing the type of frame detected
    let mut frame_types: Vec<u8> = Vec::new();

    // if a word matches the different headers then push the index to the list
    for (index, &word) in words.iter().enumerate() {
        match word {
            NEUTRON_HEADER => {
                frame_index.push(index as u32);
                frame_types.push(1)
            }
            VETO_FRAME_HEADER => {
                frame_index.push(index as u32);
                frame_types.push(2)
            }
            SE_FRAME_HEADER => {
                frame_index.push(index as u32);
                frame_types.push(3)
            }
            _ => {}
        }
    }

    // Vector of the bytes making up each frame
    let mut frame_bytes: Vec<&str> = Vec::new();

    // If no frames found return the empty Vec
    if frame_index.is_empty() {
        (frame_bytes, frame_types)
    }
    // If one frame found append entire UDP packet
    else if frame_index.len() == 1 {
        frame_bytes.push(udp_hex);
        (frame_bytes, frame_types)
    }
    // multiple frames found, append each to the vec
    else {
        for i in (0..frame_index.len()).rev() {
            // Do this backwards as removing Vec entries
            if i == frame_index.len() - 1 {
                // if data is the last frame found in the dataset
                let hex = &udp_hex[(frame_index[i] * 8) as usize..udp_hex.len()];
                if hex.len() >= 128 {
                    // Check the frame is larger than a frame header
                    frame_bytes.push(hex);
                } else {
                    frame_types.remove(i);
                }
            } else {
                // for all other frames
                let hex =
                    &udp_hex[(frame_index[i] * 8) as usize..(frame_index[i + 1] * 8) as usize];
                if hex.len() >= 128 {
                    // Check the frame is larger than a frame header
                    frame_bytes.push(hex);
                } else {
                    frame_types.remove(i);
                }
            }
        }
        frame_bytes.reverse(); // reverse the bytes vector as they were added in reverse
        (frame_bytes, frame_types)
    }
}

const HEADER_LENGTH: usize = 120;

#[allow(unused)]
pub struct FrameHeader {
    pub events_in_frame: u32,
    pub frame_number: u32,
    pub period_num: u16,
    pub ppp_in_frame: u16,
    pub total_ns: u64,
}

pub fn process_neutron_frame(
    frame_udp: &str,
    src_ip: &str,
    wiring_config: &Vec<WiringConfigRecord>,
    ev44_fb_packets: &mut Vec<Vec<u8>>,
) {
    let num_words = frame_udp.len() / 8;
    let exp_events = (num_words - 15) / 2; // could be less if PCB has added padding Zeros

    let header = header_decoder(&frame_udp[0..HEADER_LENGTH]);

    let events_only_hex = &frame_udp[HEADER_LENGTH..];

    let mut events_to_proc = header.events_in_frame;
    if events_to_proc > exp_events as u32 {
        events_to_proc = exp_events as u32;
        warn!(
            "Its Been MELON'ED :( - More Events in FHeader than Packet Size - F: {} - P: {exp_events}",
            header.events_in_frame
        );
    }

    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();
    let mut bldr = FlatBufferBuilder::new();

    // find IP address within the wiring config - get config line
    let mut packet_config_single: &WiringConfigRecord = wiring_config.first().unwrap();
    let mut packet_config_multi: Vec<&WiringConfigRecord> = Vec::new();

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
                    process_pc3634m1_events(events_only_hex, packet_config_single, events_to_proc);
            } // 128CH LVDS Card
            "PC3544MS" => {
                (tofs, det_ids) =
                    process_pc3544ms_events(events_only_hex, packet_config_multi, events_to_proc);
            } // MADC PB
            "PC3877MS" => {
                (tofs, det_ids) =
                    process_pc3877ms_events(events_only_hex, packet_config_single, events_to_proc);
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
    events_hex: &str,
    packet_config: Vec<&WiringConfigRecord>,
    events_to_proc: u32,
) -> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();
    match packet_config[0].packet_type.as_str() {
        "Position" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let channel = u8::from_str_radix(&binary_event[35..38], 2).unwrap();
                let event_position = u32::from_str_radix(&binary_event[52..64], 2).unwrap();

                let mut channel_config: &WiringConfigRecord = packet_config.first().unwrap();
                let mut matches = 0;
                for possible_channel in &packet_config {
                    if channel == possible_channel.ch {
                        channel_config = possible_channel;
                        matches += 1;
                    }
                }
                if matches == 1 {
                    let detector_id = (event_position
                        / (4096 / channel_config.mantid_detector_id_length))
                        + channel_config.mantid_detector_id_start;
                    let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();

                    tofs.push(event_tof);
                    det_ids.push(detector_id);
                    trace!("{event_i} - {event_hex} - TOF: {event_tof} - AdcCH: {channel} - VAL: {event_position} - DETID: {detector_id}");
                }
            }
            (tofs, det_ids)
        }
        "PulseHeight" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let channel = u8::from_str_radix(&binary_event[35..38], 2).unwrap();
                let event_pulse_height = u32::from_str_radix(&binary_event[40..52], 2).unwrap();

                let mut channel_config: &WiringConfigRecord = packet_config.first().unwrap();
                let mut matches = 0;
                for possible_channel in &packet_config {
                    if channel == possible_channel.ch {
                        channel_config = possible_channel;
                        matches += 1;
                    }
                }
                if matches == 1 {
                    let detector_id = (event_pulse_height
                        / (4096 / channel_config.mantid_detector_id_length))
                        + channel_config.mantid_detector_id_start;
                    let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();

                    tofs.push(event_tof);
                    det_ids.push(detector_id);
                    trace!(
                        "{event_i} - {event_hex} - TOF: {event_tof} - AdcCH: {channel} - VAL: {event_pulse_height} - DETID: {detector_id}"
                    );
                }
            }
            (tofs, det_ids)
        }
        _ => {
            error!("Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        }
    }
}

fn process_pc3634m1_events(
    events_hex: &str,
    packet_config: &WiringConfigRecord,
    events_to_proc: u32,
) -> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();

    match packet_config.packet_type.as_str() {
        "DIM_OUT" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
                let event_val = u32::from_str_radix(&event_hex[8..16], 16).unwrap();

                let det_id = event_val + packet_config.mantid_detector_id_start;

                tofs.push(event_tof);
                det_ids.push(det_id);

                trace!(
                    "{event_i} - {event_hex} - TOF: {event_tof} - VAL: {event_val} - DETID: {det_id}"
                );
            }
            (tofs, det_ids)
        }
        _ => {
            error!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        }
    }
}

fn process_pc3877ms_events(
    events_hex: &str,
    packet_config: &WiringConfigRecord,
    events_to_proc: u32,
) -> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();
    match packet_config.packet_type.as_str() {
        "Position" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let event_val = u32::from_str_radix(&binary_event[48..64], 2).unwrap();

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap() * 20;
                let det_id = event_val + packet_config.mantid_detector_id_start;

                tofs.push(event_tof);
                det_ids.push(det_id);
            }
            (tofs, det_ids)
        }
        "PulseHeight" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let event_val = u32::from_str_radix(&binary_event[36..48], 2).unwrap();

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
                let det_id = event_val + packet_config.mantid_detector_id_start;

                tofs.push(event_tof);
                det_ids.push(det_id);
            }
            (tofs, det_ids)
        }
        _ => {
            error!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        }
    }
}

/// Decode a frame header
pub fn header_decoder(header_udp: &str) -> FrameHeader {
    // Uncomment to print the header words to terminal
    // for i in 0..15{
    //     println!("i{} {}:{} - {}", i+1, i*8, i*8+8, &header_udp[i*8..i*8+8]);
    // }

    // need to get
    // - ifVeto
    // - GPS time -> Format to nS since epoch
    // - Period Number
    // - Events in Frame

    let frame_number = u32::from_str_radix(&header_udp[16..24], 16).unwrap();
    let period_num = u16::from_str_radix(&header_udp[44..48], 16).unwrap();
    let events_in_frame = u32::from_str_radix(&header_udp[48..56], 16).unwrap();
    let ppp_in_frame = u16::from_str_radix(&header_udp[60..64], 16).unwrap();

    // Get GPS Time
    let bin_time: &str = &hex_to_binary(&header_udp[24..40]); // Get data as binary string

    let years = u16::from_str_radix(&bin_time[0..8], 2).unwrap() + 2000;
    let days = u32::from_str_radix(&bin_time[8..17], 2).unwrap();
    let hours = u8::from_str_radix(&bin_time[17..22], 2).unwrap();
    let mins = u8::from_str_radix(&bin_time[22..28], 2).unwrap();
    let secs = u8::from_str_radix(&bin_time[28..34], 2).unwrap();
    let m_secs = u16::from_str_radix(&bin_time[34..44], 2).unwrap();
    let u_secs = u16::from_str_radix(&bin_time[44..54], 2).unwrap();
    let n_secs = u64::from_str_radix(&bin_time[54..64], 2).unwrap();

    // println!("F: {} - E: {} - PER: {} - PPP: {}", frame_number, events_in_frame, period_num, ppp_in_frame);
    // println!("time: Y-{years}:D-{days}:H-{hours}:M-{mins}:S-{secs}:mS-{m_secs}:uS-{u_secs}:nS-{n_secs}");
    // -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    // -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    // Need to add code to handle days = zero
    // Currently this will cause the program to crash as it cannot convert the year + days to nS since epoch
    // could just add a if statement for this
    // probably better to read docs for from_yo_opt to see why
    // -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    // -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    let mut years_days_as_ns: u64 = 0;
    if days != 0 {
        // check if days if valid, if its zero the packet likely doesn't have a valid GPS timesource
        #[allow(deprecated)] // To be fixed as per comment above
        let datetime_again: DateTime<Utc> = DateTime::from_utc(
            NaiveDateTime::from(NaiveDate::from_yo_opt(years as i32, days).unwrap()),
            Utc,
        );
        years_days_as_ns = datetime_again.timestamp() as u64 * 1e9 as u64;
    } else {
        error!("MELON'ed - invalid days, set years/day to zero");
    }

    let hours_as_ns = hours as u64 * 3.6e12 as u64;
    let mins_as_ns = mins as u64 * 6e10 as u64;
    let secs_as_ns = secs as u64 * 1e9 as u64;
    let m_secs_as_ns = m_secs as u64 * 1000000;
    let u_secs_as_ns = u_secs as u64 * 1000;

    let total_ns: u64 = n_secs
        + u_secs_as_ns
        + m_secs_as_ns
        + secs_as_ns
        + mins_as_ns
        + hours_as_ns
        + years_days_as_ns;

    trace!(
        "F: {} - E: {} - PER: {} - PPP: {} - TnS: {}",
        frame_number, events_in_frame, period_num, ppp_in_frame, total_ns
    );
    trace!("time nS: {}", total_ns);

    FrameHeader {
        events_in_frame,
        frame_number,
        period_num,
        ppp_in_frame,
        total_ns,
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

fn hex_to_binary(hex: &str) -> String {
    hex.chars().map(to_binary).collect()
}

fn to_binary(c: char) -> &'static str {
    match c {
        '0' => "0000",
        '1' => "0001",
        '2' => "0010",
        '3' => "0011",
        '4' => "0100",
        '5' => "0101",
        '6' => "0110",
        '7' => "0111",
        '8' => "1000",
        '9' => "1001",
        'a' => "1010",
        'b' => "1011",
        'c' => "1100",
        'd' => "1101",
        'e' => "1110",
        'f' => "1111",
        _ => "",
    }
}
