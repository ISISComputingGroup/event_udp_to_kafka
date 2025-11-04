mod ev42_events_generated;
mod ev44_events_generated;

use std::io::Bytes;
use std::u32;
use serde_json::from_str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::prelude::*;
use crate::wiring_config_record;
extern crate flatbuffers;

use flatbuffers::FlatBufferBuilder;
use ev42_events_generated::{EventMessage, EventMessageArgs, finish_event_message_buffer, root_as_event_message};
use ev44_events_generated::{Event44Message, Event44MessageArgs, finish_event_44_message_buffer, root_as_event_44_message};
//use crate::ev42_events_generated::{UInt32ArrayArgs, ValueUnion};

pub fn process_udp_to_kafka<'a>(udp_hex: &'a str, src_ip: &'a str, wiring_config: &'a Vec<wiring_config_record>) -> Vec<Vec<u8>>{
    use std::time::Instant;
  //  let now = Instant::now();

    // make the vector for the product now
    let mut kafka_bytes: Vec<Vec<u8>> = Vec::new();

    // Split into the different frames in the packet
    // Filters any empty frames each time
    let (frames_udp, frames_types) = packet_to_frames(udp_hex);
    if frames_types.len() == 0{
       // let elapsed = now.elapsed();
       // println!("Elapsed: {:.2?}", elapsed);
        kafka_bytes
    }
    else {  // If there are valid frames to deal with
        // println!("NFilt: {} - Types: {:?}", frames_udp.len(), frames_types);
        //println!("UDP data: {:?}", frames_udp);
        for frame_i in 0..frames_udp.len() {
            // println!("processing frame: {frame_i}");
            match frames_types[frame_i] {
                1 => {
                    //println!("PROC For Neutron Frame Header - {:?}", frames_udp[frame_i]);
                    process_neutron_frame(frames_udp[frame_i], src_ip, wiring_config, &mut kafka_bytes);
                },
                2 => {
                    // println!("PROC For Veto Frame Header")
                    process_neutron_frame(frames_udp[frame_i], src_ip, wiring_config, &mut kafka_bytes);
                },
                3 => println!("PROC For SE Frame Header"),
                _ => println!("Undefined frame type")
            }
        }
       // let elapsed = now.elapsed();
        // println!("Elapsed: {:.2?} - bytes: {:?}", elapsed, kafka_bytes);
       // println!("UDP->EV42 Time: {:.2?}", elapsed, );
        // println!();

        kafka_bytes
    }
}

fn packet_to_frames(udp_hex: &str) -> (Vec<&str>, Vec<u8>){
    // Takes in a reference to a hex string containing UDP data
    // Returns two vectors, first of each frame second of the frame type
    // Vectors will have a len of 0 if no frames found

    let veto_frame_header = "fcffffff";
    let se_frame_header = "fdffffff";
    let neutron_header = "ffffffff";

    // Convert the packet into the words
    let words = udp_hex.as_bytes()
    .chunks(8)
    .map(std::str::from_utf8)
    .collect::<Result<Vec<&str>, _>>()
    .unwrap();

    // Make a vector of the addresses for each frame header found
    let mut frame_index: Vec<u32> = Vec::new();
    // Vector to hold a number representing the type of frame detected
    let mut frame_types: Vec<u8> = Vec::new();

    // if a word matches the different headers then push the index to the list
    for index in 0..words.len() as u32{
        let word = words[index as usize];
        if word == neutron_header { frame_index.push(index); frame_types.push(1) }
        else if word == veto_frame_header { frame_index.push(index); frame_types.push(2)}
        else if word == se_frame_header { frame_index.push(index); frame_types.push(3)}
    }

    // Vector of the bytes making up each frame
    let mut frame_bytes: Vec<&str> = Vec::new();

    // If no frames found return the empty Vec
    if frame_index.len() == 0 {(frame_bytes, frame_types)}

    // If one frame found append entire UDP packet
    else if frame_index.len() == 1 {
        frame_bytes.push(udp_hex);
        (frame_bytes, frame_types)
    }

    // multiple frames found, append each to the vec
    else {
        for i in (0..frame_index.len()).rev(){  // Do this backwards as removing Vec entries
            if i == frame_index.len()-1{    // if data is the last frame found in the dataset
                let hex = &udp_hex[(frame_index[i] * 8) as usize..udp_hex.len()];
                if hex.len() >= 128{    // Check the frame is larger than a frame header
                    frame_bytes.push(hex);
                }
                else{
                    frame_types.remove(i);
                }
            }
            else{   // for all other frames
                let hex = &udp_hex[(frame_index[i] * 8) as usize..(frame_index[i+1] * 8) as usize];
                if hex.len() >= 128{    // Check the frame is larger than a frame header
                    frame_bytes.push(hex);
                }
                else{
                    frame_types.remove(i);
                }
            }
        }
        frame_bytes.reverse(); // reverse the bytes vector as they were added in reverse
        (frame_bytes, frame_types)
    }
}

pub fn process_neutron_frame(frame_udp: &str, src_ip: &str, wiring_config: &Vec<wiring_config_record>, ev42_fb_packets: &mut Vec<Vec<u8>>){
    let num_words = frame_udp.len() / 8;
    let exp_events = (num_words - 15) / 2;  // could be less if PCB has added padding Zeros
    //println!("{src_ip} NeuF - NW {}, NE {}", num_words, exp_events);
    // println!("{frame_udp}");

    //Process Header
    let (events_in_frame, frame_number, period_num, ppp_in_frame, frame_time_ns) = header_decoder(&frame_udp[0..120]);

    let events_only_hex = &frame_udp[120..];

    let mut events_to_proc = events_in_frame;
    if events_to_proc > exp_events as u32 {
        events_to_proc = exp_events as u32;
        println!("Its Been MELON'ED :( - More Events in FHeader than Packet Size - F: {events_in_frame} - P: {exp_events}");
       // println!(&frame_udp[0..120]);
    }

    let mut tofs:Vec<u32> = Vec::new();
    let mut det_ids:Vec<u32> = Vec::new();
    let mut bldr = FlatBufferBuilder::new();

    // find IP address within the wiring config - get config line
    let mut packet_config_single: &wiring_config_record = wiring_config.first().unwrap();
    let mut packet_config_multi: Vec<&wiring_config_record> = Vec::new();

    let mut num_matches = 0;
    for line in wiring_config{
        if src_ip == line.StreamingIP{
            num_matches += 1;
            packet_config_single = line;
            packet_config_multi.push(line);
        }
    }
    //println!("num matches: {}", num_matches);
    if num_matches >= 1 {   // do we want this for LVDS or have if 1, else if greater than 1?
        match packet_config_single.BRD_Type.as_str() {
            "PC3634M1S" => {(tofs, det_ids) = process_pc3634m1_events(events_only_hex, packet_config_single, events_to_proc); },    // 128CH LVDS Card
            "PC3544MS" => {(tofs, det_ids) = process_pc3544ms_events(events_only_hex, packet_config_multi, events_to_proc); },      // MADC PB
            "PC3877MS" => {(tofs, det_ids) = process_pc3877ms_events(events_only_hex, packet_config_single, events_to_proc); },     // WLSF Streaming Electronics
            _ => {
                println!("MELON'ed -> Unable to PROC -> Unknown BRD Type");
            },
        }

        // println!("Dets: {:?}", det_ids);
        // println!("TOFs: {:?}", tofs);


        // println!("Ne-{}", tofs.len());
        let mut fb_bytes: Vec<u8> = Vec::new(); //vector to hold ev42 bytes
        //encode_ev42(&mut bldr, &mut fb_bytes, "rust_proc", 0,frame_time_ns, &tofs, &det_ids);

        // Trying with EV44 Packets
        encode_ev44(&mut bldr, &mut fb_bytes, "rust_proc", 0,frame_time_ns, &tofs, &det_ids);
        //println!("fb: {:?}", fb_bytes);
        ev42_fb_packets.push(fb_bytes);

    }
    else {
        return;
    }


}

fn process_pc3544ms_events(events_hex: &str, packet_config: Vec<&wiring_config_record>, events_to_proc: u32)-> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();
    match packet_config[0].Packet_Type.as_str() {
        "Position" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let channel = u8::from_str_radix(&binary_event[35..38], 2).unwrap();
                let event_position = u32::from_str_radix(&binary_event[52..64], 2).unwrap();

                let mut channel_config: &wiring_config_record = &packet_config.first().unwrap();
                let mut matches = 0;
                for possible_channel in &packet_config{
                    if channel == possible_channel.CH{
                        channel_config = possible_channel;
                        matches += 1;
                    }
                }
                if matches == 1 {
                    let detector_id = (event_position / (4096 / channel_config.Mantid_Detector_ID_Lenght)) + channel_config.Mantid_DetectorID_Start;
                    let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();

                    tofs.push(event_tof);
                    det_ids.push(detector_id);
                    //println!("{event_i} - {event_hex} - TOF: {event_tof} - AdcCH: {channel} - VAL: {event_position} - DETID: {detector_id}");
                }


            }
            (tofs, det_ids)
        },
        "PulseHeight" => {
            println!("PulseHeight Packet");
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let channel = u8::from_str_radix(&binary_event[35..38], 2).unwrap();
                let event_pulse_height= u32::from_str_radix(&binary_event[40..52], 2).unwrap();

                let mut channel_config: &wiring_config_record = &packet_config.first().unwrap();
                let mut matches = 0;
                for possible_channel in &packet_config{
                    if channel == possible_channel.CH{
                        channel_config = possible_channel;
                        matches += 1;
                    }
                }
                if matches == 1 {
                    let detector_id = (event_pulse_height / (4096 / channel_config.Mantid_Detector_ID_Lenght)) + channel_config.Mantid_DetectorID_Start;
                    let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();

                    tofs.push(event_tof);
                    det_ids.push(detector_id);
                    println!("{event_i} - {event_hex} - TOF: {event_tof} - AdcCH: {channel} - VAL: {event_pulse_height} - DETID: {detector_id}");
                }


            }
            (tofs, det_ids)
        },
        _ => {
            println!("Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        },
    }
}

fn process_pc3634m1_events(events_hex: &str, packet_config: &wiring_config_record, events_to_proc: u32)-> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();

    match packet_config.Packet_Type.as_str() {
        "DIM_OUT" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
                let event_val = u32::from_str_radix(&event_hex[8..16], 16).unwrap();

                let det_id = event_val + packet_config.Mantid_DetectorID_Start;

                tofs.push(event_tof);
                det_ids.push(det_id);

                //println!("{event_i} - {event_hex} - TOF: {event_tof} - VAL: {event_val} - DETID: {det_id}");
            }
            (tofs, det_ids)
        }
        _ => {
            println!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        }
    }
}

fn process_pc3877ms_events(events_hex: &str, packet_config: &wiring_config_record, events_to_proc: u32)-> (Vec<u32>, Vec<u32>) {
    let mut tofs: Vec<u32> = Vec::new();
    let mut det_ids: Vec<u32> = Vec::new();
    match packet_config.Packet_Type.as_str() {
        "Position" => {
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let event_val = u32::from_str_radix(&binary_event[48..64], 2).unwrap();

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
                let det_id = event_val + packet_config.Mantid_DetectorID_Start;

                //println!("pc3877ms - {event_i} - {event_hex} - TOF: {event_tof} - VAL: {event_val} - DETID: {det_id}");
                tofs.push(event_tof);
                det_ids.push(det_id);
            }
            (tofs, det_ids)
        },
        "PulseHeight" => {
            println!("PulseHeight Packet");
            for event_i in 0..events_to_proc {
                let addr = (event_i * 16) as usize;
                let event_hex = &events_hex[addr..addr + 16];
                let binary_event: &str = &hex_to_binary(event_hex);
                let event_val = u32::from_str_radix(&binary_event[36..48], 2).unwrap();

                let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
                let det_id = event_val + packet_config.Mantid_DetectorID_Start;

                tofs.push(event_tof);
                det_ids.push(det_id);
            }
            (tofs, det_ids)
        },
        _ => {
            println!("MELON'ed -> Unable to PROC -> Unknown stream type in config");
            (tofs, det_ids)
        },
    }
}

pub fn header_decoder(header_udp: &str) -> (u32, u32, u16, u16, u64){
    // decodes a frame header

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
    let bin_time :&str = &hex_to_binary(&header_udp[24..40]);  // Get data as binary string

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
    let mut years_days_as_ns:u64 = 0;
    if days != 0{
        // check if days if valid, if its zero the packet likely doesn't have a valid GPS timesource
        let datetime_again: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from(NaiveDate::from_yo_opt(years as i32, days).unwrap()), Utc);
        years_days_as_ns = datetime_again.timestamp() as u64 * 1e9 as u64;
    }
    else{
        println!("MELON'ed - invalid days, set years/day to zero");
    }
    
    

    let hours_as_ns = hours as u64 * 3.6e12 as u64;
    let mins_as_ns = mins as u64 * 6e10 as u64;
    let secs_as_ns = secs as u64 * 1e9 as u64;
    let m_secs_as_ns = m_secs as u64 * 1000000;
    let u_secs_as_ns = u_secs as u64 * 1000;

    let total_ns: u64 = n_secs + u_secs_as_ns + m_secs_as_ns + secs_as_ns + mins_as_ns + hours_as_ns + years_days_as_ns;

    //println!("F: {} - E: {} - PER: {} - PPP: {} - TnS: {}", frame_number, events_in_frame, period_num, ppp_in_frame, total_ns);

    
    // println!("time nS: {}", total_ns);
    (events_in_frame, frame_number, period_num, ppp_in_frame, total_ns)
}

fn group_bytes_by_events(udp_hex: &str, words_per_event: usize) -> Vec<&str>{
    // Splits udp_bytes vec into a Vec of Vec, one per event
    // STD words_per_event will be two. for two words for an event message.

    let chars_per_event: usize = words_per_event * 8;

    let subs = udp_hex.as_bytes()
    .chunks(chars_per_event)
    .map(std::str::from_utf8)
    .collect::<Result<Vec<&str>, _>>()
    .unwrap();
    subs

    // let event_bytes: Vec<Vec<u8>> = udp_hex.chunks(bytes_per_event).map(|c| c.to_vec()).collect();
    // event_bytes
}

fn encode_ev42(bldr: &mut FlatBufferBuilder, dest: &mut Vec<u8>, source_name: &str, message_id: u64, pulse_time: u64, tofs: &Vec<u32>, det_ids: &Vec<u32>){
    dest.clear();
    bldr.reset();

    let args = EventMessageArgs{
        source_name: Option::from(bldr.create_string("DAE_Streamed_RustProc")),
        message_id: message_id,
        pulse_time: pulse_time,
        time_of_flight: Option::from(bldr.create_vector(tofs)),
        detector_id: Option::from(bldr.create_vector(det_ids)),
        facility_specific_data_type: Default::default(),
        facility_specific_data: None,
    };

    let ev42_offset = EventMessage::create(bldr, &args);
    finish_event_message_buffer(bldr, ev42_offset);
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

fn encode_ev44(bldr: &mut FlatBufferBuilder, dest: &mut Vec<u8>, source_name: &str, message_id: u64, pulse_time: u64, tofs: &Vec<u32>, det_ids: &Vec<u32>) {
    dest.clear();
    bldr.reset();

    let mut reference_time: Vec<i64> = Vec::new();
    reference_time.push(pulse_time as i64);

    let mut reference_time_index: Vec<i32> = Vec::new();
    reference_time_index.push(0);

    let mut tofs_i32: Vec<i32> = Vec::new();
    for tof in tofs{
        tofs_i32.push(*tof as i32);
    }

    let mut det_ids_i32: Vec<i32> = Vec::new();
    for det_id in det_ids{
        det_ids_i32.push(*det_id as i32);
    }

    let args = Event44MessageArgs{
        source_name: Option::from(bldr.create_string("DAE_Streamed_RustProc")),
        message_id: message_id as i64,
        reference_time: Option::from(bldr.create_vector(&reference_time)),
        reference_time_index: Option::from(bldr.create_vector(&reference_time_index)),
        time_of_flight: Option::from(bldr.create_vector(&tofs_i32)),
        pixel_id: Option::from(bldr.create_vector(&det_ids_i32)),
    };

    let ev44_offset = Event44Message::create(bldr, &args);
    finish_event_44_message_buffer(bldr, ev44_offset);
    let finished_data = bldr.finished_data();
    dest.extend_from_slice(finished_data);
}

fn hex_to_bool_vec(hex_str: &str) -> Result<Vec<bool>, String> {
    // Check if the hex string is of even length
    if hex_str.len() % 2 != 0 {
        return Err("Hex string length must be even".to_string());
    }

    // Convert the hex string to a vector of u8
    let u8_vec: Vec<u8> = (0..hex_str.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex_str[i..i + 2], 16)
                .map_err(|_| format!("Invalid hex character at position {}", i))
        })
        .collect::<Result<Vec<u8>, String>>()?;

    // Convert the vector of u8 into a vector of bool
    let bool_vec: Vec<bool> = u8_vec.into_iter()
        .flat_map(|byte| {
            (0..8).rev().map(move |i| (byte & (1 << i)) != 0)
        })
        .collect();

    Ok(bool_vec)
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


