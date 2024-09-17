use std::io::Bytes;
use std::u32;
use serde_json::from_str;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use chrono::prelude::*;

pub fn process_udp_to_kafka(udp_hex: &str) -> Vec<&str>{
    use std::time::Instant;
    let now = Instant::now();

    // make the vector for the product now
    let kafka_bytes: Vec<&str> = Vec::new();

    // Split into the different frames in the packet
    // Filters any empty frames each time
    let (frames_udp, frames_types) = packet_to_frames(udp_hex);
    if frames_types.len() == 0{
        let elapsed = now.elapsed();
       // println!("Elapsed: {:.2?}", elapsed);
        kafka_bytes
    }
    else {  // If there are valid frames to deal with
        println!("NFilt: {} - Types: {:?}", frames_udp.len(), frames_types);
        //println!("UDP data: {:?}", frames_udp);
        for frame_i in 0..frames_udp.len() {
            match frames_types[frame_i] {
                1 => {
                    //println!("PROC For Neutron Frame Header - {:?}", frames_udp[frame_i]);
                    process_neutron_frame(frames_udp[frame_i]);
                },
                2 => println!("PROC For Veto Frame Header"),
                3 => println!("PROC For SE Frame Header"),
                _ => println!("Undefined frame type")
            }
        }
        let elapsed = now.elapsed();
        println!("Elapsed: {:.2?}", elapsed);
        println!();

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
            if i == frame_index.len()-1{
                let hex = &udp_hex[(frame_index[i] * 8) as usize..udp_hex.len()];
                if hex.len() >= 128{    //Size checking here to see if its worth adding to the list of frames
                    frame_bytes.push(hex);
                }
                else{
                    frame_types.remove(i);
                }
            }
            else{
                let hex = &udp_hex[(frame_index[i] * 8) as usize..(frame_index[i+1] * 8) as usize];
                if hex.len() >= 128{    //Size checking here to see if its worth adding to the list of frames
                    frame_bytes.push(hex);
                }
                else{
                    frame_types.remove(i);
                }
            }
        }
        (frame_bytes, frame_types)
    }
}

pub fn process_neutron_frame(frame_udp: &str){
    let num_words = frame_udp.len() / 8;
    let exp_events = (num_words - 15) / 2;  // could be less if PCB has added padding Zeros
    println!("NeuF - NW {}, NE {}", num_words, exp_events);
    //println!("{frame_udp}");

    //Process Header
    let (events_in_frame, frame_number, period_num, ppp_in_frame, total_ns) = header_decoder(&frame_udp[0..120]);

    let events_only_hex = &frame_udp[120..];

    let mut events_to_proc = events_in_frame;
    if events_to_proc > exp_events as u32 {
        events_to_proc = exp_events as u32;
        println!("More Events in FHeader than Packet Size - F: {events_in_frame} - P: {exp_events}");
    }

    let mut tofs:Vec<u32> = Vec::new();
    let mut vals:Vec<u32> = Vec::new();

    // For each of the expected events
    for event_i in 0..events_to_proc{
        let addr = (event_i * 16) as usize;
        let event_hex = &events_only_hex[addr..addr + 16];

        let event_tof = u32::from_str_radix(&event_hex[2..8], 16).unwrap();
        let event_val = u32::from_str_radix(&event_hex[8..16], 16).unwrap();

        tofs.push(event_tof);
        vals.push(event_val);

        // Add code here to Map values to detector IDs

        //println!("{event_i} - {event_hex} - TOF: {event_tof} - VAL: {event_val}");

    }
    println!("Got - {}", tofs.len());


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
    let bin_time :&str = &bin_to_hex(&header_udp[24..40]);  // Get data as binary string

    let years = u16::from_str_radix(&bin_time[0..8], 2).unwrap() + 2000;
    let days = u32::from_str_radix(&bin_time[8..17], 2).unwrap();
    let hours = u8::from_str_radix(&bin_time[17..22], 2).unwrap();
    let mins = u8::from_str_radix(&bin_time[22..28], 2).unwrap();
    let secs = u8::from_str_radix(&bin_time[28..34], 2).unwrap();
    let m_secs = u16::from_str_radix(&bin_time[34..44], 2).unwrap();
    let u_secs = u16::from_str_radix(&bin_time[44..54], 2).unwrap();
    let n_secs = u64::from_str_radix(&bin_time[54..64], 2).unwrap();

    let datetime_again: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from(NaiveDate::from_yo_opt(years as i32, days).unwrap()), Utc);
    let years_days_as_ns = datetime_again.timestamp() as u64 * 1e9 as u64;

    let hours_as_ns = hours as u64 * 3.6e12 as u64;
    let mins_as_ns = mins as u64 * 6e10 as u64;
    let secs_as_ns = secs as u64 * 1e9 as u64;
    let m_secs_as_ns = m_secs as u64 * 1000000;
    let u_secs_as_ns = u_secs as u64 * 1000;

    let total_ns: u64 = n_secs + u_secs_as_ns + m_secs_as_ns + secs_as_ns + mins_as_ns + hours_as_ns + years_days_as_ns;

    println!("F: {} - E: {} - PER: {} - PPP: {} - TnS: {}", frame_number, events_in_frame, period_num, ppp_in_frame, total_ns);
    println!("time: Y-{years}:D-{days}:H-{hours}:M-{mins}:S-{secs}:mS-{m_secs}:uS-{u_secs}:nS-{n_secs}");
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

fn bin_to_hex(hex: &str) -> String {
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
        // 'A' => "1010",
        // 'B' => "1011",
        // 'C' => "1100",
        // 'D' => "1101",
        // 'E' => "1110",
        // 'F' => "1111",
        _ => "",
    }
}


