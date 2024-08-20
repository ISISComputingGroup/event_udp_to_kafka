use std::io::Bytes;

fn process_udp_to_kafka(udp_hex: &str) -> Vec<&str>{

    // Split into the different frames in the packet

    // Process each found frame
        // if empty frame discount quickly - by len

        // else if neutron

        // else if Veto

        // else if SE

    let returned: Vec<&str> = Vec::new();
    returned
}

fn packet_to_frames(udp_hex: &str) -> (Vec<&str>, Vec<u8>){
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
        for i in 0..frame_index.len(){
            if i == frame_index.len()-1{
                frame_bytes.push(&udp_hex[(frame_index[i] * 8) as usize..udp_hex.len()]);

            }
            else{
                frame_bytes.push(&udp_hex[(frame_index[i] * 8) as usize..(frame_index[i+1] * 8) as usize]);
            }
        }
        (frame_bytes, frame_types)
    }
}

pub fn header_decoder(bytes: Vec<u8>){
    let hex_word = "ffffffff";
    let non_header = "00000000";
    let mut hex: String = "".to_string();
    for i in 0..16 {
        println!("{i}");
        hex.push_str(hex_word);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
        hex.push_str(non_header);
    }


    // let u8_vec = hex_to_u8_vec(hex).unwrap();
    let bin = hex_to_bool_vec(&hex).unwrap();
    println!("hex: {hex}");
    println!("bin_len: {}", bin.len());

    // let words = group_bytes_by_events(&hex, 2);
    let frames = packet_to_frames(&hex);
    println!("{frames:?}");
    //println!("{words:?}");


    // let header_bytes = bytes[0..128];
    // for byte in header_bytes{
    //
    // }
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


