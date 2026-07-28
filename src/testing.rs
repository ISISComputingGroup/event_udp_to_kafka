//! Testing & benchmarking utilities.
//!
//! These utilities are not used at runtime.
use crate::udp_message::HEADER_LEN_WORDS;

/// A valid GPS timestamp
pub const TESTING_TIMESTAMP: u64 = (26 << (32 + 24))  // 2026
    + (106 << (32 + 15))  // April 16th
    + (17 << (32 + 10))  // hour 17
    + (9 << (32 + 4))  // minute 9
    + (35 << 30)  // second 35
    + (123 << 20)  // millisecond 123
    + (456 << 10)  // microsecond 456
    + (789); // nanosecond 789

pub const TESTING_TIMESTAMP_NS_SINCE_EPOCH: u64 = 1776359375123456789;

/// Fabricate a valid neutron header.
pub fn make_raw_neutron_udp_header(num_events: usize, ppp: u8, board_num: u16) -> Vec<u8> {
    let packet_length_words = HEADER_LEN_WORDS + (num_events * 2);

    [0xFF; 4] // Header word 0: 'running' header marker
        .iter()
        .chain(&[0xFF]) // Header word 1: marker
        .chain(&(0_u16).to_be_bytes()) // Header word 1: header type
        .chain(&[HEADER_LEN_WORDS as u8]) // Header word 1: header length
        .chain(&(board_num.to_be_bytes())) // Header word 2: board ID
        .chain(&[0x00, 0xFF]) // Header word 2: flags all-high (neutron)
        .chain(&TESTING_TIMESTAMP.to_be_bytes()) // Header words 3 & 4: GPS timestamp
        .chain(&[0_u8; 4]) // Header word 5: frame number
        .chain(&[0_u8; 2]) // Header word 6: period number
        .chain(&[0_u8; 2]) // Header word 6: unused
        .chain(&(num_events as u32).to_be_bytes()) // Header word 7: events in frame
        .chain(&(packet_length_words as u16).to_be_bytes()) // Header word 8: packet length in words
        .chain(&[0]) // Header word  8: unused byte
        .chain(&[ppp]) // Header word 8: protons-per-pulse
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
