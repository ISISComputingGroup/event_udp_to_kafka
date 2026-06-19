//! Testing & benchmarking utilities.
//!
//! These utilities are not used at runtime.
use crate::udp_message::{HEADER_LEN_WORDS, NEUTRON_HEADER};

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
pub fn make_raw_neutron_udp_header(num_events: usize, ppp: u8) -> Vec<u8> {
    let packet_length_words = HEADER_LEN_WORDS + (num_events * 2);

    [0xFF; 4] // Header word 0: 'running' header marker
        .iter()
        .chain(NEUTRON_HEADER) // Header word 1: neutron data header marker
        .chain(&[0_u8; 4]) // Header word 2: information
        .chain(&[0_u8; 4]) // Header word 3: frame number
        .chain(&TESTING_TIMESTAMP.to_be_bytes()) // Header words 4 & 5: GPS timestamp
        .chain(&[0_u8; 2]) // Header word 6: period number
        .chain(&[0_u8; 2]) // Header word 6: unused
        .chain(&(num_events as u32).to_be_bytes()) // Header word 7: events in frame
        .chain(&[ppp]) // Header word 8: ppp_in_frame
        .chain(&[0_u8; 3]) // Header word 8: unused bits
        .chain(&[0_u8; 4]) // Header word 9: vetoes
        .chain(&[0_u8; 4]) // Header word 10: address of next frame
        .chain(&[0_u8; 4]) // Header word 11: address of next frame (word address)
        .chain(&[0_u8; 4]) // Header word 12: streamed frame number
        .chain(&(packet_length_words as u32).to_be_bytes()) // Header word 13: number of 32-bit words in *this* message (header + data)
        .chain(&[0_u8; 4]) // Header word 14: not used
        .chain(&[0_u8; 4]) // Header word 15: not used
        .copied()
        .collect()
}
