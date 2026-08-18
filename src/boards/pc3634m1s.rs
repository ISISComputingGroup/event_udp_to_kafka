use crate::bit_utils::{extract_msb, mask};
use crate::boards::{Board, EventData, TestableBoard};
use crate::metrics::INVALID_NEUTRON_EVENTS;
use anyhow::{bail};
use metrics::counter;

pub struct Pc3634m1s;

impl Board for Pc3634m1s {
    const BOARD_ID: u16 = 3634;

    fn parse_raw_data(board_specific_parameters: &[u8], data: &[u8]) -> anyhow::Result<EventData> {
        if !data.len().is_multiple_of(8) {
            bail!("Pc3634m1s Event data is not a multiple of pairs of 4-byte words");
        }
        if board_specific_parameters.len() != 8 {
            bail!("Pc3634m1s board-specific parameters should have length 8 bytes")
        }

        const CLOCK_TICKS_TO_NS: u32 = 20;

        let channel_bits = board_specific_parameters[1];
        let position_bits_per_channel = board_specific_parameters[3];

        let detector_id_offset = u32::from_be_bytes(board_specific_parameters[4..8].try_into()?);

        let (time_of_flight, pixel_id) = data
            .as_chunks::<8>()
            .0
            .iter()
            .filter_map(|event| {
                let tof_word = u32::from_be_bytes(event[0..4].try_into().unwrap());
                let pos_word = u32::from_be_bytes(event[4..8].try_into().unwrap());

                // Top 7 bits of ToF word should always be 0b1110000 for event data.
                // If it isn't, something has gone wrong and we shouldn't use this event.
                if extract_msb(tof_word, 7) != Some(0b1110000) {
                    counter!(INVALID_NEUTRON_EVENTS).increment(1);
                    return None;
                }
                let mut tof = tof_word & mask(25);
                tof *= CLOCK_TICKS_TO_NS;

                // If we were told channel_bits = 0, we'll get None, which we want to treat as
                // being channel 0 (since that is then the only possible channel)
                let channel = extract_msb(pos_word, channel_bits.into()).unwrap_or(0);
                let mut pos = pos_word & mask(position_bits_per_channel.into());

                pos += detector_id_offset;
                pos += channel * (1_u32.unbounded_shl(position_bits_per_channel as u32));

                Some((tof as i32, pos as i32))
            })
            .unzip();

        EventData::new(time_of_flight, pixel_id)
    }
}

impl TestableBoard for Pc3634m1s {
    fn make_fake_event() -> Vec<u8> {
        // tof = 456000 ns
        // detector ID = 123456789
        vec![
            0xE0, 0x06, 0xF5, 0x40, // E0 tof marker + 456000ns
            0x07, 0x5B, 0xCD, 0x15, // Detector ID = 123456789
        ]
    }

    fn make_fake_board_specific_header_data() -> Vec<u8> {
        [
            0x78, // Board address
            0,    // Bits for channel
            0,    // Bits for diagnostic data
            32,    // Bits for position data
        ]
        .into_iter()
        .chain(282828_u32.to_be_bytes()) // Detector ID offset 282828
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pc3634_event() {
        let data = Pc3634m1s::make_fake_event();
        let board_specific_header_data = Pc3634m1s::make_fake_board_specific_header_data();

        let events = Pc3634m1s::parse_raw_data(&board_specific_header_data, &data)
            .expect("parsing should work");

        assert_eq!(events.time_of_flight(), [456_000]);
        assert_eq!(events.pixel_id(), [123456789]);
    }

    #[test]
    fn test_parse_empty_pc3634m1s_events() {
        let data = [];
        let board_specific_header_data = Pc3634m1s::make_fake_board_specific_header_data();

        let events = Pc3634m1s::parse_raw_data(&board_specific_header_data, &data)
            .expect("parsing should work");

        assert!(events.is_empty())
    }

    #[test]
    fn test_parse_pc3634_event_invalid_length() {
        let data = vec![0; 9]; // invalid length: 9 bytes
        let board_specific_header_data = Pc3634m1s::make_fake_board_specific_header_data();

        let events = Pc3634m1s::parse_raw_data(&board_specific_header_data, &data);

        assert!(events.is_err_and(|e| {
            e.to_string()
                .contains("not a multiple of pairs of 4-byte words")
        }));
    }
}
