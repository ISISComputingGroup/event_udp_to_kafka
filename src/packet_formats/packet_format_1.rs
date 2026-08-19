use crate::bit_utils::{extract_msb, mask};
use crate::metrics::INVALID_NEUTRON_EVENTS;
use crate::packet_formats::{EventData, PacketFormat, TestablePacketFormat};
use anyhow::bail;
use metrics::counter;

/// Decoder for 'packet format 1'.
///
/// The packet format is defined by a common part of the UDP header emitted by hardware.
pub struct PacketFormat1;

impl PacketFormat for PacketFormat1 {
    /// Packet format code emitted by the hardware is 1 for this type of packet.
    const PACKET_FORMAT_CODE: u16 = 1;

    /// Decode board-specific parameters + data into events
    fn parse_raw_data(board_specific_parameters: &[u8], data: &[u8]) -> anyhow::Result<EventData> {
        const CLOCK_TICKS_TO_NS: u32 = 20;

        if !data.len().is_multiple_of(8) {
            bail!("PacketFormat1 Event data is not a multiple of pairs of 4-byte words");
        }
        if board_specific_parameters.len() != 8 {
            bail!("PacketFormat1 board-specific parameters should have length 8");
        }

        let channel_bits = board_specific_parameters[1];
        let position_bits_per_channel = board_specific_parameters[3];

        let detector_id_offset = u32::from_be_bytes(board_specific_parameters[4..8].try_into()?);

        // Bit-mask for selecting position out of the second word in each event.
        let pos_mask = mask(position_bits_per_channel.into());

        // Each increment of 'channel' increments pixel_id by this many pixels.
        let channel_multiplier = 1_u32.unbounded_shl(position_bits_per_channel as u32);

        // Number of invalid neutron events that didn't start with a correct 7-bit header for
        // an event.
        let mut invalid_events = 0;

        let (time_of_flight, pixel_id) = data
            .as_chunks::<8>()
            .0
            .iter()
            .map(|event| {
                let tof_word =
                    u32::from_be_bytes(event[0..4].try_into().expect("slice of length 4"));
                let pos_word =
                    u32::from_be_bytes(event[4..8].try_into().expect("slice of length 4"));

                // Top 7 bits of ToF word should always be 0b1110000 for event data.
                if (event[0] & 0b11111110) != 0b11100000 {
                    invalid_events += 1;
                }

                let mut tof = tof_word & mask(25);
                tof *= CLOCK_TICKS_TO_NS;

                // If we were told channel_bits = 0, we'll get None, which we want to treat as
                // being channel 0 (since that is then the only possible channel)
                let channel = extract_msb(pos_word, channel_bits.into()).unwrap_or(0);
                let mut pos = pos_word & pos_mask;

                pos += detector_id_offset;
                pos += channel * channel_multiplier;

                (tof as i32, pos as i32)
            })
            .unzip();

        if invalid_events > 0 {
            // If we encountered invalid events, it might be due to:
            // - A UDP packet corruption
            // - Parsing non-event data as events, which could be due to a software bug or a
            //   firmware bug (e.g. the header declared a data length which didn't correspond
            //   with the real data length).
            //
            // In either case, the safest thing to do is to bail and refuse to interpret this
            // entire message; something has gone badly wrong, and the events cannot be
            // trusted (even the ones that *happened* to start with the right prefix)
            counter!(INVALID_NEUTRON_EVENTS).increment(invalid_events);
            bail!(
                "PacketFormat1 decoder encountered invalid neutron events; dropping all events in this message."
            )
        }

        EventData::new(time_of_flight, pixel_id)
    }
}

impl TestablePacketFormat for PacketFormat1 {
    const FAKE_EVENT_TOF: i32 = 456_000;
    const FAKE_EVENT_PIXEL: i32 = 282828 + 1234 + (2 * (2_i32.pow(12)));

    fn make_fake_event() -> Vec<u8> {
        // Position packet
        // tof = 456000 ns
        // channel 2, position 1234
        vec![
            0xE0, // E0 tof marker
            0x00, 0x59, 0x10, // 456000ns
            2,    // Channel 2
            0xFF, 0xF4, 0xD2, // 0xFFF = diagnostic data, 0x4D2 = position 1234
        ]
    }

    fn make_fake_board_specific_header_data() -> Vec<u8> {
        [
            0x78, // Board address
            8,    // Bits for channel
            12,   // Bits for diagnostic data
            12,   // Bits for position data
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
    fn test_parse_event() {
        let data = PacketFormat1::make_fake_event();
        let board_specific_header_data = PacketFormat1::make_fake_board_specific_header_data();

        let events = PacketFormat1::parse_raw_data(&board_specific_header_data, &data)
            .expect("parsing should work");

        assert_eq!(events.time_of_flight(), [PacketFormat1::FAKE_EVENT_TOF]);
        assert_eq!(events.pixel_id(), [PacketFormat1::FAKE_EVENT_PIXEL]);
    }

    #[test]
    fn test_parse_empty_events() {
        let data = [];
        let board_specific_header_data = PacketFormat1::make_fake_board_specific_header_data();

        let events = PacketFormat1::parse_raw_data(&board_specific_header_data, &data)
            .expect("parsing should work");

        assert!(events.is_empty())
    }

    #[test]
    fn test_parse_event_invalid_length() {
        let data = vec![0; 9]; // invalid length: 9 bytes
        let board_specific_header_data = PacketFormat1::make_fake_board_specific_header_data();

        let events = PacketFormat1::parse_raw_data(&board_specific_header_data, &data);

        assert!(events.is_err_and(|e| {
            e.to_string()
                .contains("not a multiple of pairs of 4-byte words")
        }));
    }
}
