use crate::event_data::EventData;
use crate::packet_formats::packet_format_1::PacketFormat1;
use anyhow::anyhow;

pub mod packet_format_1;

/// Trait defining a 'board', which contains logic for transforming raw UDP data
/// into an ``EventData`` struct.
pub trait PacketFormat {
    /// The packet format identifier, as transmitted in the UDP header.
    /// Also called "header type", though it also defines how the event-data
    /// itself should be decoded.
    const PACKET_FORMAT_CODE: u16;

    /// Decoding logic for event data from this board.
    /// Should return an ``Err`` if the data is invalid, for example an invalid length or
    /// invalid contents of the event data.
    /// Otherwise, should return ``Ok<EventData>``.
    ///
    /// Arguments:
    /// - ``data``: the raw UDP data to be parsed. Excludes all header bytes. May be empty.
    /// - ``wiring_config``: slice of ``WiringConfigRecord``s which correspond to the source IP
    ///   of this packet. This may contain multiple entries.
    fn parse_raw_data(board_specific_parameters: &[u8], data: &[u8]) -> anyhow::Result<EventData>;
}

/// Testing utilities for a packet format. These may be used by unit tests and benchmarks,
/// but are not used at runtime.
pub trait TestablePacketFormat: PacketFormat {
    const FAKE_EVENT_TOF: i32;
    const FAKE_EVENT_PIXEL: i32;

    /// Manufacture a single fake event. The data contained by this fake event is arbitrary,
    /// but should be valid for the type of board.
    /// This data is used for benchmarking and testing, so should aim to be representative
    /// of a typical real event.
    ///
    /// The combination of `make_fake_event` and `make_fake_board_specific_header_data` should
    /// produce an event with tof `FAKE_EVENT_TOF` and pixel `FAKE_EVENT_PIXEL`.
    fn make_fake_event() -> Vec<u8>;

    /// Return a board-specific header data suitable for decoding messages from this board.
    ///
    /// This should be representative of a real configuration; for example, if the board
    /// would typically have multiple channels, this should use a non-zero channel.
    fn make_fake_board_specific_header_data() -> Vec<u8>;
}

pub fn parse_board_data(
    packet_format_code: u16,
    board_specific_parameters: &[u8],
    data: &[u8],
) -> anyhow::Result<EventData> {
    if data.is_empty() {
        return Ok(EventData::default());
    }
    match packet_format_code {
        PacketFormat1::PACKET_FORMAT_CODE => {
            PacketFormat1::parse_raw_data(board_specific_parameters, data)
        }
        unknown => Err(anyhow!("unknown packet format code {}", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_board_data() {
        // empty data -> empty event list regardless of board type
        assert!(parse_board_data(0, &[], &[]).is_ok());

        // If a board type is provided but we don't have a handler for it, should get an error
        assert!(
            parse_board_data(0, &[], &[0; 8])
                .is_err_and(|e| e.to_string().contains("unknown packet format code"))
        );
    }
}
