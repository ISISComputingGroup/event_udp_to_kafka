use crate::boards::pc3544ms::Pc3544ms;
use crate::boards::pc3634m1s::Pc3634m1s;
use crate::boards::pc3877ms::Pc3877ms;
use crate::event_data::EventData;
use anyhow::anyhow;

pub mod pc3544ms;
pub mod pc3634m1s;
pub mod pc3877ms;

/// Trait defining a 'board', which contains logic for transforming raw UDP data
/// into an ``EventData`` struct.
pub trait Board {
    /// The board identifier, as transmitted in the UDP header, for this board.
    /// For example, the identifier for the "pc3544ms" board is '3544'.
    const BOARD_ID: u16;

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

/// Testing utilities for a board. These may be used by unit tests and benchmarks,
/// but are not used at runtime.
pub trait TestableBoard: Board {
    /// Manufacture a single fake event. The data contained by this fake event is arbitrary,
    /// but should be valid for the type of board.
    /// This data is used for benchmarking and testing, so should aim to be representative
    /// of a typical real event.
    fn make_fake_event() -> Vec<u8>;

    /// Return a board-specific header data suitable for decoding messages from this board.
    ///
    /// This should be representative of a real configuration; for example, if the board
    /// would typically have multiple channels, this should use a non-zero channel.
    fn make_fake_board_specific_header_data() -> Vec<u8>;
}

pub fn parse_board_data(
    board_type: u16,
    board_specific_parameters: &[u8],
    data: &[u8],
) -> anyhow::Result<EventData> {
    if data.is_empty() {
        return Ok(EventData::default());
    }
    match board_type {
        Pc3544ms::BOARD_ID => Pc3544ms::parse_raw_data(board_specific_parameters, data),
        Pc3634m1s::BOARD_ID => Pc3634m1s::parse_raw_data(board_specific_parameters, data),
        Pc3877ms::BOARD_ID => Pc3877ms::parse_raw_data(board_specific_parameters, data),
        _ => Err(anyhow!("unknown board type {}", board_type)),
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
                .is_err_and(|e| e.to_string().contains("unknown board type"))
        );
    }
}
