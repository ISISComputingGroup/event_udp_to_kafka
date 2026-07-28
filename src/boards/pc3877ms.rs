use crate::WiringConfigRecord;
use crate::boards::{Board, EventData, TestableBoard};
use anyhow::{Context, bail};
use std::net::{IpAddr, Ipv4Addr};

pub struct Pc3877ms;

impl Board for Pc3877ms {
    const BOARD_ID: u16 = 3877;

    fn parse_raw_data(
        data: &[u8],
        wiring_config: &[&WiringConfigRecord],
    ) -> anyhow::Result<EventData> {
        const CLOCK_TICKS_TO_NS: u32 = 20;

        if !data.len().is_multiple_of(8) {
            bail!("Event data is not a multiple of pairs of 4-byte words");
        }

        if wiring_config.len() > 1 {
            bail!("Got multiple wiring config rows for a PC3877 packet; this is invalid")
        }

        let packet_config = wiring_config.first().context("No wiring config")?;

        let (time_of_flight, pixel_id) = match packet_config.packet_type.as_str() {
            "Position" => data
                .as_chunks::<8>()
                .0
                .iter()
                .map(|event| {
                    let mut tof = u32::from_be_bytes(event[0..4].try_into().unwrap()) & 0xFFFFFF;
                    tof *= CLOCK_TICKS_TO_NS;

                    let mut val = u32::from_be_bytes(event[4..8].try_into().unwrap()) & 0xFFFF;
                    val += packet_config.mantid_detector_id_start;

                    (tof as i32, val as i32)
                })
                .unzip(),
            "PulseHeight" => data
                .as_chunks::<8>()
                .0
                .iter()
                .map(|event| {
                    let mut val =
                        (u32::from_be_bytes(event[4..8].try_into().unwrap()) >> 16) & 0xFFF;
                    val += packet_config.mantid_detector_id_start;

                    let tof = (u32::from_be_bytes(event[0..4].try_into().unwrap())) & 0xFFFFFF;

                    (tof as i32, val as i32)
                })
                .unzip(),
            _ => {
                bail!("Unable to process PC3877MS events: unknown stream type in config");
            }
        };

        EventData::new(time_of_flight, pixel_id)
    }
}

impl TestableBoard for Pc3877ms {
    fn make_fake_event() -> Vec<u8> {
        // tof = 456000 ns
        // val = 123
        vec![
            0xFF, 0, 89, 16, // 20ns (scaling) * (89 * 256 + 16) = 456000ns
            0xFF, 0xFF, 0, 123, // Position 123
        ]
    }

    fn make_fake_wiring_config(src_ip: Option<IpAddr>) -> Vec<WiringConfigRecord> {
        vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            packet_type: "Position".to_owned(),
            sw_pos: 0,
            streaming_ip: src_ip.unwrap_or(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1))),
            ch: 0,
            mantid_detector_id_start: 0,
            mantid_detector_id_length: 1,
            comment: "".to_owned(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pc3877_event() {
        let data = Pc3877ms::make_fake_event();
        let wiring = Pc3877ms::make_fake_wiring_config(None);

        let events = Pc3877ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert_eq!(events.time_of_flight(), [456_000]);
        assert_eq!(events.pixel_id(), [123]);
    }

    #[test]
    fn test_parse_pc3877_event_invalid_length() {
        let data = vec![0; 9]; // invalid length: 9 bytes
        let wiring = Pc3877ms::make_fake_wiring_config(None);

        let events = Pc3877ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>());

        assert!(events.is_err_and(|e| {
            e.to_string()
                .contains("not a multiple of pairs of 4-byte words")
        }));
    }

    #[test]
    fn test_parse_empty_pc3877ms_events() {
        let data = [];
        let wiring = Pc3877ms::make_fake_wiring_config(None);

        let events = Pc3877ms::parse_raw_data(&data, &wiring.iter().collect::<Vec<_>>())
            .expect("parsing should work");

        assert!(events.is_empty())
    }
}
