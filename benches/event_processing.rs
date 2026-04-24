use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use flatbuffers::FlatBufferBuilder;
use rust_data_stream_processor::WiringConfigRecord;
use rust_data_stream_processor::data_processing::process_udp_to_kafka;
use std::hint::black_box;

const VALID_TIMESTAMP: u64 = (26 << (32 + 24))
    + (106 << (32 + 15))
    + (17 << (32 + 10))
    + (9 << (32 + 4))
    + (35 << 30)
    + (123 << 20)
    + (456 << 10)
    + (789);

fn make_raw_udp_message(num_events: usize) -> Vec<u8> {
    // Note: 4-byte words
    // Total header length: 60 bytes (15 words)
    [255_u8; 4] // Header word 0: 'running' header marker
        .iter()
        .chain(&[255_u8; 4]) // Header word 1: neutron data header marker
        .chain(&[0_u8; 4]) // Header word 2: information
        .chain(&[0_u8; 4]) // Header word 3: frame number
        .chain(&VALID_TIMESTAMP.to_be_bytes()) // Header words 4 & 5: GPS timestamp
        .chain(&[0_u8; 2]) // Header word 6: period number
        .chain(&[0_u8; 2]) // Header word 6: unused
        .chain(&(num_events as u32).to_be_bytes()) // Header word 7: events in frame
        .chain(&[0_u8; 2]) // Header word 8: ppp_in_frame
        .chain(&[0_u8; 2]) // Header word 8: unused
        .chain(&[0_u8; 4]) // Header word 9: vetoes
        .chain(&[0_u8; 4]) // Header word 10: address of next frame
        .chain(&[0_u8; 4]) // Header word 11: unknown
        .chain(&[0_u8; 4]) // Header word 12: unknown
        .chain(&[0_u8; 4]) // Header word 13: unknown
        .chain(&[0_u8; 4]) // Header word 14: unknown
        .chain(&[0_u8; 4]) // Header word 15: unknown
        .chain(&vec![0_u8; num_events * 8]) // 8-byte event messages
        .copied()
        .collect()
}

fn benchmark_message_processing(c: &mut Criterion) {
    let raw_data = make_raw_udp_message(100);
    let n_bytes = raw_data.len();
    let data = hex::encode(raw_data);

    let mut group = c.benchmark_group("message_processing");
    group.throughput(Throughput::Bytes(n_bytes as u64));

    let mut fbb = FlatBufferBuilder::new();

    for board_type in ["PC3877MS", "PC3544MS", "PC3634M1S"] {
        let wiring_config = vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            brd_type: board_type.to_owned(),
            packet_type: "Position".to_owned(),
            sw_pos: 0,
            streaming_ip: "192.168.1.1".to_owned(),
            ch: 0,
            mantid_detector_id_start: 0,
            mantid_detector_id_length: 1,
            comment: "WLSF Module".to_owned(),
        }];

        group.bench_with_input(
            BenchmarkId::from_parameter(board_type),
            &wiring_config,
            |b, wiring_config| {
                b.iter(|| {
                    process_udp_to_kafka(
                        &mut fbb,
                        black_box(&data),
                        black_box("192.168.1.1"),
                        &wiring_config,
                    )
                })
            },
        );
    }
}

criterion_group! {
    benches, benchmark_message_processing
}
criterion_main!(benches);
