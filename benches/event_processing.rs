use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_udp_to_kafka::WiringConfigRecord;
use event_udp_to_kafka::data_processing::process_udp_bytes_to_kafka;
use event_udp_to_kafka::testing::make_raw_neutron_udp_header;
use flatbuffers::FlatBufferBuilder;
use std::hint::black_box;

fn make_raw_udp_message(num_events: usize) -> Vec<u8> {
    make_raw_neutron_udp_header(num_events, 123)
        .iter()
        .chain(&vec![0_u8; num_events * 8]) // 8-byte event messages
        .copied()
        .collect()
}

fn benchmark_message_processing(c: &mut Criterion) {
    let raw_data = make_raw_udp_message(100);
    let n_bytes = raw_data.len();

    let mut group = c.benchmark_group("message_processing");
    group.throughput(Throughput::Bytes(n_bytes as u64));

    let mut fbb = FlatBufferBuilder::new();

    for (board_type, packet_type) in [
        ("PC3877MS", "Position"),
        ("PC3544MS", "Position"),
        ("PC3634M1S", "DIM_OUT"),
    ] {
        let wiring_config = vec![WiringConfigRecord {
            brd_num: 0,
            brd_ref: "WLSF0".to_owned(),
            brd_type: board_type.to_owned(),
            packet_type: packet_type.to_owned(),
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
                    process_udp_bytes_to_kafka(
                        &mut fbb,
                        black_box(&raw_data),
                        black_box("192.168.1.1"),
                        &wiring_config,
                        |msg| {
                            black_box(msg);
                        },
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
