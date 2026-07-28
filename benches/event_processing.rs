use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_udp_to_kafka::boards::TestableBoard;
use event_udp_to_kafka::boards::pc3544ms::Pc3544ms;
use event_udp_to_kafka::boards::pc3634m1s::Pc3634m1s;
use event_udp_to_kafka::boards::pc3877ms::Pc3877ms;
use event_udp_to_kafka::data_processing::process_udp_bytes_to_kafka;
use event_udp_to_kafka::testing::make_raw_neutron_udp_header;
use flatbuffers::FlatBufferBuilder;
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};

fn make_raw_udp_message<T>(num_events: usize) -> Vec<u8>
where
    T: TestableBoard,
{
    make_raw_neutron_udp_header(num_events, 123, T::BOARD_ID)
        .iter()
        .chain(&T::make_fake_event().repeat(num_events))
        .copied()
        .collect()
}

fn benchmark_board<T>(c: &mut Criterion)
where
    T: TestableBoard,
{
    let mut fbb = FlatBufferBuilder::new();

    let raw_data = make_raw_udp_message::<T>(100);
    let n_bytes = raw_data.len();

    let src_ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
    let wiring_config = T::make_fake_wiring_config(Some(src_ip));

    let mut group = c.benchmark_group("message_processing");
    group.throughput(Throughput::Bytes(n_bytes as u64));

    group.bench_with_input(
        BenchmarkId::from_parameter(T::BOARD_ID),
        &wiring_config,
        |b, wiring_config| {
            b.iter(|| {
                process_udp_bytes_to_kafka(
                    &mut fbb,
                    black_box(&raw_data),
                    black_box(&src_ip),
                    wiring_config,
                    |msg| {
                        black_box(msg);
                    },
                )
            })
        },
    );
}

criterion_group! {
    benches,
    benchmark_board::<Pc3877ms>,
    benchmark_board::<Pc3544ms>,
    benchmark_board::<Pc3634m1s>
}
criterion_main!(benches);
