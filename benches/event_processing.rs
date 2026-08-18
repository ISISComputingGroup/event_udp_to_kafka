use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_udp_to_kafka::boards::TestableBoard;
use event_udp_to_kafka::boards::pc3544ms::Pc3544ms;
use event_udp_to_kafka::boards::pc3634m1s::Pc3634m1s;
use event_udp_to_kafka::boards::pc3877ms::Pc3877ms;
use event_udp_to_kafka::data_processing::process_udp_bytes_to_kafka;
use event_udp_to_kafka::testing::{make_udp_packet};
use flatbuffers::FlatBufferBuilder;
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};

fn benchmark_board<T>(c: &mut Criterion)
where
    T: TestableBoard,
{
    let mut fbb = FlatBufferBuilder::new();

    let raw_data = make_udp_packet::<T>(100, 123);
    let n_bytes = raw_data.len();

    let src_ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

    let mut group = c.benchmark_group("message_processing");
    group.throughput(Throughput::Bytes(n_bytes as u64));

    group.bench_function(BenchmarkId::from_parameter(T::BOARD_ID), |b| {
        b.iter(|| {
            process_udp_bytes_to_kafka(&mut fbb, black_box(&raw_data), black_box(&src_ip), |msg| {
                black_box(msg);
            })
        })
    });
}

criterion_group! {
    benches,
    benchmark_board::<Pc3877ms>,
    benchmark_board::<Pc3544ms>,
    benchmark_board::<Pc3634m1s>
}
criterion_main!(benches);
