use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use event_udp_to_kafka::config::EventUdpToKafkaConfig;
use event_udp_to_kafka::data_processing::process_udp_bytes_to_kafka;
use event_udp_to_kafka::packet_formats::TestablePacketFormat;
use event_udp_to_kafka::packet_formats::packet_format_1::PacketFormat1;
use event_udp_to_kafka::testing::make_udp_packet;
use flatbuffers::FlatBufferBuilder;
use std::hint::black_box;
use std::net::{IpAddr, Ipv4Addr};

fn benchmark_packet_format<T>(c: &mut Criterion)
where
    T: TestablePacketFormat,
{
    let mut fbb = FlatBufferBuilder::new();

    // 10 concatenated messages, each of which contains 100 neutron events
    // This is ~10000 bytes in total, which approximately matches a jumbo UDP message
    // so should be representative of real workload.
    let raw_data = make_udp_packet::<T>(100, 123).repeat(10);
    let n_bytes = raw_data.len();

    let src_ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));

    let mut group = c.benchmark_group("message_processing");
    group.throughput(Throughput::Bytes(n_bytes as u64));

    let config = EventUdpToKafkaConfig::make_testing_config();

    group.bench_function(
        BenchmarkId::from_parameter(format!("packet_format_code_{}", T::PACKET_FORMAT_CODE)),
        |b| {
            b.iter(|| {
                process_udp_bytes_to_kafka(
                    &mut fbb,
                    black_box(&raw_data),
                    black_box(&src_ip),
                    &config,
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
    benchmark_packet_format::<PacketFormat1>,
}
criterion_main!(benches);
