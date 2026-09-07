use clap::Parser;
use event_udp_to_kafka::config::EventUdpToKafkaConfig;
use event_udp_to_kafka::metrics::initialize_metrics;
use event_udp_to_kafka::{Args, udp_process};
use log::info;

fn main() {
    env_logger::init();

    info!("Starting event UDP to Kafka");
    let args = Args::parse();

    let config: EventUdpToKafkaConfig =
        toml::from_str(&std::fs::read_to_string(args.config).expect("Can't read config file"))
            .expect("Can't parse config from TOML");

    initialize_metrics(&config).expect("Can't initialize metrics");

    udp_process(&config)
}
