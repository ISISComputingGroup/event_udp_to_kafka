use clap::Parser;
use event_udp_to_kafka::config::EventUdpToKafkaConfig;
use event_udp_to_kafka::metrics::initialize_metrics;
use event_udp_to_kafka::{Args, WiringConfigRecord, kafka_udp_process, read_csv};
use log::info;

#[tokio::main]
async fn main() {
    info!("Starting event UDP to Kafka");

    env_logger::init();
    let args = Args::parse();

    let config: EventUdpToKafkaConfig =
        toml::from_str(&std::fs::read_to_string(args.config).expect("Can't read config file"))
            .expect("Can't parse config from TOML");

    initialize_metrics(&config).expect("Can't initialize metrics");

    let filename = config.wiring_csv_path();
    let csv_data: Vec<WiringConfigRecord> = read_csv(filename);

    kafka_udp_process(&config, csv_data).await;
}
