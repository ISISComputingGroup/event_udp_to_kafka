use influxdb2::{Client, FromDataPoint};
use influxdb2::models::Query;
use influxdb2_derive::WriteDataPoint;

#[derive(Default, WriteDataPoint)]
#[measurement = "packet_diagnostic"]
struct PacketDiagnostic {
    #[influxdb(tag)]
    src_ip: Option<String>,
    #[influxdb(tag)]
    instrument: Option<String>,
    #[influxdb(field)]
    event_count: i64,
    #[influxdb(field)]
    frame_count: i64,
    #[influxdb(timestamp)]
    time: i64,
}

#[derive(Default, WriteDataPoint)]
#[measurement = "frame_diagnostic"]
struct FrameDiagnostic {
    #[influxdb(tag)]
    src_ip: Option<String>,
    #[influxdb(tag)]
    instrument: Option<String>,
    #[influxdb(field)]
    event_count: i64,
    #[influxdb(field)]
    frame_number: i64,
    #[influxdb(field)]
    ppp_in_frame: i64,
    #[influxdb(timestamp)]
    time: i64,
}

#[derive(Default, WriteDataPoint)]
#[measurement = "event_diagnostic"]
struct EventDiagnostic {
    #[influxdb(tag)]
    src_ip: Option<String>,
    #[influxdb(tag)]
    instrument: Option<String>,
    #[influxdb(field)]
    detector_id: i64,
    #[influxdb(field)]
    frame_count: i64,
    #[influxdb(timestamp)]
    time: i64,
}

pub fn demo(){
    println!("demo test");
}