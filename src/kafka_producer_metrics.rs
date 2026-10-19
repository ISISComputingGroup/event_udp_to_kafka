use crate::metrics::{
    KAFKA_FAILED_DELIVERIES, KAFKA_MSG_CNT, KAFKA_MSG_MAX, KAFKA_MSG_SIZE, KAFKA_MSG_SIZE_MAX,
    KAFKA_REPLYQ, KAFKA_RX, KAFKA_RX_BYTES, KAFKA_TX, KAFKA_TX_BYTES,
};
use log::error;
use metrics::{counter, gauge};
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use rdkafka::{ClientContext, Statistics};

pub struct MetricsProducerContext {}

impl ClientContext for MetricsProducerContext {
    fn stats(&self, statistics: Statistics) {
        gauge!(KAFKA_REPLYQ).set(statistics.replyq as f64);
        gauge!(KAFKA_MSG_CNT).set(statistics.msg_cnt as f64);
        gauge!(KAFKA_MSG_MAX).set(statistics.msg_max as f64);
        gauge!(KAFKA_MSG_SIZE).set(statistics.msg_size as f64);
        gauge!(KAFKA_MSG_SIZE_MAX).set(statistics.msg_size_max as f64);
        gauge!(KAFKA_TX).set(statistics.tx as f64);
        gauge!(KAFKA_TX_BYTES).set(statistics.tx_bytes as f64);
        gauge!(KAFKA_RX).set(statistics.rx as f64);
        gauge!(KAFKA_RX_BYTES).set(statistics.rx_bytes as f64);
    }
}

impl ProducerContext for MetricsProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
        if let Err((kafka_error, _)) = delivery_result {
            error!("Kafka delivery failed: {:?}", kafka_error);
            counter!(KAFKA_FAILED_DELIVERIES).increment(1);
        }
    }
}
