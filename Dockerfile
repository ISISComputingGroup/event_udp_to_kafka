FROM alpine:latest
LABEL authors="Gitlab CI Build"

ADD /target/release/rust-data-stream-processor .
RUN chmod -x rust-data-stream-processor
RUN chmod 777 rust-data-stream-processor
RUN ls
ENTRYPOINT ["./rust-data-stream-processor"]