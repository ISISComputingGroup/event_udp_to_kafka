FROM rust:alpine as builder

WORKDIR /app/src
RUN USER=root

RUN apk update && apk add pkgconfig openssl-dev libc-dev openssl alpine-sdk perl
COPY ./ ./
RUN cargo build --release

FROM alpine:latest
WORKDIR /app
RUN apk update \
    && apk add openssl ca-certificates


COPY --from=builder /app/src/target/release/rust-data-stream-processor /app/rust-data-stream-processor

CMD ["/app/rust-data-stream-processor "]



#FROM alpine:latest
#LABEL authors="Gitlab CI Build"
#
##ADD /target/release/rust-data-stream-processor /
###RUN chmod -x rust-data-stream-processor
###RUN chmod 777 rust-data-stream-processor
###RUN ls
##ENTRYPOINT ["./rust-data-stream-processor"]
#
#
#
#ADD /target/release/* ./
#RUN apk update && apk add openssl ca-certificates
#RUN ls
#CMD ["./rust-data-stream-processor"]