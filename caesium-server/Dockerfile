FROM alpine:edge

ENV LOG_LEVEL info
ENV QUERY_PORT 8000
ENV INSERT_PORT 8001
ENV NUM_READ_WORKERS 5
ENV NUM_WRITE_WORKERS 5
ENV DOWNSAMPLE_INTERVAL 600

VOLUME /data

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN apk update && \
    apk add libgcc clang-libs rust cargo g++ linux-headers && \
    cargo install --root /usr/local --path caesium-server && \
    apk del --purge rust cargo g++ linux-headers && \
    rm -rf /usr/src/caesium/* && \
    rm -rf /root/.cargo

CMD RUST_LOG=caesium=$LOG_LEVEL \
    caesium-server \
    --db-path "/data/db" \
    --num-read-workers "$NUM_READ_WORKERS" \
    --num-write-workers "$NUM_WRITE_WORKERS" \
    --query-addr "0.0.0.0:$QUERY_PORT" \
    --insert-addr "0.0.0.0:$INSERT_PORT" \
    --downsample-interval "$DOWNSAMPLE_INTERVAL"
