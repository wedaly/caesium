From alpine:edge

ENV DAEMON_ADDR 127.0.0.1:8001
ENV NUM_WRITERS 1
ENV NUM_METRICS 10
ENV RATE_LIMIT 1

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN apk update && \
    apk add libgcc rust cargo && \
    cargo install --root /usr/local --path caesium-load && \
    apk del --purge rust cargo && \
    rm -rf /usr/src/caesium/* && \
    rm -rf /root/.cargo

CMD caesium-load \
    --daemon-addr $DAEMON_ADDR \
    --num-writers $NUM_WRITERS \
    -m $NUM_METRICS \
    -r $RATE_LIMIT
