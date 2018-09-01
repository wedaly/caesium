From alpine:edge

ENV DAEMON_HOST 127.0.0.1
ENV DAEMON_PORT 8001
ENV SERVER_HOST 127.0.0.1
ENV SERVER_QUERY_PORT 8000
ENV WRITE_NUM_WORKERS 1
ENV WRITE_NUM_METRICS 10
ENV WRITE_RATE_LIMIT 1
ENV READ_NUM_WORKERS 1
ENV READ_QUERY_FILE_PATH /usr/src/caesium/queries.txt
ENV READ_RATE_LIMIT 1

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN apk update && \
    apk add libgcc rust cargo && \
    cargo install --root /usr/local --path caesium-load && \
    apk del --purge rust cargo && \
    rm -rf /usr/src/caesium/* && \
    rm -rf /root/.cargo && \
    echo "quantile(fetch(\"caesium-load.0\"), 0.5)" > /usr/src/caesium/queries.txt

CMD SERVICE_NAME="caesium-load" \
    caesium-load \
    $READ_QUERY_FILE_PATH \
    --write-addr "$DAEMON_HOST:$DAEMON_PORT" \
    --write-num-workers $WRITE_NUM_WORKERS \
    --write-num-metrics $WRITE_NUM_METRICS \
    --write-rate-limit $WRITE_RATE_LIMIT \
    --read-addr "$SERVER_HOST:$SERVER_QUERY_PORT" \
    --read-num-workers $READ_NUM_WORKERS \
    --read-rate-limit $READ_RATE_LIMIT
