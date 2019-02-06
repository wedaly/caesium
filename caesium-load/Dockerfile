From alpine:edge

ENV REPORT_SAMPLE_INTERVAL 60
ENV DAEMON_HOST 127.0.0.1
ENV DAEMON_PORT 8001
ENV DAEMON_WRITE_NUM_WORKERS 1
ENV DAEMON_WRITE_NUM_METRICS 10
ENV DAEMON_WRITE_RATE_LIMIT 1
ENV SERVER_HOST 127.0.0.1
ENV SERVER_QUERY_PORT 8000
ENV SERVER_QUERY_NUM_WORKERS 1
ENV SERVER_QUERY_FILE_PATH /usr/src/caesium/queries.txt
ENV SERVER_QUERY_RATE_LIMIT 1
ENV SERVER_WRITE_PORT 8001
ENV SERVER_WRITE_NUM_WORKERS 1
ENV SERVER_WRITE_SKETCH_SIZE 1000
ENV SERVER_WRITE_RATE_LIMIT 1

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
    $SERVER_QUERY_FILE_PATH \
    --report-sample-interval $REPORT_SAMPLE_INTERVAL \
    --daemon-write-addr "$DAEMON_HOST:$DAEMON_PORT" \
    --daemon-write-num-workers $DAEMON_WRITE_NUM_WORKERS \
    --daemon-write-num-metrics $DAEMON_WRITE_NUM_METRICS \
    --daemon-write-rate-limit $DAEMON_WRITE_RATE_LIMIT \
    --server-query-addr "$SERVER_HOST:$SERVER_QUERY_PORT" \
    --server-query-num-workers $SERVER_QUERY_NUM_WORKERS \
    --server-query-rate-limit $SERVER_QUERY_RATE_LIMIT \
    --server-write-addr "$SERVER_HOST:$SERVER_WRITE_PORT" \
    --server-write-num-workers $SERVER_WRITE_NUM_WORKERS \
    --server-write-sketch-size $SERVER_WRITE_SKETCH_SIZE \
    --server-write-rate-limit $SERVER_WRITE_RATE_LIMIT
