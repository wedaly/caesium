FROM rust:1.28.0

ENV LOG_LEVEL info
ENV LISTEN_PORT 8001
ENV PUBLISH_PORT 8000
ENV WINDOW_SIZE 30

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN cargo install --path caesium-daemon

CMD RUST_LOG=caesium=$LOG_LEVEL \
    caesium-daemon \
    --listen-addr "0.0.0.0:$LISTEN_PORT" \
    --publish-addr "0.0.0.0:$PUBLISH_PORT" \
    -w $WINDOW_SIZE

