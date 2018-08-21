FROM alpine:edge

ENV PORT 8001
ENV PUBLISH_ADDR 0.0.0.0:8001
ENV WINDOW_SIZE 30

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN apk update && \
    apk add libgcc rust cargo && \
    cargo install --root /usr/local --path caesium-daemon && \
    apk del --purge rust cargo && \
    rm -rf /usr/src/caesium/* && \
    rm -rf /root/.cargo

CMD SERVICE_NAME=caesium-daemon \
    caesium-daemon \
    --listen-addr "0.0.0.0:$PORT" \
    --publish-addr "$PUBLISH_ADDR" \
    -w $WINDOW_SIZE
