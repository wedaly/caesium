From alpine:edge

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN apk update && \
    apk add libgcc rust cargo && \
    cargo install --root /usr/local --path caesium-cli --bin caesium-quantile && \
    cargo install --root /usr/local --path caesium-cli --bin caesium-query && \
    cargo install --root /usr/local --path caesium-cli --bin caesium-writer && \
    apk del --purge rust cargo && \
    rm -rf /usr/src/caesium/* && \
    rm -rf /root/.cargo

CMD ["echo", "To run CLI commands, use `docker-compose run cli [COMMAND]`"]
