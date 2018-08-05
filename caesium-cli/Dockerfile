From rust:1.28.0

ENV CAESIUM_SERVER_ADDR 0.0.0.0:8000

WORKDIR /usr/src/caesium

# Assumes the build context directory is the git repository root
COPY . .

RUN cargo install --path caesium-cli --bin caesium-quantile
RUN cargo install --path caesium-cli --bin caesium-query

CMD ["echo", "To run CLI commands, use `docker-compose run cli [COMMAND]`"]
