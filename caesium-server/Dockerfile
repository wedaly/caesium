FROM rust:1.28.0

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

RUN printf 'deb http://apt.llvm.org/stretch/ llvm-toolchain-stretch main\n \
deb-src http://apt.llvm.org/stretch/ llvm-toolchain-stretch main\n \
deb http://apt.llvm.org/stretch/ llvm-toolchain-stretch-6.0 main\n \
deb-src http://apt.llvm.org/stretch/ llvm-toolchain-stretch-6.0 main' \
>> /etc/apt/sources.list.d/llvm-sources.list
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key|apt-key add -
RUN apt-get update
RUN apt-get install -y clang-6.0
RUN cargo install --path caesium-server

CMD RUST_LOG=caesium=$LOG_LEVEL \
    caesium-server \
    --db-path "/data/db" \
    --num-read-workers "$NUM_READ_WORKERS" \
    --num-write-workers "$NUM_WRITE_WORKERS" \
    --query-addr "0.0.0.0:$QUERY_PORT" \
    --insert-addr "0.0.0.0:$INSERT_PORT" \
    --downsample-interval "$DOWNSAMPLE_INTERVAL"
