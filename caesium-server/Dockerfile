FROM rust:1.28.0

ENV LOG_LEVEL info
ENV PORT 8000

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
    --addr "0.0.0.0:$PORT" \
    --db-path "/data/db"
