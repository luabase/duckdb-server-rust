ARG PROJECT_ID

FROM rustlang/rust:nightly

WORKDIR /app

ARG GIT_HASH
ENV GIT_HASH=$GIT_HASH

RUN apt-get update && apt-get install -y build-essential lld clang bash protobuf-compiler cmake git

ENV CC=clang
ENV CXX=clang++
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"

RUN cargo install systemfd

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

COPY . .

RUN cargo install --path .

RUN echo "ulimit -n 65535" >> /etc/profile
RUN echo "session required pam_limits.so" >> /etc/pam.d/common-session
RUN echo "* soft nofile 65535" >> /etc/security/limits.conf
RUN echo "* hard nofile 65535" >> /etc/security/limits.conf
RUN echo "root soft nofile 65535" >> /etc/security/limits.d/custom.conf
RUN echo "root hard nofile 65535" >> /etc/security/limits.d/custom.conf

RUN echo "ulimit -n 65535" >> ~/.bashrc

EXPOSE 3000

CMD ["bash", "-c", "ulimit -n 65535 && exec systemfd --no-pid -s http::0.0.0.0:3000 -- duckdb-server serve $DUCKDB_ARGS"]
