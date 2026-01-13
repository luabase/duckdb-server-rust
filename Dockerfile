ARG PROJECT_ID

# Stage 1: Chef - prepare recipe
FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

# Stage 2: Planner - analyze dependencies
FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY build.rs ./
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Builder - build dependencies (cached) then build app
FROM chef AS builder

RUN apt-get update && apt-get install -y build-essential lld clang protobuf-compiler cmake

ENV CC=clang
ENV CXX=clang++
ENV RUSTFLAGS="-C force-frame-pointers=yes -C debuginfo=2 -C link-arg=-fuse-ld=lld"

# Install systemfd for socket passing
RUN cargo install systemfd

# Build dependencies (this layer is cached if recipe.json doesn't change)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
ARG GIT_HASH
ENV GIT_HASH=$GIT_HASH

COPY . .
RUN cargo build --release

# Stage 4: Runtime - minimal final image
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y \
    bash \
    ca-certificates \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Copy systemfd for socket passing (hot reload support)
COPY --from=builder /usr/local/cargo/bin/systemfd /usr/local/bin/

RUN wget -O /tmp/duckdb_cli.zip https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip && \
    unzip /tmp/duckdb_cli.zip -d /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm /tmp/duckdb_cli.zip

COPY --from=builder /app/target/release/duckdb-server /usr/local/bin/

RUN echo "ulimit -n 65535" >> /etc/profile && \
    echo "session required pam_limits.so" >> /etc/pam.d/common-session && \
    echo "* soft nofile 65535" >> /etc/security/limits.conf && \
    echo "* hard nofile 65535" >> /etc/security/limits.conf && \
    mkdir -p /etc/security/limits.d && \
    echo "root soft nofile 65535" >> /etc/security/limits.d/custom.conf && \
    echo "root hard nofile 65535" >> /etc/security/limits.d/custom.conf

ENV RUST_BACKTRACE=1
ENV RUST_BACKTRACE_FULL=1
ENV RUST_LOG=info,duckdb_server=debug

EXPOSE 3000 3030

WORKDIR /app

CMD ["bash", "-c", "ulimit -n 65535 && exec systemfd --no-pid -s http::0.0.0.0:3000 -- duckdb-server serve $DUCKDB_ARGS"]
