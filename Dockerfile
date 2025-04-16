ARG PROJECT_ID

FROM us-central1-docker.pkg.dev/$PROJECT_ID/duckdb/duckdb:latest

WORKDIR /app

RUN apt-get update && apt-get install -y lld clang bash protobuf-compiler

ENV CC=clang
ENV CXX=clang++
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"

RUN cargo install systemfd

ENV LIBRARY_PATH=/usr/local/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH

RUN cargo install --path .

RUN echo "ulimit -n 65535" >> /etc/profile
RUN echo "session required pam_limits.so" >> /etc/pam.d/common-session
RUN echo "* soft nofile 65535" >> /etc/security/limits.conf
RUN echo "* hard nofile 65535" >> /etc/security/limits.conf
RUN echo "root soft nofile 65535" >> /etc/security/limits.d/custom.conf
RUN echo "root hard nofile 65535" >> /etc/security/limits.d/custom.conf

RUN echo "ulimit -n 65535" >> ~/.bashrc

EXPOSE 3000

CMD ["bash", "-c", "ulimit -n 65535 && exec systemfd --no-pid -s http::0.0.0.0:3000 -- duckdb-server $DUCKDB_ARGS"]
