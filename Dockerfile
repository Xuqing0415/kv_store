# ===== Stage 1: Build =====
FROM alpine:3.20 AS builder

RUN apk add --no-cache \
    gcc \
    musl-dev \
    make \
    cmake \
    linux-headers

WORKDIR /src

COPY . .

RUN cmake -B build -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTS=OFF \
    -DBUILD_EXAMPLES=OFF \
    && cmake --build build -j$(nproc)

# ===== Stage 2: Runtime =====
FROM alpine:3.20

RUN addgroup -S kvstore && adduser -S kvstore -G kvstore

COPY --from=builder /src/build/kv_server /usr/local/bin/kv_server
COPY --from=builder /src/build/kv_test   /usr/local/bin/kv_test
COPY --from=builder /src/build/kv_chaos  /usr/local/bin/kv_chaos
COPY --from=builder /src/build/kv_raft   /usr/local/bin/kv_raft

RUN mkdir -p /data && chown kvstore:kvstore /data

USER kvstore
WORKDIR /data

EXPOSE 6379
EXPOSE 8001
EXPOSE 9090

# 默认启动 Raft 集群节点（通过 docker-compose 传入参数）
ENTRYPOINT ["kv_raft"]
CMD ["--id", "node1", "--raft-port", "8001", "--resp-port", "6379", \
     "--metrics-port", "9090", \
     "--peer", "node1:kv-node1:8001", \
     "--peer", "node2:kv-node2:8001", \
     "--peer", "node3:kv-node3:8001", \
     "--data-dir", "/data"]