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

RUN mkdir -p /data && chown kvstore:kvstore /data

USER kvstore
WORKDIR /data

EXPOSE 6379
EXPOSE 9090

# 默认启动 Redis 兼容服务器
CMD ["kv_server", "-h", "0.0.0.0", "-p", "6379", "-m", "9090", "-d", "/data"]