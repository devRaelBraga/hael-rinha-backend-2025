FROM golang:1.24-alpine AS builder

RUN apk add --no-cache \
    build-base \
    pkgconfig \
    czmq-dev \
    zeromq-dev \
    libsodium-dev \
    git \
    bash \
    cmake \
    autoconf \
    automake \
    libtool

WORKDIR /app

COPY hael/ ./hael
WORKDIR /app/hael

RUN go mod tidy
RUN go build -o /hael ./cli/cli.go

# ===== Runtime stage =====
FROM alpine:3.20

RUN apk add --no-cache \
    czmq \
    zeromq \
    libsodium \
    bash

WORKDIR /app

COPY --from=builder /hael /hael
COPY src/ ./src

CMD ["/hael", "run", "./src/main.hael"]
