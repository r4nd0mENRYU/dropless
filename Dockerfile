# Build on glibc (the default gnu target) and ship on distroless/cc.
#
# We deliberately avoid a musl static build: the rustls crypto provider
# (aws-lc-rs) needs cmake + a C toolchain, and cross-compiling it for musl is
# brittle. distroless/cc carries just glibc + libgcc (no shell, no OpenSSL —
# rustls needs none), so the image stays tiny while the build stays reliable.

FROM rust:1-bookworm AS builder
# cmake + a C compiler are required to build aws-lc-rs (rustls' provider).
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --bin dropless

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/dropless /usr/local/bin/dropless
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dropless"]
CMD ["serve", "--role=all"]
