# Dockerfile for basis-capi-provider
#
# Produces a minimal image that runs the basis-capi-provider binary. This is
# the image Lattice references from test-providers/infrastructure-basis/
# (pulled by the Deployment in capi-basis-system).
#
# Build:
#   docker build -t ghcr.io/evan-hines-js/basis-capi-provider:v0.1.0 .
#
# Or via scripts/build-capi-provider.sh (which also handles version tagging).

FROM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN --mount=type=cache,target=/build/target \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release -p basis-capi-provider && \
    cp target/release/basis-capi-provider /usr/local/bin/basis-capi-provider

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /usr/local/bin/basis-capi-provider /usr/local/bin/basis-capi-provider

ENTRYPOINT ["/usr/local/bin/basis-capi-provider"]
