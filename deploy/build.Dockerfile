# Cross-compile image for basis-{controller,agent}.
#
# We build this image once with `docker build`, then `docker run` it from
# bootstrap.sh. Without it, every bootstrap run re-executes `apt-get install`
# inside a fresh `--rm` container, which dominates wall time.
#
# Pin rust to an exact patch version. `rust:1` and `rust:1-bookworm` are
# moving tags — when they shift to a new rustc, every cached cargo
# fingerprint becomes stale. Keep this in sync with holo_rust_version
# in deploy/ansible/group_vars/all.yml.
FROM rust:1.88.0-bookworm

RUN apt-get update -qq \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
        cmake \
        clang \
        protobuf-compiler \
        pkg-config \
 && rm -rf /var/lib/apt/lists/*
