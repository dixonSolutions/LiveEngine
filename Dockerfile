# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.92-bookworm AS build
WORKDIR /source
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY docs/hub-contract.schema.json ./docs/hub-contract.schema.json
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/source/target \
    cargo build --locked --release --bin liveengine-hub && cp target/release/liveengine-hub /liveengine-hub

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /liveengine-hub /usr/local/bin/liveengine-hub
RUN mkdir /state && chown 65532:65532 /state
USER 65532:65532
ENV LIVEENGINE_BIND=0.0.0.0:8787
EXPOSE 8787
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s CMD curl --fail --silent http://127.0.0.1:8787/health || exit 1
ENTRYPOINT ["/usr/local/bin/liveengine-hub"]
