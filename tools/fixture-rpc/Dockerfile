# The fixture chain for the authoring eval's enforced isolation (#1050): it runs inside the eval's
# `--internal` network beside the subject, so it needs an image of its own.
#
#   docker build -t nuthatch-eval-fixture tools/fixture-rpc
FROM rust:1.95.0-slim-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/fixture-rpc /usr/local/bin/fixture-rpc
ENTRYPOINT ["fixture-rpc"]
