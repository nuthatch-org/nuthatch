# The published container image (ghcr.io/nuthatch-org/nuthatch).
#
# It copies the **same** release binary that is attached to the GitHub Release rather than rebuilding
# from source, so the image and the tarball are byte-identical. A separate build would be a second
# artifact nobody diffed, and "the image behaves differently from the binary" is a miserable thing to
# debug at 3am.
#
# Deliberately not a builder-stage image: a from-source build here would recompile DataFusion and dbsp on
# every tag for no benefit, and would make the image's provenance harder to state, not easier.
# Pinned by digest (#2735) so two builds of one tag share their base; Dependabot moves the pin.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

# The MCP Registry verifies an image belongs to its entry (server.json) by this label.
LABEL io.modelcontextprotocol.server.name="io.github.nuthatch-org/nuthatch"

# `ca-certificates` is the only runtime dependency: outbound HTTPS to RPC endpoints, ABI resolvers and
# webhook sinks. Everything else nuthatch needs is statically in the binary - that is the point of it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Run unprivileged, matching what the operator docs instruct on bare metal. The nest directory is the
# only writable state, so it is owned by this user and nothing else needs to be.
RUN useradd --system --create-home --uid 10001 nuthatch
COPY nuthatch /usr/local/bin/nuthatch
RUN chmod 0755 /usr/local/bin/nuthatch

USER nuthatch
WORKDIR /nest

# Bind inside the container and publish to loopback on the host - the image cannot know whether there
# is a gateway in front, so it does not pretend to. See docs/operators.md.
EXPOSE 8288

ENTRYPOINT ["nuthatch"]
CMD ["dev", "--dir", "/nest", "--listen", "0.0.0.0:8288"]
