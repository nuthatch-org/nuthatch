# The subject's environment for RFC-0017's authoring eval (#1058).
#
# The runner (`eval/run-authoring.py --docker-image`) builds the whole `docker run` itself and puts
# this on a Docker `--internal` network, so **isolation is not this image's job**: the repository is
# simply not mounted and there is no route off the network. What the image owes is the opposite -
# being *usable* enough that a failure is the agent's and not the environment's:
#
#   * `claude`      - the subject. The runner's preflight refuses an image without it, because
#                     otherwise every run records criteria false and reads as a failing agent.
#   * an HTTP client - `curl` here. The preflight proves it can reach the fixture chain rather than
#                     assuming so, and a missing client is a refusal, not a pass.
#   * `sh`, `git`   - a shell to work in, and enough to fetch what a user would.
#
# Deliberately NOT here: a `nuthatch` binary. The subject is expected to obtain one the way a real
# user would, and how well it manages that is part of what the eval measures. Baking one in would
# score a different, easier task.
# Pinned by **digest**, not tag. `node:22-bookworm-slim` is a moving target: the same tag six weeks
# apart is a different base, so an eval built from it cannot be compared with the run before it. A
# score is a number about a *pairing* of model, prompt and tooling, and every part of it has to hold
# still. Review of #1058.
FROM node:22-bookworm-slim@sha256:83f487e0a63425e5b4d146fb5e5be574bcbe1b7b843d3ebafdd95eaf7767a7e5

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git jq xz-utils \
 && rm -rf /var/lib/apt/lists/*

# **Apt packages are not version-pinned, and that is a stated limitation rather than an oversight.**
# Review of #1058 is right that a digest-pinned base does not make `apt-get install` reproducible:
# it pulls from the live archive. Pinning exact versions is worse, though - Debian rotates packages
# out, so a pinned build stops working later rather than reproducing earlier, which trades a small
# unreproducibility for a large one.
#
# So the versions are *recorded* instead. `/etc/nuthatch-eval-image.manifest` says exactly what a
# given image contains, which is reproducibility you can verify after the fact rather than falsely
# promise. What actually has to hold still for a score to be comparable is the base (digest-pinned
# above) and `claude` (pinned below); curl's patch version cannot move an authoring result.
RUN { echo "base: node:22-bookworm-slim (digest-pinned in this Dockerfile)"; \
      echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ)"; \
      dpkg-query -W -f='${binary:Package}=${Version}\n' ca-certificates curl git jq xz-utils; \
    } > /etc/nuthatch-eval-image.manifest

# Pinned rather than `@latest`: a score is a number about a *pairing*, and an eval whose subject
# silently changes underneath it cannot be compared with last month's run. Bump deliberately, and
# say so in the report when you do.
ARG CLAUDE_CODE_VERSION=2.0.14
RUN npm install -g "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
 && npm cache clean --force

# **Runs as root, deliberately** - though not for the reason first supposed, which is worth recording
# because the first diagnosis was wrong.
#
# The first version added `USER subject` (uid 10001), the preflight refused with *"the image cannot
# read the workdir at its own path"*, and removing the user did not fix it: the real cause was macOS
# `/var/folders` not being shared with Docker Desktop, so the bind mount silently yielded an empty
# directory. A host limitation, not an image fault.
#
# The user comes out anyway, on its own merits. The workdir is a host directory bind-mounted at its
# own path and owned by whoever invokes the runner - root, on the deployment box - so an arbitrary
# in-container uid genuinely cannot read it, and that *would* have bitten on Linux. The isolation
# here is the container and the `--internal` network, as this file says two comments up; a non-root
# user inside adds nothing to that and breaks the one thing the subject must do.
WORKDIR /work

# The preflight checks exactly these. Failing here is far cheaper than failing mid-eval.
RUN command -v claude >/dev/null && command -v curl >/dev/null && command -v sh >/dev/null
