# Multi-stage build for the sc-component-notifier service.
#
# Shape:
#   1. Node builder — compile the React SPA into service/internal/ui/dist.
#   2. Go builder — build the Go binary with the SPA embedded via
#      //go:embed all:dist.
#   3. Runtime — public.ecr.aws/amazonlinux/amazonlinux:2023.
#
# NOT `public.ecr.aws/lambda/provided:al2023`, which is what this file
# was scaffolded with. Two reasons, in order of how much they cost:
#
#   - YC rejects it. Three of its layers begin with a tar entry whose
#     path is the empty string, and Yandex Cloud answers `Bad path in
#     image` naming neither the layer nor the entry. The fleet lost a
#     day to that on forge-atrium.
#   - It buys nothing. A Go binary that links aws-lambda-go IS its own
#     runtime interface client, so the RIE and `lambda-entrypoint.sh`
#     the lambda base exists to provide are dead weight in a deployed
#     image. Observatory has run on the plain amazonlinux base since it
#     shipped.
#
# Build args:
#   - VERSION    (auto-injected by SC per fleet CLAUDE.md "How SC builds images")
#   - COMMIT     (operator-declared under client.yaml image.build.args)
#   - REVISION   (same)

# GOLANG_VERSION bumped from 1.26-alpine for go-embedb (which pins
# `go 1.27.3` in its module directive, so go.mod loading fails on older
# toolchains with `invalid go version`).
ARG GOLANG_VERSION=1.27-alpine
ARG NODE_VERSION=20-alpine
ARG RUNTIME_BASE=public.ecr.aws/amazonlinux/amazonlinux:2023

# -----------------------------
# Stage 1 — Build the SPA
# -----------------------------
FROM node:${NODE_VERSION} AS ui-builder

WORKDIR /ui
COPY internal/ui/package.json internal/ui/package-lock.json ./
# `npm install` rather than `npm ci` on purpose: with the lockfile present
# it installs from it, but it does not hard-fail when package.json and the
# lock drift. A drift is worth fixing, not worth failing a deploy eight
# minutes in from a cold build.
RUN npm install --no-audit --no-fund
COPY internal/ui/ ./
RUN npm run build:prod

# -----------------------------
# Stage 2 — Build the Go binary
# -----------------------------
FROM golang:${GOLANG_VERSION} AS go-builder

# openssh-client is for the private-module fetch below, not for the
# runtime image — stage 3 copies only the binary.
RUN apk add --no-cache ca-certificates git make openssh-client

WORKDIR /src

# go.mod routes github.com/FerretDB/FerretDB to the PRIVATE fork
# github.com/simple-container-com/go-embedb, so `go mod download`
# needs a credential. It arrives as a buildx secret mount rather than
# a COPY or a build ARG:
#
#   - a COPY would need the key inside the build context (service/,
#     see release.yml), but the key lives at the repo root — and a COPY
#     also leaves it in this stage's layer;
#   - an ARG is echoed into the build log by any docker frontend that
#     prints the command line.
#
# The mount exists only for the lifetime of the RUN and never becomes
# a layer. The release + PR workflows supply it with
# `secret-files: go_embedb_key=.sc/deploy-keys/go-embedb.pem` after
# `sc secrets reveal --force`; see README.md §Storage.
#
# GOPRIVATE does three things at once: keeps the module off the public
# proxy (which cannot read a private repo and answers with a confusing
# `invalid version` wrapping git's auth failure), off the checksum
# database, and therefore on the direct git path the insteadOf rewrite
# redirects to SSH.
ENV GOPRIVATE=github.com/simple-container-com/*

# Dependency layer first so a source-only change does not re-download
# the module cache.
COPY go.mod go.sum ./
RUN --mount=type=secret,id=go_embedb_key \
      mkdir -p /root/.ssh \
    && ssh-keyscan -t rsa,ecdsa,ed25519 github.com >> /root/.ssh/known_hosts 2>/dev/null \
    && git config --global url."git@github.com:".insteadOf "https://github.com/" \
    && GIT_SSH_COMMAND="ssh -i /run/secrets/go_embedb_key -o IdentitiesOnly=yes" \
       go mod download

COPY . .
# Overwrite internal/ui/dist with the compiled SPA from stage 1.
COPY --from=ui-builder /ui/dist ./internal/ui/dist

ARG VERSION=dev
ARG COMMIT=dev
ARG REVISION=dev

# -buildvcs=false: go-embedb's build/version/version.go init() panics
# when the git-derived vcs.revision disagrees with commit.txt. In an
# SC build the git tree in /src ≠ what go stamps as vcs.revision.
# Harmless when USE_GOEMBEDB=0 (the dep still links) and required when
# USE_GOEMBEDB=1.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -buildvcs=false \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.revision=${REVISION}" \
      -o /out/notifier ./cmd/notifier

# -----------------------------
# Stage 3 — Runtime
# -----------------------------
FROM ${RUNTIME_BASE} AS runtime

LABEL org.opencontainers.image.title="SC Component Notifier"
LABEL org.opencontainers.image.source="https://github.com/simple-container-com/sc-component-notifier"
LABEL org.opencontainers.image.licenses="MIT"

# The plain amazonlinux base ships no CA bundle; every outbound call
# this service makes is HTTPS (Telegram, operator webhooks, the
# observatory it registers itself with), so this is not optional.
RUN dnf install -y ca-certificates \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Russian CA carry-over from forge-notifier — placeholder directory, no
# certs yet. Real components talking to api.giga.chat / ngw.devices.sberbank.ru
# add the .crt files here, and this is why update-ca-trust runs after.
COPY build/ca/ /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust extract

COPY --from=go-builder /out/notifier /usr/local/bin/notifier

# For plain-HTTP mode (non-Lambda), run directly. NOTE for a future YC
# deploy: PORT is RESERVED on a YC Serverless Container and a revision
# that sets it is refused, so a YC client.yaml must write its env block
# out in full rather than inheriting one that carries this.
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/notifier"]
