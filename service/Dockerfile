# Multi-stage build for the sc-component-notifier service.
#
# Shape cribbed from forge-notifier/Dockerfile:
#   1. Node builder — compile the React SPA into service/internal/ui/dist.
#   2. Go builder — build the Go binary with the SPA embedded via
#      //go:embed all:dist.
#   3. Runtime — public.ecr.aws/amazonlinux/amazonlinux:2023, same as
#      sc-component-observatory. The binary runs under aws-lambda-go's
#      runtime or as a standalone HTTP server when PORT is set (both
#      shapes work because go-aws-lambda-sdk's sdkservice detects the
#      environment).
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
# Consequence of the swap: the binary lands at /usr/local/bin/notifier
# with an explicit ENTRYPOINT rather than at /var/task/bootstrap, which
# only the lambda base's entrypoint would have looked for.
#
# Build args:
#   - VERSION    (auto-injected by SC per fleet CLAUDE.md "How SC builds images")
#   - COMMIT     (operator-declared under client.yaml image.build.args — see
#                 sc-auto-inject-github-sha-and-repository-build-args roadmap item)
#   - REVISION   (same)

ARG GOLANG_VERSION=1.26-alpine
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

RUN apk add --no-cache ca-certificates git make

WORKDIR /src

# Copy module files first for layer-cache friendliness.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
# Overwrite internal/ui/dist with the compiled SPA from stage 1.
COPY --from=ui-builder /ui/dist ./internal/ui/dist

ARG VERSION=dev
ARG COMMIT=dev
ARG REVISION=dev

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
