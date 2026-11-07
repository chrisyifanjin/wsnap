# syntax=docker/dockerfile:1
# Two images from one build: the controller (distroless) and the agent
# (Alpine with the kopia and restic CLIs).

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH VERSION=dev
RUN LDFLAGS="-s -X github.com/chrisyifanjin/wsnap/internal/version.Version=${VERSION}"; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="$LDFLAGS" -o /out/wsnap-controller ./cmd/wsnap-controller && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="$LDFLAGS" -o /out/wsnap-agent ./cmd/wsnap-agent

# Engine CLIs, pinned and checked against the published checksums.
FROM alpine:3.22 AS engines
ARG TARGETARCH
ARG KOPIA_VERSION=0.23.1
ARG RESTIC_VERSION=0.19.1
RUN apk add --no-cache curl
WORKDIR /dl
RUN set -eu; \
    case "$TARGETARCH" in amd64) KARCH=x64 ;; arm64) KARCH=arm64 ;; *) echo "unsupported $TARGETARCH"; exit 1 ;; esac; \
    k="kopia-${KOPIA_VERSION}-linux-${KARCH}.tar.gz"; \
    curl -fsSLO "https://github.com/kopia/kopia/releases/download/v${KOPIA_VERSION}/$k"; \
    curl -fsSL "https://github.com/kopia/kopia/releases/download/v${KOPIA_VERSION}/checksums.txt" | grep " $k\$" | sha256sum -c -; \
    tar -xzf "$k" --strip-components=1 -C /usr/local/bin "kopia-${KOPIA_VERSION}-linux-${KARCH}/kopia"; \
    r="restic_${RESTIC_VERSION}_linux_${TARGETARCH}.bz2"; \
    curl -fsSLO "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/$r"; \
    curl -fsSL "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/SHA256SUMS" | grep " $r\$" | sha256sum -c -; \
    bunzip2 -c "$r" > /usr/local/bin/restic; chmod 0755 /usr/local/bin/restic /usr/local/bin/kopia

FROM gcr.io/distroless/static-debian12:nonroot AS controller
COPY --from=build /out/wsnap-controller /wsnap-controller
ENTRYPOINT ["/wsnap-controller"]

# The agent runs as root: it must read and restore files owned by any user
# in the workspace. The webhook and the Job template pin uid 0 even when the
# Pod sets runAsUser, so every agent on a volume trusts the same .wsnap.
FROM alpine:3.22 AS agent
RUN apk add --no-cache ca-certificates tar gzip
COPY --from=engines /usr/local/bin/kopia /usr/local/bin/restic /usr/local/bin/
COPY --from=build /out/wsnap-agent /usr/local/bin/wsnap-agent
COPY examples/exec-engine/tar-engine.sh /usr/local/bin/wsnap-tar-engine
ENTRYPOINT ["/usr/local/bin/wsnap-agent"]
