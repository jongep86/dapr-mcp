# syntax=docker/dockerfile:1

# Build stage - runs natively on the build host and cross-compiles for the target platform
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

WORKDIR /build

# Copy go mod files first for layer caching
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY . .

# Build the binary
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o dapr-mcp-server \
    ./cmd/dapr-mcp-server

# Runtime stage - distroless (ships CA certs and a nonroot user, no shell)
FROM gcr.io/distroless/static-debian13:nonroot

# Copy the binary from builder
COPY --from=builder /build/dapr-mcp-server /dapr-mcp-server

# Use non-root user (provided by distroless nonroot image)
USER nonroot:nonroot

# Expose default HTTP port
EXPOSE 8080

# No HEALTHCHECK: the image has no shell or probe binary. Use the HTTP health
# endpoints (/livez, /readyz) from Kubernetes probes instead.

ENTRYPOINT ["/dapr-mcp-server"]

# Default to HTTP mode
CMD ["--http", "0.0.0.0:8080"]
