# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Model Context Protocol (MCP) server in Go that exposes Dapr's microservices building blocks (state, pub/sub, service invocation, secrets, locks, bindings, conversation/LLM, crypto, actors, workflows) as MCP tools for AI agents. Module: `github.com/dapr/dapr-mcp-server`.

## Commands

```bash
# Build
go build -o dapr-mcp-server ./cmd/dapr-mcp-server

# Unit tests (CI runs these with -race and enforces 80% coverage over ./pkg/...)
go test -v -race ./pkg/...

# Single test
go test -v -race -run TestName ./pkg/state/

# Integration tests (behind a build tag)
go test -v -tags integration ./test/integration/

# Lint (config in .golangci.yml; gofmt + goimports with local prefix github.com/dapr/dapr-mcp-server)
golangci-lint run

# Run locally (requires Dapr CLI, `dapr init` done once)
dapr run --app-id dapr-mcp-server --resources-path components -- ./dapr-mcp-server --http localhost:8080
```

CI (`.github/workflows/ci.yml`) fails if total coverage of `./pkg/...` drops below 80% — new code in `pkg/` needs tests.

The `dapr-mcp-server` binary in the repo root is a build artifact; never commit it.

## Architecture

**Entry point:** `cmd/dapr-mcp-server/main.go`. Supports two transports selected by the `--http` flag: streamable HTTP/SSE (`mcp.NewSSEHandler`) or stdio (default, for direct MCP client attachment). The HTTP path additionally wires health endpoints, auth middleware, telemetry middleware, and a stub `/dapr/subscribe` endpoint.

**Tool packages (`pkg/<building-block>/tools.go`):** each Dapr building block is its own package (state, pubsub, secrets, invoke, lock, bindings, conversation, crypto, actors, metadata) following an identical pattern:

- A narrow, package-local client interface (e.g., `state.StateClient`) declaring only the Dapr SDK methods the package uses — this is what makes mocking possible, since the Dapr go-sdk `client.Client` has methods with unexported types that prevent embedding.
- The client is held in a package-level variable, set by `RegisterTools(server *mcp.Server, client dapr.Client)`.
- Args structs use `json` + `jsonschema` struct tags; the `jsonschema` tag text is the argument description shown to the AI agent.
- Each tool handler opens an OTEL span (`otel.Tracer("dapr-mcp-server")`) with `dapr.operation`/component attributes. Dapr errors are returned as `CallToolResult{IsError: true}` with a text message, not as a Go error.

**Conditional tool registration:** `main.go` calls `metadata.GetLiveComponentList` at startup and only registers a package's tools if a component of the matching type prefix (`state.`, `pubsub.`, `secretstores.`, `lock.`, `conversation.`, `crypto.`, `bindings.`) is configured in the Dapr sidecar. `metadata`, `invoke`, `actors`, and `workflow` tools are always registered (workflows are part of the runtime, not a component; the workflow client from `durabletask-go` reuses the Dapr client's gRPC connection via `GrpcClientConn()`). Workflow tools can additionally target other apps' sidecars via `DAPR_MCP_SERVER_WORKFLOW_APPS` (app-id → gRPC address pool, optional `appID` tool argument) — see `docs/specs/2026-07-14-multi-app-workflows.md`. Adding a new building block means: new `pkg/<name>` with the pattern above, plus a prefix check + `RegisterTools` call in `main.go`.

**Auth (`pkg/auth/`):** pluggable `Authenticator` interface with four implementations — OIDC (`oidc.go`), SPIFFE workload API (`spiffe.go`), Dapr Sentry JWT/JWKS (`sentry.go`, needed because Sentry JWTs carry the SPIFFE ID in `sub` with no `iss` claim), and hybrid (any combination; first authenticator to succeed wins via `middleware.go`). Everything is configured from environment variables (`config.go`, `AUTH_MODE=disabled|oidc|spiffe|dapr-sentry|hybrid`); health endpoints are skipped via `AUTH_SKIP_PATHS`. The README documents all env vars.

**Telemetry (`pkg/telemetry/`):** OTEL traces/metrics/logs initialized from standard `OTEL_*` env vars; disabled gracefully when no endpoint is set. `telemetry.HTTPMiddleware` wraps the MCP handler; `NewToolMetrics` defines the `dapr-mcp-server.tool.*` counters/histograms.

**Server instructions:** the MCP server ships behavioral safety rules for connecting agents (built in `main.go` as `mcp.ServerOptions.Instructions`) — e.g., "run get_components before guessing component names". Keep these in sync when adding tools.

**Tests:**
- Unit tests live next to code; `test/mocks/` provides testify-based mocks (`DaprClient` interface mirroring the go-sdk subset used, plus auth mocks).
- `test/integration/` has `//go:build integration`-tagged tests (e.g., full Dapr Sentry auth flow against a mock JWKS server).
- `test/app.py` + `test/components/` are a manual end-to-end harness: a Python `dapr_agents` DurableAgent that connects to the server over SSE.

**Dapr components:** `components/` holds the Dapr component YAMLs (state store, pubsub, secrets, lock, crypto keys, etc.) used for local runs via `--resources-path components`.
