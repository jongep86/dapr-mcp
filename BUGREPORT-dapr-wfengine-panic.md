# Bug report for dapr/dapr — SUBMITTED as https://github.com/dapr/dapr/issues/10217 (fix PR: https://github.com/dapr/dapr/pull/10218)

Filed 2026-07-16 from the jongep86 account.

**Title:** Runtime panic (nil pointer) in wfengine `LoadWorkflowState` when querying workflow history on a sidecar without an actor state store

---

## In what area(s)?

/area runtime

<!-- workflow engine: pkg/runtime/wfengine -->

## What version of Dapr?

Runtime **1.18.1** (CLI 1.18.0), self-hosted / slim mode on macOS (darwin/arm64).
durabletask-go **v0.12.1**.

## Expected Behavior

Calling the Workflow **GetInstanceHistory** API against a sidecar whose configured state
store is **not** an actor state store (no `actorStateStore: "true"`) — or where the
instance's workflow state cannot be loaded — should return a gRPC error to the caller.

This is how the sibling **ListInstanceIDs** call on the same TaskHub gRPC API already
behaves: against the same sidecar it returns a clean error, `rpc error: code = Unknown
desc = no state store with actor support found`.

## Actual Behavior

`daprd` **panics with SIGSEGV (nil pointer dereference)** and the process exits (status 2),
taking the app down with it. A client that can reach the workflow gRPC API can therefore
crash the sidecar — an availability / DoS concern, since the crash is remote-triggerable via
a normal read-only API call.

The nil dereference is in `pkg/runtime/wfengine/state/state.go:685` (`LoadWorkflowState`),
reached from `pkg/runtime/wfengine/backends/actors/actors.go:861` (`GetInstanceHistory`).
`LoadWorkflowState` is invoked with a nil state argument (`{0x0, 0x0}` in the trace) and
dereferences it without a nil check.

Looking at the code, `GetInstanceHistory` does:

```go
ss, err := abe.actors.State(ctx)
if err != nil {
    return nil, err
}
resp, err := state.LoadWorkflowState(ctx, ss, ...)
```

so it appears `abe.actors.State(ctx)` can return a nil state interface with a nil error
when no actor state store is configured, and `LoadWorkflowState` then calls
`state.Get(...)` on it. Either `State()` should return an error in that case (as the
list path effectively surfaces) or the callers need a nil guard.

<details>
<summary>Full stack trace</summary>

```
panic: runtime error: invalid memory address or nil pointer dereference
[signal SIGSEGV: segmentation violation code=0x2 addr=0x18 pc=0x109709594]

goroutine 418 [running]:
github.com/dapr/dapr/pkg/runtime/wfengine/state.LoadWorkflowState({0x110e741d8, 0xab132ce79e0}, {0x0, 0x0}, {0xab132a9dbc0, 0x20}, {{0x16b35e6f9, 0xf}, {0x109a29db7, 0x7}, ...})
	/Users/runner/work/dapr/dapr/pkg/runtime/wfengine/state/state.go:685 +0x114
github.com/dapr/dapr/pkg/runtime/wfengine/backends/actors.(*Actors).GetInstanceHistory(0xab131ab3950, {0x110e741d8, 0xab132ce79e0}, 0xab1331e6940)
	/Users/runner/work/dapr/dapr/pkg/runtime/wfengine/backends/actors/actors.go:861 +0xc0
github.com/dapr/durabletask-go/backend.(*grpcExecutor).GetInstanceHistory(0x1112c8e10?, {0x110e741d8?, 0xab132ce79e0?}, 0x104b5b18c?)
	/Users/runner/go/pkg/mod/github.com/dapr/durabletask-go@v0.12.1/backend/executor.go:569 +0x2c
github.com/dapr/durabletask-go/api/protos._TaskHubSidecarService_GetInstanceHistory_Handler.func1(...)
	/Users/runner/go/pkg/mod/github.com/dapr/durabletask-go@v0.12.1/api/protos/orchestrator_service_grpc.pb.go:675 +0xcc
... (grpc middleware chain) ...
github.com/dapr/durabletask-go/api/protos._TaskHubSidecarService_GetInstanceHistory_Handler(...)
	/Users/runner/go/pkg/mod/github.com/dapr/durabletask-go@v0.12.1/api/protos/orchestrator_service_grpc.pb.go:677 +0x140
google.golang.org/grpc.(*Server).processUnaryRPC(...)
	/Users/runner/go/pkg/mod/google.golang.org/grpc@v1.80.0/server.go:1430 +0xc90
google.golang.org/grpc.(*Server).handleStream(...)
	/Users/runner/go/pkg/mod/google.golang.org/grpc@v1.80.0/server.go:1856 +0x89c
created by google.golang.org/grpc.(*Server).serveStreams.func2 in goroutine 263

❌  The daprd process exited with error code: exit status 2
```

The caller (a `durabletask-go` workflow client calling `GetInstanceHistory`) received
`rpc error: code = Unavailable desc = error reading from server: EOF` as the sidecar died.
</details>

## Steps to Reproduce the Problem

1. `dapr init` (self-hosted, runtime 1.18.1).
2. Create a state store component that is **not** an actor state store (e.g. plain
   `state.redis` with **no** `actorStateStore: "true"` metadata):
   ```yaml
   apiVersion: dapr.io/v1alpha1
   kind: Component
   metadata:
     name: statestore
   spec:
     type: state.redis
     version: v1
     metadata:
       - name: redisHost
         value: localhost:6379
       - name: redisPassword
         value: ""
   ```
3. Start a sidecar with those components (no app needed):
   `dapr run --app-id demo --resources-path ./components`
4. Query workflow instance history against that sidecar for any instance ID
   (CLI equivalent of the gRPC `GetInstanceHistory` call):
   `dapr workflow history -a demo <any-instance-id>`
5. The `demo` sidecar (`daprd`) panics with the stack trace above and exits with
   status 2; the client sees `rpc error: code = Unavailable desc = error reading
   from server: EOF`.

**Contrast:** a `ListInstanceIDs` call over the same TaskHub gRPC API against the same
sidecar returns a clean error (`no state store with actor support found`) instead of
crashing — only the history path is missing the nil guard.

Originally observed while pointing a workflow client (an MCP server) at a sidecar whose
app-id did not own the queried instance; that sidecar had a non-actor state store. The
minimal repro above triggers the same panic without any workflow app involved.

## Proposed fix

`actors.State(ctx)` returns a nil interface with a nil error when the runtime is ready
but no actor state store is configured (`a.state` is only set when `storeEnabled`).
`Reminders()` in the same file already guards against this with
`messages.ErrActorRuntimeNotFound`; `State()` is missing the equivalent guard.

I have a one-line fix (mirroring the `Reminders()` guard) plus a regression test ready,
verified against this repro: with the patch, the same `GetInstanceHistory` call returns
`rpc error: code = Internal desc = the state store is not configured to use the actor
runtime. Have you set the - name: actorStateStore value: "true" in your state store
component file?` and the sidecar stays up. PR follows.

## Release Note

RELEASE NOTE: **FIX** Prevent runtime panic (nil pointer dereference) when querying workflow instance history on a sidecar without an actor state store; return an error instead.
