package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr-mcp-server/test/mocks"
)

const ownAppID = "mcp-server"

var createdAt = time.Date(2026, 7, 14, 6, 0, 0, 0, time.UTC)

// useSingleApp registers a default client with an empty pool.
func useSingleApp(t *testing.T) *mocks.MockWorkflowClient {
	t.Helper()
	def := new(mocks.MockWorkflowClient)
	clients = newRegistry(def, ownAppID, nil)
	t.Cleanup(func() { def.AssertExpectations(t) })
	return def
}

// useMultiApp registers a default client plus the given pool apps.
func useMultiApp(t *testing.T, appIDs ...string) (*mocks.MockWorkflowClient, map[string]*mocks.MockWorkflowClient) {
	t.Helper()
	def := new(mocks.MockWorkflowClient)
	pool := make(map[string]WorkflowClient)
	byApp := make(map[string]*mocks.MockWorkflowClient)
	for _, id := range appIDs {
		m := new(mocks.MockWorkflowClient)
		pool[id] = m
		byApp[id] = m
	}
	clients = newRegistry(def, ownAppID, pool)
	t.Cleanup(func() {
		def.AssertExpectations(t)
		for _, m := range byApp {
			m.AssertExpectations(t)
		}
	})
	return def, byApp
}

func text(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	require.Len(t, result.Content, 1)
	tc, ok := result.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	return tc.Text
}

func metadata(id, name string, status protos.OrchestrationStatus) *wf.WorkflowMetadata {
	return (*wf.WorkflowMetadata)(&protos.WorkflowMetadata{
		InstanceId:    id,
		Name:          name,
		RuntimeStatus: status,
		CreatedAt:     timestamppb.New(createdAt),
	})
}

func idList(token string, ids ...string) *wf.ListInstanceIDsResponse {
	resp := &protos.ListInstanceIDsResponse{InstanceIds: ids}
	if token != "" {
		resp.ContinuationToken = &token
	}
	return (*wf.ListInstanceIDsResponse)(resp)
}

func TestStartWorkflowTool(t *testing.T) {
	t.Run("starts immediately with instance ID and input", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ScheduleWorkflow", mock.Anything, "order", mock.MatchedBy(func(opts []wf.NewWorkflowOptions) bool {
			req := &protos.CreateInstanceRequest{}
			for _, opt := range opts {
				require.NoError(t, opt(req))
			}
			return len(opts) == 2 && req.GetInstanceId() == "order-1" && req.GetInput().GetValue() == `{"qty":1}`
		})).Return("order-1", nil)

		result, structured, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, StartWorkflowArgs{
			WorkflowName: "order",
			InstanceID:   "order-1",
			Input:        `{"qty":1}`,
		})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Successfully started workflow 'order' on app 'mcp-server' with instance ID 'order-1'.", text(t, result))
		assert.Equal(t, map[string]any{"app_id": ownAppID, "workflow_name": "order", "instance_id": "order-1"}, structured)
	})

	t.Run("schedules at start time", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ScheduleWorkflow", mock.Anything, "order", mock.MatchedBy(func(opts []wf.NewWorkflowOptions) bool {
			req := &protos.CreateInstanceRequest{}
			for _, opt := range opts {
				require.NoError(t, opt(req))
			}
			return req.GetScheduledStartTimestamp().AsTime().Equal(createdAt)
		})).Return("generated", nil)

		result, structured, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, StartWorkflowArgs{
			WorkflowName: "order",
			StartTime:    "2026-07-14T06:00:00Z",
		})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Contains(t, text(t, result), "to start at 2026-07-14T06:00:00Z")
		assert.Equal(t, "2026-07-14T06:00:00Z", structured.(map[string]any)["start_time"])
	})

	t.Run("invalid start time", func(t *testing.T) {
		useSingleApp(t)
		result, structured, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, StartWorkflowArgs{
			WorkflowName: "order",
			StartTime:    "tomorrow",
		})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Contains(t, text(t, result), "invalid startTime 'tomorrow'")
		assert.Nil(t, structured)
	})

	t.Run("dapr error", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ScheduleWorkflow", mock.Anything, "order", mock.Anything).Return("", errors.New("not registered"))

		result, _, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, StartWorkflowArgs{WorkflowName: "order"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to start workflow 'order' on app 'mcp-server': not registered", text(t, result))
	})

	t.Run("targets the requested app", func(t *testing.T) {
		_, apps := useMultiApp(t, "billing")
		apps["billing"].On("ScheduleWorkflow", mock.Anything, "invoice", mock.Anything).Return("inv-1", nil)

		result, _, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, StartWorkflowArgs{AppID: "billing", WorkflowName: "invoice"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Contains(t, text(t, result), "on app 'billing'")
	})
}

func TestGetWorkflowStatusTool(t *testing.T) {
	t.Run("completed with output and custom status", func(t *testing.T) {
		def := useSingleApp(t)
		meta := &protos.WorkflowMetadata{
			InstanceId:    "order-1",
			Name:          "order",
			RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			CreatedAt:     timestamppb.New(createdAt),
			LastUpdatedAt: timestamppb.New(createdAt.Add(time.Minute)),
			Output:        wrapperspb.String(`"shipped"`),
			CustomStatus:  wrapperspb.String("step 3/3"),
		}
		def.On("FetchWorkflowMetadata", mock.Anything, "order-1", mock.MatchedBy(func(opts []wf.FetchWorkflowMetadataOptions) bool {
			req := &protos.GetInstanceRequest{}
			for _, opt := range opts {
				opt(req)
			}
			return req.GetGetInputsAndOutputs()
		})).Return((*wf.WorkflowMetadata)(meta), nil)

		result, structured, err := getWorkflowStatusTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Workflow instance 'order-1' (workflow 'order', app 'mcp-server') is COMPLETED.\nCustom status: step 3/3\nOutput: \"shipped\"", text(t, result))
		assert.Equal(t, map[string]any{
			"app_id":          ownAppID,
			"instance_id":     "order-1",
			"workflow_name":   "order",
			"status":          "COMPLETED",
			"created_at":      "2026-07-14T06:00:00Z",
			"last_updated_at": "2026-07-14T06:01:00Z",
			"output":          `"shipped"`,
			"custom_status":   "step 3/3",
		}, structured)
	})

	t.Run("failed", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("FetchWorkflowMetadata", mock.Anything, "order-1", mock.Anything).Return((*wf.WorkflowMetadata)(&protos.WorkflowMetadata{
			Name:           "order",
			RuntimeStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
			FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "boom"},
		}), nil)

		result, structured, err := getWorkflowStatusTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.Contains(t, text(t, result), "is FAILED.\nFailure: boom")
		assert.Equal(t, "boom", structured.(map[string]any)["failure"])
	})

	t.Run("dapr error", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("FetchWorkflowMetadata", mock.Anything, "missing", mock.Anything).Return(nil, errors.New("not found"))

		result, structured, err := getWorkflowStatusTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "missing"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to get status of workflow instance 'missing' on app 'mcp-server': not found", text(t, result))
		assert.Nil(t, structured)
	})
}

func TestGetWorkflowHistoryTool(t *testing.T) {
	t.Run("summarizes events", func(t *testing.T) {
		def := useSingleApp(t)
		events := []*protos.HistoryEvent{
			{EventId: -1, Timestamp: timestamppb.New(createdAt), EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: "order"}}},
			{EventId: 0, EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "reserve"}}},
			{EventId: 1, EventType: &protos.HistoryEvent_TaskFailed{TaskFailed: &protos.TaskFailedEvent{FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "out of stock"}}}},
			{EventId: 2, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: "refund"}}},
			{EventId: 3, EventType: &protos.HistoryEvent_EventRaised{EventRaised: &protos.EventRaisedEvent{Name: "approval"}}},
			{EventId: 4, EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: &protos.TimerCreatedEvent{FireAt: timestamppb.New(createdAt)}}},
			{EventId: 5, EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{}}},
			{EventId: 6, EventType: &protos.HistoryEvent_ExecutionCompleted{ExecutionCompleted: &protos.ExecutionCompletedEvent{
				WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
				FailureDetails: &protos.TaskFailureDetails{ErrorMessage: "out of stock"},
			}}},
			{EventId: 7},
		}
		def.On("GetInstanceHistory", mock.Anything, "order-1", mock.Anything).
			Return((*wf.GetInstanceHistoryResponse)(&protos.GetInstanceHistoryResponse{Events: events}), nil)

		result, structured, err := getWorkflowHistoryTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Workflow instance 'order-1' on app 'mcp-server' has 9 history event(s):\n"+
			"- #-1 executionStarted (workflow: order)\n"+
			"- #0 taskScheduled (activity: reserve)\n"+
			"- #1 taskFailed (error: out of stock)\n"+
			"- #2 childWorkflowInstanceCreated (child workflow: refund)\n"+
			"- #3 eventRaised (event: approval)\n"+
			"- #4 timerCreated (fires at: 2026-07-14T06:00:00Z)\n"+
			"- #5 timerFired\n"+
			"- #6 executionCompleted (FAILED: out of stock)\n"+
			"- #7 unknown", text(t, result))
		s := structured.(map[string]any)
		assert.Equal(t, 9, s["count"])
		first := s["events"].([]map[string]any)[0]
		assert.Equal(t, map[string]any{"event_id": int32(-1), "type": "executionStarted", "timestamp": "2026-07-14T06:00:00Z", "detail": "workflow: order"}, first)
	})

	t.Run("dapr error", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("GetInstanceHistory", mock.Anything, "order-1", mock.Anything).Return(nil, errors.New("unavailable"))

		result, _, err := getWorkflowHistoryTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to get history of workflow instance 'order-1' on app 'mcp-server': unavailable", text(t, result))
	})
}

func TestRerunWorkflowTool(t *testing.T) {
	t.Run("reruns with new instance ID and raw input", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("RerunWorkflowFromEvent", mock.Anything, "order-1", uint32(3), mock.MatchedBy(func(opts []wf.RerunOptions) bool {
			req := &protos.RerunWorkflowFromEventRequest{}
			for _, opt := range opts {
				require.NoError(t, opt(req))
			}
			return req.GetNewInstanceID() == "order-1-rerun" && req.GetInput().GetValue() == `{"qty":2}` && req.GetOverwriteInput()
		})).Return("order-1-rerun", nil)

		result, structured, err := rerunWorkflowTool(context.Background(), &mcp.CallToolRequest{}, RerunWorkflowArgs{
			InstanceID:    "order-1",
			EventID:       3,
			NewInstanceID: "order-1-rerun",
			Input:         `{"qty":2}`,
		})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Successfully reran workflow instance 'order-1' on app 'mcp-server' from event 3 as new instance 'order-1-rerun'.", text(t, result))
		assert.Equal(t, "order-1-rerun", structured.(map[string]any)["new_instance_id"])
	})

	t.Run("invalid event ID", func(t *testing.T) {
		useSingleApp(t)
		for _, eventID := range []int64{-1, 1 << 32} {
			result, _, err := rerunWorkflowTool(context.Background(), &mcp.CallToolRequest{}, RerunWorkflowArgs{InstanceID: "order-1", EventID: eventID})
			require.NoError(t, err)
			assert.True(t, result.IsError)
			assert.Contains(t, text(t, result), "invalid eventID")
		}
	})

	t.Run("dapr error", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("RerunWorkflowFromEvent", mock.Anything, "order-1", uint32(0), mock.Anything).Return("", errors.New("event not found"))

		result, _, err := rerunWorkflowTool(context.Background(), &mcp.CallToolRequest{}, RerunWorkflowArgs{InstanceID: "order-1"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to rerun workflow instance 'order-1' on app 'mcp-server' from event 0: event not found", text(t, result))
	})
}

func TestInstanceActionTools(t *testing.T) {
	type call func() (*mcp.CallToolResult, any, error)
	ctx := context.Background()
	req := &mcp.CallToolRequest{}

	tests := []struct {
		name        string
		method      string
		methodArgs  []any
		run         call
		wantSuccess string
		wantError   string
		wantStatus  map[string]any
	}{
		{
			name:       "pause_workflow",
			method:     "SuspendWorkflow",
			methodArgs: []any{mock.Anything, "order-1", "maintenance"},
			run: func() (*mcp.CallToolResult, any, error) {
				return pauseWorkflowTool(ctx, req, ReasonArgs{InstanceID: "order-1", Reason: "maintenance"})
			},
			wantSuccess: "Successfully paused workflow instance 'order-1' on app 'mcp-server'.",
			wantError:   "failed to pause workflow instance 'order-1' on app 'mcp-server': boom",
			wantStatus:  map[string]any{"app_id": ownAppID, "instance_id": "order-1", "status": "SUSPENDED"},
		},
		{
			name:       "resume_workflow",
			method:     "ResumeWorkflow",
			methodArgs: []any{mock.Anything, "order-1", ""},
			run: func() (*mcp.CallToolResult, any, error) {
				return resumeWorkflowTool(ctx, req, ReasonArgs{InstanceID: "order-1"})
			},
			wantSuccess: "Successfully resumed workflow instance 'order-1' on app 'mcp-server'.",
			wantError:   "failed to resume workflow instance 'order-1' on app 'mcp-server': boom",
			wantStatus:  map[string]any{"app_id": ownAppID, "instance_id": "order-1", "status": "RUNNING"},
		},
		{
			name:       "terminate_workflow",
			method:     "TerminateWorkflow",
			methodArgs: []any{mock.Anything, "order-1", mock.Anything},
			run: func() (*mcp.CallToolResult, any, error) {
				return terminateWorkflowTool(ctx, req, InstanceArgs{InstanceID: "order-1"})
			},
			wantSuccess: "Successfully requested termination of workflow instance 'order-1' on app 'mcp-server'.",
			wantError:   "failed to terminate workflow instance 'order-1' on app 'mcp-server': boom",
			wantStatus:  map[string]any{"app_id": ownAppID, "instance_id": "order-1", "status": "TERMINATED"},
		},
		{
			name:   "raise_workflow_event",
			method: "RaiseEvent",
			methodArgs: []any{mock.Anything, "order-1", "approval", mock.MatchedBy(func(opts []wf.RaiseEventOptions) bool {
				r := &protos.RaiseEventRequest{}
				for _, opt := range opts {
					_ = opt(r)
				}
				return r.GetInput().GetValue() == `{"ok":true}`
			})},
			run: func() (*mcp.CallToolResult, any, error) {
				return raiseWorkflowEventTool(ctx, req, RaiseWorkflowEventArgs{InstanceID: "order-1", EventName: "approval", EventData: `{"ok":true}`})
			},
			wantSuccess: "Successfully raised event 'approval' for workflow instance 'order-1' on app 'mcp-server'.",
			wantError:   "failed to raise event 'approval' for workflow instance 'order-1' on app 'mcp-server': boom",
			wantStatus:  map[string]any{"app_id": ownAppID, "instance_id": "order-1", "event_name": "approval"},
		},
		{
			name:       "purge_workflow",
			method:     "PurgeWorkflowState",
			methodArgs: []any{mock.Anything, "order-1", mock.Anything},
			run: func() (*mcp.CallToolResult, any, error) {
				return purgeWorkflowTool(ctx, req, InstanceArgs{InstanceID: "order-1"})
			},
			wantSuccess: "Successfully purged the state of workflow instance 'order-1' on app 'mcp-server'.",
			wantError:   "failed to purge workflow instance 'order-1' on app 'mcp-server': boom",
			wantStatus:  map[string]any{"app_id": ownAppID, "instance_id": "order-1", "purged": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" success", func(t *testing.T) {
			def := useSingleApp(t)
			def.On(tt.method, tt.methodArgs...).Return(nil)

			result, structured, err := tt.run()

			require.NoError(t, err)
			assert.False(t, result.IsError)
			assert.Equal(t, tt.wantSuccess, text(t, result))
			assert.Equal(t, tt.wantStatus, structured)
		})

		t.Run(tt.name+" dapr error", func(t *testing.T) {
			def := useSingleApp(t)
			def.On(tt.method, tt.methodArgs...).Return(errors.New("boom"))

			result, structured, err := tt.run()

			require.NoError(t, err)
			assert.True(t, result.IsError)
			assert.Equal(t, tt.wantError, text(t, result))
			assert.Nil(t, structured)
		})
	}
}

// TestPerInstanceToolsRequireAppID checks the hardening guard: with workflow
// apps configured, no per-instance call may silently fall back to the
// server's own sidecar (dapr/dapr#10217), and unknown app-ids are rejected
// before any client is called.
func TestPerInstanceToolsRequireAppID(t *testing.T) {
	ctx := context.Background()
	req := &mcp.CallToolRequest{}
	tools := map[string]func(appID string) *mcp.CallToolResult{
		"start_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := startWorkflowTool(ctx, req, StartWorkflowArgs{AppID: appID, WorkflowName: "order"})
			return r
		},
		"get_workflow_status": func(appID string) *mcp.CallToolResult {
			r, _, _ := getWorkflowStatusTool(ctx, req, InstanceArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"get_workflow_history": func(appID string) *mcp.CallToolResult {
			r, _, _ := getWorkflowHistoryTool(ctx, req, InstanceArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"rerun_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := rerunWorkflowTool(ctx, req, RerunWorkflowArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"pause_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := pauseWorkflowTool(ctx, req, ReasonArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"resume_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := resumeWorkflowTool(ctx, req, ReasonArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"terminate_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := terminateWorkflowTool(ctx, req, InstanceArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
		"raise_workflow_event": func(appID string) *mcp.CallToolResult {
			r, _, _ := raiseWorkflowEventTool(ctx, req, RaiseWorkflowEventArgs{AppID: appID, InstanceID: "order-1", EventName: "approval"})
			return r
		},
		"purge_workflow": func(appID string) *mcp.CallToolResult {
			r, _, _ := purgeWorkflowTool(ctx, req, InstanceArgs{AppID: appID, InstanceID: "order-1"})
			return r
		},
	}

	for name, run := range tools {
		t.Run(name+" without appID", func(t *testing.T) {
			// The mocks have no expectations: any client call fails the test.
			useMultiApp(t, "billing")
			result := run("")
			assert.True(t, result.IsError)
			assert.Equal(t, "appID is required because multiple workflow apps are configured; pass one of: billing, mcp-server", text(t, result))
		})

		t.Run(name+" with unknown appID", func(t *testing.T) {
			useMultiApp(t, "billing")
			result := run("shipping")
			assert.True(t, result.IsError)
			assert.Equal(t, "unknown appID 'shipping'; configured app-ids: billing, mcp-server", text(t, result))
		})

		t.Run(name+" unknown appID in single-app mode", func(t *testing.T) {
			useSingleApp(t)
			result := run("billing")
			assert.True(t, result.IsError)
			assert.Equal(t, "unknown appID 'billing'; configured app-ids: mcp-server", text(t, result))
		})
	}
}

func TestPerInstanceToolsRouteByAppID(t *testing.T) {
	t.Run("pool app", func(t *testing.T) {
		_, apps := useMultiApp(t, "billing", "shipping")
		apps["shipping"].On("SuspendWorkflow", mock.Anything, "ship-1", "").Return(nil)

		result, structured, err := pauseWorkflowTool(context.Background(), &mcp.CallToolRequest{}, ReasonArgs{AppID: "shipping", InstanceID: "ship-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "shipping", structured.(map[string]any)["app_id"])
	})

	t.Run("own app-id targets the default client", func(t *testing.T) {
		def, _ := useMultiApp(t, "billing")
		def.On("GetInstanceHistory", mock.Anything, "order-1", mock.Anything).
			Return((*wf.GetInstanceHistoryResponse)(&protos.GetInstanceHistoryResponse{}), nil)

		result, _, err := getWorkflowHistoryTool(context.Background(), &mcp.CallToolRequest{}, InstanceArgs{AppID: ownAppID, InstanceID: "order-1"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
	})
}

func TestListWorkflowsToolSingleApp(t *testing.T) {
	pageSize := func(want uint32) any {
		return mock.MatchedBy(func(opts []wf.ListInstanceIDsOptions) bool {
			req := &protos.ListInstanceIDsRequest{}
			for _, opt := range opts {
				_ = opt(req)
			}
			return req.GetPageSize() == want && req.ContinuationToken == nil
		})
	}

	t.Run("lists with counts and continuation token", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ListInstanceIDs", mock.Anything, pageSize(100)).Return(idList("next", "a", "b", "c"), nil)
		def.On("FetchWorkflowMetadata", mock.Anything, "a", mock.Anything).Return(metadata("a", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING), nil)
		def.On("FetchWorkflowMetadata", mock.Anything, "b", mock.Anything).Return(metadata("b", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED), nil)
		def.On("FetchWorkflowMetadata", mock.Anything, "c", mock.Anything).Return(nil, errors.New("gone"))

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Found 2 workflow instance(s) on app 'mcp-server'.\n"+
			"Counts by workflow: order=2\n"+
			"Counts by status: COMPLETED=1 RUNNING=1\n"+
			"- a (app: mcp-server, workflow: order, status: RUNNING)\n"+
			"- b (app: mcp-server, workflow: order, status: COMPLETED)\n"+
			"WARNING (app 'mcp-server'): metadata of 1 instance(s) could not be fetched; they are missing\n"+
			"More instances are available on app 'mcp-server': call list_workflows again with appID 'mcp-server' and continuationToken 'next'.", text(t, result))

		s := structured.(map[string]any)
		assert.Equal(t, 2, s["count"])
		assert.Equal(t, ownAppID, s["app_id"])
		assert.Equal(t, "next", s["continuation_token"])
		assert.Equal(t, []instanceSummary{
			{AppID: ownAppID, InstanceID: "a", WorkflowName: "order", Status: "RUNNING", CreatedAt: "2026-07-14T06:00:00Z"},
			{AppID: ownAppID, InstanceID: "b", WorkflowName: "order", Status: "COMPLETED", CreatedAt: "2026-07-14T06:00:00Z"},
		}, s["instances"])
		assert.NotContains(t, s, "counts_by_app")
		assert.NotContains(t, s, "continuation_tokens")
	})

	t.Run("empty result", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(idList(""), nil)

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.Equal(t, "Found 0 workflow instance(s) on app 'mcp-server'.", text(t, result))
		assert.NotContains(t, structured.(map[string]any), "continuation_token")
	})

	t.Run("limit is capped and token forwarded", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ListInstanceIDs", mock.Anything, mock.MatchedBy(func(opts []wf.ListInstanceIDsOptions) bool {
			req := &protos.ListInstanceIDsRequest{}
			for _, opt := range opts {
				_ = opt(req)
			}
			return req.GetPageSize() == 500 && req.GetContinuationToken() == "page-2"
		})).Return(idList(""), nil)

		result, _, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{Limit: 10000, ContinuationToken: "page-2"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
	})

	t.Run("custom limit", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ListInstanceIDs", mock.Anything, pageSize(25)).Return(idList(""), nil)

		_, _, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{Limit: 25})
		require.NoError(t, err)
	})

	t.Run("dapr error", func(t *testing.T) {
		def := useSingleApp(t)
		def.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("no state store with actor support found"))

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to list workflow instances on app 'mcp-server': no state store with actor support found", text(t, result))
		assert.Nil(t, structured)
	})

	t.Run("unknown appID", func(t *testing.T) {
		useSingleApp(t)
		result, _, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{AppID: "billing"})
		require.NoError(t, err)
		assert.True(t, result.IsError)
	})
}

func TestListWorkflowsToolFanOut(t *testing.T) {
	t.Run("merges apps, reports per-app counts, warnings and tokens", func(t *testing.T) {
		def, apps := useMultiApp(t, "billing", "shipping")
		def.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(idList("", "o-1"), nil)
		def.On("FetchWorkflowMetadata", mock.Anything, "o-1", mock.Anything).Return(metadata("o-1", "order", protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING), nil)
		apps["billing"].On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("connection refused"))
		apps["shipping"].On("ListInstanceIDs", mock.Anything, mock.Anything).Return(idList("ship-next", "s-1", "s-2"), nil)
		apps["shipping"].On("FetchWorkflowMetadata", mock.Anything, "s-1", mock.Anything).Return(metadata("s-1", "ship", protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING), nil)
		apps["shipping"].On("FetchWorkflowMetadata", mock.Anything, "s-2", mock.Anything).Return(metadata("s-2", "ship", protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED), nil)

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Equal(t, "Found 3 workflow instance(s) across 3 app(s).\n"+
			"Counts by workflow: order=1 ship=2\n"+
			"Counts by status: FAILED=1 RUNNING=2\n"+
			"Counts by app: mcp-server=1 shipping=2\n"+
			"- o-1 (app: mcp-server, workflow: order, status: RUNNING)\n"+
			"- s-1 (app: shipping, workflow: ship, status: RUNNING)\n"+
			"- s-2 (app: shipping, workflow: ship, status: FAILED)\n"+
			"WARNING (app 'billing'): listing failed, instances of this app are missing: connection refused\n"+
			"More instances are available on app 'shipping': call list_workflows again with appID 'shipping' and continuationToken 'ship-next'.", text(t, result))

		s := structured.(map[string]any)
		assert.Equal(t, 3, s["count"])
		assert.Equal(t, map[string]int{"mcp-server": 1, "shipping": 2}, s["counts_by_app"])
		assert.Equal(t, map[string]int{"order": 1, "ship": 2}, s["counts_by_workflow"])
		assert.Equal(t, map[string]int{"RUNNING": 2, "FAILED": 1}, s["counts_by_status"])
		assert.Equal(t, map[string]string{"shipping": "ship-next"}, s["continuation_tokens"])
		assert.Equal(t, map[string]string{"billing": "listing failed, instances of this app are missing: connection refused"}, s["warnings"])
		assert.NotContains(t, s, "app_id")
		instances := s["instances"].([]instanceSummary)
		require.Len(t, instances, 3)
		assert.Equal(t, "shipping", instances[2].AppID)
	})

	t.Run("all apps failing still returns a result", func(t *testing.T) {
		def, apps := useMultiApp(t, "billing")
		def.On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("down"))
		apps["billing"].On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("down"))

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		assert.Len(t, structured.(map[string]any)["warnings"], 2)
		assert.Equal(t, map[string]string{}, structured.(map[string]any)["continuation_tokens"])
	})

	t.Run("continuation token requires appID", func(t *testing.T) {
		useMultiApp(t, "billing")

		result, _, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{ContinuationToken: "ship-next"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Contains(t, text(t, result), "continuationToken requires appID")
	})

	t.Run("appID lists a single app with its token", func(t *testing.T) {
		_, apps := useMultiApp(t, "billing", "shipping")
		apps["shipping"].On("ListInstanceIDs", mock.Anything, mock.MatchedBy(func(opts []wf.ListInstanceIDsOptions) bool {
			req := &protos.ListInstanceIDsRequest{}
			for _, opt := range opts {
				_ = opt(req)
			}
			return req.GetContinuationToken() == "ship-next"
		})).Return(idList("", "s-3"), nil)
		apps["shipping"].On("FetchWorkflowMetadata", mock.Anything, "s-3", mock.Anything).Return(metadata("s-3", "ship", protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED), nil)

		result, structured, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{AppID: "shipping", ContinuationToken: "ship-next"})

		require.NoError(t, err)
		assert.False(t, result.IsError)
		s := structured.(map[string]any)
		assert.Equal(t, "shipping", s["app_id"])
		assert.Equal(t, 1, s["count"])
		assert.NotContains(t, s, "counts_by_app")
	})

	t.Run("single app failure with appID is an error", func(t *testing.T) {
		_, apps := useMultiApp(t, "billing")
		apps["billing"].On("ListInstanceIDs", mock.Anything, mock.Anything).Return(nil, errors.New("down"))

		result, _, err := listWorkflowsTool(context.Background(), &mcp.CallToolRequest{}, ListWorkflowsArgs{AppID: "billing"})

		require.NoError(t, err)
		assert.True(t, result.IsError)
		assert.Equal(t, "failed to list workflow instances on app 'billing': down", text(t, result))
	})
}

func TestRegisterTools(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "v1.0.0"}, nil)
	def := new(mocks.MockWorkflowClient)
	billing := new(mocks.MockWorkflowClient)

	RegisterTools(server, def, "", map[string]WorkflowClient{"billing": billing})

	assert.Same(t, def, clients.defaultClient)
	assert.Equal(t, "default", clients.defaultAppID)
	assert.Same(t, billing, clients.byAppID["billing"])

	ctx := context.Background()
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(ctx, serverTransport, nil)
	require.NoError(t, err)
	defer func() { _ = serverSession.Close() }()

	client := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "v1.0.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(ctx, nil)
	require.NoError(t, err)
	names := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	assert.ElementsMatch(t, []string{
		"start_workflow", "get_workflow_status", "get_workflow_history", "rerun_workflow", "list_workflows",
		"pause_workflow", "resume_workflow", "terminate_workflow", "raise_workflow_event", "purge_workflow",
	}, names)
}
