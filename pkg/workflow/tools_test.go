package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// mockWorkflowClient implements WorkflowClient for testing.
type mockWorkflowClient struct {
	mock.Mock
}

func (m *mockWorkflowClient) ScheduleWorkflow(ctx context.Context, workflow string, opts ...wf.NewWorkflowOptions) (string, error) {
	args := m.Called(ctx, workflow, opts)
	return args.String(0), args.Error(1)
}

func (m *mockWorkflowClient) FetchWorkflowMetadata(ctx context.Context, id string, opts ...wf.FetchWorkflowMetadataOptions) (*wf.WorkflowMetadata, error) {
	args := m.Called(ctx, id, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*wf.WorkflowMetadata), args.Error(1)
}

func (m *mockWorkflowClient) SuspendWorkflow(ctx context.Context, id, reason string) error {
	args := m.Called(ctx, id, reason)
	return args.Error(0)
}

func (m *mockWorkflowClient) ResumeWorkflow(ctx context.Context, id, reason string) error {
	args := m.Called(ctx, id, reason)
	return args.Error(0)
}

func (m *mockWorkflowClient) TerminateWorkflow(ctx context.Context, id string, opts ...wf.TerminateOptions) error {
	args := m.Called(ctx, id, opts)
	return args.Error(0)
}

func (m *mockWorkflowClient) RaiseEvent(ctx context.Context, id, eventName string, opts ...wf.RaiseEventOptions) error {
	args := m.Called(ctx, id, eventName, opts)
	return args.Error(0)
}

func (m *mockWorkflowClient) PurgeWorkflowState(ctx context.Context, id string, opts ...wf.PurgeOptions) error {
	args := m.Called(ctx, id, opts)
	return args.Error(0)
}

func assertTextContains(t *testing.T, result *mcp.CallToolResult, want string) {
	t.Helper()
	if assert.NotEmpty(t, result.Content) {
		textContent, ok := result.Content[0].(*mcp.TextContent)
		assert.True(t, ok)
		assert.Contains(t, textContent.Text, want)
	}
}

func TestStartWorkflowTool(t *testing.T) {
	tests := []struct {
		name        string
		args        StartWorkflowArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful start with generated instance ID",
			args: StartWorkflowArgs{WorkflowName: "order_processing"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("ScheduleWorkflow", mock.Anything, "order_processing", mock.Anything).
					Return("generated-id-123", nil)
			},
			wantErr:     false,
			wantContent: "Successfully started workflow 'order_processing' with instance ID 'generated-id-123'",
		},
		{
			name: "successful start with explicit instance ID and input",
			args: StartWorkflowArgs{WorkflowName: "order_processing", InstanceID: "order-42", Input: `{"item":"widget"}`},
			setupMock: func(m *mockWorkflowClient) {
				m.On("ScheduleWorkflow", mock.Anything, "order_processing", mock.MatchedBy(func(opts []wf.NewWorkflowOptions) bool {
					return len(opts) == 2
				})).Return("order-42", nil)
			},
			wantErr:     false,
			wantContent: "instance ID 'order-42'",
		},
		{
			name: "schedule API error",
			args: StartWorkflowArgs{WorkflowName: "missing_workflow"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("ScheduleWorkflow", mock.Anything, "missing_workflow", mock.Anything).
					Return("", errors.New("workflow not registered"))
			},
			wantErr:     true,
			wantContent: "failed to start workflow 'missing_workflow'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := startWorkflowTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetWorkflowStatusTool(t *testing.T) {
	completedMeta := &wf.WorkflowMetadata{
		InstanceId:    "order-42",
		Name:          "order_processing",
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		Output:        wrapperspb.String(`{"result":"shipped"}`),
	}
	failedMeta := &wf.WorkflowMetadata{
		InstanceId:    "order-43",
		Name:          "order_processing",
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		FailureDetails: &protos.TaskFailureDetails{
			ErrorType:    "ApplicationError",
			ErrorMessage: "payment declined",
		},
	}
	runningMeta := &wf.WorkflowMetadata{
		InstanceId:    "order-44",
		Name:          "order_processing",
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
		CustomStatus:  wrapperspb.String("awaiting approval"),
	}

	tests := []struct {
		name        string
		args        GetWorkflowStatusArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "completed workflow with output",
			args: GetWorkflowStatusArgs{InstanceID: "order-42"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("FetchWorkflowMetadata", mock.Anything, "order-42", mock.Anything).
					Return(completedMeta, nil)
			},
			wantErr:     false,
			wantContent: "is COMPLETED",
		},
		{
			name: "failed workflow with failure details",
			args: GetWorkflowStatusArgs{InstanceID: "order-43"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("FetchWorkflowMetadata", mock.Anything, "order-43", mock.Anything).
					Return(failedMeta, nil)
			},
			wantErr:     false,
			wantContent: "payment declined",
		},
		{
			name: "running workflow with custom status",
			args: GetWorkflowStatusArgs{InstanceID: "order-44"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("FetchWorkflowMetadata", mock.Anything, "order-44", mock.Anything).
					Return(runningMeta, nil)
			},
			wantErr:     false,
			wantContent: "awaiting approval",
		},
		{
			name: "fetch API error",
			args: GetWorkflowStatusArgs{InstanceID: "unknown"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("FetchWorkflowMetadata", mock.Anything, "unknown", mock.Anything).
					Return(nil, errors.New("instance not found"))
			},
			wantErr:     true,
			wantContent: "failed to fetch status of workflow instance 'unknown'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := getWorkflowStatusTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetWorkflowStatusToolStructuredResult(t *testing.T) {
	mockClient := new(mockWorkflowClient)
	mockClient.On("FetchWorkflowMetadata", mock.Anything, "order-42", mock.Anything).
		Return(&wf.WorkflowMetadata{
			InstanceId:    "order-42",
			Name:          "order_processing",
			RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			Output:        wrapperspb.String(`{"result":"shipped"}`),
		}, nil)
	workflowClient = mockClient

	_, structured, err := getWorkflowStatusTool(context.Background(), &mcp.CallToolRequest{}, GetWorkflowStatusArgs{InstanceID: "order-42"})

	assert.NoError(t, err)
	structuredMap, ok := structured.(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "COMPLETED", structuredMap["status"])
	assert.Equal(t, "order_processing", structuredMap["workflow_name"])
	assert.Equal(t, `{"result":"shipped"}`, structuredMap["output"])
}

func TestPauseWorkflowTool(t *testing.T) {
	tests := []struct {
		name        string
		args        PauseWorkflowArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful pause",
			args: PauseWorkflowArgs{InstanceID: "order-42", Reason: "manual hold"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("SuspendWorkflow", mock.Anything, "order-42", "manual hold").Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully paused workflow instance 'order-42'",
		},
		{
			name: "pause API error",
			args: PauseWorkflowArgs{InstanceID: "unknown"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("SuspendWorkflow", mock.Anything, "unknown", "").Return(errors.New("instance not found"))
			},
			wantErr:     true,
			wantContent: "failed to pause workflow instance 'unknown'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := pauseWorkflowTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestResumeWorkflowTool(t *testing.T) {
	tests := []struct {
		name        string
		args        ResumeWorkflowArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful resume",
			args: ResumeWorkflowArgs{InstanceID: "order-42", Reason: "hold released"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("ResumeWorkflow", mock.Anything, "order-42", "hold released").Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully resumed workflow instance 'order-42'",
		},
		{
			name: "resume API error",
			args: ResumeWorkflowArgs{InstanceID: "unknown"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("ResumeWorkflow", mock.Anything, "unknown", "").Return(errors.New("instance not found"))
			},
			wantErr:     true,
			wantContent: "failed to resume workflow instance 'unknown'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := resumeWorkflowTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTerminateWorkflowTool(t *testing.T) {
	tests := []struct {
		name        string
		args        TerminateWorkflowArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful terminate",
			args: TerminateWorkflowArgs{InstanceID: "order-42"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("TerminateWorkflow", mock.Anything, "order-42", mock.Anything).Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully requested termination of workflow instance 'order-42'",
		},
		{
			name: "terminate API error",
			args: TerminateWorkflowArgs{InstanceID: "unknown"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("TerminateWorkflow", mock.Anything, "unknown", mock.Anything).Return(errors.New("instance not found"))
			},
			wantErr:     true,
			wantContent: "failed to terminate workflow instance 'unknown'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := terminateWorkflowTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestRaiseWorkflowEventTool(t *testing.T) {
	tests := []struct {
		name        string
		args        RaiseWorkflowEventArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful event without payload",
			args: RaiseWorkflowEventArgs{InstanceID: "order-42", EventName: "approval"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("RaiseEvent", mock.Anything, "order-42", "approval", mock.MatchedBy(func(opts []wf.RaiseEventOptions) bool {
					return len(opts) == 0
				})).Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully raised event 'approval' for workflow instance 'order-42'",
		},
		{
			name: "successful event with payload",
			args: RaiseWorkflowEventArgs{InstanceID: "order-42", EventName: "approval", EventData: `{"approved":true}`},
			setupMock: func(m *mockWorkflowClient) {
				m.On("RaiseEvent", mock.Anything, "order-42", "approval", mock.MatchedBy(func(opts []wf.RaiseEventOptions) bool {
					return len(opts) == 1
				})).Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully raised event 'approval'",
		},
		{
			name: "raise event API error",
			args: RaiseWorkflowEventArgs{InstanceID: "unknown", EventName: "approval"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("RaiseEvent", mock.Anything, "unknown", "approval", mock.Anything).
					Return(errors.New("instance not found"))
			},
			wantErr:     true,
			wantContent: "failed to raise event 'approval' for workflow instance 'unknown'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := raiseWorkflowEventTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestPurgeWorkflowTool(t *testing.T) {
	tests := []struct {
		name        string
		args        PurgeWorkflowArgs
		setupMock   func(*mockWorkflowClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful purge",
			args: PurgeWorkflowArgs{InstanceID: "order-42"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("PurgeWorkflowState", mock.Anything, "order-42", mock.Anything).Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully purged state of workflow instance 'order-42'",
		},
		{
			name: "purge API error",
			args: PurgeWorkflowArgs{InstanceID: "still-running"},
			setupMock: func(m *mockWorkflowClient) {
				m.On("PurgeWorkflowState", mock.Anything, "still-running", mock.Anything).
					Return(errors.New("instance is not in a terminal state"))
			},
			wantErr:     true,
			wantContent: "failed to purge workflow instance 'still-running'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mockWorkflowClient)
			tt.setupMock(mockClient)
			workflowClient = mockClient

			result, _, err := purgeWorkflowTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			assertTextContains(t, result, tt.wantContent)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestRegisterTools(t *testing.T) {
	mockClient := new(mockWorkflowClient)
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "v1.0.0"}, nil)

	// Should not panic
	RegisterTools(server, mockClient)

	assert.Equal(t, mockClient, workflowClient)
}
