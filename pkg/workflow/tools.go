// Package workflow exposes Dapr Workflow management operations as MCP tools.
package workflow

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// WorkflowClient defines the interface for workflow management operations.
// This allows for easier testing with mocks. *workflow.Client from
// durabletask-go satisfies this interface.
type WorkflowClient interface {
	ScheduleWorkflow(ctx context.Context, workflow string, opts ...wf.NewWorkflowOptions) (string, error)
	FetchWorkflowMetadata(ctx context.Context, id string, opts ...wf.FetchWorkflowMetadataOptions) (*wf.WorkflowMetadata, error)
	SuspendWorkflow(ctx context.Context, id, reason string) error
	ResumeWorkflow(ctx context.Context, id, reason string) error
	TerminateWorkflow(ctx context.Context, id string, opts ...wf.TerminateOptions) error
	RaiseEvent(ctx context.Context, id, eventName string, opts ...wf.RaiseEventOptions) error
	PurgeWorkflowState(ctx context.Context, id string, opts ...wf.PurgeOptions) error
}

type StartWorkflowArgs struct {
	WorkflowName string `json:"workflowName" jsonschema:"The name of the workflow to start, as registered by the workflow application (e.g., 'order_processing_workflow')."`
	InstanceID   string `json:"instanceID,omitempty" jsonschema:"Optional unique instance ID for the new workflow. If omitted, Dapr generates one."`
	Input        string `json:"input,omitempty" jsonschema:"Optional input for the workflow, typically a JSON string."`
}

type GetWorkflowStatusArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to inspect."`
}

type PauseWorkflowArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to pause."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional reason for pausing the workflow."`
}

type ResumeWorkflowArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to resume."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional reason for resuming the workflow."`
}

type TerminateWorkflowArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to terminate."`
}

type RaiseWorkflowEventArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow waiting for the event."`
	EventName  string `json:"eventName" jsonschema:"The name of the event the workflow is waiting for (must match the name used in the workflow code)."`
	EventData  string `json:"eventData,omitempty" jsonschema:"Optional payload for the event, typically a JSON string."`
}

type PurgeWorkflowArgs struct {
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the completed, failed, or terminated workflow whose state should be purged."`
}

var workflowClient WorkflowClient

// statusString converts a runtime status like ORCHESTRATION_STATUS_RUNNING to RUNNING.
func statusString(meta *protos.WorkflowMetadata) string {
	return strings.TrimPrefix(meta.GetRuntimeStatus().String(), "ORCHESTRATION_STATUS_")
}

func startWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args StartWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "start_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "start_workflow"),
		attribute.String("dapr.workflow.name", args.WorkflowName),
	)

	var opts []wf.NewWorkflowOptions
	if args.InstanceID != "" {
		opts = append(opts, wf.NewWorkflowOptions(api.WithInstanceID(api.InstanceID(args.InstanceID))))
	}
	if args.Input != "" {
		opts = append(opts, wf.NewWorkflowOptions(api.WithRawInput(wrapperspb.String(args.Input))))
	}

	id, err := workflowClient.ScheduleWorkflow(ctx, args.WorkflowName, opts...)
	if err != nil {
		log.Printf("Dapr ScheduleWorkflow failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to start workflow '%s': %v", args.WorkflowName, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully started workflow '%s' with instance ID '%s'.", args.WorkflowName, id)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"workflow_name": args.WorkflowName, "instance_id": id}, nil
}

func getWorkflowStatusTool(ctx context.Context, req *mcp.CallToolRequest, args GetWorkflowStatusArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "get_workflow_status")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "get_workflow_status"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	meta, err := workflowClient.FetchWorkflowMetadata(ctx, args.InstanceID, wf.FetchWorkflowMetadataOptions(api.WithFetchPayloads(true)))
	if err != nil {
		log.Printf("Dapr FetchWorkflowMetadata failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to fetch status of workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	// WorkflowMetadata is a defined type over the proto message and does not
	// inherit its getters, so convert back to access them nil-safely.
	pm := (*protos.WorkflowMetadata)(meta)
	status := statusString(pm)

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' (workflow: '%s') is %s.", pm.GetInstanceId(), pm.GetName(), status)
	structuredResult := map[string]any{
		"instance_id":   pm.GetInstanceId(),
		"workflow_name": pm.GetName(),
		"status":        status,
	}

	if createdAt := pm.GetCreatedAt(); createdAt != nil {
		structuredResult["created_at"] = createdAt.AsTime().String()
	}
	if lastUpdated := pm.GetLastUpdatedAt(); lastUpdated != nil {
		structuredResult["last_updated_at"] = lastUpdated.AsTime().String()
	}
	if output := pm.GetOutput().GetValue(); output != "" {
		fmt.Fprintf(&sb, " Output:\n%s", output)
		structuredResult["output"] = output
	}
	if custom := pm.GetCustomStatus().GetValue(); custom != "" {
		fmt.Fprintf(&sb, " Custom status: %s", custom)
		structuredResult["custom_status"] = custom
	}
	if failure := pm.GetFailureDetails(); failure != nil {
		fmt.Fprintf(&sb, " Failure: %s", failure.GetErrorMessage())
		structuredResult["failure"] = failure.GetErrorMessage()
	}

	result := sb.String()
	log.Println(result)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: result}},
	}, structuredResult, nil
}

func pauseWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args PauseWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "pause_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "pause_workflow"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	if err := workflowClient.SuspendWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		log.Printf("Dapr SuspendWorkflow failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to pause workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully paused workflow instance '%s'.", args.InstanceID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"instance_id": args.InstanceID, "status": "SUSPENDED"}, nil
}

func resumeWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args ResumeWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "resume_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "resume_workflow"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	if err := workflowClient.ResumeWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		log.Printf("Dapr ResumeWorkflow failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to resume workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully resumed workflow instance '%s'.", args.InstanceID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"instance_id": args.InstanceID, "status": "RUNNING"}, nil
}

func terminateWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args TerminateWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "terminate_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "terminate_workflow"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	if err := workflowClient.TerminateWorkflow(ctx, args.InstanceID); err != nil {
		log.Printf("Dapr TerminateWorkflow failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to terminate workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully requested termination of workflow instance '%s'.", args.InstanceID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"instance_id": args.InstanceID, "status": "TERMINATED"}, nil
}

func raiseWorkflowEventTool(ctx context.Context, req *mcp.CallToolRequest, args RaiseWorkflowEventArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "raise_workflow_event")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "raise_workflow_event"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
		attribute.String("dapr.workflow.event_name", args.EventName),
	)

	var opts []wf.RaiseEventOptions
	if args.EventData != "" {
		opts = append(opts, wf.RaiseEventOptions(api.WithRawEventData(wrapperspb.String(args.EventData))))
	}

	if err := workflowClient.RaiseEvent(ctx, args.InstanceID, args.EventName, opts...); err != nil {
		log.Printf("Dapr RaiseEvent failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to raise event '%s' for workflow instance '%s': %v", args.EventName, args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully raised event '%s' for workflow instance '%s'.", args.EventName, args.InstanceID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"instance_id": args.InstanceID, "event_name": args.EventName}, nil
}

func purgeWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args PurgeWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "purge_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "purge_workflow"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	if err := workflowClient.PurgeWorkflowState(ctx, args.InstanceID); err != nil {
		log.Printf("Dapr PurgeWorkflowState failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to purge workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully purged state of workflow instance '%s'.", args.InstanceID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"instance_id": args.InstanceID, "purged": true}, nil
}

func RegisterTools(server *mcp.Server, client WorkflowClient) {
	workflowClient = client

	notDestructive := false
	destructive := true
	isOpenWorld := true

	mcp.AddTool(server, &mcp.Tool{
		Name:  "start_workflow",
		Title: "Start Workflow Instance",
		Description: "Starts (schedules) a new instance of a Dapr Workflow. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `instanceID` is provided.\n\n" +
			"**GUIDANCE:**\n" +
			"1. The workflow must be registered by a workflow application connected to the same Dapr sidecar (same app-id) as this server.\n" +
			"2. Provide `input` as a JSON string when the workflow expects input.\n" +
			"3. Use `get_workflow_status` afterwards to track progress; workflows run asynchronously.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `workflowName`.\n" +
			"2. **NEVER INVENT**: You must NOT invent workflow names. If the workflow name is unknown, ask the user.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &notDestructive,
			IdempotentHint:  false,
			OpenWorldHint:   &isOpenWorld,
		},
	}, startWorkflowTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "get_workflow_status",
		Title: "Get Workflow Instance Status",
		Description: "Fetches the current status and metadata of a workflow instance, including runtime status (RUNNING, COMPLETED, FAILED, SUSPENDED, TERMINATED), output, custom status, and failure details. **This is a READ-ONLY action.**\n\n" +
			"**GUIDANCE:**\n" +
			"1. Use the instance ID returned by `start_workflow`.\n" +
			"2. Workflows are asynchronous: poll this tool to observe progress instead of assuming immediate completion.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    true,
			DestructiveHint: &notDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, getWorkflowStatusTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "pause_workflow",
		Title: "Pause Workflow Instance",
		Description: "Pauses (suspends) a running workflow instance. The instance stops processing new events until resumed with `resume_workflow`. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. Provide a short `reason` so operators can see why the workflow was paused.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &notDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, pauseWorkflowTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "resume_workflow",
		Title: "Resume Workflow Instance",
		Description: "Resumes a previously paused (suspended) workflow instance. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. Provide a short `reason` so operators can see why the workflow was resumed.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &notDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, resumeWorkflowTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "terminate_workflow",
		Title: "Terminate Workflow Instance",
		Description: "Forcefully terminates a running workflow instance. The workflow ends immediately without executing remaining steps. **This is a DESTRUCTIVE action that cannot be undone.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **CONFIRMATION**: Unless the user explicitly asked for termination, confirm before calling this tool.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &destructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, terminateWorkflowTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "raise_workflow_event",
		Title: "Raise Workflow Event",
		Description: "Delivers an external event to a running workflow instance that is waiting for it (e.g., an approval or human-in-the-loop signal). **This is a SIDE-EFFECT action that is NOT IDEMPOTENT.**\n\n" +
			"**GUIDANCE:**\n" +
			"1. The `eventName` must exactly match the event name the workflow code is waiting for.\n" +
			"2. Provide `eventData` as a JSON string when the workflow expects a payload.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide non-empty `instanceID` and `eventName` values.\n" +
			"2. **NEVER INVENT**: You must NOT invent event names.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &notDestructive,
			IdempotentHint:  false,
			OpenWorldHint:   &isOpenWorld,
		},
	}, raiseWorkflowEventTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "purge_workflow",
		Title: "Purge Workflow Instance State",
		Description: "Permanently deletes the persisted state and history of a completed, failed, or terminated workflow instance. **This is a DESTRUCTIVE action that cannot be undone.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **PRECONDITION**: The instance must be in a terminal state (COMPLETED, FAILED, or TERMINATED); purging a running instance fails.\n" +
			"3. **CONFIRMATION**: Unless the user explicitly asked for purging, confirm before calling this tool.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &destructive,
			IdempotentHint:  false,
			OpenWorldHint:   &isOpenWorld,
		},
	}, purgeWorkflowTool)
}
