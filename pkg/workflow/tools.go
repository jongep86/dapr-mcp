// Package workflow exposes Dapr Workflow management operations as MCP tools.
package workflow

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

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
	ListInstanceIDs(ctx context.Context, opts ...wf.ListInstanceIDsOptions) (*wf.ListInstanceIDsResponse, error)
	GetInstanceHistory(ctx context.Context, id string, opts ...wf.GetInstanceHistoryOptions) (*wf.GetInstanceHistoryResponse, error)
	RerunWorkflowFromEvent(ctx context.Context, id string, eventID uint32, opts ...wf.RerunOptions) (string, error)
	SuspendWorkflow(ctx context.Context, id, reason string) error
	ResumeWorkflow(ctx context.Context, id, reason string) error
	TerminateWorkflow(ctx context.Context, id string, opts ...wf.TerminateOptions) error
	RaiseEvent(ctx context.Context, id, eventName string, opts ...wf.RaiseEventOptions) error
	PurgeWorkflowState(ctx context.Context, id string, opts ...wf.PurgeOptions) error
}

type StartWorkflowArgs struct {
	AppID        string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	WorkflowName string `json:"workflowName" jsonschema:"The name of the workflow to start, as registered by the workflow application (e.g., 'order_processing_workflow')."`
	InstanceID   string `json:"instanceID,omitempty" jsonschema:"Optional unique instance ID for the new workflow. If omitted, Dapr generates one."`
	Input        string `json:"input,omitempty" jsonschema:"Optional input for the workflow, typically a JSON string."`
	StartTime    string `json:"startTime,omitempty" jsonschema:"Optional scheduled start time in RFC 3339 format (e.g., '2026-07-15T06:00:00Z'). If omitted, the workflow starts immediately."`
}

type GetWorkflowStatusArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to inspect."`
}

type PauseWorkflowArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to pause."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional reason for pausing the workflow."`
}

type ResumeWorkflowArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to resume."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional reason for resuming the workflow."`
}

type TerminateWorkflowArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow to terminate."`
}

type RaiseWorkflowEventArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow waiting for the event."`
	EventName  string `json:"eventName" jsonschema:"The name of the event the workflow is waiting for (must match the name used in the workflow code)."`
	EventData  string `json:"eventData,omitempty" jsonschema:"Optional payload for the event, typically a JSON string."`
}

type PurgeWorkflowArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the completed, failed, or terminated workflow whose state should be purged."`
}

type GetWorkflowHistoryArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow whose event history should be retrieved."`
}

type RerunWorkflowArgs struct {
	AppID         string `json:"appID,omitempty" jsonschema:"Optional app ID of the workflow application to target, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to use the server's own sidecar."`
	InstanceID    string `json:"instanceID" jsonschema:"The instance ID of the (typically failed) workflow to rerun."`
	EventID       int    `json:"eventID" jsonschema:"The eventId of the history event to rerun from. Use get_workflow_history to find it."`
	NewInstanceID string `json:"newInstanceID,omitempty" jsonschema:"Optional instance ID for the rerun instance. If omitted, Dapr generates one."`
	Input         string `json:"input,omitempty" jsonschema:"Optional replacement input for the rerun, typically a JSON string. If omitted, the original input is reused."`
}

type ListWorkflowsArgs struct {
	AppID             string `json:"appID,omitempty" jsonschema:"Optional app ID to list workflows for, as configured in DAPR_MCP_SERVER_WORKFLOW_APPS. Omit to list across the server's own sidecar AND all configured apps."`
	Limit             int    `json:"limit,omitempty" jsonschema:"Maximum number of instances to return per call (default 100, max 500)."`
	ContinuationToken string `json:"continuationToken,omitempty" jsonschema:"Optional continuation token from a previous list_workflows call to fetch the next page."`
}

var (
	// workflowClient targets the server's own sidecar (the default app).
	workflowClient WorkflowClient
	// workflowClientsByApp holds clients for additional workflow apps
	// configured via DAPR_MCP_SERVER_WORKFLOW_APPS.
	workflowClientsByApp map[string]WorkflowClient
	// defaultAppLabel is the app-id of the server's own sidecar, used to
	// label the default client in multi-app listings.
	defaultAppLabel = "default"
)

// toolError builds an error CallToolResult with the given message.
func toolError(message string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
		IsError: true,
	}
}

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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	var opts []wf.NewWorkflowOptions
	if args.InstanceID != "" {
		opts = append(opts, wf.NewWorkflowOptions(api.WithInstanceID(api.InstanceID(args.InstanceID))))
	}
	if args.Input != "" {
		opts = append(opts, wf.NewWorkflowOptions(api.WithRawInput(wrapperspb.String(args.Input))))
	}
	if args.StartTime != "" {
		startTime, parseErr := time.Parse(time.RFC3339, args.StartTime)
		if parseErr != nil {
			toolErrorMessage := fmt.Errorf("invalid startTime '%s': must be RFC 3339 (e.g., '2026-07-15T06:00:00Z'): %v", args.StartTime, parseErr).Error()
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
				IsError: true,
			}, nil, nil
		}
		opts = append(opts, wf.NewWorkflowOptions(api.WithStartTime(startTime)))
	}

	id, err := client.ScheduleWorkflow(ctx, args.WorkflowName, opts...)
	if err == nil && args.StartTime != "" {
		successMessage := fmt.Sprintf("Successfully scheduled workflow '%s' with instance ID '%s' to start at %s.", args.WorkflowName, id, args.StartTime)
		log.Println(successMessage)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
		}, map[string]any{"workflow_name": args.WorkflowName, "instance_id": id, "start_time": args.StartTime}, nil
	}
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	meta, err := client.FetchWorkflowMetadata(ctx, args.InstanceID, wf.FetchWorkflowMetadataOptions(api.WithFetchPayloads(true)))
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	if err := client.SuspendWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	if err := client.ResumeWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	if err := client.TerminateWorkflow(ctx, args.InstanceID); err != nil {
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	var opts []wf.RaiseEventOptions
	if args.EventData != "" {
		opts = append(opts, wf.RaiseEventOptions(api.WithRawEventData(wrapperspb.String(args.EventData))))
	}

	if err := client.RaiseEvent(ctx, args.InstanceID, args.EventName, opts...); err != nil {
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

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	if err := client.PurgeWorkflowState(ctx, args.InstanceID); err != nil {
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

// historyEventSummary returns the oneof event type name (e.g. taskScheduled)
// and a short human-readable detail for the most relevant event types.
func historyEventSummary(ev *protos.HistoryEvent) (string, string) {
	eventType := "unknown"
	m := ev.ProtoReflect()
	if od := m.Descriptor().Oneofs().ByName("eventType"); od != nil {
		if fd := m.WhichOneof(od); fd != nil {
			eventType = string(fd.Name())
		}
	}

	var detail string
	switch {
	case ev.GetExecutionStarted() != nil:
		detail = fmt.Sprintf("workflow: %s", ev.GetExecutionStarted().GetName())
	case ev.GetExecutionCompleted() != nil:
		detail = strings.TrimPrefix(ev.GetExecutionCompleted().GetWorkflowStatus().String(), "ORCHESTRATION_STATUS_")
		if failure := ev.GetExecutionCompleted().GetFailureDetails(); failure != nil {
			detail = fmt.Sprintf("%s: %s", detail, failure.GetErrorMessage())
		}
	case ev.GetTaskScheduled() != nil:
		detail = fmt.Sprintf("activity: %s", ev.GetTaskScheduled().GetName())
	case ev.GetTaskFailed() != nil:
		detail = fmt.Sprintf("error: %s", ev.GetTaskFailed().GetFailureDetails().GetErrorMessage())
	case ev.GetEventRaised() != nil:
		detail = fmt.Sprintf("event: %s", ev.GetEventRaised().GetName())
	case ev.GetTimerCreated() != nil:
		if fireAt := ev.GetTimerCreated().GetFireAt(); fireAt != nil {
			detail = fmt.Sprintf("fires at: %s", fireAt.AsTime().String())
		}
	}
	return eventType, detail
}

func getWorkflowHistoryTool(ctx context.Context, req *mcp.CallToolRequest, args GetWorkflowHistoryArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "get_workflow_history")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "get_workflow_history"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	resp, err := client.GetInstanceHistory(ctx, args.InstanceID)
	if err != nil {
		log.Printf("Dapr GetInstanceHistory failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to fetch history of workflow instance '%s': %v", args.InstanceID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	// GetInstanceHistoryResponse is a defined type over the proto message and
	// does not inherit its getters, so convert back to access them nil-safely.
	pr := (*protos.GetInstanceHistoryResponse)(resp)
	events := make([]map[string]any, 0, len(pr.GetEvents()))

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' has %d history event(s).\n", args.InstanceID, len(pr.GetEvents()))

	for _, ev := range pr.GetEvents() {
		eventType, detail := historyEventSummary(ev)

		event := map[string]any{
			"event_id": ev.GetEventId(),
			"type":     eventType,
		}
		if ts := ev.GetTimestamp(); ts != nil {
			event["timestamp"] = ts.AsTime().String()
		}
		fmt.Fprintf(&sb, "- #%d %s", ev.GetEventId(), eventType)
		if detail != "" {
			event["detail"] = detail
			fmt.Fprintf(&sb, " (%s)", detail)
		}
		sb.WriteString("\n")
		events = append(events, event)
	}

	result := sb.String()
	log.Println(result)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: result}},
	}, map[string]any{"instance_id": args.InstanceID, "events": events, "count": len(events)}, nil
}

func rerunWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args RerunWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "rerun_workflow")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "rerun_workflow"),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)

	if args.EventID < 0 || args.EventID > math.MaxUint32 {
		toolErrorMessage := fmt.Sprintf("invalid eventID %d: must be a valid event ID from get_workflow_history", args.EventID)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	client, err := clientFor(args.AppID)
	if err != nil {
		return toolError(err.Error()), nil, nil
	}

	var opts []wf.RerunOptions
	if args.NewInstanceID != "" {
		opts = append(opts, wf.RerunOptions(api.WithRerunNewInstanceID(api.InstanceID(args.NewInstanceID))))
	}
	if args.Input != "" {
		// api.WithRerunInput JSON-marshals its argument, which would
		// double-encode a raw JSON string, so set the raw input directly.
		opts = append(opts, func(rerunReq *protos.RerunWorkflowFromEventRequest) error {
			rerunReq.Input = wrapperspb.String(args.Input)
			rerunReq.OverwriteInput = true
			return nil
		})
	}

	newID, err := client.RerunWorkflowFromEvent(ctx, args.InstanceID, uint32(args.EventID), opts...)
	if err != nil {
		log.Printf("Dapr RerunWorkflowFromEvent failed: %v", err)
		toolErrorMessage := fmt.Errorf("failed to rerun workflow instance '%s' from event %d: %v", args.InstanceID, args.EventID, err).Error()
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: toolErrorMessage}},
			IsError: true,
		}, nil, nil
	}

	successMessage := fmt.Sprintf("Successfully started rerun of workflow instance '%s' from event %d as new instance '%s'.", args.InstanceID, args.EventID, newID)
	log.Println(successMessage)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: successMessage}},
	}, map[string]any{"source_instance_id": args.InstanceID, "event_id": args.EventID, "new_instance_id": newID}, nil
}

func listWorkflowsTool(ctx context.Context, req *mcp.CallToolRequest, args ListWorkflowsArgs) (*mcp.CallToolResult, any, error) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, "list_workflows")
	defer span.End()
	span.SetAttributes(
		attribute.String("dapr.operation", "list_workflows"),
	)

	limit := args.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	// Resolve the targets: one app when appID is set (or no extra apps are
	// configured), otherwise fan out over the default sidecar and all
	// configured apps.
	type appTarget struct {
		label  string
		client WorkflowClient
	}
	var targets []appTarget
	fanOut := false
	if args.AppID != "" {
		client, err := clientFor(args.AppID)
		if err != nil {
			return toolError(err.Error()), nil, nil
		}
		targets = []appTarget{{label: args.AppID, client: client}}
	} else if len(workflowClientsByApp) == 0 {
		targets = []appTarget{{label: defaultAppLabel, client: workflowClient}}
	} else {
		fanOut = true
		if args.ContinuationToken != "" {
			return toolError("continuationToken requires an explicit appID when multiple workflow apps are configured; pass the appID the token belongs to"), nil, nil
		}
		targets = []appTarget{{label: defaultAppLabel, client: workflowClient}}
		for _, appID := range configuredAppIDs() {
			targets = append(targets, appTarget{label: appID, client: workflowClientsByApp[appID]})
		}
	}

	instances := make([]map[string]any, 0)
	countsByWorkflow := make(map[string]int)
	countsByStatus := make(map[string]int)
	countsByApp := make(map[string]int)
	continuationTokens := make(map[string]string)
	var appErrors []string
	fetchErrors := 0
	var lines strings.Builder

	for _, target := range targets {
		opts := []wf.ListInstanceIDsOptions{
			wf.ListInstanceIDsOptions(api.WithListInstanceIDsPageSize(uint32(limit))),
		}
		if args.ContinuationToken != "" {
			opts = append(opts, wf.ListInstanceIDsOptions(api.WithListInstanceIDsContinuationToken(args.ContinuationToken)))
		}

		resp, err := target.client.ListInstanceIDs(ctx, opts...)
		if err != nil {
			log.Printf("Dapr ListInstanceIDs failed for app '%s': %v", target.label, err)
			if !fanOut {
				return toolError(fmt.Sprintf("failed to list workflow instances for app '%s': %v", target.label, err)), nil, nil
			}
			appErrors = append(appErrors, fmt.Sprintf("%s: %v", target.label, err))
			continue
		}

		// ListInstanceIDsResponse is a defined type over the proto message and
		// does not inherit its getters, so convert back to access them nil-safely.
		pr := (*protos.ListInstanceIDsResponse)(resp)

		for _, id := range pr.GetInstanceIds() {
			meta, err := target.client.FetchWorkflowMetadata(ctx, id)
			if err != nil {
				log.Printf("Dapr FetchWorkflowMetadata failed for instance '%s' (app '%s'): %v", id, target.label, err)
				fetchErrors++
				continue
			}
			pm := (*protos.WorkflowMetadata)(meta)
			status := statusString(pm)

			instance := map[string]any{
				"instance_id":   pm.GetInstanceId(),
				"workflow_name": pm.GetName(),
				"status":        status,
				"app_id":        target.label,
			}
			if createdAt := pm.GetCreatedAt(); createdAt != nil {
				instance["created_at"] = createdAt.AsTime().String()
			}
			instances = append(instances, instance)
			countsByWorkflow[pm.GetName()]++
			countsByStatus[status]++
			countsByApp[target.label]++
			if fanOut {
				fmt.Fprintf(&lines, "- %s (app: %s, workflow: %s, status: %s)\n", pm.GetInstanceId(), target.label, pm.GetName(), status)
			} else {
				fmt.Fprintf(&lines, "- %s (workflow: %s, status: %s)\n", pm.GetInstanceId(), pm.GetName(), status)
			}
		}

		if token := pr.GetContinuationToken(); token != "" {
			continuationTokens[target.label] = token
		}
	}

	var sb strings.Builder
	if fanOut {
		fmt.Fprintf(&sb, "Found %d workflow instance(s) across %d app(s).", len(instances), len(targets))
	} else {
		fmt.Fprintf(&sb, "Found %d workflow instance(s).", len(instances))
	}
	if len(countsByWorkflow) > 0 {
		sb.WriteString(" Counts by workflow:")
		for name, count := range countsByWorkflow {
			fmt.Fprintf(&sb, " %s=%d", name, count)
		}
		sb.WriteString(". Counts by status:")
		for status, count := range countsByStatus {
			fmt.Fprintf(&sb, " %s=%d", status, count)
		}
		sb.WriteString(".")
		if fanOut {
			sb.WriteString(" Counts by app:")
			for _, target := range targets {
				fmt.Fprintf(&sb, " %s=%d", target.label, countsByApp[target.label])
			}
			sb.WriteString(".")
		}
	}
	if fetchErrors > 0 {
		fmt.Fprintf(&sb, " WARNING: metadata could not be fetched for %d instance(s); they are excluded.", fetchErrors)
	}
	for _, appError := range appErrors {
		fmt.Fprintf(&sb, " WARNING: listing failed for app %s; its instances are missing.", appError)
	}
	if len(instances) > 0 {
		fmt.Fprintf(&sb, "\n%s", lines.String())
	}

	structuredResult := map[string]any{
		"instances":          instances,
		"count":              len(instances),
		"counts_by_workflow": countsByWorkflow,
		"counts_by_status":   countsByStatus,
	}
	if fanOut {
		structuredResult["counts_by_app"] = countsByApp
	}
	if len(appErrors) > 0 {
		structuredResult["app_errors"] = appErrors
	}
	if len(continuationTokens) > 0 {
		if fanOut {
			structuredResult["continuation_tokens"] = continuationTokens
			for appID, token := range continuationTokens {
				fmt.Fprintf(&sb, "\nMore instances are available for app '%s'; call again with appID '%s' and continuationToken '%s'.", appID, appID, token)
			}
		} else {
			token := continuationTokens[targets[0].label]
			structuredResult["continuation_token"] = token
			fmt.Fprintf(&sb, "\nMore instances are available; pass continuationToken '%s' to fetch the next page.", token)
		}
	}

	result := sb.String()
	log.Println(result)

	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: result}},
	}, structuredResult, nil
}

// RegisterTools registers the workflow tools. defaultClient targets the
// server's own sidecar (labeled defaultAppID in listings); byAppID holds
// clients for additional workflow apps, keyed by app-id.
func RegisterTools(server *mcp.Server, defaultClient WorkflowClient, defaultAppID string, byAppID map[string]WorkflowClient) {
	workflowClient = defaultClient
	workflowClientsByApp = byAppID
	if defaultAppID != "" {
		defaultAppLabel = defaultAppID
	}

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
			"3. Use `get_workflow_status` afterwards to track progress; workflows run asynchronously.\n" +
			"4. Provide `startTime` (RFC 3339) to schedule the workflow for a later moment instead of starting it immediately.\n\n" +
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
		Name:  "get_workflow_history",
		Title: "Get Workflow Instance History",
		Description: "Retrieves the full event history of a workflow instance: which activities were scheduled and completed, timers, raised events, and failures with error messages. **This is a READ-ONLY action.**\n\n" +
			"**GUIDANCE:**\n" +
			"1. Use this to diagnose WHY a workflow failed or appears stuck; `get_workflow_status` only shows the end result.\n" +
			"2. The `event_id` values in the history can be used with `rerun_workflow` to rerun a workflow from a specific point.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    true,
			DestructiveHint: &notDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, getWorkflowHistoryTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "rerun_workflow",
		Title: "Rerun Workflow From Event",
		Description: "Reruns a workflow instance from a specific event in its history, creating a NEW instance that reuses the results of all events before that point. Useful to retry a failed workflow without repeating already-completed work. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `newInstanceID` is provided.\n\n" +
			"**GUIDANCE:**\n" +
			"1. Use `get_workflow_history` first to find the `eventID` to rerun from (typically the failed activity's scheduling event).\n" +
			"2. Provide `input` only when the rerun should use different input than the original run.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID` and a valid `eventID`.\n" +
			"2. **NEVER INVENT**: You must NOT invent event IDs; take them from `get_workflow_history`.\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &notDestructive,
			IdempotentHint:  false,
			OpenWorldHint:   &isOpenWorld,
		},
	}, rerunWorkflowTool)

	mcp.AddTool(server, &mcp.Tool{
		Name:  "list_workflows",
		Title: "List Workflow Instances",
		Description: "Lists all workflow instances known to the workflow engine with their name, runtime status (RUNNING, COMPLETED, FAILED, SUSPENDED, TERMINATED, PENDING), and creation time, plus counts per workflow and per status. **This is a READ-ONLY action.** The tool does NOT filter; apply any filtering (e.g., only RUNNING instances) yourself on the returned list.\n\n" +
			"**GUIDANCE:**\n" +
			"1. When multiple workflow apps are configured and `appID` is omitted, the tool lists across ALL apps and additionally reports counts per app; pass `appID` to list a single app.\n" +
			"2. If a continuation token is returned, call the tool again with it (and, in multi-app setups, the matching `appID`) to fetch the remaining instances before drawing conclusions about totals.\n" +
			"3. Each listed instance ID can be passed to `get_workflow_status` for full details (pass the same `appID` the instance was listed under).\n",
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    true,
			DestructiveHint: &notDestructive,
			IdempotentHint:  true,
			OpenWorldHint:   &isOpenWorld,
		},
	}, listWorkflowsTool)

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
