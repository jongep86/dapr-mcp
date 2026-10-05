// Package workflow exposes Dapr Workflow management operations as MCP tools.
package workflow

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	wf "github.com/dapr/durabletask-go/workflow"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// WorkflowClient defines the workflow operations used by the tools.
// *workflow.Client from durabletask-go satisfies this interface.
type WorkflowClient interface {
	ScheduleWorkflow(ctx context.Context, workflow string, opts ...wf.NewWorkflowOptions) (string, error)
	FetchWorkflowMetadata(ctx context.Context, id string, opts ...wf.FetchWorkflowMetadataOptions) (*wf.WorkflowMetadata, error)
	GetInstanceHistory(ctx context.Context, id string, opts ...wf.GetInstanceHistoryOptions) (*wf.GetInstanceHistoryResponse, error)
	RerunWorkflowFromEvent(ctx context.Context, id string, eventID uint32, opts ...wf.RerunOptions) (string, error)
	ListInstanceIDs(ctx context.Context, opts ...wf.ListInstanceIDsOptions) (*wf.ListInstanceIDsResponse, error)
	SuspendWorkflow(ctx context.Context, id, reason string) error
	ResumeWorkflow(ctx context.Context, id, reason string) error
	TerminateWorkflow(ctx context.Context, id string, opts ...wf.TerminateOptions) error
	RaiseEvent(ctx context.Context, id, eventName string, opts ...wf.RaiseEventOptions) error
	PurgeWorkflowState(ctx context.Context, id string, opts ...wf.PurgeOptions) error
}

const (
	defaultListLimit = 100
	maxListLimit     = 500
)

type StartWorkflowArgs struct {
	AppID        string `json:"appID,omitempty" jsonschema:"App ID of the workflow application to target. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	WorkflowName string `json:"workflowName" jsonschema:"The name of the workflow to start, as registered by the workflow application (e.g., 'order_processing_workflow')."`
	InstanceID   string `json:"instanceID,omitempty" jsonschema:"Optional unique instance ID for the new workflow. If omitted, Dapr generates one."`
	Input        string `json:"input,omitempty" jsonschema:"Optional input for the workflow, typically a JSON string."`
	StartTime    string `json:"startTime,omitempty" jsonschema:"Optional scheduled start time in RFC 3339 format (e.g., '2026-07-15T06:00:00Z'). If omitted, the workflow starts immediately."`
}

type InstanceArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application that owns the instance. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The workflow instance ID."`
}

type ReasonArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application that owns the instance. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The workflow instance ID."`
	Reason     string `json:"reason,omitempty" jsonschema:"Optional short reason, recorded for operators."`
}

type RerunWorkflowArgs struct {
	AppID         string `json:"appID,omitempty" jsonschema:"App ID of the workflow application that owns the instance. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID    string `json:"instanceID" jsonschema:"The instance ID of the (typically failed) workflow to rerun."`
	EventID       int64  `json:"eventID" jsonschema:"The event ID from get_workflow_history to rerun from."`
	NewInstanceID string `json:"newInstanceID,omitempty" jsonschema:"Optional instance ID for the new instance. If omitted, Dapr generates one."`
	Input         string `json:"input,omitempty" jsonschema:"Optional replacement input for the rerun, typically a JSON string. If omitted, the original input is reused."`
}

type RaiseWorkflowEventArgs struct {
	AppID      string `json:"appID,omitempty" jsonschema:"App ID of the workflow application that owns the instance. REQUIRED when multiple workflow apps are configured (the server's own app-id is accepted too); may be omitted in single-app setups."`
	InstanceID string `json:"instanceID" jsonschema:"The instance ID of the workflow waiting for the event."`
	EventName  string `json:"eventName" jsonschema:"The event name the workflow is waiting for (must match the name used in the workflow code)."`
	EventData  string `json:"eventData,omitempty" jsonschema:"Optional event payload, typically a JSON string."`
}

type ListWorkflowsArgs struct {
	AppID             string `json:"appID,omitempty" jsonschema:"Optional app ID to list. When multiple workflow apps are configured and this is omitted, all apps are listed."`
	Limit             int    `json:"limit,omitempty" jsonschema:"Maximum number of instances to return per app (default 100, max 500)."`
	ContinuationToken string `json:"continuationToken,omitempty" jsonschema:"Optional continuation token from a previous list_workflows call. In multi-app setups, pass it together with the appID it was returned for."`
}

func toolError(format string, a ...any) *mcp.CallToolResult {
	message := fmt.Sprintf(format, a...)
	log.Println(message)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
		IsError: true,
	}
}

func toolSuccess(message string, structured map[string]any) (*mcp.CallToolResult, any, error) {
	log.Println(message)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: message}},
	}, structured, nil
}

func startSpan(ctx context.Context, operation string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, span := otel.Tracer("dapr-mcp-server").Start(ctx, operation)
	span.SetAttributes(append([]attribute.KeyValue{attribute.String("dapr.operation", operation)}, attrs...)...)
	return ctx, func() { span.End() }
}

// statusName turns ORCHESTRATION_STATUS_RUNNING into RUNNING.
func statusName(status protos.OrchestrationStatus) string {
	return strings.TrimPrefix(status.String(), "ORCHESTRATION_STATUS_")
}

func formatTime(ts *timestamppb.Timestamp) string {
	return ts.AsTime().UTC().Format(time.RFC3339)
}

func startWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args StartWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "start_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.name", args.WorkflowName),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}

	var opts []wf.NewWorkflowOptions
	if args.InstanceID != "" {
		opts = append(opts, wf.WithInstanceID(args.InstanceID))
	}
	if args.Input != "" {
		opts = append(opts, wf.WithRawInput(wrapperspb.String(args.Input)))
	}
	if args.StartTime != "" {
		startTime, parseErr := time.Parse(time.RFC3339, args.StartTime)
		if parseErr != nil {
			return toolError("invalid startTime '%s': expected RFC 3339 (e.g., '2026-07-15T06:00:00Z')", args.StartTime), nil, nil
		}
		opts = append(opts, wf.WithStartTime(startTime))
	}

	id, err := t.client.ScheduleWorkflow(ctx, args.WorkflowName, opts...)
	if err != nil {
		return toolError("failed to start workflow '%s' on app '%s': %v", args.WorkflowName, t.appID, err), nil, nil
	}

	structured := map[string]any{"app_id": t.appID, "workflow_name": args.WorkflowName, "instance_id": id}
	if args.StartTime != "" {
		structured["start_time"] = args.StartTime
		return toolSuccess(fmt.Sprintf("Successfully scheduled workflow '%s' on app '%s' with instance ID '%s' to start at %s.", args.WorkflowName, t.appID, id, args.StartTime), structured)
	}
	return toolSuccess(fmt.Sprintf("Successfully started workflow '%s' on app '%s' with instance ID '%s'.", args.WorkflowName, t.appID, id), structured)
}

func getWorkflowStatusTool(ctx context.Context, req *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "get_workflow_status",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}

	meta, err := t.client.FetchWorkflowMetadata(ctx, args.InstanceID, wf.WithFetchPayloads(true))
	if err != nil {
		return toolError("failed to get status of workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}

	// wf.WorkflowMetadata is a defined type over the proto message; convert
	// back to use its nil-safe getters.
	pm := (*protos.WorkflowMetadata)(meta)
	status := statusName(pm.GetRuntimeStatus())

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' (workflow '%s', app '%s') is %s.", args.InstanceID, pm.GetName(), t.appID, status)
	structured := map[string]any{
		"app_id":        t.appID,
		"instance_id":   args.InstanceID,
		"workflow_name": pm.GetName(),
		"status":        status,
	}
	if pm.GetCreatedAt() != nil {
		structured["created_at"] = formatTime(pm.GetCreatedAt())
	}
	if pm.GetLastUpdatedAt() != nil {
		structured["last_updated_at"] = formatTime(pm.GetLastUpdatedAt())
	}
	if custom := pm.GetCustomStatus().GetValue(); custom != "" {
		structured["custom_status"] = custom
		fmt.Fprintf(&sb, "\nCustom status: %s", custom)
	}
	if output := pm.GetOutput().GetValue(); output != "" {
		structured["output"] = output
		fmt.Fprintf(&sb, "\nOutput: %s", output)
	}
	if failure := pm.GetFailureDetails(); failure != nil {
		structured["failure"] = failure.GetErrorMessage()
		fmt.Fprintf(&sb, "\nFailure: %s", failure.GetErrorMessage())
	}

	return toolSuccess(sb.String(), structured)
}

// describeEvent returns the history event's type (the name of the set
// eventType oneof field, e.g. taskScheduled) and a short detail for the event
// types most useful when diagnosing a workflow.
func describeEvent(ev *protos.HistoryEvent) (string, string) {
	eventType := "unknown"
	msg := ev.ProtoReflect()
	if oneof := msg.Descriptor().Oneofs().ByName("eventType"); oneof != nil {
		if field := msg.WhichOneof(oneof); field != nil {
			eventType = string(field.Name())
		}
	}

	switch {
	case ev.GetExecutionStarted() != nil:
		return eventType, "workflow: " + ev.GetExecutionStarted().GetName()
	case ev.GetExecutionCompleted() != nil:
		completed := ev.GetExecutionCompleted()
		detail := statusName(completed.GetWorkflowStatus())
		if failure := completed.GetFailureDetails(); failure != nil {
			detail += ": " + failure.GetErrorMessage()
		}
		return eventType, detail
	case ev.GetTaskScheduled() != nil:
		return eventType, "activity: " + ev.GetTaskScheduled().GetName()
	case ev.GetTaskFailed() != nil:
		return eventType, "error: " + ev.GetTaskFailed().GetFailureDetails().GetErrorMessage()
	case ev.GetChildWorkflowInstanceCreated() != nil:
		return eventType, "child workflow: " + ev.GetChildWorkflowInstanceCreated().GetName()
	case ev.GetEventRaised() != nil:
		return eventType, "event: " + ev.GetEventRaised().GetName()
	case ev.GetTimerCreated() != nil && ev.GetTimerCreated().GetFireAt() != nil:
		return eventType, "fires at: " + formatTime(ev.GetTimerCreated().GetFireAt())
	}
	return eventType, ""
}

func getWorkflowHistoryTool(ctx context.Context, req *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "get_workflow_history",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}

	resp, err := t.client.GetInstanceHistory(ctx, args.InstanceID)
	if err != nil {
		return toolError("failed to get history of workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}

	history := (*protos.GetInstanceHistoryResponse)(resp).GetEvents()
	events := make([]map[string]any, 0, len(history))

	var sb strings.Builder
	fmt.Fprintf(&sb, "Workflow instance '%s' on app '%s' has %d history event(s):", args.InstanceID, t.appID, len(history))
	for _, ev := range history {
		eventType, detail := describeEvent(ev)
		event := map[string]any{"event_id": ev.GetEventId(), "type": eventType}
		fmt.Fprintf(&sb, "\n- #%d %s", ev.GetEventId(), eventType)
		if ev.GetTimestamp() != nil {
			event["timestamp"] = formatTime(ev.GetTimestamp())
		}
		if detail != "" {
			event["detail"] = detail
			fmt.Fprintf(&sb, " (%s)", detail)
		}
		events = append(events, event)
	}

	return toolSuccess(sb.String(), map[string]any{
		"app_id":      t.appID,
		"instance_id": args.InstanceID,
		"count":       len(events),
		"events":      events,
	})
}

// withRawRerunInput sets the rerun input verbatim. wf.WithRerunInput would
// JSON-encode it, double-encoding an input that already is a JSON string.
func withRawRerunInput(input string) wf.RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		req.Input = wrapperspb.String(input)
		req.OverwriteInput = true
		return nil
	}
}

func rerunWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args RerunWorkflowArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "rerun_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
		attribute.Int64("dapr.workflow.event_id", args.EventID),
	)
	defer end()

	if args.EventID < 0 || args.EventID > math.MaxUint32 {
		return toolError("invalid eventID %d: use an event ID from get_workflow_history", args.EventID), nil, nil
	}

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}

	var opts []wf.RerunOptions
	if args.NewInstanceID != "" {
		opts = append(opts, wf.WithRerunNewInstanceID(args.NewInstanceID))
	}
	if args.Input != "" {
		opts = append(opts, withRawRerunInput(args.Input))
	}

	newID, err := t.client.RerunWorkflowFromEvent(ctx, args.InstanceID, uint32(args.EventID), opts...)
	if err != nil {
		return toolError("failed to rerun workflow instance '%s' on app '%s' from event %d: %v", args.InstanceID, t.appID, args.EventID, err), nil, nil
	}

	return toolSuccess(
		fmt.Sprintf("Successfully reran workflow instance '%s' on app '%s' from event %d as new instance '%s'.", args.InstanceID, t.appID, args.EventID, newID),
		map[string]any{"app_id": t.appID, "source_instance_id": args.InstanceID, "event_id": args.EventID, "new_instance_id": newID},
	)
}

func pauseWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args ReasonArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "pause_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}
	if err := t.client.SuspendWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		return toolError("failed to pause workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}
	return toolSuccess(
		fmt.Sprintf("Successfully paused workflow instance '%s' on app '%s'.", args.InstanceID, t.appID),
		map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "SUSPENDED"},
	)
}

func resumeWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args ReasonArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "resume_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}
	if err := t.client.ResumeWorkflow(ctx, args.InstanceID, args.Reason); err != nil {
		return toolError("failed to resume workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}
	return toolSuccess(
		fmt.Sprintf("Successfully resumed workflow instance '%s' on app '%s'.", args.InstanceID, t.appID),
		map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "RUNNING"},
	)
}

func terminateWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "terminate_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}
	if err := t.client.TerminateWorkflow(ctx, args.InstanceID); err != nil {
		return toolError("failed to terminate workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}
	return toolSuccess(
		fmt.Sprintf("Successfully requested termination of workflow instance '%s' on app '%s'.", args.InstanceID, t.appID),
		map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "status": "TERMINATED"},
	)
}

func raiseWorkflowEventTool(ctx context.Context, req *mcp.CallToolRequest, args RaiseWorkflowEventArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "raise_workflow_event",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
		attribute.String("dapr.workflow.event_name", args.EventName),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}

	var opts []wf.RaiseEventOptions
	if args.EventData != "" {
		opts = append(opts, wf.WithRawEventData(wrapperspb.String(args.EventData)))
	}
	if err := t.client.RaiseEvent(ctx, args.InstanceID, args.EventName, opts...); err != nil {
		return toolError("failed to raise event '%s' for workflow instance '%s' on app '%s': %v", args.EventName, args.InstanceID, t.appID, err), nil, nil
	}
	return toolSuccess(
		fmt.Sprintf("Successfully raised event '%s' for workflow instance '%s' on app '%s'.", args.EventName, args.InstanceID, t.appID),
		map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "event_name": args.EventName},
	)
}

func purgeWorkflowTool(ctx context.Context, req *mcp.CallToolRequest, args InstanceArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "purge_workflow",
		attribute.String("dapr.workflow.app_id", args.AppID),
		attribute.String("dapr.workflow.instance_id", args.InstanceID),
	)
	defer end()

	t, err := clients.clientFor(args.AppID)
	if err != nil {
		return toolError("%v", err), nil, nil
	}
	if err := t.client.PurgeWorkflowState(ctx, args.InstanceID); err != nil {
		return toolError("failed to purge workflow instance '%s' on app '%s': %v", args.InstanceID, t.appID, err), nil, nil
	}
	return toolSuccess(
		fmt.Sprintf("Successfully purged the state of workflow instance '%s' on app '%s'.", args.InstanceID, t.appID),
		map[string]any{"app_id": t.appID, "instance_id": args.InstanceID, "purged": true},
	)
}

// instanceSummary is one workflow instance in a list_workflows result.
type instanceSummary struct {
	AppID        string `json:"app_id"`
	InstanceID   string `json:"instance_id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// appListing is the result of listing one app's workflow instances.
type appListing struct {
	appID             string
	instances         []instanceSummary
	continuationToken string
	// skipped counts instances whose metadata could not be fetched.
	skipped int
	err     error
}

func listApp(ctx context.Context, t target, pageSize uint32, continuationToken string) appListing {
	listing := appListing{appID: t.appID}

	opts := []wf.ListInstanceIDsOptions{wf.WithListInstanceIDsPageSize(pageSize)}
	if continuationToken != "" {
		opts = append(opts, wf.WithListInstanceIDsContinuationToken(continuationToken))
	}
	resp, err := t.client.ListInstanceIDs(ctx, opts...)
	if err != nil {
		listing.err = err
		return listing
	}

	pr := (*protos.ListInstanceIDsResponse)(resp)
	listing.continuationToken = pr.GetContinuationToken()
	for _, id := range pr.GetInstanceIds() {
		meta, err := t.client.FetchWorkflowMetadata(ctx, id)
		if err != nil {
			log.Printf("Dapr FetchWorkflowMetadata failed for instance '%s' on app '%s': %v", id, t.appID, err)
			listing.skipped++
			continue
		}
		pm := (*protos.WorkflowMetadata)(meta)
		instance := instanceSummary{
			AppID:        t.appID,
			InstanceID:   id,
			WorkflowName: pm.GetName(),
			Status:       statusName(pm.GetRuntimeStatus()),
		}
		if pm.GetCreatedAt() != nil {
			instance.CreatedAt = formatTime(pm.GetCreatedAt())
		}
		listing.instances = append(listing.instances, instance)
	}
	return listing
}

func writeCounts(sb *strings.Builder, label string, counts map[string]int) {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(sb, "\n%s:", label)
	for _, k := range keys {
		fmt.Fprintf(sb, " %s=%d", k, counts[k])
	}
}

func listWorkflowsTool(ctx context.Context, req *mcp.CallToolRequest, args ListWorkflowsArgs) (*mcp.CallToolResult, any, error) {
	ctx, end := startSpan(ctx, "list_workflows", attribute.String("dapr.workflow.app_id", args.AppID))
	defer end()

	var pageSize uint32 = defaultListLimit
	switch {
	case args.Limit > maxListLimit:
		pageSize = maxListLimit
	case args.Limit > 0:
		pageSize = uint32(args.Limit)
	}

	fanOut := args.AppID == "" && clients.multiApp()
	var targets []target
	if fanOut {
		if args.ContinuationToken != "" {
			return toolError("continuationToken requires appID when multiple workflow apps are configured; pass the appID the token was returned for"), nil, nil
		}
		targets = clients.targets()
	} else {
		t, err := clients.clientFor(args.AppID)
		if err != nil {
			return toolError("%v", err), nil, nil
		}
		targets = []target{t}
	}

	listings := make([]appListing, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			listings[i] = listApp(ctx, t, pageSize, args.ContinuationToken)
		}()
	}
	wg.Wait()

	if !fanOut && listings[0].err != nil {
		return toolError("failed to list workflow instances on app '%s': %v", listings[0].appID, listings[0].err), nil, nil
	}

	instances := make([]instanceSummary, 0)
	countsByWorkflow := make(map[string]int)
	countsByStatus := make(map[string]int)
	countsByApp := make(map[string]int)
	continuationTokens := make(map[string]string)
	warnings := make(map[string]string)
	for _, l := range listings {
		if l.err != nil {
			log.Printf("Dapr ListInstanceIDs failed on app '%s': %v", l.appID, l.err)
			warnings[l.appID] = fmt.Sprintf("listing failed, instances of this app are missing: %v", l.err)
			continue
		}
		if l.skipped > 0 {
			warnings[l.appID] = fmt.Sprintf("metadata of %d instance(s) could not be fetched; they are missing", l.skipped)
		}
		if l.continuationToken != "" {
			continuationTokens[l.appID] = l.continuationToken
		}
		countsByApp[l.appID] = len(l.instances)
		for _, instance := range l.instances {
			countsByWorkflow[instance.WorkflowName]++
			countsByStatus[instance.Status]++
			instances = append(instances, instance)
		}
	}

	var sb strings.Builder
	if fanOut {
		fmt.Fprintf(&sb, "Found %d workflow instance(s) across %d app(s).", len(instances), len(targets))
	} else {
		fmt.Fprintf(&sb, "Found %d workflow instance(s) on app '%s'.", len(instances), targets[0].appID)
	}
	if len(instances) > 0 {
		writeCounts(&sb, "Counts by workflow", countsByWorkflow)
		writeCounts(&sb, "Counts by status", countsByStatus)
	}
	if fanOut {
		writeCounts(&sb, "Counts by app", countsByApp)
	}
	for _, instance := range instances {
		fmt.Fprintf(&sb, "\n- %s (app: %s, workflow: %s, status: %s)", instance.InstanceID, instance.AppID, instance.WorkflowName, instance.Status)
	}
	for _, l := range listings {
		if warning, ok := warnings[l.appID]; ok {
			fmt.Fprintf(&sb, "\nWARNING (app '%s'): %s", l.appID, warning)
		}
		if token, ok := continuationTokens[l.appID]; ok {
			fmt.Fprintf(&sb, "\nMore instances are available on app '%s': call list_workflows again with appID '%s' and continuationToken '%s'.", l.appID, l.appID, token)
		}
	}

	structured := map[string]any{
		"instances":          instances,
		"count":              len(instances),
		"counts_by_workflow": countsByWorkflow,
		"counts_by_status":   countsByStatus,
	}
	if fanOut {
		structured["counts_by_app"] = countsByApp
		structured["continuation_tokens"] = continuationTokens
	} else {
		structured["app_id"] = targets[0].appID
		if token := listings[0].continuationToken; token != "" {
			structured["continuation_token"] = token
		}
	}
	if len(warnings) > 0 {
		structured["warnings"] = warnings
	}

	return toolSuccess(sb.String(), structured)
}

// RegisterTools registers the workflow tools. defaultClient targets the
// server's own sidecar, labeled defaultAppID ("default" if empty); byAppID
// holds the clients of additional workflow apps keyed by app-id and may be
// empty for single-app setups.
func RegisterTools(server *mcp.Server, defaultClient WorkflowClient, defaultAppID string, byAppID map[string]WorkflowClient) {
	clients = newRegistry(defaultClient, defaultAppID, byAppID)

	notDestructive := false
	destructive := true
	isOpenWorld := true

	const appIDRule = "**MULTI-APP**: When multiple workflow apps are configured, you MUST pass the `appID` of the app that owns the workflow (the tool error lists the valid app-ids). Use the `app_id` reported by `list_workflows`.\n"

	mcp.AddTool(server, &mcp.Tool{
		Name:  "start_workflow",
		Title: "Start Workflow Instance",
		Description: "Starts a new Dapr Workflow instance, immediately or at a scheduled `startTime`. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `instanceID` is provided.\n\n" +
			"**GUIDANCE:**\n" +
			"1. The workflow must be registered by the targeted workflow application.\n" +
			"2. Provide `input` as a JSON string when the workflow expects input.\n" +
			"3. Workflows run asynchronously: use `get_workflow_status` to track progress.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `workflowName`.\n" +
			"2. **NEVER INVENT**: Do NOT invent workflow names; ask the user if unknown.\n" +
			"3. " + appIDRule,
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
		Description: "Fetches the runtime status (RUNNING, COMPLETED, FAILED, SUSPENDED, TERMINATED, PENDING) of a workflow instance with its output, custom status, and failure details. **This is a READ-ONLY action.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
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
		Description: "Retrieves the event history of a workflow instance: scheduled and completed activities, timers, raised events, and failures. **This is a READ-ONLY action.**\n\n" +
			"**GUIDANCE:**\n" +
			"1. Use this to diagnose WHY a workflow failed or is stuck.\n" +
			"2. The returned `event_id` values can be passed to `rerun_workflow`.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
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
		Description: "Reruns a workflow instance from an event in its history as a NEW instance, reusing the results recorded before that event. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT** unless an explicit `newInstanceID` is provided.\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID` and an `eventID`.\n" +
			"2. **NEVER INVENT**: Take `eventID` from `get_workflow_history`.\n" +
			"3. " + appIDRule,
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
		Description: "Lists workflow instances with their app, workflow name, runtime status, and creation time, plus counts per workflow and per status. **This is a READ-ONLY action.** The tool does NOT filter; filter the returned list yourself.\n\n" +
			"**GUIDANCE:**\n" +
			"1. When multiple workflow apps are configured and `appID` is omitted, all apps are listed with counts per app; one failing app is reported as a warning instead of failing the call.\n" +
			"2. `limit` applies per app. If continuation tokens are returned, call again with the matching `appID` and `continuationToken` before drawing conclusions about totals.\n" +
			"3. Pass the `app_id` of a listed instance to the other workflow tools.\n",
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
		Description: "Pauses (suspends) a running workflow instance until it is resumed with `resume_workflow`. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
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
		Description: "Resumes a paused (suspended) workflow instance. **This is a SIDE-EFFECT action that IS IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. " + appIDRule,
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
		Description: "Forcefully terminates a running workflow instance without executing its remaining steps. **This is a DESTRUCTIVE action that cannot be undone.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **CONFIRMATION**: Unless the user explicitly asked for termination, confirm before calling this tool.\n" +
			"3. " + appIDRule,
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
		Description: "Delivers an external event (e.g., an approval) to a workflow instance waiting for it. **This is a SIDE-EFFECT action that is NOT IDEMPOTENT.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide non-empty `instanceID` and `eventName` values.\n" +
			"2. **NEVER INVENT**: `eventName` must exactly match the name the workflow waits for.\n" +
			"3. Provide `eventData` as a JSON string when the workflow expects a payload.\n" +
			"4. " + appIDRule,
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
		Description: "Permanently deletes the state and history of a COMPLETED, FAILED, or TERMINATED workflow instance. **This is a DESTRUCTIVE action that cannot be undone.**\n\n" +
			"**ARGUMENT RULES:**\n" +
			"1. **REQUIRED INPUTS**: You MUST provide a non-empty `instanceID`.\n" +
			"2. **CONFIRMATION**: Unless the user explicitly asked for purging, confirm before calling this tool.\n" +
			"3. " + appIDRule,
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:    false,
			DestructiveHint: &destructive,
			IdempotentHint:  false,
			OpenWorldHint:   &isOpenWorld,
		},
	}, purgeWorkflowTool)
}
