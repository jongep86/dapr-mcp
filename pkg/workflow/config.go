package workflow

import (
	"fmt"
	"sort"
	"strings"
)

// ParseAppsConfig parses the DAPR_MCP_SERVER_WORKFLOW_APPS environment
// variable: comma-separated app-id=host:port pairs mapping workflow
// applications to their sidecar gRPC endpoints.
func ParseAppsConfig(raw string) (map[string]string, error) {
	result := make(map[string]string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return result, nil
	}

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		appID, addr, ok := strings.Cut(entry, "=")
		appID = strings.TrimSpace(appID)
		addr = strings.TrimSpace(addr)
		if !ok || appID == "" || addr == "" {
			return nil, fmt.Errorf("invalid workflow apps entry '%s': expected app-id=host:port", entry)
		}
		if _, dup := result[appID]; dup {
			return nil, fmt.Errorf("duplicate app-id '%s' in workflow apps configuration", appID)
		}
		result[appID] = addr
	}
	return result, nil
}

// configuredAppIDs returns the sorted app-ids of the additional workflow apps.
func configuredAppIDs() []string {
	ids := make([]string, 0, len(workflowClientsByApp))
	for id := range workflowClientsByApp {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// clientFor resolves an appID to a workflow client. An empty appID selects
// the default client (the server's own sidecar).
func clientFor(appID string) (WorkflowClient, error) {
	if appID == "" {
		return workflowClient, nil
	}
	if client, ok := workflowClientsByApp[appID]; ok {
		return client, nil
	}
	if len(workflowClientsByApp) == 0 {
		return nil, fmt.Errorf("unknown appID '%s': no additional workflow apps are configured (DAPR_MCP_SERVER_WORKFLOW_APPS); omit appID to use the server's own sidecar", appID)
	}
	return nil, fmt.Errorf("unknown appID '%s': configured apps: %s (omit appID for the server's own sidecar)", appID, strings.Join(configuredAppIDs(), ", "))
}
