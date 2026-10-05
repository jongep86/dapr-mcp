package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAppsConfig(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr string
	}{
		{
			name: "empty",
			raw:  "",
			want: map[string]string{},
		},
		{
			name: "whitespace only",
			raw:  "  ",
			want: map[string]string{},
		},
		{
			name: "single app",
			raw:  "company-onboarding=localhost:54783",
			want: map[string]string{"company-onboarding": "localhost:54783"},
		},
		{
			name: "multiple apps with whitespace and trailing comma",
			raw:  " company-onboarding = localhost:54783 , estimating-gate1=localhost:60951, ",
			want: map[string]string{
				"company-onboarding": "localhost:54783",
				"estimating-gate1":   "localhost:60951",
			},
		},
		{
			name:    "missing separator",
			raw:     "company-onboarding",
			wantErr: "invalid DAPR_MCP_SERVER_WORKFLOW_APPS entry 'company-onboarding'",
		},
		{
			name:    "missing app-id",
			raw:     "=localhost:54783",
			wantErr: "expected app-id=host:port",
		},
		{
			name:    "missing address",
			raw:     "company-onboarding=",
			wantErr: "expected app-id=host:port",
		},
		{
			name:    "duplicate app-id",
			raw:     "a=localhost:1,b=localhost:2, a=localhost:3",
			wantErr: "duplicate app-id 'a'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAppsConfig(tt.raw)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
