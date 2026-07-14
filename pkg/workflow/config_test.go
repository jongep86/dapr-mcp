package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAppsConfig(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr string
	}{
		{
			name: "empty input yields empty map",
			raw:  "",
			want: map[string]string{},
		},
		{
			name: "whitespace-only input yields empty map",
			raw:  "   ",
			want: map[string]string{},
		},
		{
			name: "single entry",
			raw:  "onboarding=localhost:54783",
			want: map[string]string{"onboarding": "localhost:54783"},
		},
		{
			name: "multiple entries with whitespace",
			raw:  " onboarding = localhost:54783 , estimating = localhost:60951 ",
			want: map[string]string{
				"onboarding": "localhost:54783",
				"estimating": "localhost:60951",
			},
		},
		{
			name: "trailing comma is tolerated",
			raw:  "onboarding=localhost:54783,",
			want: map[string]string{"onboarding": "localhost:54783"},
		},
		{
			name:    "missing separator",
			raw:     "onboarding",
			wantErr: "invalid workflow apps entry",
		},
		{
			name:    "empty app-id",
			raw:     "=localhost:54783",
			wantErr: "invalid workflow apps entry",
		},
		{
			name:    "empty address",
			raw:     "onboarding=",
			wantErr: "invalid workflow apps entry",
		},
		{
			name:    "duplicate app-id",
			raw:     "onboarding=localhost:1,onboarding=localhost:2",
			wantErr: "duplicate app-id 'onboarding'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseAppsConfig(tt.raw)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
