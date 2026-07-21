package state

import (
	"context"
	"errors"
	"testing"

	"github.com/dapr/go-sdk/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/dapr/dapr-mcp-server/test/mocks"
)

func TestSaveStateTool(t *testing.T) {
	tests := []struct {
		name        string
		args        SaveStateArgs
		setupMock   func(*mocks.MockDaprClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful save",
			args: SaveStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
				Value:     `{"data": "test"}`,
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("SaveState", mock.Anything, "statestore", "test-key", []byte(`{"data": "test"}`), mock.Anything, mock.Anything).
					Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully saved key 'test-key' to state store 'statestore'.",
		},
		{
			name: "save failure",
			args: SaveStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
				Value:     `{"data": "test"}`,
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("SaveState", mock.Anything, "statestore", "test-key", []byte(`{"data": "test"}`), mock.Anything, mock.Anything).
					Return(errors.New("connection refused"))
			},
			wantErr:     true,
			wantContent: "failed to save state to store 'statestore'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mocks.MockDaprClient)
			tt.setupMock(mockClient)

			// Replace the package-level client
			stateClient = mockClient

			result, _, err := saveStateTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err) // The function doesn't return errors, it returns them in result
			assert.Equal(t, tt.wantErr, result.IsError)
			if len(result.Content) > 0 {
				textContent, ok := result.Content[0].(*mcp.TextContent)
				assert.True(t, ok)
				assert.Contains(t, textContent.Text, tt.wantContent)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetStateTool(t *testing.T) {
	tests := []struct {
		name        string
		args        GetStateArgs
		setupMock   func(*mocks.MockDaprClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful get with value",
			args: GetStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetState", mock.Anything, "statestore", "test-key", mock.Anything).
					Return(&client.StateItem{
						Key:   "test-key",
						Value: []byte(`{"data": "test"}`),
					}, nil)
			},
			wantErr:     false,
			wantContent: "Retrieved key 'test-key' from 'statestore'",
		},
		{
			name: "key not found",
			args: GetStateArgs{
				StoreName: "statestore",
				Key:       "nonexistent-key",
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetState", mock.Anything, "statestore", "nonexistent-key", mock.Anything).
					Return(&client.StateItem{
						Key:   "nonexistent-key",
						Value: []byte{},
					}, nil)
			},
			wantErr:     false,
			wantContent: "Key 'nonexistent-key' not found",
		},
		{
			name: "get failure",
			args: GetStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetState", mock.Anything, "statestore", "test-key", mock.Anything).
					Return(nil, errors.New("connection refused"))
			},
			wantErr:     true,
			wantContent: "dapr GetState failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mocks.MockDaprClient)
			tt.setupMock(mockClient)

			stateClient = mockClient

			result, _, err := getStateTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			if len(result.Content) > 0 {
				textContent, ok := result.Content[0].(*mcp.TextContent)
				assert.True(t, ok)
				assert.Contains(t, textContent.Text, tt.wantContent)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestGetBulkStateTool(t *testing.T) {
	tests := []struct {
		name        string
		args        GetBulkStateArgs
		setupMock   func(*mocks.MockDaprClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful bulk get",
			args: GetBulkStateArgs{
				StoreName: "statestore",
				Keys:      []string{"k1", "k2"},
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetBulkState", mock.Anything, "statestore", []string{"k1", "k2"}, mock.Anything, int32(0)).
					Return([]*client.BulkStateItem{
						{Key: "k1", Value: []byte(`v1`), Etag: "1"},
						{Key: "k2", Value: []byte(`v2`), Etag: "2"},
					}, nil)
			},
			wantErr:     false,
			wantContent: "Retrieved 2 key(s) from state store 'statestore'.",
		},
		{
			name: "item with per-key error is surfaced",
			args: GetBulkStateArgs{
				StoreName: "statestore",
				Keys:      []string{"k1", "k2"},
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetBulkState", mock.Anything, "statestore", []string{"k1", "k2"}, mock.Anything, int32(0)).
					Return([]*client.BulkStateItem{
						{Key: "k1", Value: []byte(`v1`), Etag: "1"},
						{Key: "k2", Error: "key not found"},
					}, nil)
			},
			wantErr:     false,
			wantContent: "Retrieved 2 key(s) from state store 'statestore'.",
		},
		{
			name: "bulk get failure",
			args: GetBulkStateArgs{
				StoreName: "statestore",
				Keys:      []string{"k1", "k2"},
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("GetBulkState", mock.Anything, "statestore", []string{"k1", "k2"}, mock.Anything, int32(0)).
					Return(nil, errors.New("connection refused"))
			},
			wantErr:     true,
			wantContent: "dapr GetBulkState failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mocks.MockDaprClient)
			tt.setupMock(mockClient)

			stateClient = mockClient

			result, structured, err := getBulkStateTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			if len(result.Content) > 0 {
				textContent, ok := result.Content[0].(*mcp.TextContent)
				assert.True(t, ok)
				assert.Contains(t, textContent.Text, tt.wantContent)
			}

			if tt.name == "item with per-key error is surfaced" {
				items, ok := structured.([]map[string]string)
				assert.True(t, ok)
				assert.Equal(t, "key not found", items[1]["error"])
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestDeleteStateTool(t *testing.T) {
	tests := []struct {
		name        string
		args        DeleteStateArgs
		setupMock   func(*mocks.MockDaprClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful delete",
			args: DeleteStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("DeleteState", mock.Anything, "statestore", "test-key", mock.Anything, mock.Anything).
					Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully deleted key 'test-key'",
		},
		{
			name: "delete failure",
			args: DeleteStateArgs{
				StoreName: "statestore",
				Key:       "test-key",
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("DeleteState", mock.Anything, "statestore", "test-key", mock.Anything, mock.Anything).
					Return(errors.New("connection refused"))
			},
			wantErr:     true,
			wantContent: "dapr DeleteState failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mocks.MockDaprClient)
			tt.setupMock(mockClient)

			stateClient = mockClient

			result, _, err := deleteStateTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			if len(result.Content) > 0 {
				textContent, ok := result.Content[0].(*mcp.TextContent)
				assert.True(t, ok)
				assert.Contains(t, textContent.Text, tt.wantContent)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestExecuteTransactionTool(t *testing.T) {
	tests := []struct {
		name        string
		args        ExecuteTransactionArgs
		setupMock   func(*mocks.MockDaprClient)
		wantErr     bool
		wantContent string
	}{
		{
			name: "successful transaction with save and delete",
			args: ExecuteTransactionArgs{
				StoreName: "statestore",
				Items: []TransactionItem{
					{Key: "key1", Value: "value1", IsDelete: false},
					{Key: "key2", Value: "", IsDelete: true},
				},
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("ExecuteStateTransaction", mock.Anything, "statestore", mock.Anything, mock.MatchedBy(func(ops []*client.StateOperation) bool {
					return len(ops) == 2
				})).Return(nil)
			},
			wantErr:     false,
			wantContent: "Successfully executed 2 state operations",
		},
		{
			name: "transaction failure",
			args: ExecuteTransactionArgs{
				StoreName: "statestore",
				Items: []TransactionItem{
					{Key: "key1", Value: "value1", IsDelete: false},
				},
			},
			setupMock: func(m *mocks.MockDaprClient) {
				m.On("ExecuteStateTransaction", mock.Anything, "statestore", mock.Anything, mock.Anything).
					Return(errors.New("transaction failed"))
			},
			wantErr:     true,
			wantContent: "ExecuteStateTransaction failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(mocks.MockDaprClient)
			tt.setupMock(mockClient)

			stateClient = mockClient

			result, _, err := executeTransactionTool(context.Background(), &mcp.CallToolRequest{}, tt.args)

			assert.NoError(t, err)
			assert.Equal(t, tt.wantErr, result.IsError)
			if len(result.Content) > 0 {
				textContent, ok := result.Content[0].(*mcp.TextContent)
				assert.True(t, ok)
				assert.Contains(t, textContent.Text, tt.wantContent)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestRegisterTools(t *testing.T) {
	mockClient := new(mocks.MockDaprClient)
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "v1.0.0"}, nil)

	// Should not panic
	RegisterTools(server, mockClient)

	assert.Equal(t, mockClient, stateClient)
}
