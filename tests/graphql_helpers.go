package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// GraphQLResponse wraps the standard GraphQL response envelope.
type GraphQLResponse struct {
	Data   map[string]interface{} `json:"data"`
	Errors []GraphQLError         `json:"errors,omitempty"`
}

// GraphQLError represents a single entry in the GraphQL "errors" array.
type GraphQLError struct {
	Message string `json:"message"`
}

// BuildGraphQLHandler wires the gateway GraphQL schema to the given per-service
// gRPC connections. Only the clients a test actually exercises need to be set;
// unset connections produce clients that are never dialed.
func BuildGraphQLHandler(log logger.LoggerInterface, redisClient *redis.Client, conns *testhelper.ServiceConnections) http.Handler {
	resolver := testhelper.NewResolverWithRedis(conns, log, redisClient)
	return testhelper.NewGraphQLHTTPHandler(resolver)
}

// ExecuteGraphQL posts a query/mutation to the handler and decodes the response.
func ExecuteGraphQL(handler http.Handler, query string, variables map[string]interface{}, authToken string) (*GraphQLResponse, error) {
	payload := map[string]interface{}{"query": query}
	if variables != nil {
		payload["variables"] = variables
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	respBody, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		return nil, err
	}

	var resp GraphQLResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GraphQLOp executes a single-root-field query/mutation and returns that root
// field's response object. It fails the test on transport or GraphQL errors.
func GraphQLOp(t require.TestingT, handler http.Handler, op, query string, variables map[string]interface{}) map[string]interface{} {
	resp, err := ExecuteGraphQL(handler, query, variables, "")
	require.NoError(t, err)
	require.Empty(t, resp.Errors, "graphql errors: %v", resp.Errors)

	raw, ok := resp.Data[op]
	require.True(t, ok, "missing operation %q in response %v", op, resp.Data)
	obj, ok := raw.(map[string]interface{})
	require.True(t, ok, "operation %q is %T, not an object", op, raw)
	return obj
}

// GraphQLRaw executes a query/mutation and returns the whole response without
// asserting on errors. Used by tests that expect a GraphQL error.
func GraphQLRaw(t require.TestingT, handler http.Handler, query string, variables map[string]interface{}) *GraphQLResponse {
	resp, err := ExecuteGraphQL(handler, query, variables, "")
	require.NoError(t, err)
	return resp
}

// GQLData returns the "data" object of an operation response.
func GQLData(op map[string]interface{}) map[string]interface{} {
	data, _ := op["data"].(map[string]interface{})
	return data
}

// GQLID extracts a numeric id from a GraphQL data object.
func GQLID(data map[string]interface{}, key string) int {
	v, ok := data[key]
	if !ok {
		return 0
	}
	f, _ := v.(float64)
	return int(f)
}

// GQLList extracts a list from a GraphQL data object.
func GQLList(data map[string]interface{}, key string) []interface{} {
	list, _ := data[key].([]interface{})
	return list
}

// GQLFieldList extracts a list-valued field directly from an operation
// response, e.g. the "data" list of an ApiResponses* payload.
func GQLFieldList(op map[string]interface{}, key string) []interface{} {
	list, _ := op[key].([]interface{})
	return list
}
