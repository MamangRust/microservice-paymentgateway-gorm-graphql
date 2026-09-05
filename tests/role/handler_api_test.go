package role_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"bytes"
	"encoding/json"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/role"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RoleGraphqlApiTestSuite exercises the apigateway GraphQL layer against a real
// role gRPC backend via the generated gqlgen schema.
type RoleGraphqlApiTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	roleID      int
}

func (s *RoleGraphqlApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user", "role"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	roleService := service.NewService(&service.Deps{
		Repositories: repository.NewRepositories(gormDB),
		Logger:       log,
		Cache:        cacheStore,
	})

	// Start backend gRPC server
	roleHandlerGrpc := handler.NewHandler(roleService)
	server := grpc.NewServer()
	pb.RegisterRoleCommandServiceServer(server, roleHandlerGrpc.RoleCommand)
	pb.RegisterRoleQueryServiceServer(server, roleHandlerGrpc.RoleQuery)
	s.grpcServer = server

	lis, err := net.Listen("tcp", "localhost:0")
	s.Require().NoError(err)

	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        conn,
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        testhelper.CreateDummyConn(),
		MerchantClient:    testhelper.CreateDummyConn(),
		SaldoClient:       testhelper.CreateDummyConn(),
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *RoleGraphqlApiTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.redisClient != nil {
		s.redisClient.Close()
	}
	if s.ts != nil {
		s.ts.Teardown()
	}
}

func (s *RoleGraphqlApiTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
	payload, _ := json.Marshal(map[string]interface{}{
		"query":     query,
		"variables": variables,
	})

	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.graph.ServeHTTP(rec, req)

	var res map[string]interface{}
	s.Require().NoError(json.Unmarshal(rec.Body.Bytes(), &res), rec.Body.String())
	return res
}

func (s *RoleGraphqlApiTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *RoleGraphqlApiTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *RoleGraphqlApiTestSuite) Test1_CreateRole() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateRoleInput!) {
			createRole(input: $input) {
				status
				message
				data { id name }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"name": "GraphQL Role"},
	}), "createRole")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal("GraphQL Role", data["name"])
	s.roleID = int(data["id"].(float64))
}

func (s *RoleGraphqlApiTestSuite) Test2_FindById() {
	s.Require().NotZero(s.roleID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdRoleInput!) {
			findByIdRole(input: $input) {
				status
				message
				data { id name }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	}), "findByIdRole")

	s.assertSuccess(field)
	s.Equal(float64(s.roleID), field["data"].(map[string]interface{})["id"])
}

func (s *RoleGraphqlApiTestSuite) Test3_UpdateRole() {
	s.Require().NotZero(s.roleID)

	field := s.dataField(s.graphql(`
		mutation($input: UpdateRoleInput!) {
			updateRole(input: $input) {
				status
				message
				data { id name }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"id":   s.roleID,
			"name": "Updated GraphQL Role",
		},
	}), "updateRole")

	s.assertSuccess(field)
	s.Equal("Updated GraphQL Role", field["data"].(map[string]interface{})["name"])
}

func (s *RoleGraphqlApiTestSuite) Test4_TrashRestoreAndDeletePermanent() {
	s.Require().NotZero(s.roleID)

	trashed := s.dataField(s.graphql(`
		mutation($input: FindByIdRoleInput!) {
			trashedRole(input: $input) { status }
		}`, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	}), "trashedRole")
	s.assertSuccess(trashed)

	restore := s.dataField(s.graphql(`
		mutation($input: FindByIdRoleInput!) {
			restoreRole(input: $input) { status }
		}`, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	}), "restoreRole")
	s.assertSuccess(restore)

	s.dataField(s.graphql(`
		mutation($input: FindByIdRoleInput!) {
			trashedRole(input: $input) { status }
		}`, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	}), "trashedRole")

	deleted := s.dataField(s.graphql(`
		mutation($input: FindByIdRoleInput!) {
			deleteRolePermanent(input: $input) { status message }
		}`, map[string]interface{}{
		"input": map[string]interface{}{"role_id": s.roleID},
	}), "deleteRolePermanent")
	s.assertSuccess(deleted)
}

func (s *RoleGraphqlApiTestSuite) Test5_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllRole { status message } }`, nil), "restoreAllRole")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllRolePermanent { status message } }`, nil), "deleteAllRolePermanent")
	s.assertSuccess(deleteAll)
}

func TestRoleGraphqlApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(RoleGraphqlApiTestSuite))
}
