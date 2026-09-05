package user_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/user"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	gapi "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// UserGraphqlHandlerTestSuite exercises the apigateway GraphQL layer end-to-end:
// it spins up a real user gRPC backend and drives it through the generated
// gqlgen executable schema via HTTP POST /query.
type UserGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	userID      int
	userEmail   string
}

func (s *UserGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user"))

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	hasher := hash.NewHashingPassword()
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	userService := service.NewService(&service.Deps{
		Cache:        cacheStore,
		Repositories: repository.NewRepositories(gormDB),
		Hash:         hasher,
		Logger:       log,
	})

	// Start backend gRPC server
	userHandler := gapi.NewHandler(userService)
	server := grpc.NewServer()
	pb.RegisterUserQueryServiceServer(server, userHandler)
	pb.RegisterUserCommandServiceServer(server, userHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)

	go func() {
		_ = server.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        conn,
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

func (s *UserGraphqlHandlerTestSuite) TearDownSuite() {
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

// graphql sends a GraphQL request against the apigateway schema and returns
// the raw response body ("data" / "errors" keys).
func (s *UserGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *UserGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *UserGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *UserGraphqlHandlerTestSuite) Test1_CreateUser() {
	s.userEmail = fmt.Sprintf("handler.user.%d@example.com", time.Now().UnixNano())

	field := s.dataField(s.graphql(`
		mutation($input: CreateUserInput!) {
			createUser(input: $input) {
				status
				message
				data { id firstname email }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Handler",
			"lastname":         "User",
			"email":            s.userEmail,
			"password":         "password123",
			"confirm_password": "password123",
		},
	}), "createUser")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.userEmail, data["email"])
	s.userID = int(data["id"].(float64))
}

func (s *UserGraphqlHandlerTestSuite) Test2_FindUserById() {
	s.Require().NotZero(s.userID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdUserInput!) {
			findByIdUser(input: $input) {
				status
				message
				data { id email }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"id": s.userID},
	}), "findByIdUser")

	s.assertSuccess(field)
	s.Equal(s.userEmail, field["data"].(map[string]interface{})["email"])
}

func (s *UserGraphqlHandlerTestSuite) Test3_UpdateUser() {
	s.Require().NotZero(s.userID)

	field := s.dataField(s.graphql(`
		mutation($input: UpdateUserInput!) {
			updateUser(input: $input) {
				status
				message
				data { id firstname }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"id":               s.userID,
			"firstname":        "Updated",
			"lastname":         "User",
			"email":            s.userEmail,
			"password":         "password123",
			"confirm_password": "password123",
		},
	}), "updateUser")

	s.assertSuccess(field)
	s.Equal("Updated", field["data"].(map[string]interface{})["firstname"])
}

func (s *UserGraphqlHandlerTestSuite) Test4_TrashAndRestoreUser() {
	s.Require().NotZero(s.userID)

	trashed := s.dataField(s.graphql(`
		mutation($input: FindByIdUserInput!) {
			trashedUser(input: $input) {
				status
				message
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"id": s.userID},
	}), "trashedUser")
	s.assertSuccess(trashed)

	restore := s.dataField(s.graphql(`
		mutation($input: FindByIdUserInput!) {
			restoreUser(input: $input) {
				status
				message
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"id": s.userID},
	}), "restoreUser")
	s.assertSuccess(restore)
}

func (s *UserGraphqlHandlerTestSuite) Test5_PermanentDeleteUser() {
	s.Require().NotZero(s.userID)

	s.dataField(s.graphql(`
		mutation($input: FindByIdUserInput!) {
			trashedUser(input: $input) {
				status
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"id": s.userID},
	}), "trashedUser")

	deleted := s.dataField(s.graphql(`
		mutation($input: FindByIdUserInput!) {
			deleteUserPermanent(input: $input) {
				status
				message
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"id": s.userID},
	}), "deleteUserPermanent")
	s.assertSuccess(deleted)
}

func (s *UserGraphqlHandlerTestSuite) Test6_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation {
			restoreAllUser { status message }
		}`, nil), "restoreAllUser")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation {
			deleteAllUserPermanent { status message }
		}`, nil), "deleteAllUserPermanent")
	s.assertSuccess(deleteAll)
}

func TestUserGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(UserGraphqlHandlerTestSuite))
}
