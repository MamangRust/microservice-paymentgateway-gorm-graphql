package merchant_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	merchanthandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/service"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MerchantGraphqlHandlerTestSuite exercises the apigateway GraphQL layer
// against a real merchant gRPC backend via the generated gqlgen schema.
type MerchantGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	userRepo    user_repo.UserCommandRepository
	userID      int
	merchantID  int
}

func (s *MerchantGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "merchant", "transaction"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	repos := repository.NewRepositories(gormDB, nil)
	s.userRepo = user_repo.NewUserCommandRepository(gormDB)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	merchantService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: repos, UserAdapter: s.ts.UserAdapter, Logger: log, Cache: cacheStore,
	})

	user, err := s.userRepo.CreateUser(s.ts.Ctx, &requests.CreateUserRequest{
		FirstName: "GraphQL", LastName: "Merchant", Email: "graphql.merchant@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)

	merchantHandler := merchanthandler.NewHandler(merchantService)

	server := grpc.NewServer()
	pb.RegisterMerchantCommandServiceServer(server, merchantHandler)
	pb.RegisterMerchantQueryServiceServer(server, merchantHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        testhelper.CreateDummyConn(),
		MerchantClient:    conn,
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

func (s *MerchantGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *MerchantGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *MerchantGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *MerchantGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *MerchantGraphqlHandlerTestSuite) Test1_CreateMerchant() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateMerchantInput!) {
			createMerchant(input: $input) {
				status
				message
				data { id name user_id api_key }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"name":   "GraphQL Merchant",
			"user_id": s.userID,
		},
	}), "createMerchant")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal("GraphQL Merchant", data["name"])
	s.merchantID = int(data["id"].(float64))
}

func (s *MerchantGraphqlHandlerTestSuite) Test2_FindMerchantById() {
	s.Require().NotZero(s.merchantID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdMerchantInput!) {
			findByIdMerchant(input: $input) {
				status
				message
				data { id name }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"merchant_id": s.merchantID},
	}), "findByIdMerchant")

	s.assertSuccess(field)
	s.Equal("GraphQL Merchant", field["data"].(map[string]interface{})["name"])
}

func (s *MerchantGraphqlHandlerTestSuite) Test3_FindByApiKey() {
	s.Require().NotZero(s.merchantID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdMerchantInput!) {
			findByIdMerchant(input: $input) {
				status
				data { id api_key }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"merchant_id": s.merchantID},
	}), "findByIdMerchant")

	apiKey, ok := field["data"].(map[string]interface{})["api_key"].(string)
	s.Require().True(ok && apiKey != "", "expected non-empty api_key")

	byKey := s.dataField(s.graphql(`
		query($input: FindByApiKeyInput!) {
			findByApiKey(input: $input) {
				status
				message
				data { id name }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"api_key": apiKey},
	}), "findByApiKey")
	s.assertSuccess(byKey)
	s.Equal(float64(s.merchantID), byKey["data"].(map[string]interface{})["id"])
}

func (s *MerchantGraphqlHandlerTestSuite) Test4_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllMerchant { status message } }`, nil), "restoreAllMerchant")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllMerchantPermanent { status message } }`, nil), "deleteAllMerchantPermanent")
	s.assertSuccess(deleteAll)
}

func TestMerchantGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(MerchantGraphqlHandlerTestSuite))
}
