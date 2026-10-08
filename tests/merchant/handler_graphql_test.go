package merchant_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/merchant"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/service"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"
	"gorm.io/gorm"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MerchantGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	chConn      clickhouse.Conn
	conn        *grpc.ClientConn
	handler     http.Handler
	userRepo    user_repo.UserCommandRepository
	userID      int
	merchantID  int
	apiKey      string
}

func (s *MerchantGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "merchant", "transaction"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	chOpts, err := clickhouse.ParseDSN(s.ts.CHURL)
	s.Require().NoError(err)
	chConn, err := clickhouse.Open(chOpts)
	s.Require().NoError(err)
	s.chConn = chConn

	_ = s.chConn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS transaction_events (
			transaction_id UInt64, transaction_no String, merchant_id UInt64, merchant_name String,
			apikey String, apikey_name String, amount Int64, payment_method String, status String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (merchant_id, created_at)`)

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
		FirstName: "Handler", LastName: "Merchant", Email: "handler.merchant@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)

	merchantHandler := handler.NewHandler(merchantService)
	chRepo := stats_repo.NewRepository(s.chConn)
	merchantStatsHandler := stats_handler.NewMerchantStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterMerchantCommandServiceServer(server, merchantHandler)
	pb.RegisterMerchantQueryServiceServer(server, merchantHandler)
	pbStats.RegisterMerchantStatsAmountServiceServer(server, merchantStatsHandler)
	pbStats.RegisterMerchantStatsMethodServiceServer(server, merchantStatsHandler)
	pbStats.RegisterMerchantStatsTotalAmountServiceServer(server, merchantStatsHandler)
	pb.RegisterMerchantTransactionServiceServer(server, merchantStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		MerchantClient: conn,
	})
}

func (s *MerchantGraphQLTestSuite) TearDownSuite() {
	s.conn.Close()
	s.grpcServer.Stop()
	s.redisClient.Close()
	if s.chConn != nil {
		s.chConn.Close()
	}
	s.ts.Teardown()
}

const createMerchantQuery = `mutation CreateMerchant($input: CreateMerchantInput!) {
  createMerchant(input: $input) { status message data { id name api_key status user_id } }
}`

const findMerchantByIdQuery = `query FindMerchantById($input: FindByIdMerchantInput!) {
  findByIdMerchant(input: $input) { status message data { id name api_key status user_id } }
}`

const findByApiKeyQuery = `query FindByApiKey($input: FindByApiKeyInput!) {
  findByApiKey(input: $input) { status message data { id name api_key status } }
}`

const findByMerchantUserIdQuery = `query FindByMerchantUserId($input: FindByMerchantUserIdInput!) {
  findByMerchantUserId(input: $input) { status message data { id name user_id } }
}`

const findAllMerchantQuery = `query FindAllMerchant($input: FindAllMerchantInput!) {
  findAllMerchant(input: $input) {
    status message
    data { id name api_key status user_id }
    pagination { current_page page_size total_pages total_records }
  }
}`

const updateMerchantQuery = `mutation UpdateMerchant($input: UpdateMerchantInput!) {
  updateMerchant(input: $input) { status message data { id name status user_id } }
}`

const findByActiveQuery = `query FindByActive($input: FindAllMerchantInput!) {
  findByActive(input: $input) {
    status message
    data { id name deleted_at }
    pagination { current_page page_size total_pages total_records }
  }
}`

const findByTrashedQuery = `query FindByTrashed($input: FindAllMerchantInput!) {
  findByTrashed(input: $input) {
    status message
    data { id name deleted_at }
    pagination { current_page page_size total_pages total_records }
  }
}`

const trashedMerchantQuery = `mutation TrashedMerchant($input: FindByIdMerchantInput!) {
  trashedMerchant(input: $input) { status message data { id name deleted_at } }
}`

const restoreMerchantQuery = `mutation RestoreMerchant($input: FindByIdMerchantInput!) {
  restoreMerchant(input: $input) { status message data { id name deleted_at } }
}`

const deleteMerchantPermanentQuery = `mutation DeleteMerchantPermanent($input: FindByIdMerchantInput!) {
  deleteMerchantPermanent(input: $input) { status message }
}`

const restoreAllMerchantQuery = `mutation RestoreAllMerchant { restoreAllMerchant { status message } }`

const deleteAllMerchantPermanentQuery = `mutation DeleteAllMerchantPermanent { deleteAllMerchantPermanent { status message } }`

func (s *MerchantGraphQLTestSuite) Test1_CreateMerchant() {
	created := tests.GraphQLOp(s.T(), s.handler, "createMerchant", createMerchantQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"name":    fmt.Sprintf("Handler Merchant %d", time.Now().UnixNano()),
			"user_id": s.userID,
		},
	})
	s.Equal("success", created["status"])

	data := tests.GQLData(created)
	s.merchantID = tests.GQLID(data, "id")
	s.Require().NotZero(s.merchantID)
	s.apiKey, _ = data["api_key"].(string)
}

func (s *MerchantGraphQLTestSuite) Test2_FindMerchantById() {
	s.Require().NotZero(s.merchantID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdMerchant", findMerchantByIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"merchant_id": s.merchantID}})
	s.Equal("success", found["status"])
	s.Equal(s.merchantID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *MerchantGraphQLTestSuite) Test3_FindByApiKey() {
	s.Require().NotEmpty(s.apiKey)

	found := tests.GraphQLOp(s.T(), s.handler, "findByApiKey", findByApiKeyQuery,
		map[string]interface{}{"input": map[string]interface{}{"api_key": s.apiKey}})
	s.Equal("success", found["status"])
	s.Equal(s.merchantID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *MerchantGraphQLTestSuite) Test4_FindByMerchantUserId() {
	s.Require().NotZero(s.userID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByMerchantUserId", findByMerchantUserIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"user_id": s.userID}})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *MerchantGraphQLTestSuite) Test5_FindAllMerchant() {
	found := tests.GraphQLOp(s.T(), s.handler, "findAllMerchant", findAllMerchantQuery, map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10, "search": ""},
	})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *MerchantGraphQLTestSuite) Test6_UpdateMerchant() {
	s.Require().NotZero(s.merchantID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateMerchant", updateMerchantQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"merchant_id": s.merchantID,
			"name":        "Updated Handler Merchant",
			"user_id":     s.userID,
			"status":      "active",
		},
	})
	s.Equal("success", updated["status"])
	s.Equal("Updated Handler Merchant", tests.GQLData(updated)["name"])
}

func (s *MerchantGraphQLTestSuite) Test8_TrashedAndRestoreMerchant() {
	s.Require().NotZero(s.merchantID)

	active := tests.GraphQLOp(s.T(), s.handler, "findByActive", findByActiveQuery, map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10, "search": ""},
	})
	s.Equal("success", active["status"])

	trashed := tests.GraphQLOp(s.T(), s.handler, "trashedMerchant", trashedMerchantQuery,
		map[string]interface{}{"input": map[string]interface{}{"merchant_id": s.merchantID}})
	s.Equal("success", trashed["status"])
	s.Equal(s.merchantID, tests.GQLID(tests.GQLData(trashed), "id"))

	trashedList := tests.GraphQLOp(s.T(), s.handler, "findByTrashed", findByTrashedQuery, map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10, "search": ""},
	})
	s.Equal("success", trashedList["status"])

	restored := tests.GraphQLOp(s.T(), s.handler, "restoreMerchant", restoreMerchantQuery,
		map[string]interface{}{"input": map[string]interface{}{"merchant_id": s.merchantID}})
	s.Equal("success", restored["status"])
	s.Equal(s.merchantID, tests.GQLID(tests.GQLData(restored), "id"))
}

func (s *MerchantGraphQLTestSuite) Test9_DeleteMerchantPermanentAndBulk() {
	s.Require().NotZero(s.merchantID)

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteMerchantPermanent", deleteMerchantPermanentQuery,
		map[string]interface{}{"input": map[string]interface{}{"merchant_id": s.merchantID}})
	s.Equal("success", deleted["status"])

	restoredAll := tests.GraphQLOp(s.T(), s.handler, "restoreAllMerchant", restoreAllMerchantQuery, nil)
	s.Equal("success", restoredAll["status"])

	deletedAll := tests.GraphQLOp(s.T(), s.handler, "deleteAllMerchantPermanent", deleteAllMerchantPermanentQuery, nil)
	s.Equal("success", deletedAll["status"])
}

func TestMerchantGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(MerchantGraphQLTestSuite))
}
