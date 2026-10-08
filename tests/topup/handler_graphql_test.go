package topup_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/topup"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/topup"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	gapi "github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/handler"
	topup_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/service"
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

type TopupGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	chConn      clickhouse.Conn
	conn        *grpc.ClientConn
	handler     http.Handler
	userRepo    user_repo.UserCommandRepository
	cardRepo    card_repo.CardCommandRepository
	saldoRepo   saldo_repo.Repositories
	topupRepo   topup_repo.Repositories
	cardNumber  string
	topupID     int
	userID      int
}

func (s *TopupGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "topup"))

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
		CREATE TABLE IF NOT EXISTS topup_events (
			topup_id UInt64, topup_no String, card_number String, card_type String,
			card_provider String, amount Int64, payment_method String, status String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (card_number, created_at)`)

	userRepos := user_repo.NewRepositories(&user_repo.Deps{Db: gormDB, RoleQueryClient: s.ts.RoleQueryClient, UserRoleClient: s.ts.UserRoleClient})
	cardRepos := card_repo.NewRepositories(gormDB, nil)
	saldoRepos := saldo_repo.NewRepositories(gormDB, nil, nil)

	s.topupRepo = topup_repo.NewRepositories(gormDB, nil, nil, nil, nil)
	s.userRepo = userRepos.UserCommand
	s.cardRepo = cardRepos.CardCommand
	s.saldoRepo = saldoRepos

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	topupService := service.NewService(&service.Deps{
		Kafka: nil, Cache: cacheStore, Repositories: s.topupRepo,
		CardAdapter: s.ts.CardAdapter, SaldoAdapter: s.ts.SaldoAdapter, Logger: log,
	})

	user, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Topup", LastName: "Owner", Email: "topup.graphql@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)

	card, err := s.cardRepo.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: s.userID, CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "123", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.cardNumber = card.CardNumber

	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{
		CardNumber: s.cardNumber, TotalBalance: 1000000,
	})
	s.Require().NoError(err)

	topupGapiHandler := gapi.NewHandler(topupService)
	chRepo := stats_repo.NewRepository(s.chConn)
	topupStatsHandler := stats_handler.NewTopupStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterTopupCommandServiceServer(server, topupGapiHandler)
	pb.RegisterTopupQueryServiceServer(server, topupGapiHandler)
	pbStats.RegisterTopupStatsAmountServiceServer(server, topupStatsHandler)
	pbStats.RegisterTopupStatsMethodServiceServer(server, topupStatsHandler)
	pbStats.RegisterTopupStatsStatusServiceServer(server, topupStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		TopupClient: conn,
	})
}

func (s *TopupGraphQLTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	s.redisClient.Close()
	if s.chConn != nil {
		s.chConn.Close()
	}
	s.ts.Teardown()
}

const createTopupQuery = `mutation CreateTopup($input: CreateTopupInput!) {
  createTopup(input: $input) { status message data { id card_number topup_no topup_amount topup_method } }
}`

const findByIdTopupQuery = `query FindByIdTopup($input: FindByIdTopupInput!) {
  findByIdTopup(input: $input) { status message data { id card_number topup_amount } }
}`

const restoreAllTopupQuery = `mutation RestoreAllTopup { restoreAllTopup { status message } }`

const deleteAllTopupPermanentQuery = `mutation DeleteAllTopupPermanent { deleteAllTopupPermanent { status message } }`

func (s *TopupGraphQLTestSuite) Test1_CreateTopup() {
	created := tests.GraphQLOp(s.T(), s.handler, "createTopup", createTopupQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":  s.cardNumber,
			"topup_no":     "TOPUP-GQL-1",
			"topup_amount": 100000,
			"topup_method": "visa",
		},
	})
	s.Equal("success", created["status"])
	s.topupID = tests.GQLID(tests.GQLData(created), "id")
	s.Require().NotZero(s.topupID)
}

func (s *TopupGraphQLTestSuite) Test2_FindById() {
	s.Require().NotZero(s.topupID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdTopup", findByIdTopupQuery,
		map[string]interface{}{"input": map[string]interface{}{"topup_id": s.topupID}})
	s.Equal("success", found["status"])
	s.Equal(s.topupID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *TopupGraphQLTestSuite) Test3_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllTopup", restoreAllTopupQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllTopupPermanent", deleteAllTopupPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestTopupGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TopupGraphQLTestSuite))
}
