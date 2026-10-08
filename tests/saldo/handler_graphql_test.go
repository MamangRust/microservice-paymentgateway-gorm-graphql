package saldo_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/saldo"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/saldo"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/handler"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/service"
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

type SaldoGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	chConn      clickhouse.Conn
	conn        *grpc.ClientConn
	handler     http.Handler
	cardNumber  string
	userID      int
	saldoID     int
}

func (s *SaldoGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo"))

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
		CREATE TABLE IF NOT EXISTS saldo_events (
			card_number String, total_balance Int64, created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (card_number, created_at)`)

	userRepos := user_repo.NewRepositories(&user_repo.Deps{Db: gormDB, RoleQueryClient: s.ts.RoleQueryClient, UserRoleClient: s.ts.UserRoleClient})
	cardRepos := card_repo.NewRepositories(gormDB, nil)
	saldoRepos := saldo_repo.NewRepositories(gormDB, nil, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	saldoService := service.NewService(&service.Deps{
		Repositories: saldoRepos, CardAdapter: s.ts.CardAdapter, Logger: log, Cache: cacheStore,
	})

	user, err := userRepos.UserCommand.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Saldo", LastName: "Graphql", Email: "saldo.graphql@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)
	card, err := cardRepos.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: s.userID, CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "321", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.cardNumber = card.CardNumber

	saldoHandler := handler.NewHandler(saldoService)
	chRepo := stats_repo.NewRepository(s.chConn)
	saldoStatsHandler := stats_handler.NewSaldoStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterSaldoCommandServiceServer(server, saldoHandler)
	pb.RegisterSaldoQueryServiceServer(server, saldoHandler)
	pbStats.RegisterSaldoStatsBalanceServiceServer(server, saldoStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		SaldoClient: conn,
	})
}

func (s *SaldoGraphQLTestSuite) TearDownSuite() {
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

const createSaldoQuery = `mutation CreateSaldo($input: CreateSaldoInput!) {
  createSaldo(input: $input) { status message data { saldo_id card_number total_balance } }
}`

const findByIdSaldoQuery = `query FindByIdSaldo($input: FindByIdSaldoInput!) {
  findByIdSaldo(input: $input) { status message data { saldo_id card_number total_balance } }
}`

const restoreAllSaldoQuery = `mutation RestoreAllSaldo { restoreAllSaldo { status message } }`

const deleteAllSaldoPermanentQuery = `mutation DeleteAllSaldoPermanent { deleteAllSaldoPermanent { status message } }`

func (s *SaldoGraphQLTestSuite) Test1_CreateSaldo() {
	created := tests.GraphQLOp(s.T(), s.handler, "createSaldo", createSaldoQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":   s.cardNumber,
			"total_balance": 500000,
		},
	})
	s.Equal("success", created["status"])
	s.saldoID = tests.GQLID(tests.GQLData(created), "saldo_id")
	s.Require().NotZero(s.saldoID)
}

func (s *SaldoGraphQLTestSuite) Test2_FindByIdSaldo() {
	s.Require().NotZero(s.saldoID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdSaldo", findByIdSaldoQuery,
		map[string]interface{}{"input": map[string]interface{}{"saldo_id": s.saldoID}})
	s.Equal("success", found["status"])
	s.Equal(s.saldoID, tests.GQLID(tests.GQLData(found), "saldo_id"))
}

func (s *SaldoGraphQLTestSuite) Test3_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllSaldo", restoreAllSaldoQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllSaldoPermanent", deleteAllSaldoPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestSaldoGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(SaldoGraphQLTestSuite))
}
