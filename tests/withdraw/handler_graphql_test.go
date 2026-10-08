package withdraw_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/withdraw"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/withdraw"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/service"
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

type WithdrawGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	chConn      clickhouse.Conn
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	handler     http.Handler
	repos       repository.Repositories
	userRepo    user_repo.UserCommandRepository
	cardRepo    card_repo.CardCommandRepository
	saldoRepo   saldo_repo.Repositories

	customerCardNumber string
	customerUserID     int
	withdrawID         int
}

func (s *WithdrawGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	// Run required migrations
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "withdraw"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	chOpts, err := clickhouse.ParseDSN(s.ts.CHURL)
	s.Require().NoError(err)
	chConn, err := clickhouse.Open(chOpts)
	s.Require().NoError(err)
	s.chConn = chConn

	// Seed CH Schema
	err = s.chConn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS withdraw_events (
			withdraw_id UInt64,
			withdraw_no String,
			card_number String,
			card_type String,
			card_provider String,
			amount Int64,
			status String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (card_number, created_at);
	`)
	s.Require().NoError(err)

	// Repositories for seeding and service dependencies
	userRepos := user_repo.NewUserCommandRepository(gormDB)
	cardRepos := card_repo.NewRepositories(gormDB, nil)
	saldoRepos := saldo_repo.NewRepositories(gormDB, nil, nil)

	s.userRepo = userRepos
	s.cardRepo = cardRepos.CardCommand
	s.saldoRepo = saldoRepos

	s.repos = repository.NewRepositories(gormDB, nil, nil, nil, nil)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	withdrawService := service.NewService(&service.Deps{
		Kafka:             nil,
		Repositories:      s.repos,
		CardAdapter:       s.ts.CardAdapter,
		SaldoAdapter:      s.ts.SaldoAdapter,
		Logger:            log,
		Cache:             cacheStore,
		AISecurityAdapter: nil,
	})

	// Seed Customer
	customer, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Withdraw",
		LastName:  "GraphQL",
		Email:     "withdraw.graphql@test.com",
		Password:  "password123",
	})
	s.Require().NoError(err)
	s.customerUserID = int(customer.UserID)

	card, err := s.cardRepo.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID:       s.customerUserID,
		CardType:     "debit",
		ExpireDate:   time.Now().AddDate(1, 0, 0),
		CVV:          "999",
		CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.customerCardNumber = card.CardNumber

	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{
		CardNumber:   s.customerCardNumber,
		TotalBalance: 1000000,
	})
	s.Require().NoError(err)

	withdrawHandler := handler.NewHandler(withdrawService)

	// Stats Handler
	chRepo := stats_repo.NewRepository(s.chConn)
	withdrawStatsHandler := stats_handler.NewWithdrawStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterWithdrawCommandServiceServer(server, withdrawHandler)
	pb.RegisterWithdrawQueryServiceServer(server, withdrawHandler)
	pbStats.RegisterWithdrawStatsAmountServiceServer(server, withdrawStatsHandler)
	pbStats.RegisterWithdrawStatsStatusServiceServer(server, withdrawStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)

	go func() {
		_ = server.Serve(lis)
	}()

	// Create gRPC Client
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	// Build the GraphQL handler that backs the gateway. The withdraw resolvers
	// only need the withdraw service connection.
	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		WithdrawClient: conn,
	})
}

func (s *WithdrawGraphQLTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.redisClient != nil {
		s.redisClient.Close()
	}
	if s.chConn != nil {
		s.chConn.Close()
	}
	if s.ts != nil {
		s.ts.Teardown()
	}
}

const createWithdrawQuery = `mutation CreateWithdraw($input: CreateWithdrawInput!) {
  createWithdraw(input: $input) { status message data { withdraw_id withdraw_no card_number withdraw_amount } }
}`

const findWithdrawByIdQuery = `query FindByIdWithdraw($input: FindByIdWithdrawInput!) {
  findByIdWithdraw(input: $input) { status message data { withdraw_id card_number withdraw_amount } }
}`

const findAllWithdrawsQuery = `query FindAllWithdraw($input: FindAllWithdrawInput!) {
  findAllWithdraw(input: $input) { status message data { withdraw_id } }
}`

const updateWithdrawQuery = `mutation UpdateWithdraw($input: UpdateWithdrawInput!) {
  updateWithdraw(input: $input) { status message data { withdraw_id withdraw_amount } }
}`

const trashedWithdrawQuery = `mutation TrashedWithdraw($input: FindByIdWithdrawInput!) {
  trashedWithdraw(input: $input) { status message data { withdraw_id deleted_at } }
}`

const restoreWithdrawQuery = `mutation RestoreWithdraw($input: FindByIdWithdrawInput!) {
  restoreWithdraw(input: $input) { status message data { withdraw_id } }
}`

const deleteWithdrawPermanentQuery = `mutation DeleteWithdrawPermanent($input: FindByIdWithdrawInput!) {
  deleteWithdrawPermanent(input: $input) { status message }
}`

const restoreAllWithdrawsQuery = `mutation RestoreAllWithdraw { restoreAllWithdraw { status message } }`

const deleteAllWithdrawsPermanentQuery = `mutation DeleteAllWithdrawPermanent { deleteAllWithdrawPermanent { status message } }`

const monthlyWithdrawAmountStatsQuery = `query FindMonthlyWithdrawAmountStats($input: FindYearStatsInput!) {
  findMonthlyWithdrawAmountStats(input: $input) { status message data { month total_amount } }
}`

const yearlyWithdrawAmountStatsQuery = `query FindYearlyWithdrawAmountStats($input: FindYearStatsInput!) {
  findYearlyWithdrawAmountStats(input: $input) { status message data { year total_amount } }
}`

const monthlyWithdrawAmountByCardStatsQuery = `query FindMonthlyWithdrawAmountByCardNumberStats($input: FindYearCardNumberStatsInput!) {
  findMonthlyWithdrawAmountByCardNumberStats(input: $input) { status message data { month total_amount } }
}`

const yearlyWithdrawAmountByCardStatsQuery = `query FindYearlyWithdrawAmountByCardNumberStats($input: FindYearCardNumberStatsInput!) {
  findYearlyWithdrawAmountByCardNumberStats(input: $input) { status message data { year total_amount } }
}`

const monthlyWithdrawStatusSuccessStatsQuery = `query FindMonthlyWithdrawStatusSuccessStats($input: FindMonthlyYearStatsInput!) {
  findMonthlyWithdrawStatusSuccessStats(input: $input) { status message data { year month total_success total_amount } }
}`

const yearlyWithdrawStatusFailedStatsQuery = `query FindYearlyWithdrawStatusFailedStats($input: FindYearStatsInput!) {
  findYearlyWithdrawStatusFailedStats(input: $input) { status message data { year total_success total_amount } }
}`

const monthlyWithdrawStatusSuccessByCardStatsQuery = `query FindMonthlyWithdrawStatusSuccessStats($input: FindMonthlyYearStatsInput!) {
  findMonthlyWithdrawStatusSuccessStats(input: $input) { status message data { year month total_success total_amount } }
}`

const yearlyWithdrawStatusFailedByCardStatsQuery = `query FindYearlyWithdrawStatusFailedStats($input: FindYearStatsInput!) {
  findYearlyWithdrawStatusFailedStats(input: $input) { status message data { year total_success total_amount } }
}`

func (s *WithdrawGraphQLTestSuite) Test1_CreateWithdraw() {
	created := tests.GraphQLOp(s.T(), s.handler, "createWithdraw", createWithdrawQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":     s.customerCardNumber,
			"withdraw_amount": 100000,
			"withdraw_time":   time.Now().Format("2006-01-02"),
		},
	})
	s.Equal("success", created["status"])
	s.withdrawID = tests.GQLID(tests.GQLData(created), "withdraw_id")
	s.Require().NotZero(s.withdrawID)

	// Verify balance
	customerSaldo, err := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Require().NoError(err)
	s.Equal(int64(900000), customerSaldo.TotalBalance)
}

func (s *WithdrawGraphQLTestSuite) Test2_FindWithdrawById() {
	s.Require().NotZero(s.withdrawID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdWithdraw", findWithdrawByIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"withdraw_id": s.withdrawID}})
	s.Equal("success", found["status"])
	s.Equal(s.withdrawID, tests.GQLID(tests.GQLData(found), "withdraw_id"))
}

func (s *WithdrawGraphQLTestSuite) Test3_FindAllWithdraws() {
	found := tests.GraphQLOp(s.T(), s.handler, "findAllWithdraw", findAllWithdrawsQuery,
		map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}})
	s.Equal("success", found["status"])
}

func (s *WithdrawGraphQLTestSuite) Test4_UpdateWithdraw() {
	s.Require().NotZero(s.withdrawID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateWithdraw", updateWithdrawQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"withdraw_id":     s.withdrawID,
			"card_number":     s.customerCardNumber,
			"withdraw_amount": 150000, // Increase by 50000
			"withdraw_time":   time.Now().Format("2006-01-02"),
		},
	})
	s.Equal("success", updated["status"])

	// Verify adjusted balance (900k - 50k = 850k)
	customerSaldo, err := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Require().NoError(err)
	s.Equal(int64(850000), customerSaldo.TotalBalance)
}

func (s *WithdrawGraphQLTestSuite) Test5_TrashedWithdraw() {
	s.Require().NotZero(s.withdrawID)

	trashed := tests.GraphQLOp(s.T(), s.handler, "trashedWithdraw", trashedWithdrawQuery,
		map[string]interface{}{"input": map[string]interface{}{"withdraw_id": s.withdrawID}})
	s.Equal("success", trashed["status"])
}

func (s *WithdrawGraphQLTestSuite) Test6_RestoreWithdraw() {
	s.Require().NotZero(s.withdrawID)

	restored := tests.GraphQLOp(s.T(), s.handler, "restoreWithdraw", restoreWithdrawQuery,
		map[string]interface{}{"input": map[string]interface{}{"withdraw_id": s.withdrawID}})
	s.Equal("success", restored["status"])
}

func (s *WithdrawGraphQLTestSuite) Test7_PermanentDeleteWithdraw() {
	s.Require().NotZero(s.withdrawID)

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteWithdrawPermanent", deleteWithdrawPermanentQuery,
		map[string]interface{}{"input": map[string]interface{}{"withdraw_id": s.withdrawID}})
	s.Equal("success", deleted["status"])
}

func (s *WithdrawGraphQLTestSuite) Test8_WithdrawStats_Amount() {
	ctx := context.Background()
	now := time.Now()

	err := s.chConn.Exec(ctx, "TRUNCATE TABLE withdraw_events")
	s.Require().NoError(err)

	seedSQL := `INSERT INTO withdraw_events (withdraw_id, withdraw_no, card_number, card_type, card_provider, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	// Success data
	err = s.chConn.Exec(ctx, seedSQL, 1, "WD001", s.customerCardNumber, "debit", "visa", 1000, "success", now)
	s.Require().NoError(err)
	// Failed data
	err = s.chConn.Exec(ctx, seedSQL, 2, "WD002", s.customerCardNumber, "debit", "visa", 2000, "failed", now)
	s.Require().NoError(err)

	s.Run("MonthlyAmount", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findMonthlyWithdrawAmountStats", monthlyWithdrawAmountStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
		s.Equal("success", resp["status"])
	})

	s.Run("YearlyAmount", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findYearlyWithdrawAmountStats", yearlyWithdrawAmountStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
		s.Equal("success", resp["status"])
	})

	s.Run("MonthlyAmountByCard", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findMonthlyWithdrawAmountByCardNumberStats", monthlyWithdrawAmountByCardStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year(), "card_number": s.customerCardNumber}})
		s.Equal("success", resp["status"])
	})

	s.Run("YearlyAmountByCard", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findYearlyWithdrawAmountByCardNumberStats", yearlyWithdrawAmountByCardStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year(), "card_number": s.customerCardNumber}})
		s.Equal("success", resp["status"])
	})
}

func (s *WithdrawGraphQLTestSuite) Test9_WithdrawStats_Status() {
	now := time.Now()

	s.Run("MonthlySuccess", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findMonthlyWithdrawStatusSuccessStats", monthlyWithdrawStatusSuccessStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year(), "month": int(now.Month())}})
		s.Equal("success", resp["status"])
	})

	s.Run("YearlyFailed", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findYearlyWithdrawStatusFailedStats", yearlyWithdrawStatusFailedStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
		s.Equal("success", resp["status"])
	})

	s.Run("MonthlySuccessByCard", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findMonthlyWithdrawStatusSuccessStats", monthlyWithdrawStatusSuccessByCardStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year(), "month": int(now.Month())}})
		s.Equal("success", resp["status"])
	})

	s.Run("YearlyFailedByCard", func() {
		resp := tests.GraphQLOp(s.T(), s.handler, "findYearlyWithdrawStatusFailedStats", yearlyWithdrawStatusFailedByCardStatsQuery,
			map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
		s.Equal("success", resp["status"])
	})
}

func (s *WithdrawGraphQLTestSuite) Test10_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllWithdraw", restoreAllWithdrawsQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllWithdrawPermanent", deleteAllWithdrawsPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestWithdrawGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WithdrawGraphQLTestSuite))
}
