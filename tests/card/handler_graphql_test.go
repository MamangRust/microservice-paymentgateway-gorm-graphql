package card_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/card"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/service"
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

type CardGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	handler     http.Handler
	chConn      clickhouse.Conn
	userID      int
	cardID      int
}

func (s *CardGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card"))

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

	// Seed CH Schema
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS topup_events (topup_id UInt64, topup_no String, card_number String, card_type String, card_provider String, amount Int64, payment_method String, status String, created_at DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY (card_number, created_at)`,
		`CREATE TABLE IF NOT EXISTS transaction_events (transaction_id UInt64, transaction_no String, card_number String, amount Int64, status String, created_at DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY (card_number, created_at)`,
		`CREATE TABLE IF NOT EXISTS transfer_events (transfer_id UInt64, transfer_no String, source_card String, destination_card String, amount Int64, status String, created_at DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY (source_card, created_at)`,
		`CREATE TABLE IF NOT EXISTS saldo_events (card_number String, total_balance Int64, created_at DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY (card_number, created_at)`,
		`CREATE TABLE IF NOT EXISTS withdraw_events (withdraw_id UInt64, withdraw_no String, card_number String, amount Int64, status String, created_at DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY (card_number, created_at)`,
	} {
		err = s.chConn.Exec(context.Background(), ddl)
		s.Require().NoError(err)
	}

	repos := repository.NewRepositories(gormDB, nil)
	userRepo := user_repo.NewRepositories(&user_repo.Deps{Db: gormDB, RoleQueryClient: s.ts.RoleQueryClient, UserRoleClient: s.ts.UserRoleClient})

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	cardService := service.NewService(&service.Deps{
		Repositories: repos,
		UserAdapter:  s.ts.UserAdapter,
		Logger:       log,
		Cache:        cacheStore,
		Kafka:        nil,
	})

	cardGapiHandler := handler.NewHandler(cardService)

	// Stats Handler
	chRepo := stats_repo.NewRepository(s.chConn)
	cardStatsHandler := stats_handler.NewCardStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterCardQueryServiceServer(server, cardGapiHandler)
	pb.RegisterCardCommandServiceServer(server, cardGapiHandler)
	pb.RegisterCardDashboardServiceServer(server, cardStatsHandler)
	pbStats.RegisterCardStatsBalanceServiceServer(server, cardStatsHandler)
	pbStats.RegisterCardStatsTopupServiceServer(server, cardStatsHandler)
	pbStats.RegisterCardStatsTransactionServiceServer(server, cardStatsHandler)
	pbStats.RegisterCardStatsTransferServiceServer(server, cardStatsHandler)
	pbStats.RegisterCardStatsWithdrawServiceServer(server, cardStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", "localhost:0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		CardClient: conn,
	})

	// Create user
	user, err := userRepo.UserCommand.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Graphql",
		LastName:  "Card",
		Email:     "graphql.card@example.com",
		Password:  "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)
}

func (s *CardGraphQLTestSuite) TearDownSuite() {
	s.conn.Close()
	s.grpcServer.Stop()
	s.redisClient.Close()
	if s.chConn != nil {
		s.chConn.Close()
	}
	s.ts.Teardown()
}

const createCardQuery = `mutation CreateCard($input: CreateCardInput!) {
  createCard(input: $input) { status message data { id user_id card_number card_type card_provider } }
}`

const findByIdCardQuery = `query FindByIdCard($input: FindByIdCardInput!) {
  findByIdCard(input: $input) { status message data { id user_id card_type card_provider } }
}`

const updateCardQuery = `mutation UpdateCard($input: UpdateCardInput!) {
  updateCard(input: $input) { status message data { id card_type card_provider } }
}`

const trashedCardQuery = `mutation TrashedCard($input: FindByIdCardInput!) {
  trashedCard(input: $input) { status message data { id deleted_at } }
}`

const restoreCardQuery = `mutation RestoreCard($input: FindByIdCardInput!) {
  restoreCard(input: $input) { status message data { id } }
}`

const deleteCardPermanentQuery = `mutation DeleteCardPermanent($input: FindByIdCardInput!) {
  deleteCardPermanent(input: $input) { status message }
}`

const restoreAllCardQuery = `mutation RestoreAllCard { restoreAllCard { status message } }`

const deleteAllCardPermanentQuery = `mutation DeleteAllCardPermanent { deleteAllCardPermanent { status message } }`

const monthlyTopupAmountQuery = `query MonthlyTopup($input: FindYearStatsInput!) {
  findMonthlyTopupAmountStats(input: $input) { status message data { month total_amount } }
}`

const monthlyTransactionAmountQuery = `query MonthlyTransaction($input: FindYearStatsInput!) {
  findMonthlyTransactionAmountStats(input: $input) { status message data { month total_amount } }
}`

const monthlyTransferSenderAmountQuery = `query MonthlyTransferSender($input: FindYearCardNumberStatsInput!) {
  findMonthlyTransferSenderAmountStats(input: $input) { status message data { month total_amount } }
}`

const monthlyWithdrawAmountQuery = `query MonthlyWithdraw($input: FindYearStatsInput!) {
  findMonthlyWithdrawAmountStats(input: $input) { status message data { month total_amount } }
}`

const monthlyBalanceQuery = `query MonthlyBalance($input: FindYearStatsInput!) {
  findMonthlyBalanceStats(input: $input) { status message data { month total_balance } }
}`

func (s *CardGraphQLTestSuite) Test1_CreateCard() {
	created := tests.GraphQLOp(s.T(), s.handler, "createCard", createCardQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"user_id":       s.userID,
			"card_type":     "debit",
			"expire_date":   time.Now().AddDate(5, 0, 0).Format("2006-01-02"),
			"cvv":           "123",
			"card_provider": "Visa",
		},
	})
	s.Equal("success", created["status"])
	s.cardID = tests.GQLID(tests.GQLData(created), "id")
	s.Require().NotZero(s.cardID)
}

func (s *CardGraphQLTestSuite) Test2_FindById() {
	s.Require().NotZero(s.cardID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdCard", findByIdCardQuery,
		map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}})
	s.Equal("success", found["status"])
	s.Equal(s.cardID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *CardGraphQLTestSuite) Test3_UpdateCard() {
	s.Require().NotZero(s.cardID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateCard", updateCardQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"card_id":       s.cardID,
			"user_id":       s.userID,
			"card_type":     "credit",
			"expire_date":   time.Now().AddDate(6, 0, 0).Format("2006-01-02"),
			"cvv":           "456",
			"card_provider": "MasterCard",
		},
	})
	s.Equal("success", updated["status"])
	s.Equal("credit", tests.GQLData(updated)["card_type"])
}

func (s *CardGraphQLTestSuite) Test4_TrashAndRestore() {
	s.Require().NotZero(s.cardID)

	trashed := tests.GraphQLOp(s.T(), s.handler, "trashedCard", trashedCardQuery,
		map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}})
	s.Equal("success", trashed["status"])

	restored := tests.GraphQLOp(s.T(), s.handler, "restoreCard", restoreCardQuery,
		map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}})
	s.Equal("success", restored["status"])
}

func (s *CardGraphQLTestSuite) Test5_DeletePermanent() {
	s.Require().NotZero(s.cardID)

	trashed := tests.GraphQLOp(s.T(), s.handler, "trashedCard", trashedCardQuery,
		map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}})
	s.Equal("success", trashed["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteCardPermanent", deleteCardPermanentQuery,
		map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}})
	s.Equal("success", deleted["status"])
}

func (s *CardGraphQLTestSuite) Test6_CardStats_MonthlyTopupAmount() {
	ctx := context.Background()
	now := time.Now()
	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE topup_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO topup_events (topup_id, topup_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`, 1, "TP001", "1234567890", 5000, "success", now)

	res := tests.GraphQLOp(s.T(), s.handler, "findMonthlyTopupAmountStats", monthlyTopupAmountQuery,
		map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
	s.Equal("success", res["status"])
}

func (s *CardGraphQLTestSuite) Test7_CardStats_Transaction() {
	ctx := context.Background()
	now := time.Now()
	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE transaction_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO transaction_events (transaction_id, transaction_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`, 1, "TX001", "1234567890", 1000, "success", now)

	res := tests.GraphQLOp(s.T(), s.handler, "findMonthlyTransactionAmountStats", monthlyTransactionAmountQuery,
		map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
	s.Equal("success", res["status"])
}

func (s *CardGraphQLTestSuite) Test8_CardStats_Transfer() {
	ctx := context.Background()
	now := time.Now()
	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE transfer_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO transfer_events (transfer_id, transfer_no, source_card, destination_card, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, 1, "TR001", "1234567890", "0987654321", 2000, "success", now)

	res := tests.GraphQLOp(s.T(), s.handler, "findMonthlyTransferSenderAmountStats", monthlyTransferSenderAmountQuery,
		map[string]interface{}{"input": map[string]interface{}{"year": now.Year(), "card_number": "1234567890"}})
	s.Equal("success", res["status"])
}

func (s *CardGraphQLTestSuite) Test9_CardStats_Withdraw() {
	ctx := context.Background()
	now := time.Now()
	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE withdraw_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO withdraw_events (withdraw_id, withdraw_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`, 1, "WD001", "1234567890", 3000, "success", now)

	res := tests.GraphQLOp(s.T(), s.handler, "findMonthlyWithdrawAmountStats", monthlyWithdrawAmountQuery,
		map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
	s.Equal("success", res["status"])
}

func (s *CardGraphQLTestSuite) Test10_CardStats_Balance_Monthly() {
	ctx := context.Background()
	now := time.Now()
	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE saldo_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO saldo_events (card_number, total_balance, created_at) VALUES (?, ?, ?)`, "1234567890", 10000, now)

	res := tests.GraphQLOp(s.T(), s.handler, "findMonthlyBalanceStats", monthlyBalanceQuery,
		map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}})
	s.Equal("success", res["status"])
}

func (s *CardGraphQLTestSuite) Test11_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllCard", restoreAllCardQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllCardPermanent", deleteAllCardPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestCardGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(CardGraphQLTestSuite))
}
