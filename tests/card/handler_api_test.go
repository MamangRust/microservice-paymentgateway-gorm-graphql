package card_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	cardhandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/card/service"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
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

type CardGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	chConn      clickhouse.Conn
	graph       http.Handler
	userID      int
	cardID      int
}

func (s *CardGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	chOpts, err := clickhouse.ParseDSN(s.ts.CHURL)
	s.Require().NoError(err)
	chConn, err := clickhouse.Open(chOpts)
	s.Require().NoError(err)
	s.chConn = chConn

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

	cardGapiHandler := cardhandler.NewHandler(cardService)

	chRepo := stats_repo.NewRepository(chConn)
	cardStatsHandler := stats_handler.NewCardStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterCardQueryServiceServer(server, cardGapiHandler)
	pb.RegisterCardCommandServiceServer(server, cardGapiHandler)
	statspb.RegisterCardStatsBalanceServiceServer(server, cardStatsHandler)
	statspb.RegisterCardStatsTopupServiceServer(server, cardStatsHandler)
	statspb.RegisterCardStatsTransactionServiceServer(server, cardStatsHandler)
	statspb.RegisterCardStatsTransferServiceServer(server, cardStatsHandler)
	statspb.RegisterCardStatsWithdrawServiceServer(server, cardStatsHandler)
	pb.RegisterCardDashboardServiceServer(server, cardStatsHandler)

	// Register domain-wide stats services so GraphQL stats queries work.
	statspb.RegisterTopupStatsAmountServiceServer(server, stats_handler.NewTopupStatsHandler(chRepo, log))
	statspb.RegisterTopupStatsMethodServiceServer(server, stats_handler.NewTopupStatsHandler(chRepo, log))
	statspb.RegisterTopupStatsStatusServiceServer(server, stats_handler.NewTopupStatsHandler(chRepo, log))
	statspb.RegisterTransactionStatsAmountServiceServer(server, stats_handler.NewTransactionStatsHandler(chRepo, log))
	statspb.RegisterTransactionStatsMethodServiceServer(server, stats_handler.NewTransactionStatsHandler(chRepo, log))
	statspb.RegisterTransactionStatsStatusServiceServer(server, stats_handler.NewTransactionStatsHandler(chRepo, log))
	statspb.RegisterTransferStatsAmountServiceServer(server, stats_handler.NewTransferStatsHandler(chRepo, log))
	statspb.RegisterTransferStatsStatusServiceServer(server, stats_handler.NewTransferStatsHandler(chRepo, log))
	statspb.RegisterWithdrawStatsAmountServiceServer(server, stats_handler.NewWithdrawStatsHandler(chRepo, log))
	statspb.RegisterWithdrawStatsStatusServiceServer(server, stats_handler.NewWithdrawStatsHandler(chRepo, log))
	s.grpcServer = server

	lis, err := net.Listen("tcp", "localhost:0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        conn,
		MerchantClient:    testhelper.CreateDummyConn(),
		SaldoClient:       testhelper.CreateDummyConn(),
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: conn,
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)

	userRepo := user_repo.NewUserCommandRepository(gormDB)
	user, err := userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "GraphQL", LastName: "Card", Email: "graphql.card@example.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(user.UserID)
}

func (s *CardGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *CardGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
	payload, _ := json.Marshal(map[string]interface{}{"query": query, "variables": variables})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.graph.ServeHTTP(rec, req)
	var res map[string]interface{}
	s.Require().NoError(json.Unmarshal(rec.Body.Bytes(), &res), rec.Body.String())
	return res
}

func (s *CardGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q: %v", key, res)
	return field
}

func (s *CardGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *CardGraphqlHandlerTestSuite) Test1_CreateCard() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateCardInput!) {
			createCard(input: $input) { status data { id user_id } }
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"user_id": s.userID, "card_type": "debit",
			"expire_date": time.Now().AddDate(5, 0, 0).Format("2006-01-02"),
			"cvv": "123", "card_provider": "Visa",
		},
	}), "createCard")
	s.assertSuccess(field)
	s.cardID = int(field["data"].(map[string]interface{})["id"].(float64))
}

func (s *CardGraphqlHandlerTestSuite) Test2_FindById() {
	s.Require().NotZero(s.cardID)
	field := s.dataField(s.graphql(`
		query($input: FindByIdCardInput!) {
			findByIdCard(input: $input) { status data { id } }
		}`, map[string]interface{}{"input": map[string]interface{}{"card_id": s.cardID}}), "findByIdCard")
	s.assertSuccess(field)
	s.Equal(float64(s.cardID), field["data"].(map[string]interface{})["id"])
}

func (s *CardGraphqlHandlerTestSuite) Test3_UpdateCard() {
	s.Require().NotZero(s.cardID)
	field := s.dataField(s.graphql(`
		mutation($input: UpdateCardInput!) {
			updateCard(input: $input) { status data { id card_type } }
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"card_id": s.cardID, "user_id": s.userID, "card_type": "credit",
			"expire_date": time.Now().AddDate(6, 0, 0).Format("2006-01-02"),
			"cvv": "456", "card_provider": "MasterCard",
		},
	}), "updateCard")
	s.assertSuccess(field)
	s.Equal("credit", field["data"].(map[string]interface{})["card_type"])
}

func (s *CardGraphqlHandlerTestSuite) Test4_TrashRestoreAndDelete() {
	s.Require().NotZero(s.cardID)
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdCardInput!) { trashedCard(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"card_id": s.cardID}}), "trashedCard"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdCardInput!) { restoreCard(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"card_id": s.cardID}}), "restoreCard"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdCardInput!) { trashedCard(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"card_id": s.cardID}}), "trashedCard"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdCardInput!) { deleteCardPermanent(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"card_id": s.cardID}}), "deleteCardPermanent"))
}

func (s *CardGraphqlHandlerTestSuite) Test5_StatsQueries_Real() {
	ctx := context.Background()
	now := time.Now()
	cardNumber := "1234567890"

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE topup_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO topup_events (topup_id, topup_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		1, "TP001", cardNumber, int64(5000), "success", now)

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE transaction_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO transaction_events (transaction_id, transaction_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		1, "TX001", cardNumber, int64(1000), "success", now)

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE transfer_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO transfer_events (transfer_id, transfer_no, source_card, destination_card, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		1, "TR001", cardNumber, "0987654321", int64(2000), "success", now)

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE withdraw_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO withdraw_events (withdraw_id, withdraw_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		1, "WD001", cardNumber, int64(3000), "success", now)

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE saldo_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO saldo_events (card_number, total_balance, created_at) VALUES (?, ?, ?)`,
		cardNumber, int64(10000), now)

	// Topup monthly
	field := s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findMonthlyTopupAmountStats(input: $input) { status data { month total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findMonthlyTopupAmountStats")
	s.Equal("success", field["status"])
	s.InDelta(float64(5000), field["data"].([]interface{})[0].(map[string]interface{})["total_amount"], 1)

	// Transaction monthly
	field = s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findMonthlyTransactionAmountStats(input: $input) { status data { month total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findMonthlyTransactionAmountStats")
	s.Equal("success", field["status"])
	s.InDelta(float64(1000), field["data"].([]interface{})[0].(map[string]interface{})["total_amount"], 1)

	// Transfer sender monthly
	field = s.dataField(s.graphql(`
		query($year: Int!, $card: String!) {
			findMonthlyTransferSenderAmountStats(input: {year: $year, card_number: $card}) { status data { month total_amount } }
		}`, map[string]interface{}{"year": now.Year(), "card": cardNumber}), "findMonthlyTransferSenderAmountStats")
	s.Equal("success", field["status"])
	s.InDelta(float64(2000), field["data"].([]interface{})[0].(map[string]interface{})["total_amount"], 1)

	// Withdraw monthly
	field = s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findMonthlyWithdrawAmountStats(input: $input) { status data { month total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findMonthlyWithdrawAmountStats")
	s.Equal("success", field["status"])
	s.InDelta(float64(3000), field["data"].([]interface{})[0].(map[string]interface{})["total_amount"], 1)

	// Saldo balance monthly
	field = s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findMonthlyBalanceStats(input: $input) { status data { month total_balance } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findMonthlyBalanceStats")
	s.Equal("success", field["status"])
	s.InDelta(float64(10000), field["data"].([]interface{})[0].(map[string]interface{})["total_balance"], 1)
}

func (s *CardGraphqlHandlerTestSuite) Test6_BulkOperations() {
	s.assertSuccess(s.dataField(s.graphql(`mutation { restoreAllCard { status } }`, nil), "restoreAllCard"))
	s.assertSuccess(s.dataField(s.graphql(`mutation { deleteAllCardPermanent { status } }`, nil), "deleteAllCardPermanent"))
}

func TestCardGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(CardGraphqlHandlerTestSuite))
}
