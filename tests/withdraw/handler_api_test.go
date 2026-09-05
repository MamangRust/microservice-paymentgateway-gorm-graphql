package withdraw_test

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
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/withdraw"
	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	withdrawhandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/withdraw/handler"
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

type WithdrawGraphqlHandlerTestSuite struct {
	suite.Suite
	ts                 *tests.TestSuite
	redisClient        *redis.Client
	grpcServer         *grpc.Server
	conn               *grpc.ClientConn
	chConn             clickhouse.Conn
	graph              http.Handler
	repos              repository.Repositories
	saldoRepo          saldo_repo.Repositories
	customerCardNumber string
	withdrawID         int
}

func (s *WithdrawGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "withdraw"))

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

	_ = s.chConn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS withdraw_events (
		withdraw_id UInt64, withdraw_no String, card_number String,
		amount Int64, status String, created_at DateTime DEFAULT now()
	) ENGINE = MergeTree() ORDER BY (card_number, created_at)`)

	userRepo := user_repo.NewUserCommandRepository(gormDB)
	cardRepos := card_repo.NewRepositories(gormDB, nil)
	s.saldoRepo = saldo_repo.NewRepositories(gormDB, nil)
	s.repos = repository.NewRepositories(gormDB, cardRepos.CardQuery, s.saldoRepo)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	withdrawService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: s.repos, CardAdapter: s.ts.CardAdapter,
		SaldoAdapter: s.ts.SaldoAdapter, Logger: log, Cache: cacheStore, AISecurityClient: nil,
	})

	customer, err := userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Withdraw", LastName: "GraphQL", Email: "withdraw.graphql@test.com", Password: "password123",
	})
	s.Require().NoError(err)
	card, err := cardRepos.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(customer.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "999", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.customerCardNumber = card.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.customerCardNumber, TotalBalance: 1000000})
	s.Require().NoError(err)

	withdrawHandler := withdrawhandler.NewHandler(withdrawService)

	chRepo := stats_repo.NewRepository(chConn)
	withdrawStatsHandler := stats_handler.NewWithdrawStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterWithdrawCommandServiceServer(server, withdrawHandler)
	pb.RegisterWithdrawQueryServiceServer(server, withdrawHandler)
	statspb.RegisterWithdrawStatsAmountServiceServer(server, withdrawStatsHandler)
	statspb.RegisterWithdrawStatsStatusServiceServer(server, withdrawStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	conns := &testhelper.ServiceConnections{
		AuthClient: testhelper.CreateDummyConn(), RoleClient: testhelper.CreateDummyConn(),
		UserClient: testhelper.CreateDummyConn(), CardClient: testhelper.CreateDummyConn(),
		MerchantClient: testhelper.CreateDummyConn(), SaldoClient: testhelper.CreateDummyConn(),
		TopupClient: testhelper.CreateDummyConn(), TransactionClient: testhelper.CreateDummyConn(),
		TransferClient: testhelper.CreateDummyConn(), WithdrawClient: conn, StatsReaderClient: conn,
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *WithdrawGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *WithdrawGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
	payload, _ := json.Marshal(map[string]interface{}{"query": query, "variables": variables})
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.graph.ServeHTTP(rec, req)
	var res map[string]interface{}
	s.Require().NoError(json.Unmarshal(rec.Body.Bytes(), &res), rec.Body.String())
	return res
}

func (s *WithdrawGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q: %v", key, res)
	return field
}

func (s *WithdrawGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *WithdrawGraphqlHandlerTestSuite) Test1_CreateWithdraw() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateWithdrawInput!) {
			createWithdraw(input: $input) { status data { withdraw_id card_number } }
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number": s.customerCardNumber, "withdraw_amount": 100000, "withdraw_time": time.Now().Format("2006-01-02"),
		},
	}), "createWithdraw")
	s.assertSuccess(field)
	s.withdrawID = int(field["data"].(map[string]interface{})["withdraw_id"].(float64))
	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(900000), customerSaldo.TotalBalance)
}

func (s *WithdrawGraphqlHandlerTestSuite) Test2_FindById() {
	s.Require().NotZero(s.withdrawID)
	field := s.dataField(s.graphql(`
		query($input: FindByIdWithdrawInput!) {
			findByIdWithdraw(input: $input) { status data { withdraw_id withdraw_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"withdraw_id": s.withdrawID}}), "findByIdWithdraw")
	s.assertSuccess(field)
	s.Equal(float64(100000), field["data"].(map[string]interface{})["withdraw_amount"])
}

func (s *WithdrawGraphqlHandlerTestSuite) Test3_FindAll() {
	field := s.dataField(s.graphql(`
		query($input: FindAllWithdrawInput!) {
			findAllWithdraw(input: $input) { status data { withdraw_id } }
		}`, map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}}), "findAllWithdraw")
	s.assertSuccess(field)
	s.NotEmpty(field["data"])
}

func (s *WithdrawGraphqlHandlerTestSuite) Test4_Update() {
	s.Require().NotZero(s.withdrawID)
	field := s.dataField(s.graphql(`
		mutation($input: UpdateWithdrawInput!) {
			updateWithdraw(input: $input) { status data { withdraw_amount } }
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"withdraw_id": s.withdrawID, "card_number": s.customerCardNumber,
			"withdraw_amount": 150000, "withdraw_time": time.Now().Format("2006-01-02"),
		},
	}), "updateWithdraw")
	s.assertSuccess(field)
	s.Equal(float64(150000), field["data"].(map[string]interface{})["withdraw_amount"])
	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(850000), customerSaldo.TotalBalance)
}

func (s *WithdrawGraphqlHandlerTestSuite) Test5_TrashRestoreDelete() {
	s.Require().NotZero(s.withdrawID)
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdWithdrawInput!) { trashedWithdraw(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"withdraw_id": s.withdrawID}}), "trashedWithdraw"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdWithdrawInput!) { restoreWithdraw(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"withdraw_id": s.withdrawID}}), "restoreWithdraw"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdWithdrawInput!) { trashedWithdraw(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"withdraw_id": s.withdrawID}}), "trashedWithdraw"))
	s.assertSuccess(s.dataField(s.graphql(`mutation($i: FindByIdWithdrawInput!) { deleteWithdrawPermanent(input: $i) { status } }`,
		map[string]interface{}{"i": map[string]interface{}{"withdraw_id": s.withdrawID}}), "deleteWithdrawPermanent"))
}

func (s *WithdrawGraphqlHandlerTestSuite) Test6_StatsQueries_Real() {
	ctx := context.Background()
	now := time.Now()
	cardNumber := s.customerCardNumber

	_ = s.chConn.Exec(ctx, "TRUNCATE TABLE withdraw_events")
	_ = s.chConn.Exec(ctx, `INSERT INTO withdraw_events (withdraw_id, withdraw_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		1, "WD001", cardNumber, int64(1000), "success", now)
	_ = s.chConn.Exec(ctx, `INSERT INTO withdraw_events (withdraw_id, withdraw_no, card_number, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		2, "WD002", cardNumber, int64(2000), "failed", now)

	// Monthly amount stats
	field := s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findMonthlyWithdrawAmountStats(input: $input) { status data { month total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findMonthlyWithdrawAmountStats")
	s.Equal("success", field["status"])
	s.NotEmpty(field["data"])

	// Yearly amount stats
	field = s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findYearlyWithdrawAmountStats(input: $input) { status data { year total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findYearlyWithdrawAmountStats")
	s.Equal("success", field["status"])
	s.NotEmpty(field["data"])

	// Monthly amount by card
	field = s.dataField(s.graphql(`
		query($year: Int!, $card: String!) {
			findMonthlyWithdrawAmountByCardNumberStats(input: {year: $year, card_number: $card}) { status data { month total_amount } }
		}`, map[string]interface{}{"year": now.Year(), "card": cardNumber}), "findMonthlyWithdrawAmountByCardNumberStats")
	s.Equal("success", field["status"])
	s.NotEmpty(field["data"])

	// Monthly status success
	field = s.dataField(s.graphql(`
		query($year: Int!, $month: Int!) {
			findMonthlyWithdrawStatusSuccessStats(input: {year: $year, month: $month}) { status data { year month total_success total_amount } }
		}`, map[string]interface{}{"year": now.Year(), "month": int(now.Month())}), "findMonthlyWithdrawStatusSuccessStats")
	s.Equal("success", field["status"])
	s.NotEmpty(field["data"])

	// Yearly status failed
	field = s.dataField(s.graphql(`
		query($input: FindYearStatsInput!) {
			findYearlyWithdrawStatusFailedStats(input: $input) { status data { year total_amount } }
		}`, map[string]interface{}{"input": map[string]interface{}{"year": now.Year()}}), "findYearlyWithdrawStatusFailedStats")
	s.Equal("success", field["status"])
	s.NotEmpty(field["data"])
}

func (s *WithdrawGraphqlHandlerTestSuite) Test7_BulkOperations() {
	s.assertSuccess(s.dataField(s.graphql(`mutation { restoreAllWithdraw { status } }`, nil), "restoreAllWithdraw"))
	s.assertSuccess(s.dataField(s.graphql(`mutation { deleteAllWithdrawPermanent { status } }`, nil), "deleteAllWithdrawPermanent"))
}

func TestWithdrawGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WithdrawGraphqlHandlerTestSuite))
}
