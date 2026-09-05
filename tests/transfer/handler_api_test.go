package transfer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transfer"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	transferhandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/service"
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

// TransferGraphqlApiTestSuite exercises the apigateway GraphQL layer against a
// real transfer gRPC backend via the generated gqlgen schema.
type TransferGraphqlApiTestSuite struct {
	suite.Suite
	ts           *tests.TestSuite
	redisClient  *redis.Client
	grpcServer   *grpc.Server
	conn         *grpc.ClientConn
	graph        http.Handler
	userRepo     user_repo.UserCommandRepository
	cardRepo     card_repo.Repositories
	saldoRepo    saldo_repo.Repositories
	senderCard   string
	receiverCard string
}

func (s *TransferGraphqlApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "transfer"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	s.userRepo = user_repo.NewUserCommandRepository(gormDB)
	s.cardRepo = *card_repo.NewRepositories(gormDB, nil)
	s.saldoRepo = saldo_repo.NewRepositories(gormDB, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	saldoAdapter := &transferSaldoRepoAdapter{saldoRepo: s.saldoRepo}
	cardAdapter := &transferCardRepoAdapter{cardRepo: s.cardRepo}
	transferRepos := repository.NewRepositories(gormDB, saldoAdapter, cardAdapter)
	transferService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: transferRepos, SaldoAdapter: s.ts.SaldoAdapter,
		CardAdapter: s.ts.CardAdapter, Logger: log, Cache: cacheStore,
	})

	// Seed sender + receiver
	sender, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Sender", LastName: "GraphQL", Email: "sender.graphql@test.com", Password: "password123",
	})
	s.Require().NoError(err)
	sCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(sender.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "111", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.senderCard = sCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.senderCard, TotalBalance: 1000000})
	s.Require().NoError(err)

	receiver, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Receiver", LastName: "GraphQL", Email: "receiver.graphql@test.com", Password: "password123",
	})
	s.Require().NoError(err)
	rCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(receiver.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "222", CardProvider: "mastercard",
	})
	s.Require().NoError(err)
	s.receiverCard = rCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.receiverCard, TotalBalance: 0})
	s.Require().NoError(err)

	transferHandler := transferhandler.NewHandler(transferService)

	server := grpc.NewServer()
	pb.RegisterTransferCommandServiceServer(server, transferHandler)
	pb.RegisterTransferQueryServiceServer(server, transferHandler)
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
		MerchantClient:    testhelper.CreateDummyConn(),
		SaldoClient:       testhelper.CreateDummyConn(),
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    conn,
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *TransferGraphqlApiTestSuite) TearDownSuite() {
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

func (s *TransferGraphqlApiTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *TransferGraphqlApiTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *TransferGraphqlApiTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *TransferGraphqlApiTestSuite) Test1_CreateTransfer() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateTransferInput!) {
			createTransfer(input: $input) {
				status
				message
				data { id transfer_from transfer_to transfer_amount }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"transfer_from":   s.senderCard,
			"transfer_to":     s.receiverCard,
			"transfer_amount": 50000,
		},
	}), "createTransfer")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.senderCard, data["transfer_from"])
	s.Equal(s.receiverCard, data["transfer_to"])

	// Verify balances moved.
	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.senderCard)
	s.Equal(int64(950000), customerSaldo.TotalBalance)
	merchantSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.receiverCard)
	s.Equal(int64(50000), merchantSaldo.TotalBalance)
}

func (s *TransferGraphqlApiTestSuite) Test2_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllTransfer { status message } }`, nil), "restoreAllTransfer")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllTransferPermanent { status message } }`, nil), "deleteAllTransferPermanent")
	s.assertSuccess(deleteAll)
}

func TestTransferGraphqlApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransferGraphqlApiTestSuite))
}
