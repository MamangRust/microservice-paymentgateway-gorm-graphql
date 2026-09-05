package transaction_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transaction"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	merchant_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	transactionhandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/service"
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

// TransactionGraphqlHandlerTestSuite exercises the apigateway GraphQL layer
// against a real transaction gRPC backend via the generated gqlgen schema.
// Merchant API-key validation is satisfied through the apigateway cache
// (no Kafka required).
type TransactionGraphqlHandlerTestSuite struct {
	suite.Suite
	ts                 *tests.TestSuite
	redisClient        *redis.Client
	grpcServer         *grpc.Server
	conn               *grpc.ClientConn
	graph              http.Handler
	userRepo           user_repo.UserCommandRepository
	cardRepo           card_repo.Repositories
	saldoRepo          saldo_repo.Repositories
	merchantRepo       merchant_repo.Repositories
	customerCardNumber string
	merchantCardNumber string
	merchantApiKey     string
	merchantID         int
	transactionID      int
}

func (s *TransactionGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "merchant", "saldo", "transaction"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	s.userRepo = user_repo.NewUserCommandRepository(gormDB)
	s.cardRepo = *card_repo.NewRepositories(gormDB, nil)
	s.saldoRepo = saldo_repo.NewRepositories(gormDB, nil)
	s.merchantRepo = merchant_repo.NewRepositories(gormDB, nil)

	cardRepoWrapper := &transactionCardRepo{
		query: s.cardRepo.CardQuery, command: s.cardRepo.CardCommand,
	}
	transactionRepos := repository.NewRepositories(gormDB, s.saldoRepo, cardRepoWrapper, s.merchantRepo)
	transactionService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: transactionRepos, MerchantAdapter: s.ts.MerchantAdapter,
		CardAdapter: s.ts.CardAdapter, SaldoAdapter: s.ts.SaldoAdapter, Logger: log,
		Cache: cacheStore, AISecurityClient: nil,
	})

	// Seed Customer
	customer, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Transaction", LastName: "GraphQL", Email: "customer.graphql@transaction.com", Password: "password123",
	})
	s.Require().NoError(err)
	cCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(customer.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "123", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.customerCardNumber = cCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.customerCardNumber, TotalBalance: 1000000})
	s.Require().NoError(err)

	// Seed Merchant
	owner, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Merchant", LastName: "GraphQL", Email: "merchant.owner.graphql@transaction.com", Password: "password123",
	})
	s.Require().NoError(err)
	merchant, err := s.merchantRepo.CreateMerchant(context.Background(), &requests.CreateMerchantRequest{
		UserID: int(owner.UserID), Name: "GraphQL Merchant",
	})
	s.Require().NoError(err)
	s.merchantID = int(merchant.MerchantID)
	_, err = s.merchantRepo.UpdateMerchantStatus(context.Background(), &requests.UpdateMerchantStatusRequest{
		MerchantID: &s.merchantID, Status: "active",
	})
	s.Require().NoError(err)
	mFull, _ := s.merchantRepo.FindByMerchantId(context.Background(), s.merchantID)
	s.merchantApiKey = mFull.ApiKey

	mCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(owner.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "321", CardProvider: "mastercard",
	})
	s.Require().NoError(err)
	s.merchantCardNumber = mCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.merchantCardNumber, TotalBalance: 0})
	s.Require().NoError(err)

	transactionHandler := transactionhandler.NewHandler(transactionService)

	server := grpc.NewServer()
	pb.RegisterTransactionCommandServiceServer(server, transactionHandler)
	pb.RegisterTransactionQueryServiceServer(server, transactionHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	// Seed the merchant API key into the apigateway permission cache so the
	// GraphQL ValidateMerchant step succeeds without Kafka.
	s.Require().NoError(testhelper.SeedMerchantCache(
		s.redisClient, log, s.merchantApiKey, strconv.Itoa(s.merchantID),
	))

	conns := &testhelper.ServiceConnections{
		AuthClient:        testhelper.CreateDummyConn(),
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        testhelper.CreateDummyConn(),
		MerchantClient:    testhelper.CreateDummyConn(),
		SaldoClient:       testhelper.CreateDummyConn(),
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: conn,
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *TransactionGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *TransactionGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *TransactionGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *TransactionGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *TransactionGraphqlHandlerTestSuite) Test1_CreateTransaction() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateTransactionInput!) {
			createTransaction(input: $input) {
				status
				message
				data { id card_number amount merchant_id }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"api_key":          s.merchantApiKey,
			"card_number":      s.customerCardNumber,
			"amount":           50000,
			"payment_method":   "visa",
			"merchant_id":      s.merchantID,
			"transaction_time": time.Now().Format("2006-01-02"),
		},
	}), "createTransaction")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.customerCardNumber, data["card_number"])
	s.transactionID = int(data["id"].(float64))

	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(950000), customerSaldo.TotalBalance)
	merchantSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.merchantCardNumber)
	s.Equal(int64(50000), merchantSaldo.TotalBalance)
}

func (s *TransactionGraphqlHandlerTestSuite) Test2_FindTransactionById() {
	s.Require().NotZero(s.transactionID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdTransactionInput!) {
			findByIdTransaction(input: $input) {
				status
				message
				data { id amount }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"transaction_id": s.transactionID},
	}), "findByIdTransaction")

	s.assertSuccess(field)
	s.Equal(float64(50000), field["data"].(map[string]interface{})["amount"])
}

func (s *TransactionGraphqlHandlerTestSuite) Test3_FindAllTransactions() {
	field := s.dataField(s.graphql(`
		query($input: FindAllTransactionInput!) {
			findAllTransaction(input: $input) {
				status
				message
				data { id }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"page": 1, "page_size": 10},
	}), "findAllTransaction")

	s.assertSuccess(field)
	rows, ok := field["data"].([]interface{})
	s.Require().True(ok, "expected data array")
	s.NotEmpty(rows)
}

func (s *TransactionGraphqlHandlerTestSuite) Test4_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllTransaction { status message } }`, nil), "restoreAllTransaction")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllTransactionPermanent { status message } }`, nil), "deleteAllTransactionPermanent")
	s.assertSuccess(deleteAll)
}

func TestTransactionGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransactionGraphqlHandlerTestSuite))
}
