package saldo_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/saldo"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	saldohandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/service"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SaldoGraphqlHandlerTestSuite exercises the apigateway GraphQL layer against a
// real saldo gRPC backend via the generated gqlgen schema.
type SaldoGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	cardNumber  string
	saldoID     int
}

func (s *SaldoGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	userRepos := user_repo.NewRepositories(gormDB)
	cardRepos := card_repo.NewRepositories(gormDB, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	saldoService := service.NewService(&service.Deps{
		Repositories: saldo_repo.NewRepositories(gormDB, nil),
		CardAdapter:  s.ts.CardAdapter,
		Logger:       log,
		Cache:        cacheStore,
	})

	user, err := userRepos.UserCommand().CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Saldo", LastName: "GraphQL", Email: "saldo.graphql@example.com", Password: "password123",
	})
	s.Require().NoError(err)

	card, err := cardRepos.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(user.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "123", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.cardNumber = card.CardNumber

	saldoHandler := saldohandler.NewHandler(saldoService)

	server := grpc.NewServer()
	pb.RegisterSaldoCommandServiceServer(server, saldoHandler)
	pb.RegisterSaldoQueryServiceServer(server, saldoHandler)
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
		SaldoClient:       conn,
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *SaldoGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *SaldoGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *SaldoGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *SaldoGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *SaldoGraphqlHandlerTestSuite) Test1_CreateSaldo() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateSaldoInput!) {
			createSaldo(input: $input) {
				status
				message
				data { saldo_id card_number total_balance }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":   s.cardNumber,
			"total_balance": 500000,
		},
	}), "createSaldo")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.cardNumber, data["card_number"])
	s.saldoID = int(data["saldo_id"].(float64))
}

func (s *SaldoGraphqlHandlerTestSuite) Test2_FindById() {
	s.Require().NotZero(s.saldoID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdSaldoInput!) {
			findByIdSaldo(input: $input) {
				status
				message
				data { saldo_id card_number total_balance }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"saldo_id": s.saldoID},
	}), "findByIdSaldo")

	s.assertSuccess(field)
	s.Equal(float64(500000), field["data"].(map[string]interface{})["total_balance"])
}

func (s *SaldoGraphqlHandlerTestSuite) Test3_FindByCardNumber() {
	s.Require().NotEmpty(s.cardNumber)

	field := s.dataField(s.graphql(`
		query($cardNumber: String!) {
			findByCardNumberSaldo(card_number: $cardNumber) {
				status
				message
				data { saldo_id card_number }
			}
		}`, map[string]interface{}{"cardNumber": s.cardNumber}), "findByCardNumberSaldo")

	s.assertSuccess(field)
	s.Equal(s.cardNumber, field["data"].(map[string]interface{})["card_number"])
}

func (s *SaldoGraphqlHandlerTestSuite) Test4_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllSaldo { status message } }`, nil), "restoreAllSaldo")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllSaldoPermanent { status message } }`, nil), "deleteAllSaldoPermanent")
	s.assertSuccess(deleteAll)
}

func TestSaldoGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(SaldoGraphqlHandlerTestSuite))
}
