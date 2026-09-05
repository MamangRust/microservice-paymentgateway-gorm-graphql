package topup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/topup"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	topup_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/repository"
	topuphandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/topup/service"
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

// TopupGraphqlHandlerTestSuite exercises the apigateway GraphQL layer against
// a real topup gRPC backend via the generated gqlgen schema.
type TopupGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	cardNumber  string
	topupID     int
}

func (s *TopupGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "topup"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	userRepos := user_repo.NewRepositories(gormDB)
	cardRepos := card_repo.NewRepositories(gormDB, nil)
	saldoRepos := saldo_repo.NewRepositories(gormDB, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	cardAdapter := &topupCardRepoAdapter{
		CardQueryRepository: cardRepos.CardQuery, CardCommandRepository: cardRepos.CardCommand,
	}
	topupRepositories := topup_repo.NewRepositories(gormDB, cardAdapter, saldoRepos)

	topupService := service.NewService(&service.Deps{
		Kafka: nil, Cache: cacheStore, Repositories: topupRepositories,
		CardAdapter: s.ts.CardAdapter, SaldoAdapter: s.ts.SaldoAdapter, Logger: log,
	})

	user, err := userRepos.UserCommand().CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Topup", LastName: "GraphQL", Email: "topup.graphql@example.com", Password: "password123",
	})
	s.Require().NoError(err)

	card, err := cardRepos.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(user.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "123", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.cardNumber = card.CardNumber

	_, err = saldoRepos.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{
		CardNumber: s.cardNumber, TotalBalance: 1000000,
	})
	s.Require().NoError(err)

	topupHandler := topuphandler.NewHandler(topupService)

	server := grpc.NewServer()
	pb.RegisterTopupCommandServiceServer(server, topupHandler)
	pb.RegisterTopupQueryServiceServer(server, topupHandler)
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
		TopupClient:       conn,
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, log, s.redisClient)
	s.graph = testhelper.NewGraphQLHTTPHandler(resolver)
}

func (s *TopupGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *TopupGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *TopupGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *TopupGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *TopupGraphqlHandlerTestSuite) Test1_CreateTopup() {
	field := s.dataField(s.graphql(`
		mutation($input: CreateTopupInput!) {
			createTopup(input: $input) {
				status
				message
				data { id card_number topup_amount }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"card_number":   s.cardNumber,
			"topup_no":      fmt.Sprintf("TP-GRAPHQL-%d", time.Now().UnixNano()),
			"topup_amount":  100000,
			"topup_method":  "visa",
		},
	}), "createTopup")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.cardNumber, data["card_number"])
	s.topupID = int(data["id"].(float64))
}

func (s *TopupGraphqlHandlerTestSuite) Test2_FindById() {
	s.Require().NotZero(s.topupID)

	field := s.dataField(s.graphql(`
		query($input: FindByIdTopupInput!) {
			findByIdTopup(input: $input) {
				status
				message
				data { id topup_amount }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"topup_id": s.topupID},
	}), "findByIdTopup")

	s.assertSuccess(field)
	s.Equal(float64(100000), field["data"].(map[string]interface{})["topup_amount"])
}

func (s *TopupGraphqlHandlerTestSuite) Test3_BulkOperations() {
	restoreAll := s.dataField(s.graphql(`
		mutation { restoreAllTopup { status message } }`, nil), "restoreAllTopup")
	s.assertSuccess(restoreAll)

	deleteAll := s.dataField(s.graphql(`
		mutation { deleteAllTopupPermanent { status message } }`, nil), "deleteAllTopupPermanent")
	s.assertSuccess(deleteAll)
}

func TestTopupGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TopupGraphqlHandlerTestSuite))
}
