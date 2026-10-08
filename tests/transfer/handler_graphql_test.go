package transfer_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/transfer"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transfer"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transfer/service"
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

type TransferGraphQLTestSuite struct {
	suite.Suite
	ts           *tests.TestSuite
	db           *gorm.DB
	redisClient  *redis.Client
	grpcServer   *grpc.Server
	chConn       clickhouse.Conn
	conn         *grpc.ClientConn
	handler      http.Handler
	userRepo     user_repo.UserCommandRepository
	cardRepo     card_repo.Repositories
	saldoRepo    saldo_repo.Repositories
	senderCard   string
	receiverCard string
	userID       int
	transferID   int
}

func (s *TransferGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "saldo", "transfer"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	chOpts, err := clickhouse.ParseDSN(s.ts.CHURL)
	s.Require().NoError(err)
	chConn, err := clickhouse.Open(chOpts)
	s.Require().NoError(err)
	s.chConn = chConn
	_ = s.chConn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS transfer_events (
			transfer_id UInt64, transfer_no String, source_card String, destination_card String,
			amount Int64, status String, created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (source_card, created_at)`)

	s.userRepo = user_repo.NewUserCommandRepository(gormDB)
	s.cardRepo = *card_repo.NewRepositories(gormDB, nil)
	s.saldoRepo = saldo_repo.NewRepositories(gormDB, nil, nil)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	transferRepos := repository.NewRepositories(gormDB, nil, nil, nil, nil)
	transferService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: transferRepos, SaldoAdapter: s.ts.SaldoAdapter,
		CardAdapter: s.ts.CardAdapter, Logger: log, Cache: cacheStore,
	})

	// Seed sender + receiver
	sender, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Sender", LastName: "Handler", Email: "sender.graphql@test.com", Password: "password123",
	})
	s.Require().NoError(err)
	s.userID = int(sender.UserID)
	sCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: s.userID, CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "111", CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.senderCard = sCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.senderCard, TotalBalance: 1000000})
	s.Require().NoError(err)

	receiver, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Receiver", LastName: "Handler", Email: "receiver.graphql@test.com", Password: "password123",
	})
	s.Require().NoError(err)
	rCard, err := s.cardRepo.CardCommand.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID: int(receiver.UserID), CardType: "debit", ExpireDate: time.Now().AddDate(1, 0, 0), CVV: "222", CardProvider: "mastercard",
	})
	s.Require().NoError(err)
	s.receiverCard = rCard.CardNumber
	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{CardNumber: s.receiverCard, TotalBalance: 0})
	s.Require().NoError(err)

	transferHandler := handler.NewHandler(transferService)
	chRepo := stats_repo.NewRepository(s.chConn)
	transferStatsHandler := stats_handler.NewTransferStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterTransferCommandServiceServer(server, transferHandler)
	pb.RegisterTransferQueryServiceServer(server, transferHandler)
	pbStats.RegisterTransferStatsAmountServiceServer(server, transferStatsHandler)
	pbStats.RegisterTransferStatsStatusServiceServer(server, transferStatsHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		TransferClient: conn,
	})
}

func (s *TransferGraphQLTestSuite) TearDownSuite() {
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
	s.ts.Teardown()
}

const createTransferQuery = `mutation CreateTransfer($input: CreateTransferInput!) {
  createTransfer(input: $input) { status message data { id transfer_no transfer_from transfer_to transfer_amount } }
}`

const findByIdTransferQuery = `query FindByIdTransfer($input: FindByIdTransferInput!) {
  findByIdTransfer(input: $input) { status message data { id transfer_from transfer_to transfer_amount } }
}`

const findAllTransferQuery = `query FindAllTransfer($input: FindAllTransferInput!) {
  findAllTransfer(input: $input) { status message data { id transfer_from transfer_to transfer_amount } }
}`

const updateTransferQuery = `mutation UpdateTransfer($input: UpdateTransferInput!) {
  updateTransfer(input: $input) { status message data { id transfer_amount } }
}`

const trashedTransferQuery = `mutation TrashedTransfer($input: FindByIdTransferInput!) {
  trashedTransfer(input: $input) { status message data { id deleted_at } }
}`

const restoreTransferQuery = `mutation RestoreTransfer($input: FindByIdTransferInput!) {
  restoreTransfer(input: $input) { status message data { id } }
}`

const deleteTransferPermanentQuery = `mutation DeleteTransferPermanent($input: FindByIdTransferInput!) {
  deleteTransferPermanent(input: $input) { status message }
}`

const restoreAllTransferQuery = `mutation RestoreAllTransfer { restoreAllTransfer { status message } }`

const deleteAllTransferPermanentQuery = `mutation DeleteAllTransferPermanent { deleteAllTransferPermanent { status message } }`

func (s *TransferGraphQLTestSuite) Test1_CreateTransfer() {
	created := tests.GraphQLOp(s.T(), s.handler, "createTransfer", createTransferQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"transfer_from":   s.senderCard,
			"transfer_to":     s.receiverCard,
			"transfer_amount": 100000,
		},
	})
	s.Equal("success", created["status"])
	data := tests.GQLData(created)
	s.transferID = tests.GQLID(data, "id")
	s.Require().NotZero(s.transferID)
	s.Equal(s.senderCard, data["transfer_from"])
	s.Equal(s.receiverCard, data["transfer_to"])
}

func (s *TransferGraphQLTestSuite) Test2_FindByIdTransfer() {
	s.Require().NotZero(s.transferID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdTransfer", findByIdTransferQuery,
		map[string]interface{}{"input": map[string]interface{}{"transfer_id": s.transferID}})
	s.Equal("success", found["status"])
	s.Equal(s.transferID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *TransferGraphQLTestSuite) Test3_FindAllTransfer() {
	found := tests.GraphQLOp(s.T(), s.handler, "findAllTransfer", findAllTransferQuery,
		map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *TransferGraphQLTestSuite) Test4_UpdateTransfer() {
	s.Require().NotZero(s.transferID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateTransfer", updateTransferQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"transfer_id":     s.transferID,
			"transfer_from":   s.senderCard,
			"transfer_to":     s.receiverCard,
			"transfer_amount": 150000,
		},
	})
	s.Equal("success", updated["status"])
	s.Equal(150000, tests.GQLID(tests.GQLData(updated), "transfer_amount"))
}

func (s *TransferGraphQLTestSuite) Test5_TrashedTransfer() {
	s.Require().NotZero(s.transferID)

	trashed := tests.GraphQLOp(s.T(), s.handler, "trashedTransfer", trashedTransferQuery,
		map[string]interface{}{"input": map[string]interface{}{"transfer_id": s.transferID}})
	s.Equal("success", trashed["status"])
}

func (s *TransferGraphQLTestSuite) Test6_RestoreTransfer() {
	s.Require().NotZero(s.transferID)

	restored := tests.GraphQLOp(s.T(), s.handler, "restoreTransfer", restoreTransferQuery,
		map[string]interface{}{"input": map[string]interface{}{"transfer_id": s.transferID}})
	s.Equal("success", restored["status"])
}

func (s *TransferGraphQLTestSuite) Test7_DeleteTransferPermanent() {
	s.Require().NotZero(s.transferID)

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteTransferPermanent", deleteTransferPermanentQuery,
		map[string]interface{}{"input": map[string]interface{}{"transfer_id": s.transferID}})
	s.Equal("success", deleted["status"])
}

func (s *TransferGraphQLTestSuite) Test8_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllTransfer", restoreAllTransferQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllTransferPermanent", deleteAllTransferPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestTransferGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransferGraphQLTestSuite))
}
