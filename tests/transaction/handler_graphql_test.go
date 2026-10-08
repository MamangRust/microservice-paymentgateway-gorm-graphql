package transaction_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	pbAISecurity "github.com/MamangRust/microservice-payment-gateway-grpc/pb/ai_security"
	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats/transaction"
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transaction"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	merchant_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	stats_handler "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	stats_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/transaction/service"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TransactionGraphQLTestSuite struct {
	suite.Suite
	ts                 *tests.TestSuite
	db                 *gorm.DB
	redisClient        *redis.Client
	grpcServer         *grpc.Server
	chConn             clickhouse.Conn
	commandClient      pb.TransactionCommandServiceClient
	conn               *grpc.ClientConn
	handler            http.Handler
	userRepo           user_repo.UserCommandRepository
	cardRepo           card_repo.Repositories
	saldoRepo          saldo_repo.Repositories
	merchantRepo       merchant_repo.Repositories
	customerCardNumber string
	merchantApiKey     string
	merchantID         int
	merchantCardNumber string
	transactionID      int
}

func (s *TransactionGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth", "card", "merchant", "saldo", "transaction"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	chOpts, err := clickhouse.ParseDSN(s.ts.CHURL)
	s.Require().NoError(err)
	chConn, err := clickhouse.Open(chOpts)
	s.Require().NoError(err)
	s.chConn = chConn
	_ = s.chConn.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS transaction_events (
			transaction_id UInt64, transaction_no String, merchant_id UInt64, merchant_name String,
			card_number String, amount Int64, payment_method String, status String,
			created_at DateTime DEFAULT now()
		) ENGINE = MergeTree() ORDER BY (merchant_id, created_at)`)

	s.userRepo = user_repo.NewUserCommandRepository(gormDB)
	s.cardRepo = *card_repo.NewRepositories(gormDB, nil)
	s.saldoRepo = saldo_repo.NewRepositories(gormDB, nil, nil)
	s.merchantRepo = merchant_repo.NewRepositories(gormDB, nil)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	transactionRepos := repository.NewRepositories(gormDB, nil, nil, nil, nil, nil)
	transactionService := service.NewService(&service.Deps{
		Kafka: nil, Repositories: transactionRepos, MerchantAdapter: s.ts.MerchantAdapter,
		CardAdapter: s.ts.CardAdapter, SaldoAdapter: s.ts.SaldoAdapter, Logger: log, Cache: cacheStore, AISecurityAdapter: nil,
	})

	// Seed Customer
	customer, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Transaction", LastName: "Customer", Email: "customer@transaction.com", Password: "password123",
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
		FirstName: "Merchant", LastName: "Owner", Email: "merchant.owner@transaction.com", Password: "password123",
	})
	s.Require().NoError(err)
	merchant, err := s.merchantRepo.CreateMerchant(context.Background(), &requests.CreateMerchantRequest{
		UserID: int(owner.UserID), Name: "Transaction Merchant",
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

	transactionHandlerGapi := handler.NewHandler(transactionService)
	chRepo := stats_repo.NewRepository(s.chConn)
	transactionStatsHandler := stats_handler.NewTransactionStatsHandler(chRepo, log)

	server := grpc.NewServer()
	pb.RegisterTransactionCommandServiceServer(server, transactionHandlerGapi)
	pb.RegisterTransactionQueryServiceServer(server, transactionHandlerGapi)
	pbStats.RegisterTransactionStatsAmountServiceServer(server, transactionStatsHandler)
	pbStats.RegisterTransactionStatsMethodServiceServer(server, transactionStatsHandler)
	pbStats.RegisterTransactionStatsStatusServiceServer(server, transactionStatsHandler)
	pbAISecurity.RegisterAISecurityServiceServer(server, &mockAISecurityServer{})
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn
	s.commandClient = pb.NewTransactionCommandServiceClient(conn)

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		TransactionClient: conn,
	})
}

func (s *TransactionGraphQLTestSuite) TearDownSuite() {
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

const findByIdTransactionQuery = `query FindByIdTransaction($input: FindByIdTransactionInput!) {
  findByIdTransaction(input: $input) { status message data { id card_number transaction_no amount payment_method merchant_id } }
}`

const findAllTransactionQuery = `query FindAllTransaction($input: FindAllTransactionInput!) {
  findAllTransaction(input: $input) { status message data { id card_number amount payment_method merchant_id } }
}`

const findTransactionByMerchantIdQuery = `query FindTransactionByMerchantId($input: FindTransactionByMerchantIdInput!) {
  findTransactionByMerchantId(input: $input) { status message data { id card_number merchant_id } }
}`

const findByActiveTransactionQuery = `query FindByActiveTransaction($input: FindAllTransactionInput!) {
  findByActiveTransaction(input: $input) { status message data { id card_number deleted_at } }
}`

const findByTrashedTransactionQuery = `query FindByTrashedTransaction($input: FindAllTransactionInput!) {
  findByTrashedTransaction(input: $input) { status message data { id card_number deleted_at } }
}`

const restoreAllTransactionQuery = `mutation RestoreAllTransaction { restoreAllTransaction { status message } }`

const deleteAllTransactionPermanentQuery = `mutation DeleteAllTransactionPermanent { deleteAllTransactionPermanent { status message } }`

// Test1_SeedTransaction seeds the transaction by calling the transaction gRPC
// command client directly. The GraphQL createTransaction resolver validates the
// merchant API key through Kafka (Permission.ValidateMerchant), which is not
// configured in tests, so the write is performed out-of-band and the gateway is
// only exercised for reads/bulk operations.
func (s *TransactionGraphQLTestSuite) Test1_SeedTransaction() {
	res, err := s.commandClient.CreateTransaction(context.Background(), &pb.CreateTransactionRequest{
		ApiKey:          s.merchantApiKey,
		CardNumber:      s.customerCardNumber,
		Amount:          50000,
		PaymentMethod:   "visa",
		MerchantId:      int32(s.merchantID),
		TransactionTime: timestamppb.New(time.Now()),
		IdempotencyKey:  "transaction-graphql-seed-1",
	})
	s.Require().NoError(err)
	s.Require().NotNil(res.Data)
	s.transactionID = int(res.Data.Id)
	s.Require().NotZero(s.transactionID)

	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(950000), customerSaldo.TotalBalance)
	merchantSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.merchantCardNumber)
	s.Equal(int64(50000), merchantSaldo.TotalBalance)
}

func (s *TransactionGraphQLTestSuite) Test2_FindByIdTransaction() {
	s.Require().NotZero(s.transactionID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdTransaction", findByIdTransactionQuery,
		map[string]interface{}{"input": map[string]interface{}{"transaction_id": s.transactionID}})
	s.Equal("success", found["status"])
	s.Equal(s.transactionID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *TransactionGraphQLTestSuite) Test3_FindAllTransaction() {
	found := tests.GraphQLOp(s.T(), s.handler, "findAllTransaction", findAllTransactionQuery,
		map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *TransactionGraphQLTestSuite) Test4_FindTransactionByMerchantId() {
	found := tests.GraphQLOp(s.T(), s.handler, "findTransactionByMerchantId", findTransactionByMerchantIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"merchant_id": s.merchantID}})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *TransactionGraphQLTestSuite) Test5_FindByActiveTransaction() {
	found := tests.GraphQLOp(s.T(), s.handler, "findByActiveTransaction", findByActiveTransactionQuery,
		map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}})
	s.Equal("success", found["status"])
	s.NotEmpty(tests.GQLFieldList(found, "data"))
}

func (s *TransactionGraphQLTestSuite) Test6_FindByTrashedTransaction() {
	found := tests.GraphQLOp(s.T(), s.handler, "findByTrashedTransaction", findByTrashedTransactionQuery,
		map[string]interface{}{"input": map[string]interface{}{"page": 1, "page_size": 10}})
	s.Equal("success", found["status"])
}

func (s *TransactionGraphQLTestSuite) Test7_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllTransaction", restoreAllTransactionQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllTransactionPermanent", deleteAllTransactionPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestTransactionGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(TransactionGraphQLTestSuite))
}
