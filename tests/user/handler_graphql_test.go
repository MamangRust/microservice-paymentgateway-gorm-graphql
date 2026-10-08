package user_test

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/user"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	gapi "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/user/service"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	tests "github.com/MamangRust/microservice-payment-gateway-test"
	"gorm.io/gorm"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UserGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	handler     http.Handler
	userID      int
	userEmail   string
}

func (s *UserGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	repos := repository.NewRepositories(&repository.Deps{Db: gormDB, RoleQueryClient: s.ts.RoleQueryClient, UserRoleClient: s.ts.UserRoleClient})

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	hasher := hash.NewHashingPassword()
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	userService := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       log,
		Hash:         hasher,
		Cache:        cacheStore,
	})

	// Start the user gRPC server that backs the gateway.
	userHandler := gapi.NewHandler(userService)
	server := grpc.NewServer()
	pb.RegisterUserQueryServiceServer(server, userHandler)
	pb.RegisterUserCommandServiceServer(server, userHandler)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		UserClient: conn,
	})
}

func (s *UserGraphQLTestSuite) TearDownSuite() {
	s.conn.Close()
	s.grpcServer.Stop()
	s.redisClient.Close()
	s.ts.Teardown()
}

const createUserQuery = `mutation CreateUser($input: CreateUserInput!) {
  createUser(input: $input) { status message data { id firstname lastname email } }
}`

const findUserByIdQuery = `query FindUserById($input: FindByIdUserInput!) {
  findByIdUser(input: $input) { status message data { id email } }
}`

const updateUserQuery = `mutation UpdateUser($input: UpdateUserInput!) {
  updateUser(input: $input) { status message data { id firstname email } }
}`

const deleteUserPermanentQuery = `mutation DeleteUserPermanent($input: FindByIdUserInput!) {
  deleteUserPermanent(input: $input) { status message }
}`

const restoreAllUsersQuery = `mutation RestoreAllUsers { restoreAllUser { status message } }`

const deleteAllUsersPermanentQuery = `mutation DeleteAllUsersPermanent { deleteAllUserPermanent { status message } }`

func (s *UserGraphQLTestSuite) Test1_CreateUser() {
	s.userEmail = fmt.Sprintf("handler.user.%d@example.com", time.Now().UnixNano())

	created := tests.GraphQLOp(s.T(), s.handler, "createUser", createUserQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Handler",
			"lastname":         "User",
			"email":            s.userEmail,
			"password":         "password123",
			"confirm_password": "password123",
		},
	})
	s.Equal("success", created["status"])
	s.userID = tests.GQLID(tests.GQLData(created), "id")
	s.Require().NotZero(s.userID)
}

func (s *UserGraphQLTestSuite) Test2_FindUserById() {
	s.Require().NotZero(s.userID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdUser", findUserByIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"id": s.userID}})
	s.Equal("success", found["status"])
	s.Equal(s.userID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *UserGraphQLTestSuite) Test3_UpdateUser() {
	s.Require().NotZero(s.userID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateUser", updateUserQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"id":               s.userID,
			"firstname":        "Updated",
			"lastname":         "User",
			"email":            s.userEmail,
			"password":         "password123",
			"confirm_password": "password123",
		},
	})
	s.Equal("success", updated["status"])
	s.Equal("Updated", tests.GQLData(updated)["firstname"])
}

func (s *UserGraphQLTestSuite) Test4_DeleteUserPermanent() {
	s.Require().NotZero(s.userID)

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteUserPermanent", deleteUserPermanentQuery,
		map[string]interface{}{"input": map[string]interface{}{"id": s.userID}})
	s.Equal("success", deleted["status"])
}

func (s *UserGraphQLTestSuite) Test5_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllUser", restoreAllUsersQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllUserPermanent", deleteAllUsersPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestUserGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(UserGraphQLTestSuite))
}
