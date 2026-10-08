package role_test

import (
	"net"
	"net/http"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/role"
	userrolepb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/user_role"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/role/service"
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

type RoleGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	handler     http.Handler
	roleID      int
}

func (s *RoleGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user", "role"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	repos := repository.NewRepositories(gormDB)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	roleService := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       log,
		Cache:        cacheStore,
	})

	// Start the role gRPC server that backs the gateway.
	roleHandler := handler.NewHandler(roleService)
	server := grpc.NewServer()
	pb.RegisterRoleCommandServiceServer(server, roleHandler.RoleCommand)
	pb.RegisterRoleQueryServiceServer(server, roleHandler.RoleQuery)
	userrolepb.RegisterUserRoleServiceServer(server, roleHandler.UserRole)
	s.grpcServer = server

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.handler = tests.BuildGraphQLHandler(log, s.redisClient, &testhelper.ServiceConnections{
		RoleClient: conn,
	})
}

func (s *RoleGraphQLTestSuite) TearDownSuite() {
	s.conn.Close()
	s.grpcServer.Stop()
	s.redisClient.Close()
	s.ts.Teardown()
}

const createRoleQuery = `mutation CreateRole($input: CreateRoleInput!) {
  createRole(input: $input) { status message data { id name } }
}`

const findRoleByIdQuery = `query FindByIdRole($input: FindByIdRoleInput!) {
  findByIdRole(input: $input) { status message data { id name } }
}`

const updateRoleQuery = `mutation UpdateRole($input: UpdateRoleInput!) {
  updateRole(input: $input) { status message data { id name } }
}`

const restoreAllRolesQuery = `mutation RestoreAllRoles { restoreAllRole { status message } }`

const deleteAllRolesPermanentQuery = `mutation DeleteAllRolesPermanent { deleteAllRolePermanent { status message } }`

func (s *RoleGraphQLTestSuite) Test1_CreateRole() {
	created := tests.GraphQLOp(s.T(), s.handler, "createRole", createRoleQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"name": "GraphQL Role",
		},
	})
	s.Equal("success", created["status"])
	s.Equal("GraphQL Role", tests.GQLData(created)["name"])
	s.roleID = tests.GQLID(tests.GQLData(created), "id")
	s.Require().NotZero(s.roleID)
}

func (s *RoleGraphQLTestSuite) Test2_FindRoleById() {
	s.Require().NotZero(s.roleID)

	found := tests.GraphQLOp(s.T(), s.handler, "findByIdRole", findRoleByIdQuery,
		map[string]interface{}{"input": map[string]interface{}{"role_id": s.roleID}})
	s.Equal("success", found["status"])
	s.Equal(s.roleID, tests.GQLID(tests.GQLData(found), "id"))
}

func (s *RoleGraphQLTestSuite) Test3_UpdateRole() {
	s.Require().NotZero(s.roleID)

	updated := tests.GraphQLOp(s.T(), s.handler, "updateRole", updateRoleQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"id":   s.roleID,
			"name": "Updated GraphQL Role",
		},
	})
	s.Equal("success", updated["status"])
	s.Equal("Updated GraphQL Role", tests.GQLData(updated)["name"])
}

func (s *RoleGraphQLTestSuite) Test4_BulkOperations() {
	restored := tests.GraphQLOp(s.T(), s.handler, "restoreAllRole", restoreAllRolesQuery, nil)
	s.Equal("success", restored["status"])

	deleted := tests.GraphQLOp(s.T(), s.handler, "deleteAllRolePermanent", deleteAllRolesPermanentQuery, nil)
	s.Equal("success", deleted["status"])
}

func TestRoleGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(RoleGraphQLTestSuite))
}
