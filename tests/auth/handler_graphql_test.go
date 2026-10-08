package auth_test

import (
	"net"
	"net/http"
	"strings"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/auth"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/service"
	tests "github.com/MamangRust/microservice-payment-gateway-test"
	"gorm.io/gorm"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AuthGraphQLTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	db          *gorm.DB
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	baseHandler http.Handler
	handler     http.Handler
	email       string
	password    string
	accessToken string
	userID      int
}

func (s *AuthGraphQLTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)
	s.db = gormDB

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	repos := repository.NewRepositories(
		gormDB,
		s.ts.UserQueryClient,
		s.ts.UserCommandClient,
		s.ts.RoleQueryClient,
		s.ts.UserRoleClient,
	)

	tokenManager, _ := auth.NewManager("mysecret")
	hasher := hash.NewHashingPassword()

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       s.ts.Logger,
		Cache:        s.ts.CacheStore,
		Token:        tokenManager,
		Hash:         hasher,
		Kafka:        nil,
	})

	h := handler.NewAuthHandleGrpc(svc, s.ts.Logger)

	s.grpcServer = grpc.NewServer()
	pb.RegisterAuthServiceServer(s.grpcServer, h)

	lis, err := net.Listen("tcp", "localhost:0")
	s.Require().NoError(err)

	go func() {
		_ = s.grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.baseHandler = tests.BuildGraphQLHandler(s.ts.Logger, s.redisClient, &testhelper.ServiceConnections{
		AuthClient: conn,
	})
	s.handler = s.baseHandler

	s.email = "auth.handler.graphql.test@example.com"
	s.password = "password123"

	// Seed ROLE_ADMIN
	_, _ = s.ts.SeedRole(gormDB, "ROLE_ADMIN")
}

func (s *AuthGraphQLTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.redisClient != nil {
		s.redisClient.Close()
	}
	s.ts.Teardown()
}

const registerQuery = `mutation RegisterUser($input: RegisterInput!) {
  registerUser(input: $input) { status message data { id firstname lastname email } }
}`

const loginQuery = `mutation LoginUser($input: LoginInput!) {
  loginUser(input: $input) { status message data { access_token refresh_token } }
}`

const getMeQuery = `query GetMe($input: GetMeInput!) {
  getMe(input: $input) { status message data { id email } }
}`

func (s *AuthGraphQLTestSuite) Test1_Register() {
	registered := tests.GraphQLOp(s.T(), s.handler, "registerUser", registerQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Auth",
			"lastname":         "GraphQL",
			"email":            s.email,
			"password":         s.password,
			"confirm_password": s.password,
		},
	})
	s.Equal("success", registered["status"])
	s.userID = tests.GQLID(tests.GQLData(registered), "id")
	s.Require().NotZero(s.userID)
}

func (s *AuthGraphQLTestSuite) Test2_Login() {
	s.Require().NotZero(s.userID)

	loggedIn := tests.GraphQLOp(s.T(), s.handler, "loginUser", loginQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"email":    s.email,
			"password": s.password,
		},
	})
	s.Equal("success", loggedIn["status"])
	s.accessToken, _ = tests.GQLData(loggedIn)["access_token"].(string)
	s.Require().NotEmpty(s.accessToken)
}

func (s *AuthGraphQLTestSuite) Test3_GetMe() {
	s.Require().NotZero(s.userID)
	s.Require().NotEmpty(s.accessToken)

	// GetMe reads the authenticated user from the request context, which the
	// production AuthMiddleware populates. Emulate that with WithUser.
	handler := testhelper.WithUser(s.baseHandler, s.userID)

	me := tests.GraphQLOp(s.T(), handler, "getMe", getMeQuery, map[string]interface{}{
		"input": map[string]interface{}{"access_token": s.accessToken},
	})
	s.Equal("success", me["status"])
	s.Equal(s.email, tests.GQLData(me)["email"])
}

func (s *AuthGraphQLTestSuite) Test4_LoginLockout() {
	email := "locked.graphql@example.com"

	registered := tests.GraphQLOp(s.T(), s.handler, "registerUser", registerQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Locked",
			"lastname":         "GraphQL",
			"email":            email,
			"password":         "correctpassword",
			"confirm_password": "correctpassword",
		},
	})
	s.Equal("success", registered["status"])

	loginVars := map[string]interface{}{
		"input": map[string]interface{}{
			"email":    email,
			"password": "wrongpassword",
		},
	}

	// Fail login 5 times.
	for i := 0; i < 5; i++ {
		resp := tests.GraphQLRaw(s.T(), s.handler, loginQuery, loginVars)
		s.NotEmpty(resp.Errors, "attempt %d should fail", i+1)
	}

	// 6th attempt should report the account as locked.
	resp := tests.GraphQLRaw(s.T(), s.handler, loginQuery, loginVars)
	s.Require().NotEmpty(resp.Errors)
	s.Contains(strings.ToLower(resp.Errors[0].Message), "lock")
}

func TestAuthGraphQLSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(AuthGraphQLTestSuite))
}
