package auth_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/auth"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	testhelper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/testhelper"
	authhandler "github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/auth/service"
	tests "github.com/MamangRust/microservice-payment-gateway-test"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// AuthGraphqlHandlerTestSuite exercises the apigateway GraphQL layer against a
// real auth gRPC backend via the generated gqlgen schema.
type AuthGraphqlHandlerTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	redisClient *redis.Client
	grpcServer  *grpc.Server
	conn        *grpc.ClientConn
	graph       http.Handler
	email       string
	password    string
	accessToken string
	userID      int
}

func (s *AuthGraphqlHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	s.Require().NoError(s.ts.RunMigrations("user", "role", "auth"))

	gormDB, err := s.ts.GormDB()
	s.Require().NoError(err)

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	repos := repository.NewRepositories(&repository.RepositoriesDeps{
		DB:                gormDB,
		UserQueryClient:   s.ts.UserQueryClient,
		UserCommandClient: s.ts.UserCommandClient,
		RoleQueryClient:   s.ts.RoleQueryClient,
		RoleCommandClient: s.ts.RoleCommandClient,
	})

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

	h := authhandler.NewAuthHandleGrpc(svc, s.ts.Logger)

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

	conns := &testhelper.ServiceConnections{
		AuthClient:        conn,
		RoleClient:        testhelper.CreateDummyConn(),
		UserClient:        testhelper.CreateDummyConn(),
		CardClient:        testhelper.CreateDummyConn(),
		MerchantClient:    testhelper.CreateDummyConn(),
		SaldoClient:       testhelper.CreateDummyConn(),
		TopupClient:       testhelper.CreateDummyConn(),
		TransactionClient: testhelper.CreateDummyConn(),
		TransferClient:    testhelper.CreateDummyConn(),
		WithdrawClient:    testhelper.CreateDummyConn(),
		StatsReaderClient: testhelper.CreateDummyConn(),
	}

	resolver := testhelper.NewResolverWithRedis(conns, s.ts.Logger, s.redisClient)
	// getMe reads the user ID from the request context (set by the production
	// JWT middleware), so mimic that with a dynamic context middleware.
	s.graph = testhelper.NewDynamicUserIDContextMiddleware(func() int { return s.userID }, testhelper.NewGraphQLHTTPHandler(resolver))

	s.email = "auth.handler.graphql.test@example.com"
	s.password = "password123"

	// Seed ROLE_ADMIN used during registration.
	_, _ = s.ts.SeedRole(gormDB, "ROLE_ADMIN")
}

func (s *AuthGraphqlHandlerTestSuite) TearDownSuite() {
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

func (s *AuthGraphqlHandlerTestSuite) graphql(query string, variables map[string]interface{}) map[string]interface{} {
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

func (s *AuthGraphqlHandlerTestSuite) dataField(res map[string]interface{}, key string) map[string]interface{} {
	data, ok := res["data"].(map[string]interface{})
	s.Require().True(ok, "expected data object in response: %v", res)
	field, ok := data[key].(map[string]interface{})
	s.Require().True(ok, "expected %q in response data: %v", key, res)
	return field
}

func (s *AuthGraphqlHandlerTestSuite) assertSuccess(field map[string]interface{}) {
	s.Equal("success", field["status"], field["message"])
}

func (s *AuthGraphqlHandlerTestSuite) Test1_Register() {
	field := s.dataField(s.graphql(`
		mutation($input: RegisterInput!) {
			registerUser(input: $input) {
				status
				message
				data { id email }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Auth",
			"lastname":         "GraphQL",
			"email":            s.email,
			"password":         s.password,
			"confirm_password": s.password,
		},
	}), "registerUser")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	s.Equal(s.email, data["email"])
	s.userID = int(data["id"].(float64))
}

func (s *AuthGraphqlHandlerTestSuite) Test2_Login() {
	field := s.dataField(s.graphql(`
		mutation($input: LoginInput!) {
			loginUser(input: $input) {
				status
				message
				data { access_token refresh_token }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"email":    s.email,
			"password": s.password,
		},
	}), "loginUser")

	s.assertSuccess(field)
	data := field["data"].(map[string]interface{})
	token, ok := data["access_token"].(string)
	s.Require().True(ok)
	s.NotEmpty(token)
	s.accessToken = token
}

func (s *AuthGraphqlHandlerTestSuite) Test3_GetMe() {
	s.Require().NotEmpty(s.accessToken)

	field := s.dataField(s.graphql(`
		query($input: GetMeInput!) {
			getMe(input: $input) {
				status
				message
				data { id email }
			}
		}`, map[string]interface{}{
		"input": map[string]interface{}{"access_token": s.accessToken},
	}), "getMe")

	s.assertSuccess(field)
	s.Equal(s.email, field["data"].(map[string]interface{})["email"])
}

func (s *AuthGraphqlHandlerTestSuite) Test4_LoginLockout() {
	email := "locked.graphql@example.com"

	// Register user first.
	reg := s.dataField(s.graphql(`
		mutation($input: RegisterInput!) {
			registerUser(input: $input) { status }
		}`, map[string]interface{}{
		"input": map[string]interface{}{
			"firstname":        "Locked",
			"lastname":         "GraphQL",
			"email":            email,
			"password":         "correctpassword",
			"confirm_password": "correctpassword",
		},
	}), "registerUser")
	s.assertSuccess(reg)

	loginQuery := `
		mutation($input: LoginInput!) {
			loginUser(input: $input) {
				status
				message
				data { access_token }
			}
		}`

	// Fail login 5 times (total 5): each attempt returns a GraphQL error.
	for i := 0; i < 5; i++ {
		res := s.graphql(loginQuery, map[string]interface{}{
			"input": map[string]interface{}{
				"email":    email,
				"password": "wrongpassword",
			},
		})
		s.NotEmpty(res["errors"], "expected error on failed login attempt %d", i+1)
	}

	// 6th attempt must report the account as locked.
	res := s.graphql(loginQuery, map[string]interface{}{
		"input": map[string]interface{}{
			"email":    email,
			"password": "wrongpassword",
		},
	})
	errs, ok := res["errors"].([]interface{})
	s.Require().True(ok && len(errs) > 0, "expected lockout error on 6th attempt: %v", res)

	firstErr, _ := errs[0].(map[string]interface{})
	msg, _ := firstErr["message"].(string)
	s.True(strings.Contains(msg, "Account temporarily locked"), "unexpected error message: %s", msg)
}

func TestAuthGraphqlHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(AuthGraphqlHandlerTestSuite))
}
