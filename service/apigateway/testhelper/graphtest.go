package testhelper

import (
	"context"
	"net/http"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	mycontext "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/context"
	graph "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/handler"
	mencache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	sharedcachehelpers "github.com/MamangRust/microservice-payment-gateway-grpc/shared/cache"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	"github.com/redis/go-redis/v9"
	"github.com/vektah/gqlparser/v2/ast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceConnections mirrors graph.ServiceConnections for test use.
type ServiceConnections = graph.ServiceConnections

// CreateDummyConn creates a lazy gRPC connection that will never actually connect.
func CreateDummyConn() *grpc.ClientConn {
	conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	return conn
}

// NewResolver creates a Resolver with the provided service connections.
func NewResolver(conns *ServiceConnections, log logger.LoggerInterface) *graph.Resolver {
	return NewResolverWithRedis(conns, log, nil)
}

// NewResolverWithRedis creates a Resolver with the provided service connections and Redis client.
func NewResolverWithRedis(conns *ServiceConnections, log logger.LoggerInterface, redisClient *redis.Client) *graph.Resolver {
	myMencache := mencache.NewCacheApiGateway(&mencache.Deps{
		Redis:  redisClient,
		Logger: log,
	})

	return graph.NewResolver(&graph.Deps{
		Clients:  conns,
		Logger:   log,
		Kafka:    nil,
		Mencache: myMencache,
	})
}

// NewGraphQLHTTPHandler creates an http.Handler from a gqlgen Resolver.
func NewGraphQLHTTPHandler(resolver *graph.Resolver) http.Handler {
	srv := handler.New(graph.NewExecutableSchema(graph.Config{
		Resolvers: resolver,
	}))

	srv.AddTransport(transport.POST{})
	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	srv.Use(extension.Introspection{})

	return srv
}

func newTestCaches(redisClient redis.UniversalClient, log logger.LoggerInterface) (mencache.MerchantCache, mencache.RoleCache) {
	metrics, _ := observability.NewCacheMetrics("test")
	store := sharedcachehelpers.NewCacheStore(redisClient, log, metrics)

	return mencache.NewMerchantCache(store), mencache.NewRoleCache(store)
}

// NewDynamicUserIDContextMiddleware mimics the production JWT auth middleware
// by injecting a user ID into the request context before each request. The ID
// is resolved lazily so tests can register/login first and set it afterwards.
func NewDynamicUserIDContextMiddleware(getUserID func() int, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := getUserID(); id != 0 {
			r = r.WithContext(mycontext.WithUserID(r.Context(), id))
		}
		next.ServeHTTP(w, r)
	})
}

// SeedMerchantCache writes an apiKey -> merchantID mapping into the apigateway
// merchant permission cache so ValidateMerchant can succeed without Kafka.
func SeedMerchantCache(redisClient redis.UniversalClient, log logger.LoggerInterface, apiKey string, merchantID string) error {
	merchantCache, _ := newTestCaches(redisClient, log)
	merchantCache.SetMerchantCache(context.Background(), merchantID, apiKey)
	return nil
}

// SeedRoleCache writes a userID -> roles mapping into the apigateway role
// permission cache so ValidateRole can succeed without Kafka.
func SeedRoleCache(redisClient redis.UniversalClient, log logger.LoggerInterface, userID string, roles []string) error {
	_, roleCache := newTestCaches(redisClient, log)
	roleCache.SetRoleCache(context.Background(), userID, roles)
	return nil
}
