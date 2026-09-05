package graph

import (
	errorstd "errors"
	"time"

	"fmt"

	authgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/auth"
	cardgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/card"
	merchantgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/merchant"
	merchantdocumentgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/merchant_document"
	rolegraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/role"
	saldographqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/saldo"
	topupgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/topup"
	transactiongraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/transaction"
	transfergraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/transfer"
	usergraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/user"
	withdrawgraphqlmapper "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/mapper/withdraw"
	merchantpermission "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/permission/merchant"
	rolepermission "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/permission/role"
	mencache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis"
	auth_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/auth"
	card_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/card"
	merchant_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/merchant"
	merchant_document_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/merchant_document"
	role_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/role"
	saldo_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/saldo"
	topup_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/topup"
	transaction_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/transaction"
	transfer_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/transfer"
	user_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/user"
	withdraw_cache "github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/redis/api/withdraw"
	authpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb"
	cardpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	merchantpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
	merchantdocumentpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant_document"
	rolepb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/role"
	saldopb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/saldo"
	topuppb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/topup"
	transactionpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transaction"
	transferpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transfer"
	userpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/user"
	withdrawpb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/withdraw"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/kafka"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/errors"
	sharedErrors "github.com/MamangRust/microservice-payment-gateway-grpc/shared/errors"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/observability"
	"github.com/go-playground/validator/v10"
	"google.golang.org/grpc"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.

type Resolver struct {
	AuthGraphql             AuthHandleGraphql
	RoleGraphql             RoleHandleGraphql
	UserGraphql             UserHandleGraphql
	CardGraphql             CardHandleGraphql
	MerchantGraphql         MerchantHandleGraphql
	MerchantDocumentGraphql MerchantDocumentHandleGraphql
	SaldoGraphql            SaldoHandleGraphql
	TopupGraphql            TopupHandleGraphql
	TransactionGraphql      TransactionHandleGraphql
	TransferGraphql         TransferHandleGraphql
	WithdrawGraphql         WithdrawHandleGraphql
	ResolverHandle          *resolverHandler
	StatsRead               StatsReadHandleGraphql
}

type UserClient struct {
	UserQueryClient   userpb.UserQueryServiceClient
	UserCommandClient userpb.UserCommandServiceClient
}

type RoleClient struct {
	RoleQueryClient   rolepb.RoleQueryServiceClient
	RoleCommandClient rolepb.RoleCommandServiceClient
}

type CardClient struct {
	CardQueryClient                  cardpb.CardQueryServiceClient
	CardCommandClient                cardpb.CardCommandServiceClient
	CardDashboardClient              cardpb.CardDashboardServiceClient
	CardStatsBalanceClient           statspb.CardStatsBalanceServiceClient
	CardStatsTopupAmountClient       statspb.CardStatsTopupServiceClient
	CardStatsTransactionAmountClient statspb.CardStatsTransactionServiceClient
	CardStatsWithdrawAmountClient    statspb.CardStatsWithdrawServiceClient
	CardStatsTransferAmountClient    statspb.
						CardStatsTransferServiceClient
}

type MerchantClient struct {
	MerchantQuery            merchantpb.MerchantQueryServiceClient
	MerchantCommand          merchantpb.MerchantCommandServiceClient
	MerchantTransaction      merchantpb.MerchantTransactionServiceClient
	MerchantStatsAmount      statspb.MerchantStatsAmountServiceClient
	MerchantStatsTotalAmount statspb.MerchantStatsTotalAmountServiceClient
	MerchantStatsMethod      statspb.MerchantStatsMethodServiceClient
}

type MerchantDocumentClient struct {
	MerchantDocumentQueryClient   merchantdocumentpb.MerchantDocumentQueryServiceClient
	MerchantDocumentCommandClient merchantdocumentpb.MerchantDocumentCommandServiceClient
}

type SaldoClient struct {
	SaldoQueryClient             saldopb.SaldoQueryServiceClient
	SaldoCommandClient           saldopb.SaldoCommandServiceClient
	SaldoStatsBalanceClient      statspb.SaldoStatsBalanceServiceClient
	SaldoStatsTotalBalanceClient statspb.SaldoStatsTotalBalanceClient
}

type TopupClient struct {
	TopupQueryClient   topuppb.TopupQueryServiceClient
	TopupCommandClient topuppb.TopupCommandServiceClient
	TopupStatsAmount   statspb.TopupStatsAmountServiceClient
	TopupStatsMethod   statspb.TopupStatsMethodServiceClient
	TopupStatsStatus   statspb.TopupStatsStatusServiceClient
}

type TransactionClient struct {
	TransactionQueryClient   transactionpb.TransactionQueryServiceClient
	TransactionCommandClient transactionpb.TransactionCommandServiceClient
	TransactionStatsAmount   statspb.TransactionStatsAmountServiceClient
	TransactionStatsMethod   statspb.TransactionStatsMethodServiceClient
	TransactionStatsStatus   statspb.TransactionStatsStatusServiceClient
}

type TransferClient struct {
	TransferQueryClient   transferpb.TransferQueryServiceClient
	TransferCommandClient transferpb.TransferCommandServiceClient
	TransferStatsAmount   statspb.TransferStatsAmountServiceClient
	TransferStatsStatus   statspb.TransferStatsStatusServiceClient
}
type WithdrawClient struct {
	WithdrawQueryClient   withdrawpb.WithdrawQueryServiceClient
	WithdrawCommandClient withdrawpb.WithdrawCommandServiceClient
	WithdrawStatsAmount   statspb.WithdrawStatsAmountServiceClient
	WithdrawStatsStatus   statspb.WithdrawStatsStatusServiceClient
}

type AuthHandleGraphql struct {
	AuthClient authpb.AuthServiceClient
	Logger     logger.LoggerInterface
	Mapping    authgraphqlmapper.AuthGraphqlMapper
	Cache      auth_cache.AuthMencache
}

type RoleHandleGraphql struct {
	RoleClient RoleClient
	Logger     logger.LoggerInterface
	Mapping    rolegraphqlmapper.RoleGraphqlMapper
	Kafka      *kafka.Kafka
	CacheRole  mencache.RoleCache
	Permission rolepermission.RolePermission
	Cache      role_cache.RoleMencache
}

type UserHandleGraphql struct {
	UserClient UserClient
	Logger     logger.LoggerInterface
	Mapping    usergraphqlmapper.UserGraphqlMapper
	Cache      user_cache.UserMencache
}

type CardHandleGraphql struct {
	CardClient CardClient
	Logger     logger.LoggerInterface
	Mapping    cardgraphqlmapper.CardGraphqlMapper
	Cache      card_cache.CardMencache
}

type MerchantHandleGraphql struct {
	MerchantClient MerchantClient
	Logger         logger.LoggerInterface
	Mapping        merchantgraphqlmapper.MerchantGraphqlMapper
	Cache          merchant_cache.MerchantMencache
}

type MerchantDocumentHandleGraphql struct {
	MerchantClient MerchantDocumentClient
	Logger         logger.LoggerInterface
	Mapping        merchantdocumentgraphqlmapper.MerchantDocumentGraphqlMapper
	Cache          merchant_document_cache.MerchantDocumentMencache
}

type SaldoHandleGraphql struct {
	SaldoClient SaldoClient
	Logger      logger.LoggerInterface
	Mapping     saldographqlmapper.SaldoGraphqlMapper
	Cache       saldo_cache.SaldoMencache
}

type TopupHandleGraphql struct {
	TopupClient TopupClient
	Logger      logger.LoggerInterface
	Mapping     topupgraphqlmapper.TopupGraphqlMapper
	Cache       topup_cache.TopupMencach
}

type TransactionHandleGraphql struct {
	TransactionClient TransactionClient
	Logger            logger.LoggerInterface
	Mapping           transactiongraphqlmapper.TransactionGraphqlMapper
	Permission        merchantpermission.MerchantPermission
	CacheMerchant     mencache.MerchantCache
	Cache             transaction_cache.TransactionMencache
}

type TransferHandleGraphql struct {
	TransferClient TransferClient
	Logger         logger.LoggerInterface
	Mapping        transfergraphqlmapper.TransferGraphqlMapper
	Cache          transfer_cache.TransferMencache
}

type WithdrawHandleGraphql struct {
	WithdrawClient WithdrawClient
	Logger         logger.LoggerInterface
	Mapping        withdrawgraphqlmapper.WithdrawGraphqlMapper
	Cache          withdraw_cache.WithdrawMencache
}

type ServiceConnections struct {
	AuthClient        *grpc.ClientConn
	RoleClient        *grpc.ClientConn
	UserClient        *grpc.ClientConn
	CardClient        *grpc.ClientConn
	MerchantClient    *grpc.ClientConn
	SaldoClient       *grpc.ClientConn
	TopupClient       *grpc.ClientConn
	TransactionClient *grpc.ClientConn
	TransferClient    *grpc.ClientConn
	WithdrawClient    *grpc.ClientConn
	StatsReaderClient *grpc.ClientConn
}

type StatsReadHandleGraphql struct {
	CardStatsBalance            statspb.CardStatsBalanceServiceClient
	CardStatsTopup              statspb.CardStatsTopupServiceClient
	CardStatsTransaction        statspb.CardStatsTransactionServiceClient
	CardStatsTransfer           statspb.CardStatsTransferServiceClient
	CardStatsWithdraw           statspb.CardStatsWithdrawServiceClient
	CardDashboard               cardpb.CardDashboardServiceClient
	TopupStatsAmount            statspb.TopupStatsAmountServiceClient
	TopupStatsMethod            statspb.TopupStatsMethodServiceClient
	TopupStatsStatus            statspb.TopupStatsStatusServiceClient
	WithdrawStatsAmount         statspb.WithdrawStatsAmountServiceClient
	WithdrawStatsStatus         statspb.WithdrawStatsStatusServiceClient
	TransactionStatsAmount      statspb.TransactionStatsAmountServiceClient
	TransactionStatsMethod      statspb.TransactionStatsMethodServiceClient
	TransactionStatsStatus      statspb.TransactionStatsStatusServiceClient
	TransferStatsAmount         statspb.TransferStatsAmountServiceClient
	TransferStatsStatus         statspb.TransferStatsStatusServiceClient
	MerchantStatsAmount         statspb.MerchantStatsAmountServiceClient
	MerchantStatsMethod         statspb.MerchantStatsMethodServiceClient
	MerchantStatsTotalAmount    statspb.MerchantStatsTotalAmountServiceClient
	MerchantTransaction         merchantpb.MerchantTransactionServiceClient
	SaldoStatsBalance           statspb.SaldoStatsBalanceServiceClient
	SaldoStatsTotal             statspb.SaldoStatsTotalBalanceClient
}

type Deps struct {
	Clients  *ServiceConnections
	Logger   logger.LoggerInterface
	Kafka    *kafka.Kafka
	Mencache mencache.CacheApiGateway
}

func NewResolver(
	deps *Deps,
) *Resolver {
	observability, _ := observability.NewObservability(
		"graphql-client",
		deps.Logger,
	)

	resolverHandle := NewResolverHandler(observability, deps.Logger)

	store := deps.Mencache.GetStore()
	cacheAuth := auth_cache.NewMencache(store)
	cacheUser := user_cache.NewUserMencache(store)
	cacheRole := role_cache.NewRoleMencache(store)
	cacheSaldo := saldo_cache.NewSaldoMencache(store)
	cacheTopup := topup_cache.NewTopupMencache(store)
	cacheTransaction := transaction_cache.NewTransactionMencache(store)
	cacheTransfer := transfer_cache.NewTransferMencache(store)
	cacheWithdraw := withdraw_cache.NewWithdrawMencache(store)
	cacheMerchant := merchant_cache.NewMerchantMencache(store)
	cacheCard := card_cache.NewCardMencache(store)
	cacheMerchantDocument := merchant_document_cache.NewMerchantDocumentMencache(store)

	result := &Resolver{
		ResolverHandle: resolverHandle,
		AuthGraphql: AuthHandleGraphql{
			AuthClient: authpb.NewAuthServiceClient(deps.Clients.AuthClient),
			Logger:     deps.Logger,
			Mapping:    authgraphqlmapper.NewAuthGraphqlMapper(),
			Cache:      cacheAuth,
		},
		RoleGraphql: RoleHandleGraphql{
			RoleClient: RoleClient{
				RoleQueryClient:   rolepb.NewRoleQueryServiceClient(deps.Clients.RoleClient),
				RoleCommandClient: rolepb.NewRoleCommandServiceClient(deps.Clients.RoleClient),
			},
			Kafka:      deps.Kafka,
			Logger:     deps.Logger,
			Mapping:    rolegraphqlmapper.NewRoleGraphqlMapper(),
			Permission: rolepermission.NewRolePermission(deps.Kafka, "request-role", "response-role", 5*time.Second, deps.Logger, deps.Mencache),
			Cache:      cacheRole,
		},
		UserGraphql: UserHandleGraphql{
			UserClient: UserClient{
				UserQueryClient:   userpb.NewUserQueryServiceClient(deps.Clients.UserClient),
				UserCommandClient: userpb.NewUserCommandServiceClient(deps.Clients.UserClient),
			},
			Logger:  deps.Logger,
			Mapping: usergraphqlmapper.NewUserGraphqlMapper(),
			Cache:   cacheUser,
		},
		CardGraphql: CardHandleGraphql{
			CardClient: CardClient{
				CardQueryClient:                  cardpb.NewCardQueryServiceClient(deps.Clients.CardClient),
				CardCommandClient:                cardpb.NewCardCommandServiceClient(deps.Clients.CardClient),
				CardDashboardClient:              cardpb.NewCardDashboardServiceClient(deps.Clients.CardClient),
				CardStatsBalanceClient:           statspb.NewCardStatsBalanceServiceClient(deps.Clients.CardClient),
				CardStatsTopupAmountClient:       statspb.NewCardStatsTopupServiceClient(deps.Clients.CardClient),
				CardStatsTransactionAmountClient: statspb.NewCardStatsTransactionServiceClient(deps.Clients.CardClient),
				CardStatsWithdrawAmountClient:    statspb.NewCardStatsWithdrawServiceClient(deps.Clients.CardClient),
				CardStatsTransferAmountClient:    statspb.NewCardStatsTransferServiceClient(deps.Clients.CardClient),
			},
			Logger:  deps.Logger,
			Mapping: cardgraphqlmapper.NewCardResponseMapper(),
			Cache:   cacheCard,
		},
		MerchantGraphql: MerchantHandleGraphql{
			MerchantClient: MerchantClient{
				MerchantQuery:            merchantpb.NewMerchantQueryServiceClient(deps.Clients.MerchantClient),
				MerchantCommand:          merchantpb.NewMerchantCommandServiceClient(deps.Clients.MerchantClient),
				MerchantTransaction:      merchantpb.NewMerchantTransactionServiceClient(deps.Clients.MerchantClient),
				MerchantStatsAmount:      statspb.NewMerchantStatsAmountServiceClient(deps.Clients.MerchantClient),
				MerchantStatsTotalAmount: statspb.NewMerchantStatsTotalAmountServiceClient(deps.Clients.MerchantClient),
				MerchantStatsMethod:      statspb.NewMerchantStatsMethodServiceClient(deps.Clients.MerchantClient),
			},
			Logger:  deps.Logger,
			Mapping: merchantgraphqlmapper.NewMerchantResponseMapper(),
			Cache:   cacheMerchant,
		},
		MerchantDocumentGraphql: MerchantDocumentHandleGraphql{
			MerchantClient: MerchantDocumentClient{
				MerchantDocumentQueryClient:   merchantdocumentpb.NewMerchantDocumentQueryServiceClient(deps.Clients.MerchantClient),
				MerchantDocumentCommandClient: merchantdocumentpb.NewMerchantDocumentCommandServiceClient(deps.Clients.MerchantClient),
			},
			Logger:  deps.Logger,
			Mapping: merchantdocumentgraphqlmapper.NewMerchantDocumentGraphqlMapper(),
			Cache:   cacheMerchantDocument,
		},
		SaldoGraphql: SaldoHandleGraphql{
			SaldoClient: SaldoClient{
				SaldoQueryClient:             saldopb.NewSaldoQueryServiceClient(deps.Clients.SaldoClient),
				SaldoCommandClient:           saldopb.NewSaldoCommandServiceClient(deps.Clients.SaldoClient),
				SaldoStatsBalanceClient:      statspb.NewSaldoStatsBalanceServiceClient(deps.Clients.SaldoClient),
				SaldoStatsTotalBalanceClient: statspb.NewSaldoStatsTotalBalanceClient(deps.Clients.SaldoClient),
			},
			Logger:  deps.Logger,
			Mapping: saldographqlmapper.NewSaldoGraphqlMapper(),
			Cache:   cacheSaldo,
		},
		TopupGraphql: TopupHandleGraphql{
			TopupClient: TopupClient{
				TopupQueryClient:   topuppb.NewTopupQueryServiceClient(deps.Clients.TopupClient),
				TopupCommandClient: topuppb.NewTopupCommandServiceClient(deps.Clients.TopupClient),
				TopupStatsAmount:   statspb.NewTopupStatsAmountServiceClient(deps.Clients.TopupClient),
				TopupStatsMethod:   statspb.NewTopupStatsMethodServiceClient(deps.Clients.TopupClient),
				TopupStatsStatus:   statspb.NewTopupStatsStatusServiceClient(deps.Clients.TopupClient),
			},
			Logger:  deps.Logger,
			Mapping: topupgraphqlmapper.NewTopupGraphqlMapper(),
			Cache:   cacheTopup,
		},
		TransactionGraphql: TransactionHandleGraphql{
			TransactionClient: TransactionClient{
				TransactionQueryClient:   transactionpb.NewTransactionQueryServiceClient(deps.Clients.TransactionClient),
				TransactionCommandClient: transactionpb.NewTransactionCommandServiceClient(deps.Clients.TransactionClient),
				TransactionStatsAmount:   statspb.NewTransactionStatsAmountServiceClient(deps.Clients.TransactionClient),
				TransactionStatsMethod:   statspb.NewTransactionStatsMethodServiceClient(deps.Clients.TransactionClient),
				TransactionStatsStatus:   statspb.NewTransactionStatsStatusServiceClient(deps.Clients.TransactionClient),
			},
			Logger:     deps.Logger,
			Mapping:    transactiongraphqlmapper.NewTransactionGraphqlMapper(),
			Permission: merchantpermission.NewMerchantPermission(deps.Kafka, "request-transaction", "response-transaction", 5*time.Second, deps.Logger, deps.Mencache),
			Cache:      cacheTransaction,
		},
		TransferGraphql: TransferHandleGraphql{
			TransferClient: TransferClient{
				TransferQueryClient:   transferpb.NewTransferQueryServiceClient(deps.Clients.TransferClient),
				TransferCommandClient: transferpb.NewTransferCommandServiceClient(deps.Clients.TransferClient),
				TransferStatsAmount:   statspb.NewTransferStatsAmountServiceClient(deps.Clients.TransferClient),
				TransferStatsStatus:   statspb.NewTransferStatsStatusServiceClient(deps.Clients.TransferClient),
			},
			Logger:  deps.Logger,
			Mapping: transfergraphqlmapper.NewTransferGraphqlMapper(),
			Cache:   cacheTransfer,
		},
		WithdrawGraphql: WithdrawHandleGraphql{
			WithdrawClient: WithdrawClient{
				WithdrawQueryClient:   withdrawpb.NewWithdrawQueryServiceClient(deps.Clients.WithdrawClient),
				WithdrawCommandClient: withdrawpb.NewWithdrawCommandServiceClient(deps.Clients.WithdrawClient),
				WithdrawStatsAmount:   statspb.NewWithdrawStatsAmountServiceClient(deps.Clients.WithdrawClient),
				WithdrawStatsStatus:   statspb.NewWithdrawStatsStatusServiceClient(deps.Clients.WithdrawClient),
			},
			Logger:  deps.Logger,
			Mapping: withdrawgraphqlmapper.NewWithdrawGraphqlMapper(),			Cache:        cacheWithdraw,
		},
	}

	// Stats reader clients — all wired to the single StatsReaderClient conn.
	sr := deps.Clients.StatsReaderClient
	result.StatsRead = StatsReadHandleGraphql{
		CardStatsBalance:            statspb.NewCardStatsBalanceServiceClient(sr),
		CardStatsTopup:              statspb.NewCardStatsTopupServiceClient(sr),
		CardStatsTransaction:        statspb.NewCardStatsTransactionServiceClient(sr),
		CardStatsTransfer:           statspb.NewCardStatsTransferServiceClient(sr),
		CardStatsWithdraw:           statspb.NewCardStatsWithdrawServiceClient(sr),
		CardDashboard:               cardpb.NewCardDashboardServiceClient(sr),
		TopupStatsAmount:            statspb.NewTopupStatsAmountServiceClient(sr),
		TopupStatsMethod:            statspb.NewTopupStatsMethodServiceClient(sr),
		TopupStatsStatus:            statspb.NewTopupStatsStatusServiceClient(sr),
		WithdrawStatsAmount:         statspb.NewWithdrawStatsAmountServiceClient(sr),
		WithdrawStatsStatus:         statspb.NewWithdrawStatsStatusServiceClient(sr),
		TransactionStatsAmount:      statspb.NewTransactionStatsAmountServiceClient(sr),
		TransactionStatsMethod:      statspb.NewTransactionStatsMethodServiceClient(sr),
		TransactionStatsStatus:      statspb.NewTransactionStatsStatusServiceClient(sr),
		TransferStatsAmount:         statspb.NewTransferStatsAmountServiceClient(sr),
		TransferStatsStatus:         statspb.NewTransferStatsStatusServiceClient(sr),
		MerchantStatsAmount:         statspb.NewMerchantStatsAmountServiceClient(sr),
		MerchantStatsMethod:         statspb.NewMerchantStatsMethodServiceClient(sr),
		MerchantStatsTotalAmount:    statspb.NewMerchantStatsTotalAmountServiceClient(sr),
		MerchantTransaction:         merchantpb.NewMerchantTransactionServiceClient(sr),
		SaldoStatsBalance:           statspb.NewSaldoStatsBalanceServiceClient(sr),
		SaldoStatsTotal:             statspb.NewSaldoStatsTotalBalanceClient(sr),
	}

	return result
}

func (h *Resolver) handleGraphQLError(err error, operation string) *errors.AppError {
	if err == nil {
		return nil
	}

	var appErr *errors.AppError
	if errorstd.As(err, &appErr) {
		return appErr
	}

	return errors.NewInternalError(err).WithMessage("Failed to " + operation)
}

func (r *Resolver) parseValidationErrors(err error) []sharedErrors.ValidationError {
	var validationErrs []sharedErrors.ValidationError

	if ve, ok := err.(validator.ValidationErrors); ok {
		for _, fe := range ve {
			validationErrs = append(validationErrs, sharedErrors.ValidationError{
				Field:   fe.Field(),
				Message: r.getValidationMessage(fe),
			})
		}
		return validationErrs
	}

	return []sharedErrors.ValidationError{
		{
			Field:   "general",
			Message: err.Error(),
		},
	}
}

func (r *Resolver) getValidationMessage(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "This field is required"
	case "email":
		return "Invalid email format"
	case "min":
		return fmt.Sprintf("Must be at least %s", fe.Param())
	case "max":
		return fmt.Sprintf("Must be at most %s", fe.Param())
	case "gte":
		return fmt.Sprintf("Must be greater than or equal to %s", fe.Param())
	case "lte":
		return fmt.Sprintf("Must be less than or equal to %s", fe.Param())
	case "oneof":
		return fmt.Sprintf("Must be one of: %s", fe.Param())
	default:
		return fmt.Sprintf("Validation failed on '%s' tag", fe.Tag())
	}
}
