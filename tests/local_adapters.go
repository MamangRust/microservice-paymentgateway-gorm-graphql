package tests

import (
	"context"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/adapter"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/database/models"
	card_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/card/repository"
	merchant_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/merchant/repository"
	saldo_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/saldo/repository"
	user_repo "github.com/MamangRust/microservice-payment-gateway-grpc/service/user/repository"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/requests"
)

// The adapters below satisfy the pkg/adapter contracts by delegating straight to
// the owning service's repository. They let a service suite run without dialing
// its dependencies over gRPC. They live here (not in pkg/adapter) so the shared
// package does not have to import service/*/repository.

type localUserAdapter struct {
	repo user_repo.UserQueryRepository
}

func newLocalUserAdapter(repo user_repo.UserQueryRepository) adapter.UserAdapter {
	return &localUserAdapter{repo: repo}
}

func (a *localUserAdapter) FindById(ctx context.Context, userID int) (*models.User, error) {
	return a.repo.FindById(ctx, userID)
}

type localCardAdapter struct {
	queryRepo   card_repo.CardQueryRepository
	commandRepo card_repo.CardCommandRepository
}

func newLocalCardAdapter(queryRepo card_repo.CardQueryRepository, commandRepo card_repo.CardCommandRepository) adapter.CardAdapter {
	return &localCardAdapter{queryRepo: queryRepo, commandRepo: commandRepo}
}

func (a *localCardAdapter) FindCardByUserId(ctx context.Context, user_id int) (*models.Card, error) {
	return a.queryRepo.FindCardByUserId(ctx, user_id)
}

func (a *localCardAdapter) FindUserCardByCardNumber(ctx context.Context, card_number string) (*models.Card, error) {
	return a.queryRepo.FindUserCardByCardNumber(ctx, card_number)
}

func (a *localCardAdapter) FindCardByCardNumber(ctx context.Context, card_number string) (*models.Card, error) {
	return a.queryRepo.FindCardByCardNumber(ctx, card_number)
}

func (a *localCardAdapter) UpdateCard(ctx context.Context, request *requests.UpdateCardRequest) (*models.Card, error) {
	return a.commandRepo.UpdateCard(ctx, request)
}

type localSaldoAdapter struct {
	repo saldo_repo.Repositories
}

func newLocalSaldoAdapter(repo saldo_repo.Repositories) adapter.SaldoAdapter {
	return &localSaldoAdapter{repo: repo}
}

func (a *localSaldoAdapter) FindByCardNumber(ctx context.Context, card_number string) (*models.Saldo, error) {
	return a.repo.FindByCardNumber(ctx, card_number)
}

func (a *localSaldoAdapter) UpdateSaldoBalance(ctx context.Context, request *requests.UpdateSaldoBalance) (*models.SaldoMutationResult, error) {
	return a.repo.UpdateSaldoBalance(ctx, request)
}

func (a *localSaldoAdapter) DebitSaldo(ctx context.Context, request *requests.DebitSaldoRequest) (*models.SaldoMutationResult, error) {
	return a.repo.DebitSaldo(ctx, request)
}

func (a *localSaldoAdapter) CreditSaldo(ctx context.Context, request *requests.CreditSaldoRequest) (*models.SaldoMutationResult, error) {
	return a.repo.CreditSaldo(ctx, request)
}

func (a *localSaldoAdapter) UpdateSaldoWithdraw(ctx context.Context, request *requests.UpdateSaldoWithdraw) (*models.SaldoMutationResult, error) {
	return a.repo.UpdateSaldoWithdraw(ctx, request)
}

type localMerchantAdapter struct {
	repo merchant_repo.MerchantQueryRepository
}

func newLocalMerchantAdapter(repo merchant_repo.MerchantQueryRepository) adapter.MerchantAdapter {
	return &localMerchantAdapter{repo: repo}
}

func (a *localMerchantAdapter) FindByApiKey(ctx context.Context, api_key string) (*models.Merchant, error) {
	return a.repo.FindByApiKey(ctx, api_key)
}

func (a *localMerchantAdapter) FindByMerchantId(ctx context.Context, merchant_id int) (*models.Merchant, error) {
	return a.repo.FindByMerchantId(ctx, merchant_id)
}
