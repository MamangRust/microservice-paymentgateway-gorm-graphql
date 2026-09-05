package seeder

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/hash"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"gorm.io/gorm"
)

// Deps is a struct that contains the dependencies for the seeder
type Deps struct {
	DB     *gorm.DB
	Hash   hash.HashPassword
	Ctx    context.Context
	Logger logger.LoggerInterface
}

// Seeder is a struct that contains all the seeders
type Seeder struct {
	User        *userSeeder
	Role        *roleSeeder
	Saldo       *saldoSeeder
	Topup       *topupSeeder
	Withdraw    *withdrawSeeder
	Transfer    *transferSeeder
	Merchant    *merchantSeeder
	Card        *cardSeeder
	Transaction *transactionSeeder
}

// NewSeeder initializes and returns the Seeder.
func NewSeeder(deps Deps) *Seeder {
	return &Seeder{
		User:        NewUserSeeder(deps.DB, deps.Ctx, deps.Hash, deps.Logger),
		Role:        NewRoleSeeder(deps.DB, deps.Ctx, deps.Logger),
		Saldo:       NewSaldoSeeder(deps.DB, deps.Ctx, deps.Logger),
		Topup:       NewTopupSeeder(deps.DB, deps.Ctx, deps.Logger),
		Withdraw:    NewWithdrawSeeder(deps.DB, deps.Ctx, deps.Logger),
		Transfer:    NewTransferSeeder(deps.DB, deps.Ctx, deps.Logger),
		Merchant:    NewMerchantSeeder(deps.DB, deps.Ctx, deps.Logger),
		Card:        NewCardSeeder(deps.DB, deps.Ctx, deps.Logger),
		Transaction: NewTransactionSeeder(deps.DB, deps.Ctx, deps.Logger),
	}
}

// Run runs the seeders based on the SEED_ONLY environment variable.
func (s *Seeder) Run() error {
	seedOnly := os.Getenv("SEED_ONLY")
	shouldSeed := func(name string) bool {
		if seedOnly == "" || seedOnly == "all" {
			return true
		}
		parts := strings.Split(seedOnly, ",")
		for _, p := range parts {
			if strings.TrimSpace(p) == name {
				return true
			}
		}
		return false
	}

	if shouldSeed("roles") {
		if err := s.seedWithDelay("roles", s.Role.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("users") {
		if err := s.seedWithDelay("users", s.User.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("cards") {
		if err := s.seedWithDelay("cards", s.Card.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("saldo") {
		if err := s.seedWithDelay("saldo", s.Saldo.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("topups") {
		if err := s.seedWithDelay("topups", s.Topup.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("withdrawals") {
		if err := s.seedWithDelay("withdrawals", s.Withdraw.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("transfers") {
		if err := s.seedWithDelay("transfers", s.Transfer.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("merchants") {
		if err := s.seedWithDelay("merchants", s.Merchant.Seed); err != nil {
			return err
		}
	}

	if shouldSeed("transactions") {
		if err := s.seedWithDelay("transactions", s.Transaction.Seed); err != nil {
			return err
		}
	}

	return nil
}

func (s *Seeder) seedWithDelay(entityName string, seedFunc func() error) error {
	if err := seedFunc(); err != nil {
		return fmt.Errorf("failed to seed %s: %w", entityName, err)
	}

	time.Sleep(30 * time.Second)
	return nil
}
