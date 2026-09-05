package seeder

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/database/models"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type saldoSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewSaldoSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *saldoSeeder {
	return &saldoSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *saldoSeeder) Seed() error {
	totalSaldos := 100
	activeSaldos := 90

	var cards []models.Card
	for i := 1; i <= totalSaldos; i++ {
		var card models.Card
		err := r.db.WithContext(r.ctx).Where("user_id = ? AND deleted_at IS NULL", int32(i)).First(&card).Error
		if err != nil {
			r.logger.Error("failed to get card for user", zap.Int("userID", i), zap.Error(err))
			return fmt.Errorf("failed to get card for user %d: %w", i, err)
		}
		cards = append(cards, card)
	}

	if len(cards) < totalSaldos {
		r.logger.Error("not enough cards to seed saldo", zap.Int("required", totalSaldos), zap.Int("available", len(cards)))
		return fmt.Errorf("not enough cards to seed saldo: required %d, got %d", totalSaldos, len(cards))
	}

	for i, card := range cards {
		totalBalance := amountToInt64(int64(rand.Intn(9_000_000) + 1_000_000))
		saldo := &models.Saldo{
			CardNumber:   card.CardNumber,
			TotalBalance: totalBalance,
		}

		if err := r.db.WithContext(r.ctx).Create(saldo).Error; err != nil {
			r.logger.Error("failed to seed saldo", zap.Int("index", i), zap.String("card", card.CardNumber), zap.Error(err))
			return fmt.Errorf("failed to seed saldo for card %s: %w", card.CardNumber, err)
		}

		if i >= activeSaldos {
			if err := r.db.WithContext(r.ctx).Model(&models.Saldo{}).Where("saldo_id = ?", saldo.SaldoID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash saldo", zap.Int("index", i), zap.String("card", card.CardNumber), zap.Error(err))
				return fmt.Errorf("failed to trash saldo %d for card %s: %w", i+1, card.CardNumber, err)
			}
		}
	}

	r.logger.Info("saldo seeded successfully",
		zap.Int("totalSaldos", totalSaldos),
		zap.Int("activeSaldos", activeSaldos),
		zap.Int("trashedSaldos", totalSaldos-activeSaldos))

	return nil
}
