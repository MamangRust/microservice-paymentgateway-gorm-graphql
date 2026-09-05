package seeder

import (
	"context"
	"fmt"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/database/models"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/date"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/randomvcc"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type cardSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewCardSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *cardSeeder {
	return &cardSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *cardSeeder) Seed() error {
	cardTypes := []string{"credit", "debit"}
	cardProviders := []string{"mandiri", "bni", "bri"}

	totalCards := 120
	activeCards := 100
	trashedCards := totalCards - activeCards

	cardNumbers := make([]string, totalCards)
	for i := 0; i < totalCards; i++ {
		cardNumber, err := randomvcc.RandomCardNumber()
		if err != nil {
			r.logger.Error("failed to generate card number", zap.Int("index", i), zap.Error(err))
			return fmt.Errorf("failed to generate card number: %w", err)
		}
		cardNumbers[i] = cardNumber
	}

	for i := 0; i < totalCards; i++ {
		expireDate := date.GenerateExpireDate()

		card := &models.Card{
			UserID:       int32(i + 1),
			CardNumber:   cardNumbers[i],
			CardType:     cardTypes[i%len(cardTypes)],
			ExpireDate:   expireDate,
			Cvv:          fmt.Sprintf("%03d", i%1000),
			CardProvider: cardProviders[i%len(cardProviders)],
		}

		if err := r.db.WithContext(r.ctx).Create(card).Error; err != nil {
			r.logger.Error("failed to seed card", zap.Int("card", i+1), zap.Error(err))
			return fmt.Errorf("failed to seed card %d: %w", i+1, err)
		}

		if i >= activeCards {
			if err := r.db.WithContext(r.ctx).Model(&models.Card{}).Where("card_id = ?", card.CardID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash card", zap.Int("card", i+1), zap.Error(err))
				return fmt.Errorf("failed to trash card %d: %w", i+1, err)
			}
		}
	}

	r.logger.Info("card seeded successfully", zap.Int("totalCards", totalCards), zap.Int("activeCards", activeCards), zap.Int("trashedCards", trashedCards))
	return nil
}
