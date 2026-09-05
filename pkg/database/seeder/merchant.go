package seeder

import (
	"context"
	"fmt"

	apikey "github.com/MamangRust/microservice-payment-gateway-grpc/pkg/api-key"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/database/models"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type merchantSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewMerchantSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *merchantSeeder {
	return &merchantSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *merchantSeeder) Seed() error {
	adjectives := []string{"Blue", "Green", "Red", "Yellow", "Fast"}
	nouns := []string{"Shop", "Store", "Mart", "Market", "Hub"}

	totalMerchants := 40
	activeMerchants := 35
	trashedMerchants := totalMerchants - activeMerchants

	for i := 0; i < totalMerchants; i++ {
		adjective := adjectives[i%len(adjectives)]
		noun := nouns[i%len(nouns)]
		merchantName := fmt.Sprintf("%s %s", adjective, noun)

		apiKey, _ := apikey.GenerateApiKey()

		var status string
		if i < activeMerchants {
			status = "active"
		} else {
			status = "deactive"
		}

		merchant := &models.Merchant{
			Name:   merchantName,
			UserID: int32((i % 5) + 1),
			ApiKey: apiKey,
			Status: status,
		}

		if err := r.db.WithContext(r.ctx).Create(merchant).Error; err != nil {
			r.logger.Error("failed to seed merchant", zap.Int("merchant", i+1), zap.Error(err))
			return fmt.Errorf("failed to seed merchant %d: %w", i+1, err)
		}

		if i >= activeMerchants {
			if err := r.db.WithContext(r.ctx).Model(&models.Merchant{}).Where("merchant_id = ?", merchant.MerchantID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash merchant", zap.Int("merchant", i+1), zap.Error(err))
				return fmt.Errorf("failed to trash merchant %d: %w", i+1, err)
			}
		}
	}

	r.logger.Info("merchant seeded successfully",
		zap.Int("totalMerchants", totalMerchants),
		zap.Int("activeMerchants", activeMerchants),
		zap.Int("trashedMerchants", trashedMerchants))

	return nil
}
