package seeder

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/database/models"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type transferSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewTransferSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *transferSeeder {
	return &transferSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *transferSeeder) Seed() error {
	total := 300
	active := 280

	var cards []models.Card
	for i := 1; i <= total; i++ {
		var card models.Card
		err := r.db.WithContext(r.ctx).Where("user_id = ? AND deleted_at IS NULL", int32(i)).First(&card).Error
		if err != nil {
			r.logger.Debug("failed to get card for user", zap.Int("userID", i), zap.Error(err))
			continue
		}
		cards = append(cards, card)
	}

	if len(cards) < 2 {
		r.logger.Error("not enough cards available for transfer seeding", zap.Int("available", len(cards)))
		return fmt.Errorf("need at least 2 cards, got %d", len(cards))
	}

	statusOptions := []string{"pending", "success", "failed"}

	months := make([]time.Time, 12)
	currentYear := time.Now().Year()
	for i := 0; i < 12; i++ {
		months[i] = time.Date(currentYear, time.Month(i+1), 1, 0, 0, 0, 0, time.UTC)
	}

	for i := 0; i < total; i++ {
		fromIndex := rand.Intn(len(cards))
		toIndex := rand.Intn(len(cards))
		for fromIndex == toIndex {
			toIndex = rand.Intn(len(cards))
		}

		transferFrom := cards[fromIndex].CardNumber
		transferTo := cards[toIndex].CardNumber
		amount := amountToInt64(int64(rand.Intn(1000000) + 50000))
		status := statusOptions[rand.Intn(len(statusOptions))]

		monthIndex := i % 12
		transferTime := months[monthIndex].Add(time.Duration(rand.Intn(28)) * 24 * time.Hour)

		transfer := &models.Transfer{
			TransferFrom:   transferFrom,
			TransferTo:     transferTo,
			TransferAmount: amount,
			TransferTime:   transferTime,
			Status:         status,
		}

		if err := r.db.WithContext(r.ctx).Create(transfer).Error; err != nil {
			r.logger.Error("failed to seed transfer", zap.Int("transfer", i+1), zap.Error(err))
			return fmt.Errorf("failed to seed transfer %d: %w", i+1, err)
		}

		if i >= active {
			if err := r.db.WithContext(r.ctx).Model(&models.Transfer{}).Where("transfer_id = ?", transfer.TransferID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash transfer", zap.Int("transfer", i+1), zap.Error(err))
				return fmt.Errorf("failed to trash transfer %d: %w", i+1, err)
			}
		}
	}

	r.logger.Info("transfer seeded successfully",
		zap.Int("total", total),
		zap.Int("active", active),
		zap.Int("trashed", total-active))

	return nil
}
