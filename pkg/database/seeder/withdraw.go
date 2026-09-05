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

type withdrawSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewWithdrawSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *withdrawSeeder {
	return &withdrawSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *withdrawSeeder) Seed() error {
	total := 200
	active := 180

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

	if len(cards) < total {
		r.logger.Error("not enough cards for withdraw seeding", zap.Int("required", total), zap.Int("available", len(cards)))
		return fmt.Errorf("not enough cards: required %d, got %d", total, len(cards))
	}

	statusOptions := []string{"pending", "success", "failed"}

	months := make([]time.Time, 12)
	currentYear := time.Now().Year()
	for i := 0; i < 12; i++ {
		months[i] = time.Date(currentYear, time.Month(i+1), 1, 0, 0, 0, 0, time.UTC)
	}

	for i := 0; i < total; i++ {
		card := cards[i]
		status := statusOptions[rand.Intn(len(statusOptions))]

		monthIndex := i % 12
		withdrawTime := months[monthIndex].Add(time.Duration(rand.Intn(28)) * 24 * time.Hour)

		withdrawAmount := amountToInt64(int64(rand.Intn(1000000) + 50000))
		withdraw := &models.Withdraw{
			CardNumber:     card.CardNumber,
			WithdrawAmount: withdrawAmount,
			WithdrawTime:   withdrawTime,
		}

		if err := r.db.WithContext(r.ctx).Create(withdraw).Error; err != nil {
			r.logger.Error("failed to seed withdraw", zap.Int("index", i), zap.Error(err))
			return fmt.Errorf("failed to create withdraw %d: %w", i, err)
		}

		if err := r.db.WithContext(r.ctx).Model(&models.Withdraw{}).Where("withdraw_id = ?", withdraw.WithdrawID).Update("status", status).Error; err != nil {
			r.logger.Error("failed to update withdraw status", zap.Int("withdraw.id", int(withdraw.WithdrawID)), zap.Error(err))
			return fmt.Errorf("failed to update status: %w", err)
		}

		if i >= active {
			if err := r.db.WithContext(r.ctx).Model(&models.Withdraw{}).Where("withdraw_id = ?", withdraw.WithdrawID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash withdraw", zap.Int("withdraw.id", int(withdraw.WithdrawID)), zap.Error(err))
				return fmt.Errorf("failed to trash withdraw %d: %w", i, err)
			}
		}
	}

	r.logger.Info("withdraw seeding completed",
		zap.Int("total", total),
		zap.Int("active", active),
		zap.Int("trashed", total-active))

	return nil
}
