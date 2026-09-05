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

type transactionSeeder struct {
	db     *gorm.DB
	ctx    context.Context
	logger logger.LoggerInterface
}

func NewTransactionSeeder(db *gorm.DB, ctx context.Context, logger logger.LoggerInterface) *transactionSeeder {
	return &transactionSeeder{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (r *transactionSeeder) Seed() error {
	total := 500
	active := 450

	paymentMethods := []string{"Bank Alpha", "Bank Beta", "Bank Gamma"}
	statusOptions := []string{"pending", "success", "failed"}

	var cards []models.Card
	for i := 1; i <= total; i++ {
		var card models.Card
		err := r.db.WithContext(r.ctx).Where("user_id = ? AND deleted_at IS NULL", int32(i)).First(&card).Error
		if err != nil {
			r.logger.Error("failed to get card for user", zap.Int("userID", i), zap.Error(err))
			return fmt.Errorf("failed to get card for user %d: %w", i, err)
		}
		cards = append(cards, card)
	}

	if len(cards) < total {
		r.logger.Error("not enough cards for transaction seeding", zap.Int("required", total), zap.Int("available", len(cards)))
		return fmt.Errorf("not enough cards for transaction seeding: required %d, got %d", total, len(cards))
	}

	var merchants []models.Merchant
	if err := r.db.WithContext(r.ctx).Where("deleted_at IS NULL").Limit(total).Find(&merchants).Error; err != nil {
		r.logger.Error("failed to get merchant list", zap.Error(err))
		return fmt.Errorf("failed to get merchant list: %w", err)
	}

	if len(merchants) < total {
		r.logger.Error("not enough merchants for transaction seeding", zap.Int("required", total), zap.Int("available", len(merchants)))
		return fmt.Errorf("not enough merchants: required %d, got %d", total, len(merchants))
	}

	months := make([]time.Time, 12)
	currentYear := time.Now().Year()
	for i := 0; i < 12; i++ {
		months[i] = time.Date(currentYear, time.Month(i+1), 1, 0, 0, 0, 0, time.UTC)
	}

	for i := 0; i < total; i++ {
		card := cards[i]
		merchant := merchants[i%len(merchants)]
		paymentMethod := paymentMethods[i%len(paymentMethods)]
		status := statusOptions[i%len(statusOptions)]

		monthIndex := i % 12
		transactionTime := months[monthIndex].Add(time.Duration(rand.Intn(28)) * 24 * time.Hour)

		amount := amountToInt64(int64(rand.Intn(1000000-50000) + 50000))
		txn := &models.Transaction{
			CardNumber:      card.CardNumber,
			Amount:          amount,
			PaymentMethod:   paymentMethod,
			MerchantID:      merchant.MerchantID,
			TransactionTime: transactionTime,
		}

		if err := r.db.WithContext(r.ctx).Create(txn).Error; err != nil {
			r.logger.Error("failed to seed transaction", zap.Int("index", i), zap.Error(err))
			return fmt.Errorf("failed to seed transaction %d: %w", i, err)
		}

		if err := r.db.WithContext(r.ctx).Model(&models.Transaction{}).Where("transaction_id = ?", txn.TransactionID).Update("status", status).Error; err != nil {
			r.logger.Error("failed to update transaction status", zap.Int("transactionID", int(txn.TransactionID)), zap.String("status", status), zap.Error(err))
			return fmt.Errorf("failed to update status for transaction ID %d: %w", txn.TransactionID, err)
		}

		if i >= active {
			if err := r.db.WithContext(r.ctx).Model(&models.Transaction{}).Where("transaction_id = ?", txn.TransactionID).Update("deleted_at", "NOW()").Error; err != nil {
				r.logger.Error("failed to trash transaction", zap.Int("index", i), zap.Error(err))
				return fmt.Errorf("failed to trash transaction %d: %w", i, err)
			}
		}
	}

	r.logger.Info("transaction seeded successfully", zap.Int("total", total), zap.Int("active", active), zap.Int("trashed", total-active))
	return nil
}
