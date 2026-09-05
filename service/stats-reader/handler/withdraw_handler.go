package handler

import (
	"context"

	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
)

type WithdrawRepository interface {
	GetMonthlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyAmount, error)
	GetYearlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyAmount, error)
	GetMonthlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int, targetStatus string) ([]repository.MonthlyStatusStats, error)
	GetYearlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, currentYear int, targetStatus string) ([]repository.YearlyStatusStats, error)
}

type WithdrawStatsHandler struct {
	statspb.UnimplementedWithdrawStatsAmountServiceServer
	statspb.UnimplementedWithdrawStatsStatusServiceServer
	repo WithdrawRepository
	log  logger.LoggerInterface
}

func NewWithdrawStatsHandler(repo WithdrawRepository, log logger.LoggerInterface) *WithdrawStatsHandler {
	return &WithdrawStatsHandler{
		repo: repo,
		log:  log,
	}
}

// --- Withdraw Stats Amount Service ---

func (h *WithdrawStatsHandler) FindMonthlyWithdraws(ctx context.Context, req *statspb.FindYearWithdrawStatus) (*statspb.ApiResponseWithdrawMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "withdraw_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly withdraw amounts",
		Data:    h.mapToWithdrawMonthAmountData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdraws(ctx context.Context, req *statspb.FindYearWithdrawStatus) (*statspb.ApiResponseWithdrawYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "withdraw_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearAmount{
		Status:  "success",
		Message: "Retrieved yearly withdraw amounts",
		Data:    h.mapToWithdrawYearAmountData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindMonthlyWithdrawsByCardNumber(ctx context.Context, req *statspb.FindYearWithdrawCardNumber) (*statspb.ApiResponseWithdrawMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly withdraw amounts by card number",
		Data:    h.mapToWithdrawMonthAmountData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdrawsByCardNumber(ctx context.Context, req *statspb.FindYearWithdrawCardNumber) (*statspb.ApiResponseWithdrawYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearAmount{
		Status:  "success",
		Message: "Retrieved yearly withdraw amounts by card number",
		Data:    h.mapToWithdrawYearAmountData(data),
	}, nil
}

// --- Withdraw Stats Status Service ---

func (h *WithdrawStatsHandler) FindMonthlyWithdrawStatusSuccess(ctx context.Context, req *statspb.FindMonthlyWithdrawStatus) (*statspb.ApiResponseWithdrawMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "withdraw_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly withdraw status success",
		Data:    h.mapToWithdrawMonthStatusSuccessData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdrawStatusSuccess(ctx context.Context, req *statspb.FindYearWithdrawStatus) (*statspb.ApiResponseWithdrawYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "withdraw_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly withdraw status success",
		Data:    h.mapToWithdrawYearStatusSuccessData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindMonthlyWithdrawStatusFailed(ctx context.Context, req *statspb.FindMonthlyWithdrawStatus) (*statspb.ApiResponseWithdrawMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "withdraw_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly withdraw status failed",
		Data:    h.mapToWithdrawMonthStatusFailedData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdrawStatusFailed(ctx context.Context, req *statspb.FindYearWithdrawStatus) (*statspb.ApiResponseWithdrawYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "withdraw_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly withdraw status failed",
		Data:    h.mapToWithdrawYearStatusFailedData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindMonthlyWithdrawStatusSuccessCardNumber(ctx context.Context, req *statspb.FindMonthlyWithdrawStatusCardNumber) (*statspb.ApiResponseWithdrawMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly withdraw status success by card number",
		Data:    h.mapToWithdrawMonthStatusSuccessData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdrawStatusSuccessCardNumber(ctx context.Context, req *statspb.FindYearWithdrawStatusCardNumber) (*statspb.ApiResponseWithdrawYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly withdraw status success by card number",
		Data:    h.mapToWithdrawYearStatusSuccessData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindMonthlyWithdrawStatusFailedCardNumber(ctx context.Context, req *statspb.FindMonthlyWithdrawStatusCardNumber) (*statspb.ApiResponseWithdrawMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly withdraw status failed by card number",
		Data:    h.mapToWithdrawMonthStatusFailedData(data),
	}, nil
}

func (h *WithdrawStatsHandler) FindYearlyWithdrawStatusFailedCardNumber(ctx context.Context, req *statspb.FindYearWithdrawStatusCardNumber) (*statspb.ApiResponseWithdrawYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "withdraw_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseWithdrawYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly withdraw status failed by card number",
		Data:    h.mapToWithdrawYearStatusFailedData(data),
	}, nil
}

// --- Mappers ---

func (h *WithdrawStatsHandler) mapToWithdrawMonthAmountData(data []repository.MonthlyAmount) []*statspb.WithdrawMonthlyAmountResponse {
	var results []*statspb.WithdrawMonthlyAmountResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawMonthlyAmountResponse{
			Month:       d.Month,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *WithdrawStatsHandler) mapToWithdrawYearAmountData(data []repository.YearlyAmount) []*statspb.WithdrawYearlyAmountResponse {
	var results []*statspb.WithdrawYearlyAmountResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawYearlyAmountResponse{
			Year:        d.Year,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *WithdrawStatsHandler) mapToWithdrawMonthStatusSuccessData(data []repository.MonthlyStatusStats) []*statspb.WithdrawMonthStatusSuccessResponse {
	var results []*statspb.WithdrawMonthStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawMonthStatusSuccessResponse{
			Year:         d.Year,
			Month:        d.Month,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *WithdrawStatsHandler) mapToWithdrawYearStatusSuccessData(data []repository.YearlyStatusStats) []*statspb.WithdrawYearStatusSuccessResponse {
	var results []*statspb.WithdrawYearStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawYearStatusSuccessResponse{
			Year:         d.Year,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *WithdrawStatsHandler) mapToWithdrawMonthStatusFailedData(data []repository.MonthlyStatusStats) []*statspb.WithdrawMonthStatusFailedResponse {
	var results []*statspb.WithdrawMonthStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawMonthStatusFailedResponse{
			Year:        d.Year,
			Month:       d.Month,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *WithdrawStatsHandler) mapToWithdrawYearStatusFailedData(data []repository.YearlyStatusStats) []*statspb.WithdrawYearStatusFailedResponse {
	var results []*statspb.WithdrawYearStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.WithdrawYearStatusFailedResponse{
			Year:        d.Year,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}
