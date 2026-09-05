package handler

import (
	"context"

	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
)

type TransferRepository interface {
	GetMonthlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyAmount, error)
	GetYearlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyAmount, error)
	GetMonthlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int, targetStatus string) ([]repository.MonthlyStatusStats, error)
	GetYearlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, currentYear int, targetStatus string) ([]repository.YearlyStatusStats, error)
}

type TransferStatsHandler struct {
	statspb.UnimplementedTransferStatsAmountServiceServer
	statspb.UnimplementedTransferStatsStatusServiceServer
	repo TransferRepository
	log  logger.LoggerInterface
}

func NewTransferStatsHandler(repo TransferRepository, log logger.LoggerInterface) *TransferStatsHandler {
	return &TransferStatsHandler{
		repo: repo,
		log:  log,
	}
}

// --- Transfer Stats Service ---

func (h *TransferStatsHandler) FindMonthlyTransferAmounts(ctx context.Context, req *statspb.FindYearTransferStatus) (*statspb.ApiResponseTransferMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "transfer_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly transfer amounts",
		Data:    h.mapToTransferMonthAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferAmounts(ctx context.Context, req *statspb.FindYearTransferStatus) (*statspb.ApiResponseTransferYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "transfer_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearAmount{
		Status:  "success",
		Message: "Retrieved yearly transfer amounts",
		Data:    h.mapToTransferYearAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferAmountsBySenderCardNumber(ctx context.Context, req *statspb.FindByCardNumberTransferRequest) (*statspb.ApiResponseTransferMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly transfer amounts by sender card number",
		Data:    h.mapToTransferMonthAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferAmountsByReceiverCardNumber(ctx context.Context, req *statspb.FindByCardNumberTransferRequest) (*statspb.ApiResponseTransferMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "transfer_events", "destination_card", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly transfer amounts by receiver card number",
		Data:    h.mapToTransferMonthAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferAmountsBySenderCardNumber(ctx context.Context, req *statspb.FindByCardNumberTransferRequest) (*statspb.ApiResponseTransferYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearAmount{
		Status:  "success",
		Message: "Retrieved yearly transfer amounts by sender card number",
		Data:    h.mapToTransferYearAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferAmountsByReceiverCardNumber(ctx context.Context, req *statspb.FindByCardNumberTransferRequest) (*statspb.ApiResponseTransferYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "transfer_events", "destination_card", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearAmount{
		Status:  "success",
		Message: "Retrieved yearly transfer amounts by receiver card number",
		Data:    h.mapToTransferYearAmountData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferStatusSuccess(ctx context.Context, req *statspb.FindMonthlyTransferStatus) (*statspb.ApiResponseTransferMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transfer_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly transfer status success",
		Data:    h.mapToTransferMonthStatusSuccessData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferStatusSuccess(ctx context.Context, req *statspb.FindYearTransferStatus) (*statspb.ApiResponseTransferYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transfer_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly transfer status success",
		Data:    h.mapToTransferYearStatusSuccessData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferStatusFailed(ctx context.Context, req *statspb.FindMonthlyTransferStatus) (*statspb.ApiResponseTransferMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transfer_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly transfer status failed",
		Data:    h.mapToTransferMonthStatusFailedData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferStatusFailed(ctx context.Context, req *statspb.FindYearTransferStatus) (*statspb.ApiResponseTransferYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transfer_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly transfer status failed",
		Data:    h.mapToTransferYearStatusFailedData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindMonthlyTransferStatusCardNumber) (*statspb.ApiResponseTransferMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly transfer status success by card number",
		Data:    h.mapToTransferMonthStatusSuccessData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindYearTransferStatusCardNumber) (*statspb.ApiResponseTransferYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly transfer status success by card number",
		Data:    h.mapToTransferYearStatusSuccessData(data),
	}, nil
}

func (h *TransferStatsHandler) FindMonthlyTransferStatusFailedByCardNumber(ctx context.Context, req *statspb.FindMonthlyTransferStatusCardNumber) (*statspb.ApiResponseTransferMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly transfer status failed by card number",
		Data:    h.mapToTransferMonthStatusFailedData(data),
	}, nil
}

func (h *TransferStatsHandler) FindYearlyTransferStatusFailedByCardNumber(ctx context.Context, req *statspb.FindYearTransferStatusCardNumber) (*statspb.ApiResponseTransferYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transfer_events", "source_card", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransferYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly transfer status failed by card number",
		Data:    h.mapToTransferYearStatusFailedData(data),
	}, nil
}

// --- Mappers ---

func (h *TransferStatsHandler) mapToTransferMonthAmountData(data []repository.MonthlyAmount) []*statspb.TransferMonthAmountResponse {
	var results []*statspb.TransferMonthAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TransferMonthAmountResponse{
			Month:       d.Month,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransferStatsHandler) mapToTransferYearAmountData(data []repository.YearlyAmount) []*statspb.TransferYearAmountResponse {
	var results []*statspb.TransferYearAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TransferYearAmountResponse{
			Year:        d.Year,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransferStatsHandler) mapToTransferMonthStatusSuccessData(data []repository.MonthlyStatusStats) []*statspb.TransferMonthStatusSuccessResponse {
	var results []*statspb.TransferMonthStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TransferMonthStatusSuccessResponse{
			Year:         d.Year,
			Month:        d.Month,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TransferStatsHandler) mapToTransferYearStatusSuccessData(data []repository.YearlyStatusStats) []*statspb.TransferYearStatusSuccessResponse {
	var results []*statspb.TransferYearStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TransferYearStatusSuccessResponse{
			Year:         d.Year,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TransferStatsHandler) mapToTransferMonthStatusFailedData(data []repository.MonthlyStatusStats) []*statspb.TransferMonthStatusFailedResponse {
	var results []*statspb.TransferMonthStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TransferMonthStatusFailedResponse{
			Year:        d.Year,
			Month:       d.Month,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransferStatsHandler) mapToTransferYearStatusFailedData(data []repository.YearlyStatusStats) []*statspb.TransferYearStatusFailedResponse {
	var results []*statspb.TransferYearStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TransferYearStatusFailedResponse{
			Year:        d.Year,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}
