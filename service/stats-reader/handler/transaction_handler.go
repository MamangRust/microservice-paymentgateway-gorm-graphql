package handler

import (
	"context"

	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
)

type TransactionRepository interface {
	GetMonthlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyAmount, error)
	GetYearlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyAmount, error)
	GetMonthlyMethodStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyMethodStats, error)
	GetYearlyMethodStats(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyMethodStats, error)
	GetMonthlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int, targetStatus string) ([]repository.MonthlyStatusStats, error)
	GetYearlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, currentYear int, targetStatus string) ([]repository.YearlyStatusStats, error)
}

type TransactionStatsHandler struct {
	statspb.UnimplementedTransactionStatsAmountServiceServer
	statspb.UnimplementedTransactionStatsMethodServiceServer
	statspb.UnimplementedTransactionStatsStatusServiceServer
	repo TransactionRepository
	log  logger.LoggerInterface
}

func NewTransactionStatsHandler(repo TransactionRepository, log logger.LoggerInterface) *TransactionStatsHandler {
	return &TransactionStatsHandler{
		repo: repo,
		log:  log,
	}
}

// --- Transaction Stats Amount Service ---

func (h *TransactionStatsHandler) FindMonthlyAmounts(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "transaction_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly transaction amounts",
		Data:    h.mapToTransactionMonthAmountData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyAmounts(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "transaction_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearAmount{
		Status:  "success",
		Message: "Retrieved yearly transaction amounts",
		Data:    h.mapToTransactionYearAmountData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindMonthlyAmountsByCardNumber(ctx context.Context, req *statspb.FindByYearCardNumberTransactionRequest) (*statspb.ApiResponseTransactionMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly transaction amounts by card number",
		Data:    h.mapToTransactionMonthAmountData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyAmountsByCardNumber(ctx context.Context, req *statspb.FindByYearCardNumberTransactionRequest) (*statspb.ApiResponseTransactionYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearAmount{
		Status:  "success",
		Message: "Retrieved yearly transaction amounts by card number",
		Data:    h.mapToTransactionYearAmountData(data),
	}, nil
}

// --- Transaction Stats Method Service ---

func (h *TransactionStatsHandler) FindMonthlyPaymentMethods(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionMonthMethod, error) {
	data, err := h.repo.GetMonthlyMethodStats(ctx, "transaction_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthMethod{
		Status:  "success",
		Message: "Retrieved monthly transaction methods",
		Data:    h.mapToTransactionMonthMethodData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyPaymentMethods(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionYearMethod, error) {
	data, err := h.repo.GetYearlyMethodStats(ctx, "transaction_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearMethod{
		Status:  "success",
		Message: "Retrieved yearly transaction methods",
		Data:    h.mapToTransactionYearMethodData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindMonthlyPaymentMethodsByCardNumber(ctx context.Context, req *statspb.FindByYearCardNumberTransactionRequest) (*statspb.ApiResponseTransactionMonthMethod, error) {
	data, err := h.repo.GetMonthlyMethodStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthMethod{
		Status:  "success",
		Message: "Retrieved monthly transaction methods by card number",
		Data:    h.mapToTransactionMonthMethodData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyPaymentMethodsByCardNumber(ctx context.Context, req *statspb.FindByYearCardNumberTransactionRequest) (*statspb.ApiResponseTransactionYearMethod, error) {
	data, err := h.repo.GetYearlyMethodStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearMethod{
		Status:  "success",
		Message: "Retrieved yearly transaction methods by card number",
		Data:    h.mapToTransactionYearMethodData(data),
	}, nil
}

// --- Transaction Stats Status Service ---

func (h *TransactionStatsHandler) FindMonthlyTransactionStatusSuccess(ctx context.Context, req *statspb.FindMonthlyTransactionStatus) (*statspb.ApiResponseTransactionMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transaction_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly transaction status success",
		Data:    h.mapToTransactionMonthStatusSuccessData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyTransactionStatusSuccess(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transaction_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly transaction status success",
		Data:    h.mapToTransactionYearStatusSuccessData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindMonthlyTransactionStatusFailed(ctx context.Context, req *statspb.FindMonthlyTransactionStatus) (*statspb.ApiResponseTransactionMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transaction_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly transaction status failed",
		Data:    h.mapToTransactionMonthStatusFailedData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyTransactionStatusFailed(ctx context.Context, req *statspb.FindYearTransactionStatus) (*statspb.ApiResponseTransactionYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transaction_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly transaction status failed",
		Data:    h.mapToTransactionYearStatusFailedData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindMonthlyTransactionStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindMonthlyTransactionStatusCardNumber) (*statspb.ApiResponseTransactionMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly transaction status success by card number",
		Data:    h.mapToTransactionMonthStatusSuccessData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyTransactionStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindYearTransactionStatusCardNumber) (*statspb.ApiResponseTransactionYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly transaction status success by card number",
		Data:    h.mapToTransactionYearStatusSuccessData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindMonthlyTransactionStatusFailedByCardNumber(ctx context.Context, req *statspb.FindMonthlyTransactionStatusCardNumber) (*statspb.ApiResponseTransactionMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly transaction status failed by card number",
		Data:    h.mapToTransactionMonthStatusFailedData(data),
	}, nil
}

func (h *TransactionStatsHandler) FindYearlyTransactionStatusFailedByCardNumber(ctx context.Context, req *statspb.FindYearTransactionStatusCardNumber) (*statspb.ApiResponseTransactionYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "transaction_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTransactionYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly transaction status failed by card number",
		Data:    h.mapToTransactionYearStatusFailedData(data),
	}, nil
}

// --- Mappers ---

func (h *TransactionStatsHandler) mapToTransactionMonthAmountData(data []repository.MonthlyAmount) []*statspb.TransactionMonthAmountResponse {
	var results []*statspb.TransactionMonthAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionMonthAmountResponse{
			Month:       d.Month,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionYearAmountData(data []repository.YearlyAmount) []*statspb.TransactionYearlyAmountResponse {
	var results []*statspb.TransactionYearlyAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionYearlyAmountResponse{
			Year:        d.Year,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionMonthMethodData(data []repository.MonthlyMethodStats) []*statspb.TransactionMonthMethodResponse {
	var results []*statspb.TransactionMonthMethodResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionMonthMethodResponse{
			Month:             d.Month,
			PaymentMethod:     d.PaymentMethod,
			TotalTransactions: int32(d.TotalTransactions),
			TotalAmount:       d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionMonthStatusSuccessData(data []repository.MonthlyStatusStats) []*statspb.TransactionMonthStatusSuccessResponse {
	var results []*statspb.TransactionMonthStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionMonthStatusSuccessResponse{
			Year:         d.Year,
			Month:        d.Month,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionYearStatusSuccessData(data []repository.YearlyStatusStats) []*statspb.TransactionYearStatusSuccessResponse {
	var results []*statspb.TransactionYearStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionYearStatusSuccessResponse{
			Year:         d.Year,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionMonthStatusFailedData(data []repository.MonthlyStatusStats) []*statspb.TransactionMonthStatusFailedResponse {
	var results []*statspb.TransactionMonthStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionMonthStatusFailedResponse{
			Year:        d.Year,
			Month:       d.Month,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionYearStatusFailedData(data []repository.YearlyStatusStats) []*statspb.TransactionYearStatusFailedResponse {
	var results []*statspb.TransactionYearStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionYearStatusFailedResponse{
			Year:        d.Year,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TransactionStatsHandler) mapToTransactionYearMethodData(data []repository.YearlyMethodStats) []*statspb.TransactionYearMethodResponse {
	var results []*statspb.TransactionYearMethodResponse
	for _, d := range data {
		results = append(results, &statspb.TransactionYearMethodResponse{
			Year:              d.Year,
			PaymentMethod:     d.PaymentMethod,
			TotalTransactions: int32(d.TotalTransactions),
			TotalAmount:       d.TotalAmount,
		})
	}
	return results
}
