package handler

import (
	"context"

	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
)

type TopupRepository interface {
	GetMonthlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyAmount, error)
	GetYearlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyAmount, error)
	GetMonthlyMethodStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyMethodStats, error)
	GetYearlyMethodStats(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyMethodStats, error)
	GetMonthlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, year int, targetStatus string) ([]repository.MonthlyStatusStats, error)
	GetYearlyStatusStats(ctx context.Context, table string, filterField string, filterValue interface{}, currentYear int, targetStatus string) ([]repository.YearlyStatusStats, error)
}

type TopupStatsHandler struct {
	statspb.UnimplementedTopupStatsAmountServiceServer
	statspb.UnimplementedTopupStatsMethodServiceServer
	statspb.UnimplementedTopupStatsStatusServiceServer
	repo TopupRepository
	log  logger.LoggerInterface
}

func NewTopupStatsHandler(repo TopupRepository, log logger.LoggerInterface) *TopupStatsHandler {
	return &TopupStatsHandler{
		repo: repo,
		log:  log,
	}
}

// --- Topup Stats Amount Service ---

func (h *TopupStatsHandler) FindMonthlyTopupAmounts(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "topup_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly topup amounts",
		Data:    h.mapToTopupMonthAmountData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupAmounts(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "topup_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearAmount{
		Status:  "success",
		Message: "Retrieved yearly topup amounts",
		Data:    h.mapToTopupYearAmountData(data),
	}, nil
}

func (h *TopupStatsHandler) FindMonthlyTopupAmountsByCardNumber(ctx context.Context, req *statspb.FindYearTopupCardNumber) (*statspb.ApiResponseTopupMonthAmount, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthAmount{
		Status:  "success",
		Message: "Retrieved monthly topup amounts by card number",
		Data:    h.mapToTopupMonthAmountData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupAmountsByCardNumber(ctx context.Context, req *statspb.FindYearTopupCardNumber) (*statspb.ApiResponseTopupYearAmount, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearAmount{
		Status:  "success",
		Message: "Retrieved yearly topup amounts by card number",
		Data:    h.mapToTopupYearAmountData(data),
	}, nil
}

// --- Topup Stats Method Service ---

func (h *TopupStatsHandler) FindMonthlyTopupMethods(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupMonthMethod, error) {
	data, err := h.repo.GetMonthlyMethodStats(ctx, "topup_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthMethod{
		Status:  "success",
		Message: "Retrieved monthly topup methods",
		Data:    h.mapToTopupMonthMethodData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupMethods(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupYearMethod, error) {
	data, err := h.repo.GetYearlyMethodStats(ctx, "topup_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearMethod{
		Status:  "success",
		Message: "Retrieved yearly topup methods",
		Data:    h.mapToTopupYearMethodData(data),
	}, nil
}

func (h *TopupStatsHandler) FindMonthlyTopupMethodsByCardNumber(ctx context.Context, req *statspb.FindYearTopupCardNumber) (*statspb.ApiResponseTopupMonthMethod, error) {
	data, err := h.repo.GetMonthlyMethodStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthMethod{
		Status:  "success",
		Message: "Retrieved monthly topup methods by card number",
		Data:    h.mapToTopupMonthMethodData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupMethodsByCardNumber(ctx context.Context, req *statspb.FindYearTopupCardNumber) (*statspb.ApiResponseTopupYearMethod, error) {
	data, err := h.repo.GetYearlyMethodStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearMethod{
		Status:  "success",
		Message: "Retrieved yearly topup methods by card number",
		Data:    h.mapToTopupYearMethodData(data),
	}, nil
}

// --- Topup Stats Status Service ---

func (h *TopupStatsHandler) FindMonthlyTopupStatusSuccess(ctx context.Context, req *statspb.FindMonthlyTopupStatus) (*statspb.ApiResponseTopupMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "topup_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly topup status success",
		Data:    h.mapToTopupMonthStatusSuccessData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupStatusSuccess(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "topup_events", "", nil, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly topup status success",
		Data:    h.mapToTopupYearStatusSuccessData(data),
	}, nil
}

func (h *TopupStatsHandler) FindMonthlyTopupStatusFailed(ctx context.Context, req *statspb.FindMonthlyTopupStatus) (*statspb.ApiResponseTopupMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "topup_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly topup status failed",
		Data:    h.mapToTopupMonthStatusFailedData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupStatusFailed(ctx context.Context, req *statspb.FindYearTopupStatus) (*statspb.ApiResponseTopupYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "topup_events", "", nil, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly topup status failed",
		Data:    h.mapToTopupYearStatusFailedData(data),
	}, nil
}

func (h *TopupStatsHandler) FindMonthlyTopupStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindMonthlyTopupStatusCardNumber) (*statspb.ApiResponseTopupMonthStatusSuccess, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthStatusSuccess{
		Status:  "success",
		Message: "Retrieved monthly topup status success by card number",
		Data:    h.mapToTopupMonthStatusSuccessData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupStatusSuccessByCardNumber(ctx context.Context, req *statspb.FindYearTopupStatusCardNumber) (*statspb.ApiResponseTopupYearStatusSuccess, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), "success")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearStatusSuccess{
		Status:  "success",
		Message: "Retrieved yearly topup status success by card number",
		Data:    h.mapToTopupYearStatusSuccessData(data),
	}, nil
}

func (h *TopupStatsHandler) FindMonthlyTopupStatusFailedByCardNumber(ctx context.Context, req *statspb.FindMonthlyTopupStatusCardNumber) (*statspb.ApiResponseTopupMonthStatusFailed, error) {
	data, err := h.repo.GetMonthlyStatusStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupMonthStatusFailed{
		Status:  "success",
		Message: "Retrieved monthly topup status failed by card number",
		Data:    h.mapToTopupMonthStatusFailedData(data),
	}, nil
}

func (h *TopupStatsHandler) FindYearlyTopupStatusFailedByCardNumber(ctx context.Context, req *statspb.FindYearTopupStatusCardNumber) (*statspb.ApiResponseTopupYearStatusFailed, error) {
	data, err := h.repo.GetYearlyStatusStats(ctx, "topup_events", "card_number", req.CardNumber, int(req.Year), "failed")
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseTopupYearStatusFailed{
		Status:  "success",
		Message: "Retrieved yearly topup status failed by card number",
		Data:    h.mapToTopupYearStatusFailedData(data),
	}, nil
}

// --- Mappers ---

func (h *TopupStatsHandler) mapToTopupMonthAmountData(data []repository.MonthlyAmount) []*statspb.TopupMonthAmountResponse {
	var results []*statspb.TopupMonthAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TopupMonthAmountResponse{
			Month:       d.Month,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupYearAmountData(data []repository.YearlyAmount) []*statspb.TopupYearlyAmountResponse {
	var results []*statspb.TopupYearlyAmountResponse
	for _, d := range data {
		results = append(results, &statspb.TopupYearlyAmountResponse{
			Year:        d.Year,
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupMonthMethodData(data []repository.MonthlyMethodStats) []*statspb.TopupMonthMethodResponse {
	var results []*statspb.TopupMonthMethodResponse
	for _, d := range data {
		results = append(results, &statspb.TopupMonthMethodResponse{
			Month:       d.Month,
			TopupMethod: d.PaymentMethod,
			TotalTopups: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupMonthStatusSuccessData(data []repository.MonthlyStatusStats) []*statspb.TopupMonthStatusSuccessResponse {
	var results []*statspb.TopupMonthStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TopupMonthStatusSuccessResponse{
			Year:         d.Year,
			Month:        d.Month,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupYearStatusSuccessData(data []repository.YearlyStatusStats) []*statspb.TopupYearStatusSuccessResponse {
	var results []*statspb.TopupYearStatusSuccessResponse
	for _, d := range data {
		results = append(results, &statspb.TopupYearStatusSuccessResponse{
			Year:         d.Year,
			TotalSuccess: int32(d.TotalTransactions),
			TotalAmount:  d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupMonthStatusFailedData(data []repository.MonthlyStatusStats) []*statspb.TopupMonthStatusFailedResponse {
	var results []*statspb.TopupMonthStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TopupMonthStatusFailedResponse{
			Year:        d.Year,
			Month:       d.Month,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupYearStatusFailedData(data []repository.YearlyStatusStats) []*statspb.TopupYearStatusFailedResponse {
	var results []*statspb.TopupYearStatusFailedResponse
	for _, d := range data {
		results = append(results, &statspb.TopupYearStatusFailedResponse{
			Year:        d.Year,
			TotalFailed: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}

func (h *TopupStatsHandler) mapToTopupYearMethodData(data []repository.YearlyMethodStats) []*statspb.TopupYearlyMethodResponse {
	var results []*statspb.TopupYearlyMethodResponse
	for _, d := range data {
		results = append(results, &statspb.TopupYearlyMethodResponse{
			Year:        d.Year,
			TopupMethod: d.PaymentMethod,
			TotalTopups: int32(d.TotalTransactions),
			TotalAmount: d.TotalAmount,
		})
	}
	return results
}
