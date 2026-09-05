package handler

import (
	"context"

	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
)

type SaldoRepository interface {
	GetMonthlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, year int) ([]repository.MonthlyAmount, error)
	GetYearlyAmounts(ctx context.Context, table string, filterField string, filterValue interface{}, startYear, endYear int) ([]repository.YearlyAmount, error)
	GetMonthlyTotalSaldo(ctx context.Context, year int) ([]repository.MonthlyAmount, error)
	GetYearlyTotalSaldo(ctx context.Context, startYear, endYear int) ([]repository.YearlyAmount, error)
}

type SaldoStatsHandler struct {
	statspb.UnimplementedSaldoStatsBalanceServiceServer
	statspb.UnimplementedSaldoStatsTotalBalanceServer
	repo SaldoRepository
	log  logger.LoggerInterface
}

func NewSaldoStatsHandler(repo SaldoRepository, log logger.LoggerInterface) *SaldoStatsHandler {
	return &SaldoStatsHandler{
		repo: repo,
		log:  log,
	}
}

// --- Saldo Stats Balance Service ---

func (h *SaldoStatsHandler) FindMonthlySaldoBalances(ctx context.Context, req *statspb.FindYearlySaldo) (*statspb.ApiResponseMonthSaldoBalances, error) {
	data, err := h.repo.GetMonthlyAmounts(ctx, "saldo_events", "", nil, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseMonthSaldoBalances{
		Status:  "success",
		Message: "Retrieved monthly saldo balances",
		Data:    h.mapToSaldoMonthBalanceData(data),
	}, nil
}

func (h *SaldoStatsHandler) FindYearlySaldoBalances(ctx context.Context, req *statspb.FindYearlySaldo) (*statspb.ApiResponseYearSaldoBalances, error) {
	data, err := h.repo.GetYearlyAmounts(ctx, "saldo_events", "", nil, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseYearSaldoBalances{
		Status:  "success",
		Message: "Retrieved yearly saldo balances",
		Data:    h.mapToSaldoYearBalanceData(data),
	}, nil
}

// --- Saldo Stats Total Balance Service ---

func (h *SaldoStatsHandler) FindMonthlyTotalSaldoBalance(ctx context.Context, req *statspb.FindMonthlySaldoTotalBalance) (*statspb.ApiResponseMonthTotalSaldo, error) {
	data, err := h.repo.GetMonthlyTotalSaldo(ctx, int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseMonthTotalSaldo{
		Status:  "success",
		Message: "Retrieved monthly total saldo",
		Data:    h.mapToSaldoMonthTotalData(data),
	}, nil
}

func (h *SaldoStatsHandler) FindYearTotalSaldoBalance(ctx context.Context, req *statspb.FindYearlySaldo) (*statspb.ApiResponseYearTotalSaldo, error) {
	data, err := h.repo.GetYearlyTotalSaldo(ctx, int(req.Year), int(req.Year))
	if err != nil {
		return nil, err
	}
	return &statspb.ApiResponseYearTotalSaldo{
		Status:  "success",
		Message: "Retrieved yearly total saldo",
		Data:    h.mapToSaldoYearTotalData(data),
	}, nil
}


// --- Mappers ---

func (h *SaldoStatsHandler) mapToSaldoMonthBalanceData(data []repository.MonthlyAmount) []*statspb.SaldoMonthBalanceResponse {
	var results []*statspb.SaldoMonthBalanceResponse
	for _, d := range data {
		results = append(results, &statspb.SaldoMonthBalanceResponse{
			Month:        d.Month,
			TotalBalance: d.TotalAmount,
		})
	}
	return results
}

func (h *SaldoStatsHandler) mapToSaldoYearBalanceData(data []repository.YearlyAmount) []*statspb.SaldoYearBalanceResponse {
	var results []*statspb.SaldoYearBalanceResponse
	for _, d := range data {
		results = append(results, &statspb.SaldoYearBalanceResponse{
			Year:         d.Year,
			TotalBalance: d.TotalAmount,
		})
	}
	return results
}

func (h *SaldoStatsHandler) mapToSaldoMonthTotalData(data []repository.MonthlyAmount) []*statspb.SaldoMonthTotalBalanceResponse {
	var results []*statspb.SaldoMonthTotalBalanceResponse
	for _, d := range data {
		results = append(results, &statspb.SaldoMonthTotalBalanceResponse{
			Month:        d.Month,
			Year:         d.Year,
			TotalBalance: d.TotalAmount,
		})
	}
	return results
}

func (h *SaldoStatsHandler) mapToSaldoYearTotalData(data []repository.YearlyAmount) []*statspb.SaldoYearTotalBalanceResponse {
	var results []*statspb.SaldoYearTotalBalanceResponse
	for _, d := range data {
		results = append(results, &statspb.SaldoYearTotalBalanceResponse{
			Year:         d.Year,
			TotalBalance: d.TotalAmount,
		})
	}
	return results
}
