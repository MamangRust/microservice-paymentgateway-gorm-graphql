package graph

import (
	"context"

	"github.com/MamangRust/microservice-payment-gateway-grpc/service/apigateway/internal/model"
	card "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	merchant "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ─── mapper helpers ───────────────────────────────────────────────────────────

type amountR interface { GetMonth() string; GetTotalAmount() int64 }

type yearAmountR interface { GetYear() string; GetTotalAmount() int64 }

type monthStatusR interface { GetYear() string; GetMonth() string; GetTotalSuccess() int32; GetTotalAmount() int64 }

type yearStatusR interface { GetYear() string; GetTotalSuccess() int32; GetTotalAmount() int64 }

type monthStatusFailedR interface { GetYear() string; GetMonth() string; GetTotalFailed() int32; GetTotalAmount() int64 }

type yearStatusFailedR interface { GetYear() string; GetTotalFailed() int32; GetTotalAmount() int64 }

type monthBalanceR interface { GetMonth() string; GetTotalBalance() int64 }

type yearBalanceR interface { GetYear() string; GetTotalBalance() int64 }

type merchantTxR interface {
	GetId() int32; GetCardNumber() string; GetAmount() int64; GetPaymentMethod() string;
	GetMerchantId() int32; GetMerchantName() string; GetTransactionTime() string;
	GetCreatedAt() string; GetUpdatedAt() string
}

func mapMonthAmounts[T amountR](rows []T) []*model.MonthlyAmountResponse {
	out := make([]*model.MonthlyAmountResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyAmountResponse{Month: r.GetMonth(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapYearAmounts[T yearAmountR](rows []T) []*model.YearlyAmountResponse {
	out := make([]*model.YearlyAmountResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyAmountResponse{Year: r.GetYear(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapMonthStatus[T monthStatusR](rows []T) []*model.MonthlyStatusResponse {
	out := make([]*model.MonthlyStatusResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyStatusResponse{Year: r.GetYear(), Month: r.GetMonth(), TotalSuccess: r.GetTotalSuccess(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapYearStatus[T yearStatusR](rows []T) []*model.YearlyStatusResponse {
	out := make([]*model.YearlyStatusResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyStatusResponse{Year: r.GetYear(), TotalSuccess: r.GetTotalSuccess(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapMonthStatusFailed[T monthStatusFailedR](rows []T) []*model.MonthlyStatusResponse {
	out := make([]*model.MonthlyStatusResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyStatusResponse{Year: r.GetYear(), Month: r.GetMonth(), TotalSuccess: r.GetTotalFailed(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapYearStatusFailed[T yearStatusFailedR](rows []T) []*model.YearlyStatusResponse {
	out := make([]*model.YearlyStatusResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyStatusResponse{Year: r.GetYear(), TotalSuccess: r.GetTotalFailed(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapMonthBalance[T monthBalanceR](rows []T) []*model.MonthlyBalanceResponse {
	out := make([]*model.MonthlyBalanceResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyBalanceResponse{Month: r.GetMonth(), TotalBalance: int32(r.GetTotalBalance())})
	}
	return out
}

func mapYearBalance[T yearBalanceR](rows []T) []*model.YearlyBalanceResponse {
	out := make([]*model.YearlyBalanceResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyBalanceResponse{Year: r.GetYear(), TotalBalance: int32(r.GetTotalBalance())})
	}
	return out
}

func mapMerchantTx[T merchantTxR](rows []T) []*model.MerchantTransactionStatsResponse {
	out := make([]*model.MerchantTransactionStatsResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MerchantTransactionStatsResponse{
			ID: r.GetId(), CardNumber: r.GetCardNumber(), Amount: int32(r.GetAmount()),
			PaymentMethod: r.GetPaymentMethod(), MerchantID: r.GetMerchantId(), MerchantName: r.GetMerchantName(),
			TransactionTime: r.GetTransactionTime(), CreatedAt: r.GetCreatedAt(), UpdatedAt: r.GetUpdatedAt(),
		})
	}
	return out
}

// ─── Dashboard ────────────────────────────────────────────────────────────────

func (r *queryResolver) DashboardCardStats(ctx context.Context) (*model.APIResponseDashboardCard, error) {
	res, err := r.StatsRead.CardDashboard.DashboardCard(ctx, &emptypb.Empty{})
	if err != nil {
		return &model.APIResponseDashboardCard{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseDashboardCard{Status: "success", Message: res.Message, Data: &model.DashboardCardResponse{
		TotalBalance: int32(res.Data.TotalBalance), TotalTopup: int32(res.Data.TotalTopup),
		TotalTransaction: int32(res.Data.TotalTransaction), TotalTransfer: int32(res.Data.TotalTransfer),
		TotalWithdraw: int32(res.Data.TotalWithdraw),
	}}, nil
}

func (r *queryResolver) DashboardCardByNumberStats(ctx context.Context, input model.FindByCardNumberInput) (*model.APIResponseDashboardCardByNumber, error) {
	res, err := r.StatsRead.CardDashboard.DashboardCardNumber(ctx, &card.FindByCardNumberRequest{CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseDashboardCardByNumber{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseDashboardCardByNumber{Status: "success", Message: res.Message, Data: &model.DashboardCardByNumberResponse{
		TotalBalance: int32(res.Data.TotalBalance), TotalTopup: int32(res.Data.TotalTopup),
		TotalTransaction: int32(res.Data.TotalTransaction),
		TotalTransferSend: int32(res.Data.TotalTransferSend), TotalTransferReceiver: int32(res.Data.TotalTransferReceiver),
		TotalWithdraw: int32(res.Data.TotalWithdraw),
	}}, nil
}

// ─── Balance Stats ────────────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyBalanceStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyBalance, error) {
	res, err := r.StatsRead.CardStatsBalance.FindMonthlyBalance(ctx, &statspb.FindYearAmount{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyBalance{Status: "success", Message: "ok", Data: mapMonthBalance(res.Data)}, nil
}

func (r *queryResolver) FindYearlyBalanceStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyBalance, error) {
	res, err := r.StatsRead.CardStatsBalance.FindYearlyBalance(ctx, &statspb.FindYearAmount{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyBalance{Status: "success", Message: "ok", Data: mapYearBalance(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyBalanceByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyBalance, error) {
	res, err := r.StatsRead.CardStatsBalance.FindMonthlyBalanceByCardNumber(ctx, &statspb.FindYearAmountCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyBalance{Status: "success", Message: "ok", Data: mapMonthBalance(res.Data)}, nil
}

func (r *queryResolver) FindYearlyBalanceByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyBalance, error) {
	res, err := r.StatsRead.CardStatsBalance.FindYearlyBalanceByCardNumber(ctx, &statspb.FindYearAmountCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyBalance{Status: "success", Message: "ok", Data: mapYearBalance(res.Data)}, nil
}

// ─── Topup Stats Amount ───────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTopupAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TopupStatsAmount.FindMonthlyTopupAmounts(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTopupAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TopupStatsAmount.FindYearlyTopupAmounts(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTopupAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TopupStatsAmount.FindMonthlyTopupAmountsByCardNumber(ctx, &statspb.FindYearTopupCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTopupAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TopupStatsAmount.FindYearlyTopupAmountsByCardNumber(ctx, &statspb.FindYearTopupCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Topup Stats Status ───────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTopupStatusSuccessStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TopupStatsStatus.FindMonthlyTopupStatusSuccess(ctx, &statspb.FindMonthlyTopupStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatus(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTopupStatusSuccessStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TopupStatsStatus.FindYearlyTopupStatusSuccess(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatus(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTopupStatusFailedStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TopupStatsStatus.FindMonthlyTopupStatusFailed(ctx, &statspb.FindMonthlyTopupStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatusFailed(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTopupStatusFailedStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TopupStatsStatus.FindYearlyTopupStatusFailed(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatusFailed(res.Data)}, nil
}

// ─── Topup Stats Method ───────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTopupMethodsStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyMethod, error) {
	res, err := r.StatsRead.TopupStatsMethod.FindMonthlyTopupMethods(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyMethod{Status: "success", Message: "ok", Data: mapTopupMonthMethod(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTopupMethodsStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyMethod, error) {
	res, err := r.StatsRead.TopupStatsMethod.FindYearlyTopupMethods(ctx, &statspb.FindYearTopupStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyMethod{Status: "success", Message: "ok", Data: mapTopupYearMethod(res.Data)}, nil
}

// ─── Withdraw Stats Amount ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyWithdrawAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.WithdrawStatsAmount.FindMonthlyWithdraws(ctx, &statspb.FindYearWithdrawStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyWithdrawAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.WithdrawStatsAmount.FindYearlyWithdraws(ctx, &statspb.FindYearWithdrawStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyWithdrawAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.WithdrawStatsAmount.FindMonthlyWithdrawsByCardNumber(ctx, &statspb.FindYearWithdrawCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyWithdrawAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.WithdrawStatsAmount.FindYearlyWithdrawsByCardNumber(ctx, &statspb.FindYearWithdrawCardNumber{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Withdraw Stats Status ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyWithdrawStatusSuccessStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.WithdrawStatsStatus.FindMonthlyWithdrawStatusSuccess(ctx, &statspb.FindMonthlyWithdrawStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatus(res.Data)}, nil
}

func (r *queryResolver) FindYearlyWithdrawStatusSuccessStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.WithdrawStatsStatus.FindYearlyWithdrawStatusSuccess(ctx, &statspb.FindYearWithdrawStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatus(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyWithdrawStatusFailedStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.WithdrawStatsStatus.FindMonthlyWithdrawStatusFailed(ctx, &statspb.FindMonthlyWithdrawStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatusFailed(res.Data)}, nil
}

func (r *queryResolver) FindYearlyWithdrawStatusFailedStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.WithdrawStatsStatus.FindYearlyWithdrawStatusFailed(ctx, &statspb.FindYearWithdrawStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatusFailed(res.Data)}, nil
}

// ─── Transaction Stats Amount ─────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTransactionAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TransactionStatsAmount.FindMonthlyAmounts(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransactionAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TransactionStatsAmount.FindYearlyAmounts(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTransactionAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TransactionStatsAmount.FindMonthlyAmountsByCardNumber(ctx, &statspb.FindByYearCardNumberTransactionRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransactionAmountByCardNumberStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TransactionStatsAmount.FindYearlyAmountsByCardNumber(ctx, &statspb.FindByYearCardNumberTransactionRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Transaction Stats Status ─────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTransactionStatusSuccessStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TransactionStatsStatus.FindMonthlyTransactionStatusSuccess(ctx, &statspb.FindMonthlyTransactionStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatus(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransactionStatusSuccessStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TransactionStatsStatus.FindYearlyTransactionStatusSuccess(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatus(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTransactionStatusFailedStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TransactionStatsStatus.FindMonthlyTransactionStatusFailed(ctx, &statspb.FindMonthlyTransactionStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatusFailed(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransactionStatusFailedStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TransactionStatsStatus.FindYearlyTransactionStatusFailed(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatusFailed(res.Data)}, nil
}

// ─── Transaction Stats Method ─────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTransactionMethodsStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyMethod, error) {
	res, err := r.StatsRead.TransactionStatsMethod.FindMonthlyPaymentMethods(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyMethod{Status: "success", Message: "ok", Data: mapTxMonthMethod(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransactionMethodsStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyMethod, error) {
	res, err := r.StatsRead.TransactionStatsMethod.FindYearlyPaymentMethods(ctx, &statspb.FindYearTransactionStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyMethod{Status: "success", Message: "ok", Data: mapTxYearMethod(res.Data)}, nil
}

// ─── Transfer Stats Amount ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTransferAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindMonthlyTransferAmounts(ctx, &statspb.FindYearTransferStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransferAmountStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindYearlyTransferAmounts(ctx, &statspb.FindYearTransferStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTransferSenderAmountStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindMonthlyTransferAmountsBySenderCardNumber(ctx, &statspb.FindByCardNumberTransferRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransferSenderAmountStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindYearlyTransferAmountsBySenderCardNumber(ctx, &statspb.FindByCardNumberTransferRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTransferReceiverAmountStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindMonthlyTransferAmountsByReceiverCardNumber(ctx, &statspb.FindByCardNumberTransferRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransferReceiverAmountStats(ctx context.Context, input model.FindYearCardNumberStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.TransferStatsAmount.FindYearlyTransferAmountsByReceiverCardNumber(ctx, &statspb.FindByCardNumberTransferRequest{Year: input.Year, CardNumber: input.CardNumber})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Transfer Stats Status ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyTransferStatusSuccessStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TransferStatsStatus.FindMonthlyTransferStatusSuccess(ctx, &statspb.FindMonthlyTransferStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatus(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransferStatusSuccessStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TransferStatsStatus.FindYearlyTransferStatusSuccess(ctx, &statspb.FindYearTransferStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatus(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyTransferStatusFailedStats(ctx context.Context, input model.FindMonthlyYearStatsInput) (*model.APIResponseMonthlyStatus, error) {
	res, err := r.StatsRead.TransferStatsStatus.FindMonthlyTransferStatusFailed(ctx, &statspb.FindMonthlyTransferStatus{Year: input.Year, Month: input.Month})
	if err != nil {
		return &model.APIResponseMonthlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyStatus{Status: "success", Message: "ok", Data: mapMonthStatusFailed(res.Data)}, nil
}

func (r *queryResolver) FindYearlyTransferStatusFailedStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyStatus, error) {
	res, err := r.StatsRead.TransferStatsStatus.FindYearlyTransferStatusFailed(ctx, &statspb.FindYearTransferStatus{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyStatus{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyStatus{Status: "success", Message: "ok", Data: mapYearStatusFailed(res.Data)}, nil
}

// ─── Merchant Stats Amount ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyMerchantAmountStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindMonthlyAmountMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantAmountStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindYearlyAmountMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantAmountByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindMonthlyAmountByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantAmountByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindYearlyAmountByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantAmountByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindMonthlyAmountByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantAmountByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsAmount.FindYearlyAmountByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Merchant Stats Method ────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlyMerchantMethodStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseMonthlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindMonthlyPaymentMethodsMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyMethod{Status: "success", Message: "ok", Data: mapMerchantMonthMethod(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantMethodStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseYearlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindYearlyPaymentMethodMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyMethod{Status: "success", Message: "ok", Data: mapMerchantYearMethod(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantMethodByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseMonthlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindMonthlyPaymentMethodByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseMonthlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyMethod{Status: "success", Message: "ok", Data: mapMerchantMonthMethod(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantMethodByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseYearlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindYearlyPaymentMethodByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseYearlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyMethod{Status: "success", Message: "ok", Data: mapMerchantYearMethod(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantMethodByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseMonthlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindMonthlyPaymentMethodByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseMonthlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyMethod{Status: "success", Message: "ok", Data: mapMerchantMonthMethod(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantMethodByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseYearlyMethod, error) {
	res, err := r.StatsRead.MerchantStatsMethod.FindYearlyPaymentMethodByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseYearlyMethod{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyMethod{Status: "success", Message: "ok", Data: mapMerchantYearMethod(res.Data)}, nil
}

// ─── Merchant Stats TotalAmount ───────────────────────────────────────────────

func (r *queryResolver) FindMonthlyMerchantTotalAmountStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindMonthlyTotalAmountMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantTotalAmountStats(ctx context.Context, input model.FindYearMerchantStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindYearlyTotalAmountMerchant(ctx, &statspb.FindYearMerchant{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantTotalAmountByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindMonthlyTotalAmountByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantTotalAmountByIDStats(ctx context.Context, input model.FindYearMerchantByIDStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindYearlyTotalAmountByMerchants(ctx,	&statspb.FindYearMerchantById{Year: input.Year, MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

func (r *queryResolver) FindMonthlyMerchantTotalAmountByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindMonthlyTotalAmountByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapMonthAmounts(res.Data)}, nil
}

func (r *queryResolver) FindYearlyMerchantTotalAmountByApikeyStats(ctx context.Context, input model.FindYearMerchantByApikeyStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.MerchantStatsTotalAmount.FindYearlyTotalAmountByApikey(ctx, &statspb.FindYearMerchantByApikey{Year: input.Year, ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapYearAmounts(res.Data)}, nil
}

// ─── Merchant Transactions ────────────────────────────────────────────────────

func (r *queryResolver) FindMerchantTransactionsStats(ctx context.Context, input model.FindMerchantTransactionsInput) (*model.APIResponseMerchantTransactions, error) {
	res, err := r.StatsRead.MerchantTransaction.FindAllTransactionByMerchant(ctx,	&merchant.FindAllMerchantTransaction{MerchantId: input.MerchantID})
	if err != nil {
		return &model.APIResponseMerchantTransactions{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMerchantTransactions{Status: "success", Message: "ok", Data: mapMerchantTx(res.Data)}, nil
}

func (r *queryResolver) FindMerchantTransactionsByApikeyStats(ctx context.Context, input model.FindMerchantTransactionsByApikeyInput) (*model.APIResponseMerchantTransactions, error) {
	res, err := r.StatsRead.MerchantTransaction.FindAllTransactionByApikey(ctx, &merchant.FindAllMerchantApikey{ApiKey:	input.APIKey})
	if err != nil {
		return &model.APIResponseMerchantTransactions{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMerchantTransactions{Status: "success", Message: "ok", Data: mapMerchantTx(res.Data)}, nil
}

// ─── Saldo Stats ──────────────────────────────────────────────────────────────

func (r *queryResolver) FindMonthlySaldoBalanceStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyBalance, error) {
	res, err := r.StatsRead.SaldoStatsBalance.FindMonthlySaldoBalances(ctx, &statspb.FindYearlySaldo{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyBalance{Status: "success", Message: "ok", Data: mapSaldoMonthBalance(res.Data)}, nil
}

func (r *queryResolver) FindYearlySaldoBalanceStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyBalance, error) {
	res, err := r.StatsRead.SaldoStatsBalance.FindYearlySaldoBalances(ctx, &statspb.FindYearlySaldo{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyBalance{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyBalance{Status: "success", Message: "ok", Data: mapSaldoYearBalance(res.Data)}, nil
}

func (r *queryResolver) FindMonthlySaldoTotalStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseMonthlyAmount, error) {
	res, err := r.StatsRead.SaldoStatsTotal.FindMonthlyTotalSaldoBalance(ctx, &statspb.FindMonthlySaldoTotalBalance{Year: input.Year})
	if err != nil {
		return &model.APIResponseMonthlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseMonthlyAmount{Status: "success", Message: "ok", Data: mapSaldoMonthTotal(res.Data)}, nil
}

func (r *queryResolver) FindYearlySaldoTotalStats(ctx context.Context, input model.FindYearStatsInput) (*model.APIResponseYearlyAmount, error) {
	res, err := r.StatsRead.SaldoStatsTotal.FindYearTotalSaldoBalance(ctx, &statspb.FindYearlySaldo{Year: input.Year})
	if err != nil {
		return &model.APIResponseYearlyAmount{Status: "error", Message: err.Error()}, nil
	}
	return &model.APIResponseYearlyAmount{Status: "success", Message: "ok", Data: mapSaldoYearTotal(res.Data)}, nil
}

// ─── Stats Writer (stub) ─────────────────────────────────────────────────────

func (r *mutationResolver) WriteTopupEvent(ctx context.Context, input model.TopupEventInput) (*model.APIResponseVerifyCode, error) {
	return &model.APIResponseVerifyCode{Status: "success", Message: "not implemented"}, nil
}

func (r *mutationResolver) WriteTransactionEvent(ctx context.Context, input model.TransactionEventInput) (*model.APIResponseVerifyCode, error) {
	return &model.APIResponseVerifyCode{Status: "success", Message: "not implemented"}, nil
}

func (r *mutationResolver) WriteTransferEvent(ctx context.Context, input model.TransferEventInput) (*model.APIResponseVerifyCode, error) {
	return &model.APIResponseVerifyCode{Status: "success", Message: "not implemented"}, nil
}

func (r *mutationResolver) WriteSaldoEvent(ctx context.Context, input model.SaldoEventInput) (*model.APIResponseVerifyCode, error) {
	return &model.APIResponseVerifyCode{Status: "success", Message: "not implemented"}, nil
}

func (r *mutationResolver) WriteWithdrawEvent(ctx context.Context, input model.WithdrawEventInput) (*model.APIResponseVerifyCode, error) {
	return &model.APIResponseVerifyCode{Status: "success", Message: "not implemented"}, nil
}

// ─── Domain-specific mapper helpers ───────────────────────────────────────────

type topupMethodR interface { GetMonth() string; GetTopupMethod() string; GetTotalAmount() int64 }

type topupYearMethodR interface { GetYear() string; GetTopupMethod() string; GetTotalAmount() int64 }

type txMethodR interface { GetMonth() string; GetPaymentMethod() string; GetTotalAmount() int64 }

type txYearMethodR interface { GetYear() string; GetTotalAmount() int64 }

type merchantMethodR interface { GetMonth() string; GetPaymentMethod() string; GetTotalAmount() int64 }

type merchantYearMethodR interface { GetYear() string; GetTotalAmount() int64 }

type saldoMonthBalR interface { GetMonth() string; GetTotalBalance() int64 }

type saldoYearBalR interface { GetYear() string; GetTotalBalance() int64 }

func mapTopupMonthMethod[T topupMethodR](rows []T) []*model.MonthlyMethodResponse {
	out := make([]*model.MonthlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyMethodResponse{Month: r.GetMonth(), PaymentMethod: r.GetTopupMethod(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapTopupYearMethod[T topupYearMethodR](rows []T) []*model.YearlyMethodResponse {
	out := make([]*model.YearlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyMethodResponse{Year: r.GetYear(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapTxMonthMethod[T txMethodR](rows []T) []*model.MonthlyMethodResponse {
	out := make([]*model.MonthlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyMethodResponse{Month: r.GetMonth(), PaymentMethod: r.GetPaymentMethod(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapTxYearMethod[T txYearMethodR](rows []T) []*model.YearlyMethodResponse {
	out := make([]*model.YearlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyMethodResponse{Year: r.GetYear(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapMerchantMonthMethod[T merchantMethodR](rows []T) []*model.MonthlyMethodResponse {
	out := make([]*model.MonthlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyMethodResponse{Month: r.GetMonth(), PaymentMethod: r.GetPaymentMethod(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapMerchantYearMethod[T merchantYearMethodR](rows []T) []*model.YearlyMethodResponse {
	out := make([]*model.YearlyMethodResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyMethodResponse{Year: r.GetYear(), TotalAmount: int32(r.GetTotalAmount())})
	}
	return out
}

func mapSaldoMonthBalance[T saldoMonthBalR](rows []T) []*model.MonthlyBalanceResponse {
	out := make([]*model.MonthlyBalanceResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyBalanceResponse{Month: r.GetMonth(), TotalBalance: int32(r.GetTotalBalance())})
	}
	return out
}

func mapSaldoYearBalance[T saldoYearBalR](rows []T) []*model.YearlyBalanceResponse {
	out := make([]*model.YearlyBalanceResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyBalanceResponse{Year: r.GetYear(), TotalBalance: int32(r.GetTotalBalance())})
	}
	return out
}

func mapSaldoMonthTotal[T saldoMonthBalR](rows []T) []*model.MonthlyAmountResponse {
	out := make([]*model.MonthlyAmountResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.MonthlyAmountResponse{Month: r.GetMonth(), TotalAmount: int32(r.GetTotalBalance())})
	}
	return out
}

func mapSaldoYearTotal[T saldoYearBalR](rows []T) []*model.YearlyAmountResponse {
	out := make([]*model.YearlyAmountResponse, 0, len(rows))
	for _, r := range rows {
		out = append(out, &model.YearlyAmountResponse{Year: r.GetYear(), TotalAmount: int32(r.GetTotalBalance())})
	}
	return out
}
