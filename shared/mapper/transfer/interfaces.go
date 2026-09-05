package transferapimapper

import (
	pb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/transfer"
	statspb "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	"github.com/MamangRust/microservice-payment-gateway-grpc/shared/domain/response"
)

type TransferBaseResponseMapper interface {
	// Converts a single transfer response into an API response.
	ToApiResponseTransfer(pbResponse *pb.ApiResponseTransfer) *response.ApiResponseTransfer
}

type TransferQueryResponseMapper interface {
	TransferBaseResponseMapper

	// Converts a list of transfers into a grouped API response.
	ToApiResponseTransfers(pbResponse *pb.ApiResponseTransfers) *response.ApiResponseTransfers

	// Converts paginated transfer records into an API response.
	ToApiResponsePaginationTransfer(pbResponse *pb.ApiResponsePaginationTransfer) *response.ApiResponsePaginationTransfer

	// Converts paginated soft-deleted transfer records into an API response.
	ToApiResponsePaginationTransferDeleteAt(pbResponse *pb.ApiResponsePaginationTransferDeleteAt) *response.ApiResponsePaginationTransferDeleteAt
}

type TransferCommandResponseMapper interface {
	TransferBaseResponseMapper

	ToApiResponseTransferDeleteAt(pbResponse *pb.ApiResponseTransferDeleteAt) *response.ApiResponseTransferDeleteAt

	// Converts a deleted transfer response into an API response.
	ToApiResponseTransferDelete(pbResponse *pb.ApiResponseTransferDelete) *response.ApiResponseTransferDelete

	// Converts all transfer records into an API response.
	ToApiResponseTransferAll(pbResponse *pb.ApiResponseTransferAll) *response.ApiResponseTransferAll
}

type TransferStatsStatusResponseMapper interface {
	// Converts monthly successful transfer statistics into an API response.
	ToApiResponseTransferMonthStatusSuccess(pbResponse *statspb.ApiResponseTransferMonthStatusSuccess) *response.ApiResponseTransferMonthStatusSuccess

	// Converts yearly successful transfer statistics into an API response.
	ToApiResponseTransferYearStatusSuccess(pbResponse *statspb.ApiResponseTransferYearStatusSuccess) *response.ApiResponseTransferYearStatusSuccess

	// Converts monthly failed transfer statistics into an API response.
	ToApiResponseTransferMonthStatusFailed(pbResponse *statspb.ApiResponseTransferMonthStatusFailed) *response.ApiResponseTransferMonthStatusFailed

	// Converts yearly failed transfer statistics into an API response.
	ToApiResponseTransferYearStatusFailed(pbResponse *statspb.ApiResponseTransferYearStatusFailed) *response.ApiResponseTransferYearStatusFailed
}

type TransferStatsAmountResponseMapper interface {
	// Converts monthly total transfer amount statistics into an API response.
	ToApiResponseTransferMonthAmount(pbResponse *statspb.ApiResponseTransferMonthAmount) *response.ApiResponseTransferMonthAmount

	// Converts yearly total transfer amount statistics into an API response.
	ToApiResponseTransferYearAmount(pbResponse *statspb.ApiResponseTransferYearAmount) *response.ApiResponseTransferYearAmount
}
