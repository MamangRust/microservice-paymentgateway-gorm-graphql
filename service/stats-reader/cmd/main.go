package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	pbStats "github.com/MamangRust/microservice-payment-gateway-grpc/pb/stats"
	pbCardBase "github.com/MamangRust/microservice-payment-gateway-grpc/pb/card"
	pbMerchantBase "github.com/MamangRust/microservice-payment-gateway-grpc/pb/merchant"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/clickhouse"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/dotenv"
	"github.com/MamangRust/microservice-payment-gateway-grpc/pkg/logger"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/handler"
	"github.com/MamangRust/microservice-payment-gateway-grpc/service/stats-reader/repository"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := dotenv.Viper(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load configuration: %v\n", err)
	}
	log, _ := logger.NewLogger("stats-reader", nil)
	
	chConn, err := clickhouse.NewClient(log)
	if err != nil {
		log.Fatal("Failed to connect to ClickHouse", zap.Error(err))
	}

	repo := repository.NewClickHouseReaderRepository(chConn)
	cardStatsHandler := handler.NewCardStatsHandler(repo, log)
	merchantStatsHandler := handler.NewMerchantStatsHandler(repo, log)
	saldoStatsHandler := handler.NewSaldoStatsHandler(repo, log)
	topupStatsHandler := handler.NewTopupStatsHandler(repo, log)
	transactionStatsHandler := handler.NewTransactionStatsHandler(repo, log)
	transferStatsHandler := handler.NewTransferStatsHandler(repo, log)
	withdrawStatsHandler := handler.NewWithdrawStatsHandler(repo, log)

	grpcServer := grpc.NewServer()
	
	pbStats.RegisterCardStatsBalanceServiceServer(grpcServer, cardStatsHandler)
	pbStats.RegisterCardStatsTopupServiceServer(grpcServer, cardStatsHandler)
	pbStats.RegisterCardStatsTransactionServiceServer(grpcServer, cardStatsHandler)
	pbStats.RegisterCardStatsTransferServiceServer(grpcServer, cardStatsHandler)
	pbStats.RegisterCardStatsWithdrawServiceServer(grpcServer, cardStatsHandler)
	pbCardBase.RegisterCardDashboardServiceServer(grpcServer, cardStatsHandler)

	pbStats.RegisterMerchantStatsAmountServiceServer(grpcServer, merchantStatsHandler)
	pbStats.RegisterMerchantStatsMethodServiceServer(grpcServer, merchantStatsHandler)
	pbStats.RegisterMerchantStatsTotalAmountServiceServer(grpcServer, merchantStatsHandler)
	pbMerchantBase.RegisterMerchantTransactionServiceServer(grpcServer, merchantStatsHandler)

	pbStats.RegisterSaldoStatsBalanceServiceServer(grpcServer, saldoStatsHandler)
	pbStats.RegisterSaldoStatsTotalBalanceServer(grpcServer, saldoStatsHandler)

	pbStats.RegisterTopupStatsAmountServiceServer(grpcServer, topupStatsHandler)
	pbStats.RegisterTopupStatsMethodServiceServer(grpcServer, topupStatsHandler)
	pbStats.RegisterTopupStatsStatusServiceServer(grpcServer, topupStatsHandler)

	pbStats.RegisterTransactionStatsAmountServiceServer(grpcServer, transactionStatsHandler)
	pbStats.RegisterTransactionStatsMethodServiceServer(grpcServer, transactionStatsHandler)
	pbStats.RegisterTransactionStatsStatusServiceServer(grpcServer, transactionStatsHandler)

	pbStats.RegisterTransferStatsAmountServiceServer(grpcServer, transferStatsHandler)
	pbStats.RegisterTransferStatsStatusServiceServer(grpcServer, transferStatsHandler)

	pbStats.RegisterWithdrawStatsAmountServiceServer(grpcServer, withdrawStatsHandler)
	pbStats.RegisterWithdrawStatsStatusServiceServer(grpcServer, withdrawStatsHandler)

	reflection.Register(grpcServer)

	port := ":50062"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Failed to listen", zap.Error(err))
	}

	log.Info("Stats Reader starting", zap.String("port", port))

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down Stats Reader...")
	grpcServer.GracefulStop()
}
