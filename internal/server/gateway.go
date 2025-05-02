package server

import (
	"context"
	"websocket-server/api/proto"
	"websocket-server/pkg/logger"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewGatewayServer(ctx context.Context, logger *zap.Logger, grpcAddr string) (*runtime.ServeMux, error) {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// gRPC 서비스 등록
	err := proto.RegisterWebSocketServiceHandlerFromEndpoint(
		ctx,
		mux,
		grpcAddr,
		opts,
	)
	if err != nil {
		logger.Error("Failed to register gateway", zap.Error(err))
		return nil, err
	}

	return mux, nil
} 