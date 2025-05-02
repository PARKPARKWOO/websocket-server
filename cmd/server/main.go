package main

import (
	"context"
	"net"
	"net/http"
	"websocket-server/internal/config"
	"websocket-server/internal/handler"
	"websocket-server/internal/server"
	"websocket-server/internal/service"
	"websocket-server/pkg/logger"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/cors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	// 로거 초기화
	log := logger.InitLogger()
	defer log.Sync()

	// 설정 로드
	cfg := config.LoadConfig()

	// 웹소켓 서비스 생성
	wsService := service.NewWebSocketService(log)
	go wsService.StartBroadcasting()

	// gRPC 서버 생성
	grpcServer := server.NewGRPCServer(log, wsService)
	grpcListener, err := net.Listen("tcp", ":"+cfg.Server.GRPCPort)
	if err != nil {
		log.Fatal("Failed to listen gRPC", zap.Error(err))
	}
	go func() {
		log.Info("Starting gRPC server", zap.String("port", cfg.Server.GRPCPort))
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Fatal("Failed to serve gRPC", zap.Error(err))
		}
	}()

	// gRPC-Gateway 서버 생성
	ctx := context.Background()
	gatewayMux, err := server.NewGatewayServer(ctx, log, "localhost:"+cfg.Server.GRPCPort)
	if err != nil {
		log.Fatal("Failed to create gateway server", zap.Error(err))
	}

	// Echo 인스턴스 생성
	e := echo.New()

	// 미들웨어 설정
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET, echo.POST, echo.PUT, echo.DELETE},
	}))

	// CORS 설정
	corsMiddleware := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})
	e.Use(corsMiddleware.Handler)

	// 웹소켓 핸들러 등록
	wsHandler := handler.NewWebSocketHandler(log, wsService)
	e.GET("/ws", wsHandler.HandleWebSocket)

	// gRPC-Gateway 핸들러 등록
	e.Any("/v1/*", echo.WrapHandler(gatewayMux))

	// 서버 시작
	log.Info("Starting HTTP server", zap.String("port", cfg.Server.Port))
	e.Logger.Fatal(e.Start(":" + cfg.Server.Port))
} 