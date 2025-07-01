package main

import (
	"context"
	"net"
	"net/http"
	"websocket-server/external"
	"websocket-server/internal/config"
	"websocket-server/internal/middleware"
	"websocket-server/internal/server"
	"websocket-server/internal/service"
	"websocket-server/pkg/logger"
	pb "websocket-server/proto"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	echoMiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 설정 로드
	cfg := config.LoadConfig()

	// 로거 초기화
	log := logger.InitLogger(cfg)

	// Auth Client 초기화
	authConn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to auth service")
	}
	defer authConn.Close()

	authClient := external.NewAuthClient(authConn)

	// 웹소켓 서비스 생성
	wsService := service.NewWebSocketService(log)
	go wsService.StartBroadcasting()

	// gRPC 서버 생성
	grpcServer := server.NewGRPCServer(log, wsService)
	grpcListener, err := net.Listen("tcp", ":"+cfg.Server.GRPCPort)
	if err != nil {
		log.WithError(err).Fatal("Failed to listen gRPC")
	}
	go func() {
		log.WithField("port", cfg.Server.GRPCPort).Info("Starting gRPC server")
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.WithError(err).Fatal("Failed to serve gRPC")
		}
	}()

	// gRPC-Gateway 서버 생성
	ctx := context.Background()
	gatewayMux, err := server.NewGatewayServer(ctx, log, "localhost:"+cfg.Server.GRPCPort)
	if err != nil {
		log.WithError(err).Fatal("Failed to create gateway server")
	}

	// Echo 인스턴스 생성
	e := echo.New()

	// 미들웨어 설정
	e.Use(echoMiddleware.Logger())
	e.Use(echoMiddleware.Recover())
	e.Use(echoMiddleware.CORSWithConfig(echoMiddleware.CORSConfig{
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
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return next(c)
		}
	})

	// 인증된 웹소켓 핸들러 등록 (함수형 스타일)
	authenticatedHandler := func(conn *websocket.Conn, passport *pb.Passport) {
		log.WithFields(logrus.Fields{
			"user_id": passport.Id,
			"role":    passport.Role,
		}).Info("Authenticated WebSocket connection established")

		wsService.HandleClient(conn)
	}

	// 인증 미들웨어를 사용한 웹소켓 핸들러
	wsHandler := middleware.WithAuth(authClient, log, authenticatedHandler)
	e.GET("/ws", echo.WrapHandler(http.HandlerFunc(wsHandler)))

	// gRPC-Gateway 핸들러 등록
	e.Any("/v1/*", echo.WrapHandler(gatewayMux))

	// 서버 시작
	log.WithField("port", cfg.Server.Port).Info("Starting HTTP server")
	e.Logger.Fatal(e.Start(":" + cfg.Server.Port))
}
