package server

import (
	"context"
	"time"
	"websocket-server/api/proto"
	"websocket-server/internal/model"
	"websocket-server/internal/service"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type grpcServer struct {
	proto.UnimplementedWebSocketServiceServer
	logger  *logrus.Logger
	service *service.WebSocketService
}

func NewGRPCServer(logger *logrus.Logger, wsService *service.WebSocketService) *grpc.Server {
	srv := &grpcServer{
		logger:  logger,
		service: wsService,
	}

	grpcServer := grpc.NewServer()
	proto.RegisterWebSocketServiceServer(grpcServer, srv)
	return grpcServer
}

func (s *grpcServer) SendMessage(ctx context.Context, req *proto.MessageRequest) (*proto.MessageResponse, error) {
	// 메시지 처리 로직
	response := &proto.MessageResponse{
		Type:      req.Type,
		Payload:   req.Payload,
		From:      req.From,
		To:        req.To,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// 웹소켓 서비스로 메시지 전달
	msg := model.Message{
		Type:    req.Type,
		Payload: req.Payload,
		From:    req.From,
		To:      req.To,
	}

	// 브로드캐스트 채널에 메시지 전송
	go func() {
		// 실제 구현에서는 서비스에 메시지를 전달하는 메서드가 필요
		s.logger.WithField("message_type", msg.Type).Info("Received message from gRPC")
	}()

	return response, nil
}

func (s *grpcServer) Subscribe(req *proto.SubscribeRequest, stream proto.WebSocketService_SubscribeServer) error {
	// 구독 처리 로직
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			// 실제 구현에서는 메시지 스트리밍 로직이 필요
			time.Sleep(1 * time.Second)
		}
	}
}
