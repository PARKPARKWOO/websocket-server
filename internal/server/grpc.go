package server

import (
	"context"
	"time"
	"websocket-server/api/proto"
	"websocket-server/internal/service"
	"websocket-server/pkg/logger"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type grpcServer struct {
	proto.UnimplementedWebSocketServiceServer
	logger  *zap.Logger
	service *service.WebSocketService
}

func NewGRPCServer(logger *zap.Logger, wsService *service.WebSocketService) *grpc.Server {
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
	msg := service.Message{
		Type:    req.Type,
		Payload: req.Payload,
		From:    req.From,
		To:      req.To,
	}
	s.service.Broadcast <- msg

	return response, nil
}

func (s *grpcServer) Subscribe(req *proto.SubscribeRequest, stream proto.WebSocketService_SubscribeServer) error {
	// 구독 처리 로직
	for {
		select {
		case msg := <-s.service.Broadcast:
			response := &proto.MessageResponse{
				Type:      msg.Type,
				Payload:   msg.Payload.(string),
				From:      msg.From,
				To:        msg.To,
				Timestamp: time.Now().Format(time.RFC3339),
			}
			if err := stream.Send(response); err != nil {
				s.logger.Error("Failed to send message", zap.Error(err))
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
} 