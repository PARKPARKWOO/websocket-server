package service

import (
	"encoding/json"
	"websocket-server/internal/model"
	"websocket-server/pkg/logger"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type WebSocketService struct {
	logger    *zap.Logger
	clients   map[*websocket.Conn]bool
	broadcast chan model.Message
}

func NewWebSocketService(logger *zap.Logger) *WebSocketService {
	return &WebSocketService{
		logger:    logger,
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan model.Message),
	}
}

func (s *WebSocketService) HandleClient(conn *websocket.Conn) {
	// 클라이언트 등록
	s.clients[conn] = true
	defer func() {
		delete(s.clients, conn)
		conn.Close()
	}()

	// 메시지 수신 루프
	for {
		var msg model.Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			s.logger.Error("Error reading message", zap.Error(err))
			break
		}

		// 메시지 브로드캐스트
		s.broadcast <- msg
	}
}

func (s *WebSocketService) StartBroadcasting() {
	for {
		msg := <-s.broadcast
		messageBytes, err := json.Marshal(msg)
		if err != nil {
			s.logger.Error("Error marshaling message", zap.Error(err))
			continue
		}

		// 모든 클라이언트에게 메시지 전송
		for client := range s.clients {
			err := client.WriteMessage(websocket.TextMessage, messageBytes)
			if err != nil {
				s.logger.Error("Error sending message", zap.Error(err))
				client.Close()
				delete(s.clients, client)
			}
		}
	}
} 