package service

import (
	"encoding/json"
	"websocket-server/internal/model"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

type WebSocketService struct {
	logger    *logrus.Logger
	clients   map[*websocket.Conn]bool
	broadcast chan model.Message
}

func NewWebSocketService(logger *logrus.Logger) *WebSocketService {
	return &WebSocketService{
		logger:    logger,
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan model.Message),
	}
}

// HandleClient 함수형 스타일로 클라이언트 처리
func (s *WebSocketService) HandleClient(conn *websocket.Conn) {
	// 클라이언트 등록
	s.registerClient(conn)
	defer s.unregisterClient(conn)

	// 메시지 처리 루프
	s.messageLoop(conn)
}

// registerClient 클라이언트 등록
func (s *WebSocketService) registerClient(conn *websocket.Conn) {
	s.clients[conn] = true
	s.logger.WithField("client_count", len(s.clients)).Info("Client connected")
}

// unregisterClient 클라이언트 해제
func (s *WebSocketService) unregisterClient(conn *websocket.Conn) {
	delete(s.clients, conn)
	conn.Close()
	s.logger.WithField("client_count", len(s.clients)).Info("Client disconnected")
}

// messageLoop 메시지 처리 루프
func (s *WebSocketService) messageLoop(conn *websocket.Conn) {
	for {
		var msg model.Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			s.logger.WithError(err).Error("Error reading message")
			break
		}

		// 메시지 브로드캐스트
		s.broadcast <- msg
	}
}

// StartBroadcasting 브로드캐스팅 시작
func (s *WebSocketService) StartBroadcasting() {
	for {
		msg := <-s.broadcast
		s.broadcastMessage(msg)
	}
}

// broadcastMessage 메시지 브로드캐스트
func (s *WebSocketService) broadcastMessage(msg model.Message) {
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		s.logger.WithError(err).Error("Error marshaling message")
		return
	}

	// 모든 클라이언트에게 메시지 전송
	for client := range s.clients {
		err := client.WriteMessage(websocket.TextMessage, messageBytes)
		if err != nil {
			s.logger.WithError(err).Error("Error sending message")
			client.Close()
			delete(s.clients, client)
		}
	}
}

// GetClientCount 클라이언트 수 반환
func (s *WebSocketService) GetClientCount() int {
	return len(s.clients)
}

// SendToClient 특정 클라이언트에게 메시지 전송
func (s *WebSocketService) SendToClient(conn *websocket.Conn, msg model.Message) error {
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return conn.WriteMessage(websocket.TextMessage, messageBytes)
}
