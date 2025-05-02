package handler

import (
	"websocket-server/internal/service"
	"websocket-server/pkg/logger"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type WebSocketHandler struct {
	logger  *zap.Logger
	service *service.WebSocketService
}

func NewWebSocketHandler(logger *zap.Logger) *WebSocketHandler {
	return &WebSocketHandler{
		logger:  logger,
		service: service.NewWebSocketService(logger),
	}
}

func (h *WebSocketHandler) HandleWebSocket(c echo.Context) error {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		h.logger.Error("Failed to upgrade connection", zap.Error(err))
		return err
	}
	defer ws.Close()

	// 클라이언트 연결 처리
	h.service.HandleClient(ws)

	return nil
} 