package service

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	externalClient "websocket-server/external"

	"github.com/gorilla/websocket"
)

type WebSocketService struct {
	upgrader   websocket.Upgrader
	rooms      map[string]*Room
	roomsMux   sync.RWMutex
	clients    map[string]map[*Client]bool // userId -> clients (한 유저가 여러 기기에서 접속 가능)
	clientsMux sync.RWMutex
}

type Room struct {
	ID      string
	Clients map[*Client]bool
	mux     sync.RWMutex
}

type Client struct {
	Conn    *websocket.Conn
	UserID  string
	Rooms   map[string]bool // 참여 중인 방 목록
	Send    chan []byte
	service *WebSocketService
	roomMux sync.RWMutex
}

// ClientAction 클라이언트에서 보내는 메시지 형식
type ClientAction struct {
	Action      string `json:"action"`      // join_room, leave_room, chat (생략 시 chat)
	RoomId      string `json:"roomId"`
	Payload     string `json:"payload"`
	MessageType string `json:"messageType"`
	Sender      string `json:"sender"`
}

func NewWebSocketService() *WebSocketService {
	return &WebSocketService{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		rooms:   make(map[string]*Room),
		clients: make(map[string]map[*Client]bool),
	}
}

// ── Room 관리 ──

func (ws *WebSocketService) getOrCreateRoom(roomID string) *Room {
	ws.roomsMux.Lock()
	defer ws.roomsMux.Unlock()

	if room, exists := ws.rooms[roomID]; exists {
		return room
	}

	room := &Room{
		ID:      roomID,
		Clients: make(map[*Client]bool),
	}
	ws.rooms[roomID] = room
	log.Printf("새로운 방 생성: %s", roomID)
	return room
}

func (ws *WebSocketService) removeRoom(roomID string) {
	ws.roomsMux.Lock()
	defer ws.roomsMux.Unlock()

	if room, exists := ws.rooms[roomID]; exists && len(room.Clients) == 0 {
		delete(ws.rooms, roomID)
		log.Printf("빈 방 제거: %s", roomID)
	}
}

// ── 클라이언트 관리 ──

func (ws *WebSocketService) registerClient(client *Client) {
	ws.clientsMux.Lock()
	defer ws.clientsMux.Unlock()

	if ws.clients[client.UserID] == nil {
		ws.clients[client.UserID] = make(map[*Client]bool)
	}
	ws.clients[client.UserID][client] = true
	log.Printf("클라이언트 등록: UserID=%s (연결 수: %d)", client.UserID, len(ws.clients[client.UserID]))
}

func (ws *WebSocketService) unregisterClient(client *Client) {
	// 참여 중인 모든 방에서 제거
	client.roomMux.RLock()
	rooms := make([]string, 0, len(client.Rooms))
	for roomID := range client.Rooms {
		rooms = append(rooms, roomID)
	}
	client.roomMux.RUnlock()

	for _, roomID := range rooms {
		ws.leaveRoom(client, roomID)
	}

	// 클라이언트 인덱스에서 제거
	ws.clientsMux.Lock()
	if userClients, exists := ws.clients[client.UserID]; exists {
		delete(userClients, client)
		if len(userClients) == 0 {
			delete(ws.clients, client.UserID)
		}
	}
	ws.clientsMux.Unlock()

	close(client.Send)
	client.Conn.Close()
	log.Printf("클라이언트 연결 해제: UserID=%s", client.UserID)
}

// ── 방 참가/퇴장 ──

func (ws *WebSocketService) joinRoom(client *Client, roomID string) {
	room := ws.getOrCreateRoom(roomID)

	room.mux.Lock()
	room.Clients[client] = true
	count := len(room.Clients)
	room.mux.Unlock()

	client.roomMux.Lock()
	client.Rooms[roomID] = true
	client.roomMux.Unlock()

	log.Printf("사용자 %s가 방 %s에 참가 (현재 인원: %d)", client.UserID, roomID, count)
}

func (ws *WebSocketService) leaveRoom(client *Client, roomID string) {
	ws.roomsMux.RLock()
	room, exists := ws.rooms[roomID]
	ws.roomsMux.RUnlock()

	if !exists {
		return
	}

	room.mux.Lock()
	delete(room.Clients, client)
	count := len(room.Clients)
	room.mux.Unlock()

	client.roomMux.Lock()
	delete(client.Rooms, roomID)
	client.roomMux.Unlock()

	log.Printf("사용자 %s가 방 %s에서 퇴장 (현재 인원: %d)", client.UserID, roomID, count)

	if count == 0 {
		ws.removeRoom(roomID)
	}
}

// ── 메시지 전송 ──

func (ws *WebSocketService) broadcastToRoom(roomID string, message []byte) {
	ws.roomsMux.RLock()
	room, exists := ws.rooms[roomID]
	ws.roomsMux.RUnlock()

	if !exists {
		return
	}

	room.mux.RLock()
	clients := make([]*Client, 0, len(room.Clients))
	for client := range room.Clients {
		clients = append(clients, client)
	}
	room.mux.RUnlock()

	for _, client := range clients {
		select {
		case client.Send <- message:
		default:
			client.Conn.Close()
		}
	}
}

// BroadcastToRoom Redis에서 받은 메시지를 특정 방의 모든 클라이언트에게 브로드캐스트
func (ws *WebSocketService) BroadcastToRoom(roomID string, message []byte) {
	ws.broadcastToRoom(roomID, message)
}

// SendToUser 특정 userId를 가진 모든 클라이언트에게 메시지 전송
func (ws *WebSocketService) SendToUser(userID string, message []byte) {
	ws.clientsMux.RLock()
	userClients, exists := ws.clients[userID]
	if !exists {
		ws.clientsMux.RUnlock()
		log.Printf("사용자 %s가 연결되어 있지 않음", userID)
		return
	}

	clients := make([]*Client, 0, len(userClients))
	for client := range userClients {
		clients = append(clients, client)
	}
	ws.clientsMux.RUnlock()

	for _, client := range clients {
		select {
		case client.Send <- message:
		default:
			client.Conn.Close()
		}
	}
	log.Printf("사용자 %s에게 알림 전송 완료 (%d개 연결)", userID, len(clients))
}

// ── WebSocket 연결 핸들러 ──

// HandleConnections WebSocket 연결 처리
// chat_room_id가 있으면 해당 방에 자동 참가 (하위호환)
func (ws *WebSocketService) HandleConnections(
	writer http.ResponseWriter,
	r *http.Request,
	authClient *externalClient.AuthClient,
	kafkaService *KafkaService,
	chatService *ChatService,
) {
	bearerToken, err := GetBearerToken(r)
	if err != nil {
		http.Error(writer, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	passport, err := authClient.GetPassportByBearerWithTimeout(bearerToken, 5*time.Second)
	if err != nil {
		log.Printf("토큰 인증 실패: %v\n", err)
		http.Error(writer, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	log.Printf("사용자 인증 성공: UserID=%s, Role=%s\n", passport.Id, passport.Role)

	conn, err := ws.upgrader.Upgrade(writer, r, nil)
	if err != nil {
		log.Println("WebSocket 업그레이드 실패:", err)
		return
	}

	client := &Client{
		Conn:    conn,
		UserID:  passport.Id,
		Rooms:   make(map[string]bool),
		Send:    make(chan []byte, 256),
		service: ws,
	}

	ws.registerClient(client)

	// chat_room_id가 있으면 자동 참가 (하위호환)
	chatRoomId := r.URL.Query().Get("chat_room_id")
	if chatRoomId != "" {
		ws.joinRoom(client, chatRoomId)
	}

	go client.writePump()
	go func() {
		defer ws.unregisterClient(client)
		client.readPump(kafkaService, chatService)
	}()
}

// ── Read/Write Pump ──

func (c *Client) readPump(kafkaService *KafkaService, chatService *ChatService) {
	c.Conn.SetReadLimit(4096)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, p, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("클라이언트 %s 예상치 못한 연결 종료: %v", c.UserID, err)
			}
			break
		}

		var action ClientAction
		if err := json.Unmarshal(p, &action); err != nil {
			log.Printf("JSON 파싱 실패: %v", err)
			continue
		}

		switch action.Action {
		case "join_room":
			if action.RoomId == "" {
				// roomId 없으면 새 채팅방 생성
				if chatService != nil {
					roomId, err := chatService.CreateChatRoom(c.UserID)
					if err != nil {
						log.Printf("채팅방 생성 실패: %v", err)
						continue
					}
					action.RoomId = roomId
				} else {
					continue
				}
			}
			c.service.joinRoom(c, action.RoomId)
			// 참가 확인 메시지 전송
			ack, _ := json.Marshal(map[string]string{
				"action": "joined",
				"roomId": action.RoomId,
			})
			c.Send <- ack

		case "leave_room":
			if action.RoomId != "" {
				c.service.leaveRoom(c, action.RoomId)
			}

		default:
			// chat 메시지 → Kafka 발행
			if kafkaService == nil {
				continue
			}
			kafkaMsg := KafkaMessage{
				Sender:      c.UserID,
				Payload:     action.Payload,
				MessageType: action.MessageType,
				RoomId:      action.RoomId,
			}
			if kafkaMsg.RoomId == "" {
				// 첫 번째 참가 방을 기본값으로
				c.roomMux.RLock()
				for roomID := range c.Rooms {
					kafkaMsg.RoomId = roomID
					break
				}
				c.roomMux.RUnlock()
			}
			if kafkaMsg.RoomId == "" {
				continue
			}
			if err := kafkaService.PublishMessage(context.Background(), kafkaMsg); err != nil {
				log.Printf("Kafka 메시지 발행 실패: %v", err)
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// ── 통계/기타 ──

func (ws *WebSocketService) GetRoomStats() map[string]int {
	ws.roomsMux.RLock()
	defer ws.roomsMux.RUnlock()

	stats := make(map[string]int)
	for roomID, room := range ws.rooms {
		room.mux.RLock()
		stats[roomID] = len(room.Clients)
		room.mux.RUnlock()
	}
	return stats
}

func (ws *WebSocketService) HandlePrivateNetworkConnection(writer http.ResponseWriter, r *http.Request) {
	conn, err := ws.upgrader.Upgrade(writer, r, nil)
	if err != nil {
		log.Printf("upgrade error")
		return
	}
	defer conn.Close()

	for {
		_, p, err := conn.ReadMessage()
		if err != nil {
			log.Printf("error %v\n", err)
			break
		}
		log.Println(p)
	}
}
