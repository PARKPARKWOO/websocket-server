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
	upgrader websocket.Upgrader
	// RoomId 기반 세션 관리
	rooms    map[string]*Room
	roomsMux sync.RWMutex
}

type Room struct {
	ID      string
	Clients map[*Client]bool
	mux     sync.RWMutex
}

type Client struct {
	Conn    *websocket.Conn
	UserID  string
	RoomID  string
	Send    chan []byte
	service *WebSocketService
}

type ClientMessage struct {
	Payload     string `json:"payload"`
	MessageType string `json:"messageType"`
}

// NewWebSocketService WebSocket 서비스 생성자
func NewWebSocketService() *WebSocketService {
	return &WebSocketService{
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		rooms: make(map[string]*Room),
	}
}

// getOrCreateRoom 방을 가져오거나 생성
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

// removeRoom 방 제거 (사용자가 없을 때)
func (ws *WebSocketService) removeRoom(roomID string) {
	ws.roomsMux.Lock()
	defer ws.roomsMux.Unlock()

	if room, exists := ws.rooms[roomID]; exists && len(room.Clients) == 0 {
		delete(ws.rooms, roomID)
		log.Printf("빈 방 제거: %s", roomID)
	}
}

// broadcastToRoom 특정 방의 모든 클라이언트에게 메시지 전송
func (ws *WebSocketService) broadcastToRoom(roomID string, message []byte) {
	ws.roomsMux.RLock()
	room, exists := ws.rooms[roomID]
	ws.roomsMux.RUnlock()

	if !exists {
		log.Printf("방을 찾을 수 없음: %s", roomID)
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
			// 클라이언트가 메시지를 받을 수 없는 경우 연결 종료
			client.Conn.Close()
		}
	}
}

// HandleConnections WebSocket 연결 처리 및 Kafka로 메시지 발행
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

	chatRoomId := r.URL.Query().Get("chat_room_id")
	if chatRoomId == "" {
		var err error
		chatRoomId, err = chatService.CreateChatRoom(passport.Id)
		if err != nil {
			log.Printf("채팅방 생성 실패: %v\n", err)
			http.Error(writer, "Internal Server Error: Failed to create chat room", http.StatusInternalServerError)
			return
		}
		log.Printf("새로운 채팅방 생성: %s\n", chatRoomId)
	} else {
		log.Printf("기존 채팅방 사용: %s\n", chatRoomId)
	}

	conn, err := ws.upgrader.Upgrade(writer, r, nil)
	if err != nil {
		log.Println("WebSocket 업그레이드 실패:", err)
		return
	}

	// 클라이언트 생성
	client := &Client{
		Conn:    conn,
		UserID:  passport.Id,
		RoomID:  chatRoomId,
		Send:    make(chan []byte, 256),
		service: ws,
	}

	// 방에 클라이언트 추가
	room := ws.getOrCreateRoom(chatRoomId)
	room.mux.Lock()
	room.Clients[client] = true
	room.mux.Unlock()

	log.Printf("사용자 %s가 방 %s에 참가했습니다. (현재 인원: %d)", passport.Id, chatRoomId, len(room.Clients))

	// 클라이언트 처리 고루틴 시작
	go client.writePump()
	client.readPump(kafkaService)

	// 연결 종료 시 정리
	defer func() {
		// 클라이언트 채널 닫기
		close(client.Send)

		// 방에서 클라이언트 제거
		room.mux.Lock()
		delete(room.Clients, client)
		clientCount := len(room.Clients)
		room.mux.Unlock()

		// 연결 종료
		conn.Close()

		log.Printf("사용자 %s가 방 %s에서 나갔습니다. (현재 인원: %d)", passport.Id, chatRoomId, clientCount)

		// 방이 비어있으면 제거
		if clientCount == 0 {
			ws.removeRoom(chatRoomId)
		}
	}()
}

// readPump 클라이언트로부터 메시지 읽기
func (c *Client) readPump(kafkaService *KafkaService) {
	defer func() {
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(512)
	c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, p, err := c.Conn.ReadMessage()
		if err != nil {
			// 정상적인 연결 종료인지 확인
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("클라이언트 %s 예상치 못한 연결 종료: %v", c.UserID, err)
			} else {
				log.Printf("클라이언트 %s 연결 종료: %v", c.UserID, err)
			}
			break
		}

		var kafkaMsg KafkaMessage
		if err := json.Unmarshal(p, &kafkaMsg); err != nil {
			log.Printf("JSON 언마샬링 실패: %v\n", err)
			continue
		}

		if kafkaMsg.Sender == "" {
			kafkaMsg.Sender = c.UserID
		}
		if kafkaMsg.RoomId == "" {
			kafkaMsg.RoomId = c.RoomID
		}

		err = kafkaService.PublishMessage(context.Background(), kafkaMsg)
		if err != nil {
			log.Printf("Kafka 메시지 발행 실패: %v\n", err)
		}
	}
}

// writePump 클라이언트에게 메시지 쓰기
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
				// 채널이 닫혔으면 정상적인 종료 메시지 전송
				c.Conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}

			w, err := c.Conn.NextWriter(websocket.TextMessage)
			if err != nil {
				log.Printf("클라이언트 %s 메시지 쓰기 실패: %v", c.UserID, err)
				return
			}
			w.Write(message)

			if err := w.Close(); err != nil {
				log.Printf("클라이언트 %s 메시지 전송 완료 실패: %v", c.UserID, err)
				return
			}
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("클라이언트 %s Ping 전송 실패: %v", c.UserID, err)
				return
			}
		}
	}
}

// BroadcastToRoom Redis에서 받은 메시지를 특정 방의 모든 클라이언트에게 브로드캐스트
func (ws *WebSocketService) BroadcastToRoom(roomID string, message []byte) {
	ws.broadcastToRoom(roomID, message)
}

// GetRoomStats 방별 통계 정보 반환
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

// HandlePrivateNetworkConnection 프라이빗 네트워크 연결 처리
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
