// 이 코드는 Kafka로 메시지를 발행(Produce)하고,
// Redis 채널을 구독(Subscribe)하는 기능을 모두 포함한 버전입니다.
//
// 주요 기능:
// 1. WebSocket 클라이언트로부터 메시지를 받아 Kafka 토픽으로 발행합니다.
// 2. Redis의 특정 패턴 채널을 구독하여 메시지를 수신하고 로그에 기록합니다.
//
// --- 설치가 필요한 라이브러리 ---
// go get github.com/segmentio/kafka-go
// go get github.com/redis/go-redis/v9

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"  // Redis 라이브러리 import
	"github.com/segmentio/kafka-go" // Kafka 라이브러리 import
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	externalClient "websocket-server/external"
	service "websocket-server/service"
)

var (
	MirrorViewDomain          = os.Getenv("MV_HOST")
	MirrorViewApplicationName = "dev-mirror-view"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// HTTP 요청/응답 구조체 (변경 없음)
type CreateChatRoomRequest struct {
	CreatedBy string `json:"createdBy"`
}

type SucceededApiResponseBody struct {
	Data string `json:"data"`
}

// 클라이언트에서 받는 메시지 구조체 (변경 없음)
type ClientMessage struct {
	Payload     string `json:"payload"`
	MessageType string `json:"messageType"`
}

// Kafka로 전송할 메시지 구조체 (기존 SubscribeMessage와 동일)
type KafkaMessage struct {
	Sender      string `json:"sender"`
	Payload     string `json:"payload"`
	MessageType string `json:"messageType"`
	RoomId      string `json:"roomId"`
}

// 클라이언트 연결을 처리하고 Kafka로 메시지를 발행하는 핸들러 함수입니다.
func handleConnections(w http.ResponseWriter, r *http.Request, authClient *externalClient.AuthClient, producer *kafka.Writer) {
	bearerToken, err := service.GetBearerToken(r)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	passport, err := authClient.GetPassportByBearerWithTimeout(bearerToken, 5*time.Second)
	if err != nil {
		log.Printf("토큰 인증 실패: %v\n", err)
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	log.Printf("사용자 인증 성공: UserID=%s, Role=%s\n", passport.Id, passport.Role)

	chatRoomId := r.URL.Query().Get("chat_room_id")
	if chatRoomId == "" {
		var err error
		chatRoomId, err = createChatRoom(passport.Id)
		if err != nil {
			log.Printf("채팅방 생성 실패: %v\n", err)
			http.Error(w, "Internal Server Error: Failed to create chat room", http.StatusInternalServerError)
			return
		}
		log.Printf("새로운 채팅방 생성: %s\n", chatRoomId)
	} else {
		log.Printf("기존 채팅방 사용: %s\n", chatRoomId)
	}

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket 업그레이드 실패:", err)
		return
	}
	defer ws.Close()

	log.Println("새로운 클라이언트가 연결되었습니다.")

	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			log.Println("클라이언트 연결 종료:", err)
			break
		}

		var kafkaMsg KafkaMessage
		if err := json.Unmarshal(p, &kafkaMsg); err != nil {
			log.Printf("JSON 언마샬링 실패: %v\n", err)
			continue
		}

		if kafkaMsg.Sender == "" {
			kafkaMsg.Sender = passport.Id
		}
		if kafkaMsg.RoomId == "" {
			kafkaMsg.RoomId = chatRoomId
		}

		data, err := json.Marshal(kafkaMsg)
		if err != nil {
			log.Printf("JSON 마샬링 실패: %v\n", err)
			continue
		}

		err = producer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(kafkaMsg.RoomId),
				Value: data,
			},
		)

		if err != nil {
			log.Printf("Kafka publish 실패: %v\n", err)
		} else {
			log.Printf("Published to Kafka topic '%s': %s", producer.Topic, data)
		}
	}
}

func handlePrivateNetworkConnection(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade error")
	}

	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			log.Printf("error %v\n", err)
		}
		log.Println(p)
	}
}

// 채팅방 생성 함수 (변경 없음)
func createChatRoom(userId string) (string, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	requestBody := CreateChatRoomRequest{
		CreatedBy: userId,
	}
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("JSON 마샬링 실패: %v", err)
	}
	domain := MirrorViewDomain
	if domain == "" {
		domain = "http://mirror-view-backend:8080"
	}
	url := fmt.Sprintf("%s/api/v1/chat/room", domain)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("HTTP 요청 생성 실패: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP 요청 실패: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP 요청 실패: status code %d", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("응답 바디 읽기 실패: %v", err)
	}
	var response SucceededApiResponseBody
	err = json.Unmarshal(bodyBytes, &response)
	if err != nil {
		return "", fmt.Errorf("응답 JSON 파싱 실패: %v", err)
	}
	return response.Data, nil
}

// Kafka Producer를 생성하는 헬퍼 함수
func NewKafkaProducer(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	// Kafka Producer 설정
	kafkaBrokers := os.Getenv("KAFKA_HOST")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	topic := "websocket-chat-message-streams"
	producer := NewKafkaProducer(strings.Split(kafkaBrokers, ","), topic)
	defer producer.Close()

	log.Printf("Kafka Producer가 브로커 %s, 토픽 %s 로 연결되었습니다.", kafkaBrokers, topic)

	// --- Redis 구독 로직 다시 추가 ---
	// Redis Pub/Sub 설정
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	if redisHost == "" {
		redisHost = "localhost"
	}
	if redisPort == "" {
		redisPort = "6379"
	}
	redisAddr := redisHost + ":" + redisPort
	redisPassword := os.Getenv("REDIS_PASSWORD")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       0,
	})

	ctx := context.Background()
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis 연결 실패: %v", err)
	}
	log.Println("Redis 연결 성공")

	// 패턴으로 구독
	patternChannel := fmt.Sprintf("%s:*", MirrorViewApplicationName)
	pubsub := rdb.PSubscribe(ctx, patternChannel)
	_, err = pubsub.Receive(ctx)
	if err != nil {
		log.Fatalf("Redis 패턴 구독 실패: %v", err)
	}

	// Redis 메시지 수신을 위한 goroutine
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for msg := range ch {
			log.Printf("Redis 채널 '%s'에서 메시지 수신: %s", msg.Channel, msg.Payload)
			// 여기에 수신된 메시지를 모든 웹소켓 클라이언트에게 브로드캐스트하는 로직을 추가할 수 있습니다.
		}
	}()
	log.Printf("'%s' 패턴을 구독합니다.", patternChannel)
	// --- Redis 구독 로직 끝 ---

	// 정적 파일 서버 설정 (변경 없음)
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	// gRPC 클라이언트 설정 (변경 없음)
	authHost := os.Getenv("AUTH_HOST")
	conn, err := grpc.Dial(authHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	authClient := externalClient.NewAuthClient(conn)

	// HTTP 핸들러에 Kafka Producer를 전달합니다.
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleConnections(w, r, authClient, producer)
	})

	http.HandleFunc("/private/ws", func(w http.ResponseWriter, r *http.Request) {
		handlePrivateNetworkConnection(w, r)
	})

	// 서버 시작
	log.Println("HTTP 서버가 8080 포트에서 시작됩니다.")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
