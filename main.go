package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"net/http"
	"os"
	//"strings"
	"time"
	externalClient "websocket-server/external"
	service "websocket-server/service"
)

var (
	MirrorViewDomain = os.Getenv("MV_HOST")
	//TODO: 환경변수/DB 조회
	MirrorViewApplicationName = "dev-mirror-view"
)

// WebSocket 연결을 HTTP 연결에서 업그레이드하는 역할을 합니다.
// 기본 설정을 사용하며, 모든 출처(Origin)의 요청을 허용하도록 설정합니다.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 모든 오리진 허용 (개발용)
	},
}

// HTTP 요청/응답 구조체
type CreateChatRoomRequest struct {
	CreatedBy string `json:"createdBy"`
}

type SucceededApiResponseBody struct {
	Data string `json:"data"`
}

// 클라이언트 연결을 처리하는 핸들러 함수입니다.
func handleConnections(w http.ResponseWriter, r *http.Request, authClient *externalClient.AuthClient, rdb *redis.Client) {
	bearerToken, err := service.GetBearerToken(r)
	if err != nil {
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	passport, err := authClient.GetPassportByBearerWithTimeout(bearerToken, 5*time.Second)

	if err != nil {
		// 토큰이 유효하지 않으면 401 Unauthorized 에러 반환
		log.Printf("토큰 인증 실패: %v\n", err)
		http.Error(w, "Unauthorized: Invalid token", http.StatusUnauthorized)
		return
	}

	// 3. 인증 성공! 검증된 사용자 정보를 로그에 남기고, WebSocket 업그레이드 진행
	log.Printf("사용자 인증 성공: UserID=%s, Role=%s\n", passport.Id, passport.Role)

	// Chat Room ID 처리
	chatRoomId := r.URL.Query().Get("chat_room_id")
	if chatRoomId == "" {
		// chat_room_id가 없으면 새로운 채팅방 생성
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

	// (선택사항) 인증된 사용자 정보와 채팅방 ID를 context에 담아 다음 로직으로 전달
	ctx := context.WithValue(r.Context(), "passport", passport)
	ctx = context.WithValue(ctx, "chatRoomId", chatRoomId)
	r = r.WithContext(ctx)

	// HTTP GET 요청을 WebSocket 연결로 업그레이드합니다.
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket 업그레이드 실패:", err)
		return
	}

	// 함수가 종료될 때 웹소켓 연결을 반드시 닫도록 합니다.
	defer ws.Close()

	log.Println("새로운 클라이언트가 연결되었습니다.")

	// 클라이언트로부터 메시지를 지속적으로 읽기 위한 무한 루프입니다.
	for {
		// 메시지 타입 (이진/텍스트), 메시지 내용, 에러를 반환합니다.
		messageType, p, err := ws.ReadMessage()
		if err != nil {
			// 클라이언트 연결이 끊어지는 등 에러가 발생하면 루프를 종료합니다.
			log.Println("클라이언트 연결이 끊어졌습니다: ", err)
			break
		}

		// 받은 메시지를 서버 로그에 출력합니다.
		log.Printf("받은 메시지: %s", p)

		// Redis에 메시지 발행 (publish) - 채팅방 ID를 채널명으로 사용
		ctx := context.Background()
		channelName := fmt.Sprintf("%s:%s", MirrorViewApplicationName, chatRoomId)
		err = rdb.Publish(ctx, channelName, string(p)).Err()
		if err != nil {
			log.Printf("Redis 메시지 발행 실패: %v", err)
		} else {
			log.Printf("Redis 채널 '%s'에 메시지 발행: %s", channelName, string(p))
		}

		// 받은 메시지를 그대로 클라이언트에게 다시 보냅니다 (에코).
		if err := ws.WriteMessage(messageType, p); err != nil {
			log.Println("메시지 전송 실패: ", err)
			break
		}
	}
}

func handlePrivateNetworkConnection(w http.ResponseWriter, r *http.Request) {

}

// 채팅방 생성 함수
func createChatRoom(userId string) (string, error) {
	// HTTP 클라이언트 생성 (타임아웃 설정)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// 요청 바디 생성
	requestBody := CreateChatRoomRequest{
		CreatedBy: userId,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("JSON 마샬링 실패: %v", err)
	}

	// MirrorViewDomain이 설정되지 않았을 경우 기본값 사용
	domain := MirrorViewDomain
	if domain == "" {
		// Docker Swarm 환경에서는 서비스 이름으로 통신
		domain = "http://mirror-view-backend:8080" // 또는 환경에 맞는 서비스명
	}

	url := fmt.Sprintf("%s/api/v1/chat/room", domain)

	// HTTP POST 요청 생성
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("HTTP 요청 생성 실패: %v", err)
	}

	// Content-Type 헤더 설정
	req.Header.Set("Content-Type", "application/json")

	// 요청 전송
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP 요청 실패: %v", err)
	}
	defer resp.Body.Close()

	// 응답 상태 코드 확인
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP 요청 실패: status code %d", resp.StatusCode)
	}

	// 응답 바디 읽기
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("응답 바디 읽기 실패: %v", err)
	}

	// 응답 JSON 파싱
	var response SucceededApiResponseBody
	err = json.Unmarshal(bodyBytes, &response)
	if err != nil {
		return "", fmt.Errorf("응답 JSON 파싱 실패: %v", err)
	}

	return response.Data, nil
}

func main() {
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
		DB:       0, // use default DB
	})

	ctx := context.Background()

	// Ping the Redis server to check the connection.
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Redis 연결 실패: %v", err)
	}
	log.Println("Redis 연결 성공")

	pubsub := rdb.Subscribe(ctx, MirrorViewApplicationName)

	// Wait for confirmation that subscription is created before publishing anything.
	_, err = pubsub.Receive(ctx)
	if err != nil {
		log.Fatalf("Redis 구독 실패: %v", err)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	log.Printf("'%s' 채널을 구독합니다.", MirrorViewApplicationName)

	// Start a goroutine to process incoming messages from the channel.
	go func() {
		defer pubsub.Close()
		for msg := range ch {
			log.Printf("Redis 채널 '%s'에서 메시지 수신: %s", msg.Channel, msg.Payload)
			// 여기에 수신된 메시지를 처리하는 로직을 추가합니다.
			// 예를 들어, 연결된 모든 웹소켓 클라이언트에게 메시지를 브로드캐스트할 수 있습니다.
		}
	}()

	// 정적 파일(HTML, CSS, JS)을 제공하기 위한 파일 서버를 설정합니다.
	// 현재 디렉토리의 "public" 폴더를 웹 루트로 사용합니다.
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)
	authHost := os.Getenv("AUTH_HOST")
	conn, err := grpc.Dial(authHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	authClient := externalClient.NewAuthClient(conn)
	// "/ws" 경로로 들어오는 요청을 handleConnections 함수가 처리하도록 라우팅합니다.
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleConnections(w, r, authClient, rdb)
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
