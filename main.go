package main

import (
	"context"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	externalClient "websocket-server/external"
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

// 클라이언트 연결을 처리하는 핸들러 함수입니다.
func handleConnections(w http.ResponseWriter, r *http.Request, authClient *externalClient.AuthClient) {
	var bearerToken string

	// 1. Authorization 헤더에서 토큰 먼저 확인
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
		bearerToken = strings.TrimPrefix(authHeader, "Bearer ")
	}

	// 2. 헤더에 토큰이 없으면 쿠키에서 확인
	if bearerToken == "" {
		tokenCookie, err := r.Cookie("accessToken")
		if err == nil {
			bearerToken = tokenCookie.Value
		}
	}

	// 3. 헤더와 쿠키 모두에 토큰이 없는 경우, 연결 거부
	if bearerToken == "" {
		log.Println("인증 토큰을 찾을 수 없습니다 (헤더 및 쿠키 확인 완료).")
		http.Error(w, "Unauthorized: Missing access token", http.StatusUnauthorized)
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

	// (선택사항) 인증된 사용자 정보를 context에 담아 다음 로직으로 전달
	ctx := context.WithValue(r.Context(), "passport", passport)
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

		// 받은 메시지를 그대로 클라이언트에게 다시 보냅니다 (에코).
		if err := ws.WriteMessage(messageType, p); err != nil {
			log.Println("메시지 전송 실패: ", err)
			break
		}
	}
}

func main() {
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
		handleConnections(w, r, authClient)
	})

	// 서버 시작
	log.Println("HTTP 서버가 8080 포트에서 시작됩니다.")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
