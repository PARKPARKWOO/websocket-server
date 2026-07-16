// 이 코드는 Kafka로 메시지를 발행(Produce)하고,
// Redis 채널을 구독(Subscribe)하는 기능을 모두 포함한 버전입니다.
//
// 주요 기능:
// 1. WebSocket 클라이언트로부터 메시지를 받아 Kafka 토픽으로 발행합니다.
// 2. Redis의 특정 패턴 채널을 구독하여 메시지를 수신하고 로그에 기록합니다.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	externalClient "websocket-server/external"
	service "websocket-server/service"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	MirrorViewDomain          = os.Getenv("MV_HOST")
	MirrorViewApplicationName = "dev-mirror-view"
	BbrApplicationName        = "dev-bbr"
)

func loadResellWebSocketConfig() (*service.ResellWebSocketConfig, error) {
	rawEnabled := strings.TrimSpace(os.Getenv("RESELL_WEBSOCKET_ENABLED"))
	if rawEnabled == "" {
		return nil, nil
	}
	enabled, err := strconv.ParseBool(rawEnabled)
	if err != nil {
		return nil, fmt.Errorf("RESELL_WEBSOCKET_ENABLED must be true or false: %w", err)
	}
	if !enabled {
		return nil, nil
	}
	config, err := service.NewResellWebSocketConfig(
		os.Getenv("RESELL_APPLICATION_ID"),
		os.Getenv("WEBSOCKET_ALLOWED_ORIGINS"),
		os.Getenv("RESELL_REDIS_CHANNEL_PREFIX"),
	)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func main() {
	resellConfig, err := loadResellWebSocketConfig()
	if err != nil {
		log.Fatalf("Resell WebSocket 설정 오류: %v", err)
	}

	// 서비스들 초기화
	kafkaService := service.NewKafkaService()
	defer kafkaService.Close()

	redisService := service.NewRedisService()
	defer redisService.Close()

	chatService := service.NewChatService()

	websocketService := service.NewWebSocketService()

	// gRPC 클라이언트 설정
	authHost := os.Getenv("AUTH_HOST")
	if authHost == "" {
		authHost = "auth-service:9090"
	}

	conn, err := grpc.Dial(authHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	authClient := externalClient.NewAuthClient(conn)

	// Redis 패턴 구독 시작 - Mirror-View: RoomId 기반 메시지 처리
	mvPattern := MirrorViewApplicationName + ":*"
	err = redisService.SubscribePattern(mvPattern, func(channel, payload string) {
		log.Printf("Redis 채널 '%s'에서 메시지 수신", channel)

		parts := strings.Split(channel, ":")
		if len(parts) < 2 {
			log.Printf("잘못된 채널 형식: %s", channel)
			return
		}
		roomID := parts[1]

		websocketService.BroadcastToRoom(roomID, []byte(payload))
		log.Printf("방 %s의 모든 클라이언트에게 메시지 전송 완료", roomID)
	})
	if err != nil {
		log.Fatalf("Mirror-View Redis 구독 실패: %v", err)
	}

	// Redis 패턴 구독 - BBR: UserId 기반 개인 알림 처리
	bbrPattern := BbrApplicationName + ":*"
	err = redisService.SubscribePattern(bbrPattern, func(channel, payload string) {
		log.Printf("BBR Redis 채널 '%s'에서 메시지 수신", channel)

		parts := strings.Split(channel, ":")
		if len(parts) < 2 {
			log.Printf("잘못된 채널 형식: %s", channel)
			return
		}
		userID := parts[1]

		websocketService.SendToUser(userID, []byte(payload))
	})
	if err != nil {
		log.Fatalf("BBR Redis 구독 실패: %v", err)
	}

	if resellConfig != nil {
		// Resell: base64url owner ID 기반의 검증된 metadata-only wake-up 알림.
		err = redisService.SubscribePattern(resellConfig.RedisChannelPattern, func(channel, payload string) {
			ownerID, decodeErr := resellConfig.DecodeOwnerID(channel)
			if decodeErr != nil {
				log.Printf("Resell Redis 메시지 폐기: 잘못된 채널 (%v)", decodeErr)
				return
			}
			if sendErr := websocketService.SendToResellUser(ownerID, payload); sendErr != nil {
				log.Printf("Resell Redis 메시지 폐기: 잘못된 envelope (%v)", sendErr)
				return
			}
		})
		if err != nil {
			log.Fatalf("Resell Redis 구독 실패: %v", err)
		}
	}

	// 정적 파일 서버 설정
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)

	// HTTP 핸들러 설정
	// /ws - 통합 WebSocket 엔드포인트 (연결만 맺고, 채팅방은 메시지로 join/leave)
	// chat_room_id 쿼리 파라미터 있으면 자동 참가 (하위호환)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocketService.HandleConnections(w, r, authClient, kafkaService, chatService)
	})

	if resellConfig != nil {
		// /ws/resell - 인증·Origin·application scope가 고정된 알림 전용 endpoint.
		http.HandleFunc("/ws/resell", func(w http.ResponseWriter, r *http.Request) {
			websocketService.HandleResellConnections(w, r, authClient, *resellConfig)
		})
	}

	http.HandleFunc("/private/ws", func(w http.ResponseWriter, r *http.Request) {
		websocketService.HandlePrivateNetworkConnection(w, r)
	})

	// 방별 통계 정보 엔드포인트 추가
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := websocketService.GetRoomStats()
		statsJSON, err := json.Marshal(stats)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(statsJSON)
	})

	// Liveness - 프로세스 살아있는지만 확인
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Readiness - 의존성(Redis) 연결 상태까지 체크
	http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if websocketService.IsResellDraining() {
			http.Error(w, "service draining", http.StatusServiceUnavailable)
			return
		}
		if err := redisService.Ping(r.Context()); err != nil {
			http.Error(w, "redis unreachable: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})

	// Prometheus scrape
	http.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              ":8080",
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       75 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		log.Println("HTTP 서버가 8080 포트에서 시작됩니다.")
		serverErrors <- server.ListenAndServe()
	}()

	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server stopped unexpectedly: %v", err)
		}
		return
	case <-signalContext.Done():
	}

	connectionCount := websocketService.BeginResellServiceRestart()
	log.Printf("graceful shutdown started: resell_connections=%d", connectionCount)
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- server.Shutdown(shutdownContext)
	}()

	drainContext, cancelDrain := context.WithTimeout(shutdownContext, 5*time.Second)
	if err := websocketService.WaitForResellDrain(drainContext); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("Resell WebSocket drain reached its deadline: %v", err)
	}
	cancelDrain()
	if err := <-shutdownDone; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("HTTP graceful shutdown failed: %v", err)
	}
}
