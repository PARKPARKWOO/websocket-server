package service

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

type RedisService struct {
	client *redis.Client
	ctx    context.Context
}

// NewRedisService Redis 서비스 생성자
func NewRedisService() *RedisService {
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

	return &RedisService{
		client: rdb,
		ctx:    ctx,
	}
}

// Ping Redis 서버 연결 헬스체크용
func (r *RedisService) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// SubscribePattern 패턴으로 Redis 채널 구독
func (r *RedisService) SubscribePattern(pattern string, messageHandler func(channel, payload string)) error {
	pubsub := r.client.PSubscribe(r.ctx, pattern)
	_, err := pubsub.Receive(r.ctx)
	if err != nil {
		return fmt.Errorf("Redis 패턴 구독 실패: %v", err)
	}

	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for msg := range ch {
			log.Printf("Redis 채널 '%s'에서 메시지 수신: %s", msg.Channel, msg.Payload)
			if messageHandler != nil {
				messageHandler(msg.Channel, msg.Payload)
			}
		}
	}()

	log.Printf("'%s' 패턴을 구독합니다.", pattern)
	return nil
}

// PublishMessage Redis 채널로 메시지 발행
func (r *RedisService) PublishMessage(channel, message string) error {
	return r.client.Publish(r.ctx, channel, message).Err()
}

// PublishToRoom RoomId 기반으로 메시지 발행 (특정 방의 사용자들에게만 전송)
func (r *RedisService) PublishToRoom(roomID, message string) error {
	// 애플리케이션 이름을 접두사로 사용하여 방별 채널 생성
	channel := fmt.Sprintf("dev-mirror-view:%s", roomID)
	return r.PublishMessage(channel, message)
}

// PublishToRoomWithAppName 애플리케이션 이름을 지정하여 RoomId 기반 메시지 발행
func (r *RedisService) PublishToRoomWithAppName(appName, roomID, message string) error {
	channel := fmt.Sprintf("%s:%s", appName, roomID)
	return r.PublishMessage(channel, message)
}

// GetRoomSubscriberCount 특정 방의 구독자 수 조회
func (r *RedisService) GetRoomSubscriberCount(roomID string) (int64, error) {
	channel := fmt.Sprintf("dev-mirror-view:%s", roomID)
	result, err := r.client.PubSubNumSub(r.ctx, channel).Result()
	if err != nil {
		return 0, err
	}
	return result[channel], nil
}

// Close Redis 연결 종료
func (r *RedisService) Close() error {
	return r.client.Close()
}
