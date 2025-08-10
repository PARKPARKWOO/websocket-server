package service

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

type KafkaService struct {
	producer *kafka.Writer
	topic    string
}

type KafkaMessage struct {
	Sender      string `json:"sender"`
	Payload     string `json:"payload"`
	MessageType string `json:"messageType"`
	RoomId      string `json:"roomId"`
}

// NewKafkaService Kafka 서비스 생성자
func NewKafkaService() *KafkaService {
	kafkaBrokers := os.Getenv("KAFKA_HOST")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	topic := "websocket-chat-message-streams"

	producer := &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(kafkaBrokers, ",")...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	log.Printf("Kafka Producer가 브로커 %s, 토픽 %s 로 연결되었습니다.", kafkaBrokers, topic)

	return &KafkaService{
		producer: producer,
		topic:    topic,
	}
}

// PublishMessage Kafka로 메시지 발행
func (k *KafkaService) PublishMessage(ctx context.Context, message KafkaMessage) error {
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	err = k.producer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(message.RoomId),
			Value: data,
		},
	)

	if err != nil {
		log.Printf("Kafka publish 실패: %v\n", err)
		return err
	}

	log.Printf("Published to Kafka topic '%s': %s", k.topic, data)
	return nil
}

// Close Kafka 연결 종료
func (k *KafkaService) Close() error {
	return k.producer.Close()
}

