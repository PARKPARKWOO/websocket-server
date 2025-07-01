package logger

import (
	"context"
	"os"
	"time"
	"websocket-server/internal/config"

	"github.com/grafana/loki/pkg/logproto"
	"github.com/sirupsen/logrus"
	"github.com/weaveworks/common/server"
)

type LokiLogger struct {
	client *server.Client
	labels map[string]string
}

func NewLokiLogger(cfg *config.Config) (*LokiLogger, error) {
	client, err := server.NewClient(cfg.Loki.URL, cfg.Loki.Username, cfg.Loki.Password)
	if err != nil {
		return nil, err
	}

	hostname, _ := os.Hostname()
	labels := map[string]string{
		"app":      cfg.App.Name,
		"host":     hostname,
		"env":      cfg.App.Environment,
		"instance": cfg.App.Instance,
	}

	// 설정에서 추가 레이블 병합
	for k, v := range cfg.Loki.Labels {
		labels[k] = v
	}

	return &LokiLogger{
		client: client,
		labels: labels,
	}, nil
}

func (l *LokiLogger) Write(p []byte) (n int, err error) {
	entry := &logproto.Entry{
		Timestamp: time.Now(),
		Line:      string(p),
	}

	ctx := context.Background()
	err = l.client.Push(ctx, l.labels, entry)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (l *LokiLogger) Sync() error {
	return nil
}

func (l *LokiLogger) Close() error {
	return l.client.Close()
}

// LogrusHook는 Logrus의 Hook 인터페이스를 구현합니다
type LogrusHook struct {
	writer *LokiLogger
}

func NewLogrusHook(cfg *config.Config) (*LogrusHook, error) {
	writer, err := NewLokiLogger(cfg)
	if err != nil {
		return nil, err
	}

	return &LogrusHook{
		writer: writer,
	}, nil
}

func (h *LogrusHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}

	_, err = h.writer.Write([]byte(line))
	return err
}

func (h *LogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
} 