package logger

import (
	"os"
	"websocket-server/internal/config"

	"github.com/sirupsen/logrus"
)

func InitLogger(cfg *config.Config) *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
		DisableColors:   false,
	})

	// 로그 레벨 설정
	logger.SetLevel(logrus.InfoLevel)
	if cfg.App.Environment == "local" {
		logger.SetLevel(logrus.DebugLevel)
	}

	// 콘솔 출력 설정
	logger.SetOutput(os.Stdout)

	return logger
} 