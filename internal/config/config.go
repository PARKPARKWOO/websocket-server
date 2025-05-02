package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	Server ServerConfig `json:"server"`
}

type ServerConfig struct {
	Port     string `json:"port"`
	GRPCPort string `json:"grpc_port"`
}

func LoadConfig() *Config {
	config := &Config{
		Server: ServerConfig{
			Port:     "8080",
			GRPCPort: "50051",
		},
	}

	// config.json 파일이 있다면 로드
	configPath := filepath.Join("config", "config.json")
	if _, err := os.Stat(configPath); err == nil {
		file, err := os.ReadFile(configPath)
		if err == nil {
			json.Unmarshal(file, config)
		}
	}

	return config
} 