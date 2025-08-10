package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

type ChatService struct {
	domain string
}

type CreateChatRoomRequest struct {
	CreatedBy string `json:"createdBy"`
}

type SucceededApiResponseBody struct {
	Data string `json:"data"`
}

// NewChatService 채팅 서비스 생성자
func NewChatService() *ChatService {
	domain := os.Getenv("MV_HOST")
	if domain == "" {
		domain = "http://mirror-view-backend:8080"
	}

	return &ChatService{
		domain: domain,
	}
}

// CreateChatRoom 채팅방 생성
func (c *ChatService) CreateChatRoom(userId string) (string, error) {
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

	url := fmt.Sprintf("%s/api/v1/chat/room", c.domain)
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

	log.Printf("채팅방 생성 성공: %s", response.Data)
	return response.Data, nil
}

