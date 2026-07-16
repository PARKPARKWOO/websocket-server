package service

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testResellConfig(t *testing.T) ResellWebSocketConfig {
	t.Helper()
	config, err := NewResellWebSocketConfig(
		"resell-application-id",
		"https://resell.platformholder.site,http://localhost:5173",
		"",
	)
	if err != nil {
		t.Fatalf("NewResellWebSocketConfig() error = %v", err)
	}
	return config
}

func TestResellOriginAllowlistIsExactAndRequired(t *testing.T) {
	config := testResellConfig(t)
	if config.RedisChannelPattern != DefaultResellRedisChannelPattern {
		t.Fatalf("default Redis pattern = %q", config.RedisChannelPattern)
	}

	tests := []struct {
		name    string
		origins []string
		allowed bool
	}{
		{name: "production", origins: []string{"https://resell.platformholder.site"}, allowed: true},
		{name: "default https port normalizes", origins: []string{"https://resell.platformholder.site:443"}, allowed: true},
		{name: "explicit local development", origins: []string{"http://localhost:5173"}, allowed: true},
		{name: "subdomain is not inherited", origins: []string{"https://evil.resell.platformholder.site"}, allowed: false},
		{name: "scheme mismatch", origins: []string{"http://resell.platformholder.site"}, allowed: false},
		{name: "path rejected", origins: []string{"https://resell.platformholder.site/path"}, allowed: false},
		{name: "missing", origins: nil, allowed: false},
		{name: "multiple headers", origins: []string{"https://resell.platformholder.site", "http://localhost:5173"}, allowed: false},
		{name: "null origin", origins: []string{"null"}, allowed: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "https://ws.platformholder.site/ws/resell", nil)
			if test.origins != nil {
				request.Header["Origin"] = test.origins
			}
			if got := config.OriginAllowed(request); got != test.allowed {
				t.Fatalf("OriginAllowed() = %v, want %v", got, test.allowed)
			}
		})
	}

	if _, err := NewResellWebSocketConfig("", "https://resell.platformholder.site", ""); err == nil {
		t.Fatal("blank RESELL_APPLICATION_ID was accepted")
	}
	if _, err := NewResellWebSocketConfig(" resell ", "https://resell.platformholder.site", ""); err == nil {
		t.Fatal("padded RESELL_APPLICATION_ID was accepted")
	}
	if _, err := NewResellWebSocketConfig("resell", "", ""); err == nil {
		t.Fatal("blank WEBSOCKET_ALLOWED_ORIGINS was accepted")
	}
	if _, err := NewResellWebSocketConfig("resell", "https://*.platformholder.site", ""); err == nil {
		t.Fatal("wildcard origin was accepted")
	}
	if _, err := NewResellWebSocketConfig("resell", "https://resell.platformholder.site", "dev-resell:*"); err != nil {
		t.Fatalf("valid explicit Redis pattern rejected: %v", err)
	}
	if _, err := NewResellWebSocketConfig("resell", "https://resell.platformholder.site", "dev-*:resell:*"); err == nil {
		t.Fatal("broad Redis glob was accepted")
	}
}

func TestDecodeResellOwnerIDRequiresCanonicalBase64URL(t *testing.T) {
	config := testResellConfig(t)
	ownerID := "550e8400-e29b-41d4-a716-446655440000"
	encoded := base64.RawURLEncoding.EncodeToString([]byte(ownerID))

	got, err := config.DecodeOwnerID(config.RedisChannelPrefix + encoded)
	if err != nil {
		t.Fatalf("DecodeOwnerID() error = %v", err)
	}
	if got != ownerID {
		t.Fatalf("DecodeOwnerID() = %q, want %q", got, ownerID)
	}

	invalidChannels := []string{
		"other:" + encoded,
		config.RedisChannelPrefix,
		config.RedisChannelPrefix + encoded + "=",
		config.RedisChannelPrefix + "not+base64url",
		config.RedisChannelPrefix + base64.RawURLEncoding.EncodeToString([]byte("owner id with spaces")),
		config.RedisChannelPrefix + strings.Repeat("a", base64.RawURLEncoding.EncodedLen(128)+1),
	}
	for _, channel := range invalidChannels {
		if _, err := config.DecodeOwnerID(channel); err == nil {
			t.Fatalf("invalid channel was accepted: %q", channel)
		}
	}
}

func TestParseResellWakeEnvelopeStrictSchema(t *testing.T) {
	valid := `{"schemaVersion":1,"eventId":"019f5508-6bdb-7d60-b480-cbf2a32683d6","type":"dashboard.changed","occurredAt":"2026-07-15T13:05:00Z","data":{"snapshotRequired":true}}`
	canonical, err := ParseResellWakeEnvelope(valid)
	if err != nil {
		t.Fatalf("ParseResellWakeEnvelope() error = %v", err)
	}
	if string(canonical) != valid {
		t.Fatalf("canonical envelope = %s", canonical)
	}

	invalidPayloads := map[string]string{
		"unknown top-level field": strings.TrimSuffix(valid, "}") + `,"ownerId":"secret"}`,
		"unknown nested field":    strings.Replace(valid, `"snapshotRequired":true`, `"snapshotRequired":true,"jobId":"secret"`, 1),
		"wrong version":           strings.Replace(valid, `"schemaVersion":1`, `"schemaVersion":2`, 1),
		"wrong type":              strings.Replace(valid, `"dashboard.changed"`, `"job.command"`, 1),
		"invalid event id":        strings.Replace(valid, `"019f5508-6bdb-7d60-b480-cbf2a32683d6"`, `"bad"`, 1),
		"invalid timestamp":       strings.Replace(valid, `"2026-07-15T13:05:00Z"`, `"yesterday"`, 1),
		"snapshot false":          strings.Replace(valid, `"snapshotRequired":true`, `"snapshotRequired":false`, 1),
		"trailing JSON":           valid + `{}`,
		"duplicate field":         strings.Replace(valid, `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1),
		"oversized":               valid + strings.Repeat(" ", resellWakeMaxBytes),
	}
	for name, payload := range invalidPayloads {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseResellWakeEnvelope(payload); err == nil {
				t.Fatal("invalid envelope was accepted")
			}
		})
	}
}

func TestWebSocketUserScopesAreIsolated(t *testing.T) {
	webSockets := NewWebSocketService()
	chatClient := &Client{
		UserID: "owner-1",
		Scope:  clientScopeChat,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	resellClient := &Client{
		UserID: "owner-1",
		Scope:  clientScopeResell,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	webSockets.registerClient(chatClient)
	if !webSockets.registerClientWithLimit(resellClient, resellMaxConnectionsPerUser) {
		t.Fatal("first Resell connection was rejected")
	}

	webSockets.SendToUser("owner-1", []byte("chat"))
	if got := string(<-chatClient.Send); got != "chat" {
		t.Fatalf("chat message = %q", got)
	}
	select {
	case unexpected := <-resellClient.Send:
		t.Fatalf("chat message leaked to Resell scope: %q", unexpected)
	default:
	}

	validWake := `{"schemaVersion":1,"eventId":"019f5508-6bdb-7d60-b480-cbf2a32683d6","type":"dashboard.changed","occurredAt":"2026-07-15T13:05:00Z","data":{"snapshotRequired":true}}`
	if err := webSockets.SendToResellUser("owner-1", validWake); err != nil {
		t.Fatalf("SendToResellUser() error = %v", err)
	}
	if got := string(<-resellClient.Send); got != validWake {
		t.Fatalf("Resell message = %q", got)
	}
	select {
	case unexpected := <-chatClient.Send:
		t.Fatalf("Resell message leaked to chat scope: %q", unexpected)
	default:
	}
}

func TestResellConnectionLimitDoesNotConsumeChatScope(t *testing.T) {
	webSockets := NewWebSocketService()
	for index := 0; index < resellMaxConnectionsPerUser; index++ {
		client := &Client{
			UserID: "owner-limited",
			Scope:  clientScopeResell,
			Rooms:  make(map[string]bool),
			Send:   make(chan []byte, 1),
		}
		if !webSockets.registerClientWithLimit(client, resellMaxConnectionsPerUser) {
			t.Fatalf("Resell connection %d was unexpectedly rejected", index+1)
		}
	}
	overLimit := &Client{
		UserID: "owner-limited",
		Scope:  clientScopeResell,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	if webSockets.registerClientWithLimit(overLimit, resellMaxConnectionsPerUser) {
		t.Fatal("Resell connection above the per-user limit was accepted")
	}
	chatClient := &Client{
		UserID: "owner-limited",
		Scope:  clientScopeChat,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	if !webSockets.registerClientWithLimit(chatClient, 1) {
		t.Fatal("Resell connections consumed the independent chat scope limit")
	}
}

func TestResellServiceRestartSignalsOnlyResellAndRejectsNewConnections(t *testing.T) {
	webSockets := NewWebSocketService()
	resellClient := &Client{
		UserID:  "owner-restart",
		Scope:   clientScopeResell,
		Rooms:   make(map[string]bool),
		Send:    make(chan []byte, 1),
		Restart: make(chan struct{}),
	}
	chatClient := &Client{
		UserID:  "owner-restart",
		Scope:   clientScopeChat,
		Rooms:   make(map[string]bool),
		Send:    make(chan []byte, 1),
		Restart: make(chan struct{}),
	}
	if !webSockets.registerClientWithLimit(resellClient, resellMaxConnectionsPerUser) {
		t.Fatal("Resell connection was rejected before drain")
	}
	webSockets.registerClient(chatClient)

	if count := webSockets.BeginResellServiceRestart(); count != 1 {
		t.Fatalf("restart signal count = %d, want 1", count)
	}
	if !webSockets.IsResellDraining() {
		t.Fatal("service did not enter Resell drain mode")
	}
	select {
	case <-resellClient.Restart:
	default:
		t.Fatal("existing Resell client did not receive restart signal")
	}
	select {
	case <-chatClient.Restart:
		t.Fatal("restart signal leaked to chat scope")
	default:
	}

	newResellClient := &Client{
		UserID: "owner-restart",
		Scope:  clientScopeResell,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	if webSockets.registerClientWithLimit(newResellClient, resellMaxConnectionsPerUser) {
		t.Fatal("new Resell connection was accepted during drain")
	}
	newChatClient := &Client{
		UserID: "owner-restart-2",
		Scope:  clientScopeChat,
		Rooms:  make(map[string]bool),
		Send:   make(chan []byte, 1),
	}
	if !webSockets.registerClientWithLimit(newChatClient, 1) {
		t.Fatal("Resell drain unexpectedly blocked the independent chat scope")
	}

	if count := webSockets.BeginResellServiceRestart(); count != 1 {
		t.Fatalf("idempotent restart signal count = %d, want 1", count)
	}
}

func TestResellWritePumpUsesServiceRestartCloseCode(t *testing.T) {
	webSockets := NewWebSocketService()
	serverClient := make(chan *Client, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		client := &Client{
			Conn:    connection,
			UserID:  "owner-restart-frame",
			Scope:   clientScopeResell,
			Rooms:   make(map[string]bool),
			Send:    make(chan []byte, 1),
			Restart: make(chan struct{}),
			service: webSockets,
		}
		if !webSockets.registerClientWithLimit(client, resellMaxConnectionsPerUser) {
			connection.Close()
			return
		}
		serverClient <- client
		client.resellWritePump()
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	connection, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("WebSocket dial failed: %v", err)
	}
	defer connection.Close()
	client := <-serverClient
	defer webSockets.unregisterClient(client)
	if count := webSockets.BeginResellServiceRestart(); count != 1 {
		t.Fatalf("restart connection count = %d, want 1", count)
	}
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = connection.ReadMessage()
	closeError, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("ReadMessage() error = %T %v, want CloseError", err, err)
	}
	if closeError.Code != websocket.CloseServiceRestart {
		t.Fatalf("close code = %d, want %d", closeError.Code, websocket.CloseServiceRestart)
	}
}
