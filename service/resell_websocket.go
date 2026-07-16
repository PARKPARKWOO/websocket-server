package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
	externalClient "websocket-server/external"

	"github.com/gorilla/websocket"
)

const (
	DefaultResellRedisChannelPattern = "dev-resell:*"
	resellWakeType                   = "dashboard.changed"
	resellWakeSchemaVersion          = 1
	resellWakeMaxBytes               = 2 * 1024
	resellClientReadLimit            = 256
	resellClientSendBuffer           = 64
	resellMaxConnectionsPerUser      = 5
	resellPongWait                   = 60 * time.Second
	resellPingInterval               = 25 * time.Second
	resellWriteWait                  = 2 * time.Second
	resellRestartWriteWait           = 1 * time.Second
)

var (
	resellOwnerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@+\-]{0,127}$`)
	resellEventIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:\-]{7,127}$`)
	redisPrefixPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:\-]{1,126}:$`)
)

type ResellWebSocketConfig struct {
	ApplicationID       string
	RedisChannelPattern string
	RedisChannelPrefix  string
	allowedOrigins      map[string]struct{}
}

func NewResellWebSocketConfig(applicationID, allowedOriginsCSV, redisChannelPattern string) (ResellWebSocketConfig, error) {
	trimmedApplicationID := strings.TrimSpace(applicationID)
	if applicationID != trimmedApplicationID || !validConfigIdentifier(applicationID) {
		return ResellWebSocketConfig{}, errors.New("RESELL_APPLICATION_ID must be a nonempty identifier of at most 128 bytes")
	}

	allowedOrigins := make(map[string]struct{})
	if strings.TrimSpace(allowedOriginsCSV) == "" {
		return ResellWebSocketConfig{}, errors.New("WEBSOCKET_ALLOWED_ORIGINS is required")
	}
	for _, candidate := range strings.Split(allowedOriginsCSV, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return ResellWebSocketConfig{}, errors.New("WEBSOCKET_ALLOWED_ORIGINS contains an empty origin")
		}
		origin, err := canonicalOrigin(candidate)
		if err != nil {
			return ResellWebSocketConfig{}, fmt.Errorf("invalid WEBSOCKET_ALLOWED_ORIGINS entry: %w", err)
		}
		allowedOrigins[origin] = struct{}{}
	}

	redisChannelPattern = strings.TrimSpace(redisChannelPattern)
	if redisChannelPattern == "" {
		redisChannelPattern = DefaultResellRedisChannelPattern
	}
	if strings.Count(redisChannelPattern, "*") != 1 || !strings.HasSuffix(redisChannelPattern, "*") {
		return ResellWebSocketConfig{}, errors.New("RESELL_REDIS_CHANNEL_PREFIX must contain exactly one trailing wildcard")
	}
	redisChannelPrefix := strings.TrimSuffix(redisChannelPattern, "*")
	if !redisPrefixPattern.MatchString(redisChannelPrefix) {
		return ResellWebSocketConfig{}, errors.New("RESELL_REDIS_CHANNEL_PREFIX must be a bounded literal prefix ending in ':' followed by '*'")
	}

	return ResellWebSocketConfig{
		ApplicationID:       applicationID,
		RedisChannelPattern: redisChannelPattern,
		RedisChannelPrefix:  redisChannelPrefix,
		allowedOrigins:      allowedOrigins,
	}, nil
}

func validConfigIdentifier(value string) bool {
	if value == "" || len(value) > 128 || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}

func canonicalOrigin(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) {
		return "", errors.New("origin must not be blank or padded")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", errors.New("origin is not a valid URL")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "https" && scheme != "http" {
		return "", errors.New("origin scheme must be http or https")
	}
	if parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("origin must contain only scheme and host")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", errors.New("origin must not contain a path")
	}
	hostname := strings.ToLower(parsed.Hostname())
	if strings.ContainsAny(hostname, " \t\r\n*?[]") {
		return "", errors.New("origin host is invalid")
	}
	port := parsed.Port()
	if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
		port = ""
	}
	host := hostname
	if port != "" {
		host = net.JoinHostPort(hostname, port)
	} else if strings.Contains(hostname, ":") {
		host = "[" + hostname + "]"
	}
	return scheme + "://" + host, nil
}

func (config ResellWebSocketConfig) OriginAllowed(request *http.Request) bool {
	origins := request.Header.Values("Origin")
	if len(origins) != 1 || origins[0] != strings.TrimSpace(origins[0]) {
		return false
	}
	origin, err := canonicalOrigin(origins[0])
	if err != nil {
		return false
	}
	_, allowed := config.allowedOrigins[origin]
	return allowed
}

func validResellOwnerID(ownerID string) bool {
	return len(ownerID) <= 128 && utf8.ValidString(ownerID) && resellOwnerIDPattern.MatchString(ownerID)
}

func (config ResellWebSocketConfig) DecodeOwnerID(channel string) (string, error) {
	if !strings.HasPrefix(channel, config.RedisChannelPrefix) {
		return "", errors.New("channel prefix mismatch")
	}
	encodedOwnerID := strings.TrimPrefix(channel, config.RedisChannelPrefix)
	if encodedOwnerID == "" || len(encodedOwnerID) > base64.RawURLEncoding.EncodedLen(128) || strings.Contains(encodedOwnerID, "=") {
		return "", errors.New("owner suffix is missing or oversized")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encodedOwnerID)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != encodedOwnerID {
		return "", errors.New("owner suffix is not canonical base64url")
	}
	ownerID := string(decoded)
	if !validResellOwnerID(ownerID) {
		return "", errors.New("decoded owner identifier is invalid")
	}
	return ownerID, nil
}

type ResellWakeEnvelope struct {
	SchemaVersion int            `json:"schemaVersion"`
	EventID       string         `json:"eventId"`
	Type          string         `json:"type"`
	OccurredAt    string         `json:"occurredAt"`
	Data          ResellWakeData `json:"data"`
}

type ResellWakeData struct {
	SnapshotRequired bool `json:"snapshotRequired"`
}

func ParseResellWakeEnvelope(payload string) ([]byte, error) {
	if payload == "" || len(payload) > resellWakeMaxBytes || !utf8.ValidString(payload) {
		return nil, errors.New("wake envelope is empty, oversized, or invalid UTF-8")
	}
	if err := rejectDuplicateJSONKeys([]byte(payload)); err != nil {
		return nil, fmt.Errorf("wake envelope JSON is invalid: %w", err)
	}

	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var envelope ResellWakeEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, errors.New("wake envelope does not match the required schema")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, errors.New("wake envelope contains trailing JSON")
	}
	if envelope.SchemaVersion != resellWakeSchemaVersion {
		return nil, errors.New("wake envelope schema version is unsupported")
	}
	if !resellEventIDPattern.MatchString(envelope.EventID) {
		return nil, errors.New("wake envelope eventId is invalid")
	}
	if envelope.Type != resellWakeType {
		return nil, errors.New("wake envelope type is unsupported")
	}
	if len(envelope.OccurredAt) > 40 {
		return nil, errors.New("wake envelope occurredAt is oversized")
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt); err != nil {
		return nil, errors.New("wake envelope occurredAt is invalid")
	}
	if !envelope.Data.SnapshotRequired {
		return nil, errors.New("wake envelope must require a snapshot refresh")
	}

	canonical, err := json.Marshal(envelope)
	if err != nil {
		return nil, errors.New("wake envelope could not be canonicalized")
	}
	return canonical, nil
}

func rejectDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("array is not closed")
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing token")
		}
		return err
	}
	return nil
}

func (ws *WebSocketService) HandleResellConnections(
	writer http.ResponseWriter,
	request *http.Request,
	authClient *externalClient.AuthClient,
	config ResellWebSocketConfig,
) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if ws.IsResellDraining() {
		writer.Header().Set("Retry-After", "5")
		http.Error(writer, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	if !config.OriginAllowed(request) {
		http.Error(writer, "Forbidden", http.StatusForbidden)
		return
	}
	bearerToken, err := GetBearerToken(request)
	if err != nil {
		http.Error(writer, "Unauthorized", http.StatusUnauthorized)
		return
	}
	passport, err := authClient.GetPassportByBearerWithTimeout(bearerToken, 5*time.Second)
	if err != nil {
		http.Error(writer, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if passport.ApplicationId != config.ApplicationID || !validResellOwnerID(passport.Id) {
		http.Error(writer, "Forbidden", http.StatusForbidden)
		return
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  256,
		WriteBufferSize: resellWakeMaxBytes,
		CheckOrigin:     config.OriginAllowed,
	}
	connection, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	client := &Client{
		Conn:    connection,
		UserID:  passport.Id,
		Scope:   clientScopeResell,
		Rooms:   make(map[string]bool),
		Send:    make(chan []byte, resellClientSendBuffer),
		Restart: make(chan struct{}),
		service: ws,
	}
	if !ws.registerClientWithLimit(client, resellMaxConnectionsPerUser) {
		closeCode := websocket.ClosePolicyViolation
		closeReason := "connection limit reached"
		if ws.IsResellDraining() {
			closeCode = websocket.CloseServiceRestart
			closeReason = "service restart"
		}
		_ = connection.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(closeCode, closeReason),
			time.Now().Add(resellWriteWait),
		)
		connection.Close()
		return
	}

	go client.resellWritePump()
	go client.resellReadPump()
}

func (client *Client) resellReadPump() {
	defer client.service.unregisterClient(client)
	client.Conn.SetReadLimit(resellClientReadLimit)
	_ = client.Conn.SetReadDeadline(time.Now().Add(resellPongWait))
	client.Conn.SetPongHandler(func(string) error {
		return client.Conn.SetReadDeadline(time.Now().Add(resellPongWait))
	})
	for {
		_, _, err := client.Conn.ReadMessage()
		if err != nil {
			return
		}
		_ = client.Conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "application messages are not accepted"),
			time.Now().Add(resellWriteWait),
		)
		return
	}
}

func (client *Client) resellWritePump() {
	ticker := time.NewTicker(resellPingInterval)
	defer func() {
		ticker.Stop()
		client.Conn.Close()
	}()
	for {
		select {
		case <-client.Restart:
			client.writeServiceRestart()
			return
		default:
		}
		select {
		case message, ok := <-client.Send:
			_ = client.Conn.SetWriteDeadline(time.Now().Add(resellWriteWait))
			if !ok {
				select {
				case <-client.Restart:
					client.writeServiceRestart()
					return
				default:
				}
				_ = client.Conn.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				)
				return
			}
			if err := client.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			_ = client.Conn.SetWriteDeadline(time.Now().Add(resellWriteWait))
			if err := client.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-client.Restart:
			client.writeServiceRestart()
			return
		}
	}
}
