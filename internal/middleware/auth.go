package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"websocket-server/external"
	pb "websocket-server/proto"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// AuthResult 인증 결과를 나타내는 타입
type AuthResult struct {
	Success  bool
	Passport *pb.Passport
	Error    error
}

// AuthMiddleware 인증 미들웨어 함수 타입
type AuthMiddleware func(ctx context.Context, token string) AuthResult

// WithAuth 인증을 포함한 웹소켓 핸들러를 생성하는 고차 함수
func WithAuth(
	authClient *external.AuthClient,
	logger *logrus.Logger,
	handler func(conn *websocket.Conn, passport *pb.Passport),
) func(w http.ResponseWriter, r *http.Request) {

	// 인증 미들웨어 함수
	authMiddleware := func(ctx context.Context, token string) AuthResult {
		if token == "" {
			return AuthResult{
				Success: false,
				Error:   fmt.Errorf("missing authorization token"),
			}
		}

		// Bearer 토큰에서 실제 토큰 추출
		if !strings.HasPrefix(token, "Bearer ") {
			return AuthResult{
				Success: false,
				Error:   fmt.Errorf("invalid token format"),
			}
		}
		bearerToken := strings.TrimPrefix(token, "Bearer ")

		// Auth Client를 사용하여 Passport 조회
		passport, err := authClient.GetPassportByBearer(ctx, bearerToken)
		if err != nil {
			return AuthResult{
				Success: false,
				Error:   fmt.Errorf("authentication failed: %w", err),
			}
		}

		return AuthResult{
			Success:  true,
			Passport: passport,
		}
	}

	// 웹소켓 업그레이더
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true // CORS는 별도로 처리
		},
	}

	// 실제 HTTP 핸들러 반환
	return func(w http.ResponseWriter, r *http.Request) {
		// Authorization 헤더에서 토큰 추출
		token := r.Header.Get("Authorization")

		// 인증 수행
		authResult := authMiddleware(r.Context(), token)

		if !authResult.Success {
			logger.WithError(authResult.Error).Warn("Authentication failed")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// 웹소켓 연결 업그레이드
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.WithError(err).Error("Failed to upgrade connection")
			return
		}
		defer conn.Close()

		// 인증된 핸들러 호출
		handler(conn, authResult.Passport)
	}
}

// ComposeAuth 미들웨어를 조합하는 함수
func ComposeAuth(middlewares ...AuthMiddleware) AuthMiddleware {
	return func(ctx context.Context, token string) AuthResult {
		for _, middleware := range middlewares {
			result := middleware(ctx, token)
			if !result.Success {
				return result
			}
		}
		return AuthResult{Success: true}
	}
}

// WithLogging 로깅을 추가하는 미들웨어
func WithLogging(logger *logrus.Logger) func(AuthMiddleware) AuthMiddleware {
	return func(next AuthMiddleware) AuthMiddleware {
		return func(ctx context.Context, token string) AuthResult {
			logger.WithField("token_length", len(token)).Debug("Authenticating user")

			result := next(ctx, token)

			if result.Success {
				logger.WithField("user_id", result.Passport.Id).Info("User authenticated successfully")
			} else {
				logger.WithError(result.Error).Warn("Authentication failed")
			}

			return result
		}
	}
}

// WithTimeout 타임아웃을 추가하는 미들웨어
func WithTimeout(timeout time.Duration) func(AuthMiddleware) AuthMiddleware {
	return func(next AuthMiddleware) AuthMiddleware {
		return func(ctx context.Context, token string) AuthResult {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			return next(ctx, token)
		}
	}
}
