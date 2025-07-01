package external

import (
	"context"
	"fmt"
	"time"
	pb "websocket-server/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

type AuthClient struct {
	conn           *grpc.ClientConn
	userInfoClient pb.UserInfoServiceClient
	tokenClient    pb.TokenServiceClient
	serverAddress  string
}

// NewAuthClient AuthClient 생성자
func NewAuthClient(conn *grpc.ClientConn) *AuthClient {
	return &AuthClient{
		conn:           conn,
		userInfoClient: pb.NewUserInfoServiceClient(conn),
		tokenClient:    pb.NewTokenServiceClient(conn),
		serverAddress:  "dd",
	}
}

func (c *AuthClient) Close() error {
	return c.conn.Close()
}

// GetPassportByBearer Bearer 토큰으로 Passport 정보 조회
func (c *AuthClient) GetPassportByBearer(ctx context.Context, bearerToken string) (*pb.Passport, error) {
	// Bearer 토큰을 메타데이터에 추가
	md := metadata.New(map[string]string{
		"Authorization": "Bearer " + bearerToken,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := c.userInfoClient.GetPassportByBearer(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("GetPassportByBearer failed: %w", err)
	}

	return resp, nil
}

// GetUserInfoByBearer Bearer 토큰으로 사용자 정보 조회
func (c *AuthClient) GetUserInfoByBearer(ctx context.Context, bearerToken string) (*pb.UserInfoResponse, error) {
	// Bearer 토큰을 메타데이터에 추가
	md := metadata.New(map[string]string{
		"Authorization": "Bearer " + bearerToken,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	resp, err := c.userInfoClient.GetUserInfoByBearer(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("GetUserInfoByBearer failed: %w", err)
	}

	return resp, nil
}

// GetUserInfoInApplication 애플리케이션 내 사용자 정보 페이징 조회
func (c *AuthClient) GetUserInfoInApplication(ctx context.Context, bearerToken string, page, size int32) (*pb.UserInfoInApplicationPageResponse, error) {
	// Bearer 토큰을 메타데이터에 추가
	md := metadata.New(map[string]string{
		"Authorization": "Bearer " + bearerToken,
	})
	ctx = metadata.NewOutgoingContext(ctx, md)

	req := &pb.UserInfoInApplicationRequest{
		Page: page,
		Size: size,
	}

	resp, err := c.userInfoClient.GetUserInfoInApplication(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("GetUserInfoInApplication failed: %w", err)
	}

	return resp, nil
}

// ReissueToken 토큰 재발급
func (c *AuthClient) ReissueToken(ctx context.Context, idempotentKey, refreshToken string) (*pb.JwtTokenResponse, error) {
	req := &pb.ReissueTokenRequest{
		IdempotentKey: idempotentKey,
		RefreshToken:  refreshToken,
	}

	resp, err := c.tokenClient.ReissueToken(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ReissueToken failed: %w", err)
	}

	return resp, nil
}

// 편의 메서드들

// GetPassportByBearerWithTimeout 타임아웃이 있는 Passport 조회
func (c *AuthClient) GetPassportByBearerWithTimeout(bearerToken string, timeout time.Duration) (*pb.Passport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.GetPassportByBearer(ctx, bearerToken)
}

// GetUserInfoByBearerWithTimeout 타임아웃이 있는 사용자 정보 조회
func (c *AuthClient) GetUserInfoByBearerWithTimeout(bearerToken string, timeout time.Duration) (*pb.UserInfoResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return c.GetUserInfoByBearer(ctx, bearerToken)
}

// GetAllUsersInApplication 모든 사용자 정보 조회 (페이징 자동 처리)
func (c *AuthClient) GetAllUsersInApplication(ctx context.Context, bearerToken string) ([]*pb.UserInfoInApplicationResponse, error) {
	var allUsers []*pb.UserInfoInApplicationResponse
	page := int32(1)
	size := int32(100) // 한 번에 가져올 사이즈

	for {
		resp, err := c.GetUserInfoInApplication(ctx, bearerToken, page, size)
		if err != nil {
			return nil, err
		}

		allUsers = append(allUsers, resp.Items...)

		// 마지막 페이지인지 확인
		if page >= resp.TotalPages {
			break
		}

		page++
	}

	return allUsers, nil
}
