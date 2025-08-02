package service

import (
	"fmt"
	"net/http"
	"strings"
	//externalClient "websocket-server/external"
)

type TokenService struct {
}

const (
	BearerPrefix = "Bearer "
)

func GetBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, BearerPrefix) {
		return strings.TrimPrefix(authHeader, BearerPrefix), nil
	}

	if cookie, err := r.Cookie("accessToken"); err == nil {
		return cookie.Value, nil
	}

	return "", fmt.Errorf("no bearer token")
}
