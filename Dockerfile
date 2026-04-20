# 빌드 스테이지
FROM golang:1.23-alpine AS builder

# 필요한 시스템 패키지 설치
RUN apk add --no-cache git ca-certificates tzdata

# 작업 디렉토리 설정
WORKDIR /app

# Go 모듈 파일 복사
COPY go.mod go.sum ./

# 의존성 다운로드
RUN go mod download

# 소스 코드 복사
COPY . .

# 프로토버프 파일 생성 (필요한 경우)
RUN if [ -f "proto/auth.proto" ]; then \
        apk add --no-cache protobuf-dev && \
        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
        protoc --go_out=. --go_opt=paths=source_relative \
               --go-grpc_out=. --go-grpc_opt=paths=source_relative \
               proto/auth.proto; \
    fi

# 바이너리 빌드
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o websocket-server .

# 실행 스테이지
FROM alpine:latest

# 필요한 패키지 설치
RUN apk --no-cache add ca-certificates tzdata

# 비root 사용자 생성
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# 작업 디렉토리 설정
WORKDIR /app

# 빌드 스테이지에서 바이너리 복사
COPY --from=builder /app/websocket-server .

# 정적 파일이 있다면 복사 (public 폴더)
#COPY --from=builder /app/public ./public 2>/dev/null || true

# 소유권 변경
RUN chown -R appuser:appgroup /app

# 비root 사용자로 전환
USER appuser

# 헬스체크: /healthz 엔드포인트로 Liveness 확인
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

# 포트 노출
EXPOSE 8080

# 환경 변수 설정
ENV GIN_MODE=release

# 애플리케이션 실행
CMD ["./websocket-server"] 