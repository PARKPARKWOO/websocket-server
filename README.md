# WebSocket Server

Go로 작성된 WebSocket 서버로, Kafka와 Redis를 통한 메시지 처리 기능을 제공합니다.

## 주요 기능

- **WebSocket 연결 관리**: 클라이언트와의 실시간 양방향 통신
- **RoomId 기반 세션 관리**: 방별로 사용자 세션을 관리하여 효율적인 메시지 라우팅
- **Kafka 메시지 발행**: WebSocket으로 받은 메시지를 Kafka 토픽으로 발행
- **Redis 채널 구독**: Redis Pub/Sub을 통한 방별 메시지 수신 및 브로드캐스트
- **인증 서비스 연동**: gRPC를 통한 사용자 인증
- **채팅방 관리**: 동적 채팅방 생성 및 관리

## 프로젝트 구조

```
websocket-server/
├── main.go                 # 메인 애플리케이션 진입점
├── Dockerfile             # Docker 이미지 빌드 설정
├── docker-compose.yml     # Docker Swarm 배포 설정
├── deploy.sh              # 배포 스크립트
├── go.mod                 # Go 모듈 의존성
├── go.sum                 # Go 모듈 체크섬
├── service/               # 서비스 레이어
│   ├── kafka_service.go   # Kafka 관련 기능
│   ├── redis_service.go   # Redis 관련 기능
│   ├── websocket_service.go # WebSocket 관련 기능 (RoomId 기반 세션 관리)
│   ├── chat_service.go    # 채팅방 관리 기능
│   └── token_service.go   # 토큰 처리 기능
├── external/              # 외부 서비스 클라이언트
│   └── auth_client.go     # 인증 서비스 클라이언트
├── proto/                 # Protocol Buffers 정의
└── public/                # 정적 파일 (선택사항)
```

## 서비스 아키텍처

### Service Layer
- **KafkaService**: Kafka 메시지 발행 관리 (RoomId 기반 파티셔닝)
- **RedisService**: Redis Pub/Sub 구독 및 발행 (방별 채널 관리)
- **WebSocketService**: WebSocket 연결 및 RoomId 기반 세션 관리
- **ChatService**: 채팅방 생성 및 관리
- **TokenService**: Bearer 토큰 처리

## RoomId 기반 세션 관리

### 핵심 특징
- **방별 세션 관리**: 각 방(RoomId)별로 독립적인 사용자 세션 관리
- **효율적인 메시지 라우팅**: 특정 방의 메시지는 해당 방 사용자들에게만 전송
- **메모리 최적화**: 빈 방은 자동으로 제거되어 메모리 사용량 최적화
- **확장성**: 방별로 독립적인 처리로 수평 확장 가능

### 세션 관리 구조
```
WebSocketService
├── rooms: map[string]*Room
│   ├── Room
│   │   ├── ID: string (RoomId)
│   │   └── Clients: map[*Client]bool
│   └── Client
│       ├── Conn: *websocket.Conn
│       ├── UserID: string
│       ├── RoomID: string
│       └── Send: chan []byte
```

### 메시지 흐름
1. **클라이언트 연결**: 사용자가 특정 RoomId로 WebSocket 연결
2. **세션 등록**: 해당 방에 클라이언트 세션 등록
3. **메시지 수신**: 클라이언트로부터 메시지 수신
4. **Kafka 발행**: RoomId를 키로 하여 Kafka에 메시지 발행
5. **Redis 구독**: Redis에서 방별 메시지 수신
6. **방별 브로드캐스트**: 해당 방의 모든 클라이언트에게 메시지 전송

## 환경 변수

| 변수명 | 기본값 | 설명 |
|--------|--------|------|
| `AUTH_HOST` | `auth-service:9090` | 인증 서비스 gRPC 주소 |
| `KAFKA_HOST` | `kafka:9092` | Kafka 브로커 주소 |
| `REDIS_HOST` | `redis` | Redis 서버 주소 |
| `REDIS_PORT` | `6379` | Redis 포트 |
| `REDIS_PASSWORD` | - | Redis 비밀번호 (선택사항) |
| `MV_HOST` | `http://mirror-view-backend:8080` | Mirror View 백엔드 주소 |

## 로컬 개발

### 필수 요구사항
- Go 1.23+
- Docker & Docker Compose
- Kafka (로컬 또는 원격)
- Redis (로컬 또는 원격)

### 실행 방법

1. **의존성 설치**
```bash
go mod download
```

2. **환경 변수 설정**
```bash
cp .env.example .env
# .env 파일을 편집하여 필요한 환경 변수 설정
```

3. **로컬 실행**
```bash
go run main.go
```

## Docker Swarm 배포

### 1. 배포 스크립트 사용 (권장)
```bash
chmod +x deploy.sh
./deploy.sh
```

### 2. 수동 배포
```bash
# Docker Swarm 초기화
docker swarm init

# 이미지 빌드
docker build -t websocket-server:latest .

# 스택 배포
docker stack deploy -c docker-compose.yml websocket-stack
```

### 배포 확인
```bash
# 서비스 상태 확인
docker stack services websocket-stack

# 로그 확인
docker service logs websocket-stack_websocket-server

# 스택 제거
docker stack rm websocket-stack
```

## API 엔드포인트

### WebSocket 연결
- **URL**: `ws://localhost:8080/ws?chat_room_id=<room_id>`
- **인증**: Bearer 토큰 (Authorization 헤더 또는 accessToken 쿠키)
- **기능**: 실시간 메시지 송수신 (RoomId 기반 세션 관리)

### 프라이빗 네트워크 연결
- **URL**: `ws://localhost:8080/private/ws`
- **인증**: 없음
- **기능**: 내부 네트워크용 WebSocket 연결

### 방별 통계 정보
- **URL**: `GET http://localhost:8080/stats`
- **응답**: JSON 형태의 방별 사용자 수
```json
{
  "room-123": 5,
  "room-456": 3,
  "room-789": 0
}
```

## 메시지 형식

### 클라이언트 → 서버 (WebSocket)
```json
{
  "sender": "user123",
  "payload": "Hello, World!",
  "messageType": "chat",
  "roomId": "room-123"
}
```

### Kafka 토픽
- **토픽명**: `websocket-chat-message-streams`
- **키**: `roomId` (방별 파티셔닝)
- **값**: JSON 형태의 메시지

### Redis 채널
- **패턴**: `dev-mirror-view:*`
- **채널 형식**: `dev-mirror-view:{roomId}`
- **형식**: 문자열 메시지

## 성능 및 확장성

### RoomId 기반 최적화
- **메모리 효율성**: 방별 세션 관리로 불필요한 메모리 사용 방지
- **메시지 라우팅**: 방별 독립적인 메시지 처리로 성능 향상
- **확장성**: 방별로 독립적인 처리로 수평 확장 가능
- **장애 격리**: 한 방의 문제가 다른 방에 영향을 주지 않음

### 모니터링
- **방별 통계**: `/stats` 엔드포인트로 실시간 방별 사용자 수 확인
- **로그 추적**: 방 생성/제거, 사용자 입장/퇴장 로그
- **성능 메트릭**: 방별 메시지 처리량 및 응답 시간 모니터링

## 보안 고려사항

- **인증 필수**: 모든 WebSocket 연결은 Bearer 토큰 인증 필요
- **방 접근 제어**: 사용자는 지정된 방에만 접근 가능
- **메시지 검증**: 클라이언트로부터 받은 메시지 형식 검증
- **연결 제한**: 클라이언트당 연결 수 및 메시지 크기 제한

