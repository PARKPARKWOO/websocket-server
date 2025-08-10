#!/bin/bash

# Docker Swarm 배포 스크립트
set -e

echo "🚀 Docker Swarm 배포를 시작합니다..."

# 환경 변수 파일이 있으면 로드
if [ -f .env ]; then
    echo "📄 환경 변수 파일을 로드합니다..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Docker Swarm 초기화 (이미 초기화되어 있으면 무시)
if ! docker info | grep -q "Swarm: active"; then
    echo "🐳 Docker Swarm을 초기화합니다..."
    docker swarm init
else
    echo "✅ Docker Swarm이 이미 활성화되어 있습니다."
fi

# 이미지 빌드
echo "🔨 Docker 이미지를 빌드합니다..."
docker build -t websocket-server:latest .

# 스택 배포
echo "📦 스택을 배포합니다..."
docker stack deploy -c docker-compose.yml websocket-stack

# 배포 상태 확인
echo "📊 배포 상태를 확인합니다..."
sleep 10
docker stack services websocket-stack

echo "✅ 배포가 완료되었습니다!"
echo "📋 유용한 명령어:"
echo "  - 서비스 상태 확인: docker stack services websocket-stack"
echo "  - 로그 확인: docker service logs websocket-stack_websocket-server"
echo "  - 스택 제거: docker stack rm websocket-stack"
echo "  - 스웜 종료: docker swarm leave --force"

