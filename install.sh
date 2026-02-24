#!/usr/bin/env bash
set -euo pipefail

INSTALL_DIR="/opt/media-relay"
SERVICE_NAME="media-relay"
SERVICE_USER="media-relay"
LOG_DIR="/var/log/media-relay"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "========================================="
echo " Media Relay - Install Script"
echo "========================================="

# 1. 시스템 패키지 업데이트 및 설치
echo ""
echo "[1/7] 시스템 패키지 설치 …"
sudo apt-get update -y
sudo apt-get install -y \
    python3 \
    python3-pip \
    ffmpeg \
    git

# 2. FFmpeg SRT 지원 확인
echo ""
echo "[2/7] FFmpeg SRT 지원 확인 …"
if ffmpeg -protocols 2>/dev/null | grep -q srt; then
    echo "  ✓ SRT 프로토콜 지원 확인됨"
else
    echo "  ✗ FFmpeg에 SRT 지원이 없습니다. PPA에서 재설치합니다 …"
    sudo add-apt-repository -y ppa:ubuntuhandbook1/ffmpeg7
    sudo apt-get update -y
    sudo apt-get install -y ffmpeg
    if ffmpeg -protocols 2>/dev/null | grep -q srt; then
        echo "  ✓ SRT 프로토콜 지원 확인됨"
    else
        echo "  ✗ SRT 지원 설치 실패. 수동 확인이 필요합니다."
        exit 1
    fi
fi

# 3. Python 의존성 설치
echo ""
echo "[3/7] Python 의존성 설치 …"
sudo pip3 install -r "$SCRIPT_DIR/requirements.txt"

# 4. 시스템 유저 생성 + 로그 디렉토리
echo ""
echo "[4/7] 시스템 유저 및 디렉토리 설정 …"
if ! id "$SERVICE_USER" &>/dev/null; then
    sudo useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    echo "  ✓ 시스템 유저 '$SERVICE_USER' 생성됨"
else
    echo "  ✓ 시스템 유저 '$SERVICE_USER' 이미 존재함"
fi

sudo mkdir -p "$LOG_DIR"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$LOG_DIR"
echo "  ✓ 로그 디렉토리 $LOG_DIR 준비됨"

# 5. 파일 복사
echo ""
echo "[5/7] $INSTALL_DIR 에 파일 복사 …"
sudo mkdir -p "$INSTALL_DIR"
sudo cp "$SCRIPT_DIR/relay.py" "$INSTALL_DIR/"
sudo cp "$SCRIPT_DIR/config.json" "$INSTALL_DIR/"
sudo cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"
# 입력 파일이 있으면 복사
if [ -f "$SCRIPT_DIR/input_2audio.mp4" ]; then
    sudo cp "$SCRIPT_DIR/input_2audio.mp4" "$INSTALL_DIR/"
fi
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$INSTALL_DIR"
echo "  ✓ 파일 복사 완료"

# 6. systemd 서비스 등록
echo ""
echo "[6/7] systemd 서비스 등록 …"
sudo cp "$SCRIPT_DIR/media-relay.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
echo "  ✓ $SERVICE_NAME 서비스 등록 및 활성화됨"

# 7. UFW 방화벽 설정
echo ""
echo "[7/7] UFW 방화벽 설정 …"
if command -v ufw &>/dev/null; then
    sudo ufw allow 8080/tcp comment "media-relay API"
    echo "  ✓ UFW 8080/tcp 허용됨"
else
    echo "  ⚠ UFW가 설치되어 있지 않습니다. 수동으로 포트 8080을 열어주세요."
fi

# 완료
echo ""
echo "========================================="
echo " 설치 완료!"
echo ""
echo " 사용법:"
echo "   # config.json 수정"
echo "   sudo vi $INSTALL_DIR/config.json"
echo "   # 입력 영상 파일 준비"
echo "   sudo cp /path/to/video.mp4 $INSTALL_DIR/input_2audio.mp4"
echo "   # 서비스 시작"
echo "   sudo systemctl start $SERVICE_NAME"
echo "   # 상태 확인"
echo "   sudo systemctl status $SERVICE_NAME"
echo "   # 로그 확인"
echo "   sudo journalctl -u $SERVICE_NAME -f"
echo "   # HTTP API 확인"
echo "   curl http://localhost:8080/health"
echo "   curl http://localhost:8080/status"
echo "========================================="
