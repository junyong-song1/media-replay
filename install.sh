#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo " Media Relay - Install Script"
echo "========================================="

# 1. 시스템 패키지 업데이트 및 설치
echo ""
echo "[1/4] 시스템 패키지 설치 …"
sudo apt-get update -y
sudo apt-get install -y \
    python3 \
    python3-pip \
    ffmpeg \
    git

# 2. FFmpeg SRT 지원 확인
echo ""
echo "[2/4] FFmpeg SRT 지원 확인 …"
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

# 3. 프로젝트 클론
echo ""
echo "[3/4] 프로젝트 클론 …"
INSTALL_DIR="$HOME/media-relay"
if [ -d "$INSTALL_DIR" ]; then
    echo "  디렉토리가 이미 존재합니다 — pull …"
    git -C "$INSTALL_DIR" pull
else
    git clone https://github.com/junyong-song1/media-replay.git "$INSTALL_DIR"
fi

# 4. 버전 확인
echo ""
echo "[4/4] 설치 확인 …"
echo "  Python : $(python3 --version)"
echo "  FFmpeg : $(ffmpeg -version 2>&1 | head -1)"
echo ""
echo "========================================="
echo " 설치 완료!"
echo ""
echo " 사용법:"
echo "   cd $INSTALL_DIR"
echo "   # config.json 에서 엔드포인트 설정 수정"
echo "   vi config.json"
echo "   # 입력 영상 파일 준비"
echo "   cp /path/to/video.mp4 input.mp4"
echo "   # 실행"
echo "   python3 relay.py"
echo "========================================="
