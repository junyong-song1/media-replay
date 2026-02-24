# Media Relay

FFmpeg 기반 SRT 멀티 엔드포인트 동시 전송 프로그램.
CVM에서 AWS MediaConnect 등 다수의 SRT 수신지로 실시간 스트림을 릴레이한다.

## 주요 기능

- FFmpeg SRT 16채널 동시 전송 (stream type별 매핑)
- 프로세스 자동 재시작 (exponential backoff)
- SRT 통계 수집 (bitrate, speed, out_time 등)
- HTTP 상태/관리 API (aiohttp)
- 일별 로그 로테이션 (TimedRotatingFileHandler)
- systemd 서비스 지원

## 요구사항

- Python 3.10+
- FFmpeg (SRT 프로토콜 지원)
- aiohttp (`pip install aiohttp`)

## 파일 구조

```
media-relay/
├── relay.py                # 메인 프로그램
├── config.json             # 전체 16채널 설정
├── config_type1.json       # type1 (PGM+IS) 전용
├── config_type2.json       # type2 (PGM) 전용
├── config_type3.json       # type3 (IS) 전용
├── requirements.txt        # Python 의존성
├── install.sh              # 설치 스크립트 (systemd, UFW 포함)
└── media-relay.service     # systemd unit 파일
```

## Stream Types

| Type | 설명 | 오디오 매핑 | 워커 |
|------|------|-------------|------|
| type1 | PGM(한국어) + IS(현장음) | 0:a:0, 0:a:1 | mc-01~02 |
| type2 | PGM(한국어) | 0:a:0 | mc-03~10 |
| type3 | IS(현장음) | 0:a:1 | mc-11~16 |

## 실행

```bash
# 전체 16채널
nohup python3 relay.py -c config.json > /dev/null 2>&1 &

# type별 개별 실행 (테스트용)
nohup python3 relay.py -c config_type1.json > /dev/null 2>&1 &
nohup python3 relay.py -c config_type2.json > /dev/null 2>&1 &
nohup python3 relay.py -c config_type3.json > /dev/null 2>&1 &

# 중지
pkill -f relay.py
```

## systemd (install.sh 실행 후)

```bash
sudo systemctl start media-relay
sudo systemctl stop media-relay
sudo systemctl restart media-relay
sudo systemctl status media-relay
sudo journalctl -u media-relay -f
```

## HTTP API

### 상태 조회

```bash
# 헬스체크 (running > 0 → 200, 아니면 503)
curl http://<서버IP>:8080/health

# 전체 워커 상세 (type, state, pid, bitrate, speed 등)
curl -s http://<서버IP>:8080/status | python3 -m json.tool
```

### 개별 워커 제어

```bash
curl -X POST http://<서버IP>:8080/workers/mc-01/stop
curl -X POST http://<서버IP>:8080/workers/mc-01/start
curl -X POST http://<서버IP>:8080/workers/mc-01/restart
```

### Type별 제어

```bash
curl -X POST http://<서버IP>:8080/workers/type/type1/stop
curl -X POST http://<서버IP>:8080/workers/type/type1/start
curl -X POST http://<서버IP>:8080/workers/type/type1/restart
```

### 전체 재시작

```bash
curl -X POST http://<서버IP>:8080/workers/restart-all
```

## 설정 (config.json)

| 키 | 설명 | 기본값 |
|----|------|--------|
| `input` | 입력 파일 경로 | `input.mp4` |
| `loop` | 무한 반복 여부 | `true` |
| `max_restarts` | 최대 재시작 횟수 | `5` |
| `api_port` | HTTP API 포트 | `8080` |
| `log_file` | 로그 파일 경로 (미설정 시 콘솔만) | - |
| `log_backup_count` | 로그 보관 일수 | `30` |

## 로그

```bash
# 실시간 로그
tail -f ~/media-relay/logs/relay.log

# 일별 로테이션 파일
ls ~/media-relay/logs/
# relay.log              ← 오늘
# relay.log.2026-02-24   ← 어제
# relay.log.2026-02-23   ← 그제
```

## 설치 (신규 서버)

```bash
git clone https://github.com/junyong-song1/media-replay.git
cd media-replay
sudo bash install.sh
```

install.sh가 수행하는 작업:
1. 시스템 패키지 설치 (python3, ffmpeg, git)
2. FFmpeg SRT 지원 확인
3. Python 의존성 설치 (aiohttp)
4. 시스템 유저 `media-relay` 생성
5. `/opt/media-relay`에 파일 복사
6. systemd 서비스 등록 + enable
7. UFW 8080/tcp 허용
