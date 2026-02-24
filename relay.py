#!/usr/bin/env python3
"""Media Relay - FFmpeg SRT 16채널 동시 전송 프로그램 (asyncio)."""

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("media-relay")


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class WorkerState(Enum):
    IDLE = "idle"
    RUNNING = "running"
    BACKOFF = "backoff"
    DEAD = "dead"


@dataclass
class Worker:
    """단일 FFmpeg 프로세스의 수명주기를 관리한다."""

    name: str
    cmd: list[str]
    max_restarts: int = 5
    backoff_base: float = 2.0
    backoff_max: float = 30.0

    _proc: asyncio.subprocess.Process | None = field(default=None, repr=False)
    _state: WorkerState = field(default=WorkerState.IDLE)
    _restarts: int = 0
    _started_at: float = 0.0
    _last_exit_code: int | None = None

    @property
    def state(self) -> WorkerState:
        return self._state

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc else None

    @property
    def restarts(self) -> int:
        return self._restarts

    @property
    def uptime(self) -> float:
        if self._state == WorkerState.RUNNING and self._started_at:
            loop = asyncio.get_event_loop()
            return loop.time() - self._started_at
        return 0.0

    async def run(self) -> None:
        """워커의 전체 수명주기: 시작 → 감시 → 재시작을 반복한다.
        CancelledError로 외부에서 중단할 수 있다."""
        while True:
            await self._start()
            assert self._proc is not None
            stderr_task = asyncio.create_task(self._stream_stderr())
            self._last_exit_code = await self._proc.wait()
            stderr_task.cancel()
            self._state = WorkerState.IDLE
            log.warning("[%s] Exited with code %d (uptime %.1fs)",
                        self.name, self._last_exit_code, self.uptime)

            if self._restarts >= self.max_restarts:
                log.error("[%s] Max restarts (%d) reached — giving up",
                          self.name, self.max_restarts)
                self._state = WorkerState.DEAD
                return

            self._restarts += 1
            delay = min(self.backoff_base ** self._restarts, self.backoff_max)
            self._state = WorkerState.BACKOFF
            log.info("[%s] Restarting in %.1fs (%d/%d) …",
                     self.name, delay, self._restarts, self.max_restarts)
            await asyncio.sleep(delay)

    async def stop(self, timeout: float = 5.0) -> None:
        if self._proc is None or self._proc.returncode is not None:
            return
        log.info("[%s] Sending SIGTERM to PID %d …", self.name, self._proc.pid)
        self._proc.terminate()
        try:
            await asyncio.wait_for(self._proc.wait(), timeout=timeout)
            log.info("[%s] PID %d terminated", self.name, self._proc.pid)
        except asyncio.TimeoutError:
            log.warning("[%s] PID %d did not exit — sending SIGKILL",
                        self.name, self._proc.pid)
            self._proc.kill()
            await self._proc.wait()
            log.info("[%s] PID %d killed", self.name, self._proc.pid)

    async def _start(self) -> None:
        log.info("[%s] Starting: %s", self.name, " ".join(self.cmd))
        self._proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._started_at = asyncio.get_event_loop().time()
        self._state = WorkerState.RUNNING
        log.info("[%s] PID %d started", self.name, self._proc.pid)

    async def _stream_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        try:
            async for raw_line in self._proc.stderr:
                line = raw_line.decode(errors="replace").rstrip()
                if line:
                    log.debug("[%s] %s", self.name, line)
        except asyncio.CancelledError:
            return

    def status_line(self) -> str:
        parts = [f"{self.name}: {self._state.value}"]
        if self._state == WorkerState.RUNNING:
            parts.append(f"pid={self.pid}")
            parts.append(f"uptime={self.uptime:.0f}s")
        if self._restarts:
            parts.append(f"restarts={self._restarts}/{self.max_restarts}")
        if self._last_exit_code is not None:
            parts.append(f"last_exit={self._last_exit_code}")
        return " | ".join(parts)


# ---------------------------------------------------------------------------
# RelayManager
# ---------------------------------------------------------------------------

class RelayManager:
    """모든 Worker를 관리하고, 시그널 처리와 모니터 루프를 담당한다."""

    def __init__(self, input_file: str, loop: bool, max_restarts: int,
                 stream_types: dict, endpoints: list[dict]):
        self._workers: list[Worker] = []
        for ep in endpoints:
            name = ep.get("name", f"{ep['host']}:{ep['port']}")
            st = stream_types[ep["type"]]
            cmd = self._build_cmd(input_file, loop, ep, st)
            self._workers.append(Worker(name=name, cmd=cmd, max_restarts=max_restarts))
            log.info("[%s] type=%s (%s)", name, ep["type"], st["description"])

    @staticmethod
    def _build_cmd(input_file: str, loop: bool, ep: dict, st: dict) -> list[str]:
        cmd = ["ffmpeg", "-hide_banner", "-re"]
        if loop:
            cmd += ["-stream_loop", "-1"]
        cmd += ["-i", input_file]
        # stream mapping
        for m in st["maps"]:
            cmd += ["-map", m]
        cmd += ["-c", "copy", "-f", "mpegts"]
        cmd += ["-mpegts_start_pid", str(st["mpegts_start_pid"])]
        if "pmt_pid" in ep:
            cmd += ["-mpegts_pmt_start_pid", str(ep["pmt_pid"])]
        srt_mode = ep.get("srt_mode", "caller")
        srt_url = f"srt://{ep['host']}:{ep['port']}?mode={srt_mode}&pkt_size=1316"
        cmd.append(srt_url)
        return cmd

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        for sig_name in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(__import__("signal"), sig_name),
                lambda: stop_event.set(),
            )

        log.info("Starting %d workers …", len(self._workers))

        tasks = [asyncio.create_task(w.run()) for w in self._workers]
        status_task = asyncio.create_task(self._status_loop(stop_event))

        done, _ = await asyncio.wait(
            [asyncio.create_task(stop_event.wait()),
             asyncio.gather(*tasks, return_exceptions=True)],
            return_when=asyncio.FIRST_COMPLETED,
        )

        status_task.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, status_task, return_exceptions=True)

        log.info("Stopping remaining processes …")
        await asyncio.gather(*(w.stop() for w in self._workers))

        self._print_summary()
        log.info("Goodbye.")

    async def _status_loop(self, stop_event: asyncio.Event) -> None:
        try:
            while not stop_event.is_set():
                await asyncio.sleep(60)
                self._print_summary()
        except asyncio.CancelledError:
            return

    def _print_summary(self) -> None:
        log.info("--- Status ---")
        for w in self._workers:
            log.info("  %s", w.status_line())
        alive = sum(1 for w in self._workers if w.state == WorkerState.RUNNING)
        dead = sum(1 for w in self._workers if w.state == WorkerState.DEAD)
        log.info("  [total] running=%d  dead=%d  total=%d",
                 alive, dead, len(self._workers))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(
        description="Media Relay — FFmpeg SRT multi-endpoint sender")
    parser.add_argument("-c", "--config", default="config.json",
                        help="Config file (default: config.json)")
    parser.add_argument("-i", "--input", default=None,
                        help="Override input file")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_file():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    config = load_config(str(config_path))

    input_file = args.input or config.get("input", "input.mp4")
    if not Path(input_file).is_file():
        log.error("Input file not found: %s", input_file)
        sys.exit(1)

    stream_types = config.get("stream_types", {})
    if not stream_types:
        log.error("No stream_types defined in config")
        sys.exit(1)

    endpoints = config.get("endpoints", [])
    if not endpoints:
        log.error("No endpoints defined in config")
        sys.exit(1)

    # validate endpoint types
    for ep in endpoints:
        if ep.get("type") not in stream_types:
            log.error("[%s] Unknown type '%s' — available: %s",
                      ep.get("name", "?"), ep.get("type"),
                      ", ".join(stream_types.keys()))
            sys.exit(1)

    loop = config.get("loop", True)
    max_restarts = config.get("max_restarts", 5)

    log.info("Input: %s | Loop: %s | Endpoints: %d | Max restarts: %d",
             input_file, loop, len(endpoints), max_restarts)

    manager = RelayManager(input_file, loop, max_restarts, stream_types, endpoints)
    asyncio.run(manager.run())


if __name__ == "__main__":
    main()
