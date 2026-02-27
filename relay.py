#!/usr/bin/env python3
"""Media Relay - FFmpeg SRT 16채널 동시 전송 프로그램 (asyncio)."""

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import signal as _signal
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from aiohttp import web

log = logging.getLogger("media-relay")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(config: dict) -> None:
    """콘솔 + 선택적 TimedRotatingFileHandler(일별) 로깅을 구성한다."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    # optional file handler (daily rotation)
    log_file = config.get("log_file")
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        backup_count = config.get("log_backup_count", 30)
        fh = logging.handlers.TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=backup_count,
        )
        fh.suffix = "%Y-%m-%d"
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        root.addHandler(fh)
        log.info("File logging enabled: %s (daily rotation, keep %d days)",
                 log_file, backup_count)


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
    stream_type: str = ""
    pair: str = ""
    max_restarts: int = 5
    backoff_base: float = 2.0
    backoff_max: float = 30.0

    _proc: asyncio.subprocess.Process | None = field(default=None, repr=False)
    _state: WorkerState = field(default=WorkerState.IDLE)
    _restarts: int = 0
    _started_at: float = 0.0
    _last_exit_code: int | None = None
    _stats: dict = field(default_factory=dict)
    _suspended: bool = field(default=False, repr=False)

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

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    async def prepare(self) -> None:
        """FFmpeg 프로세스를 생성하고 즉시 SIGSTOP으로 정지시킨다."""
        log.info("[%s] Preparing (fork+suspend): %s", self.name, " ".join(self.cmd))
        self._stats.clear()
        self._proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            os.kill(self._proc.pid, _signal.SIGSTOP)
            self._suspended = True
        except ProcessLookupError:
            self._suspended = False
        log.info("[%s] PID %d prepared%s", self.name, self._proc.pid,
                 " (suspended)" if self._suspended else "")

    def release(self) -> None:
        """SIGCONT로 정지된 FFmpeg 프로세스를 재개한다."""
        if self._proc and self._suspended:
            try:
                os.kill(self._proc.pid, _signal.SIGCONT)
            except ProcessLookupError:
                pass
            self._suspended = False
        self._started_at = asyncio.get_event_loop().time()
        self._state = WorkerState.RUNNING
        log.info("[%s] PID %d released", self.name, self._proc.pid)

    async def run(self, skip_first_start: bool = False) -> None:
        """워커의 전체 수명주기: 시작 → 감시 → 재시작을 반복한다.
        skip_first_start=True이면 prepare()+release()로 이미 시작된 프로세스를 감시한다."""
        first = True
        while True:
            if first and skip_first_start:
                first = False
            else:
                await self._start()
            assert self._proc is not None
            stderr_task = asyncio.create_task(self._stream_stderr())
            stdout_task = asyncio.create_task(self._stream_stdout())
            self._last_exit_code = await self._proc.wait()
            stderr_task.cancel()
            stdout_task.cancel()
            await asyncio.gather(stderr_task, stdout_task, return_exceptions=True)
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
        if self._suspended:
            try:
                os.kill(self._proc.pid, _signal.SIGCONT)
            except ProcessLookupError:
                pass
            self._suspended = False
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
        self._stats.clear()
        self._proc = await asyncio.create_subprocess_exec(
            *self.cmd,
            stdout=asyncio.subprocess.PIPE,
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

    async def _stream_stdout(self) -> None:
        """FFmpeg -progress pipe:1 출력을 파싱하여 통계를 수집한다."""
        assert self._proc is not None and self._proc.stdout is not None
        try:
            async for raw_line in self._proc.stdout:
                line = raw_line.decode(errors="replace").rstrip()
                if "=" in line:
                    key, _, value = line.partition("=")
                    self._stats[key.strip()] = value.strip()
        except asyncio.CancelledError:
            return

    def status_line(self) -> str:
        parts = [f"{self.name}: {self._state.value}"]
        if self._state == WorkerState.RUNNING:
            parts.append(f"pid={self.pid}")
            parts.append(f"uptime={self.uptime:.0f}s")
            if self._stats.get("bitrate"):
                parts.append(f"bitrate={self._stats['bitrate']}")
            if self._stats.get("speed"):
                parts.append(f"speed={self._stats['speed']}")
        if self._restarts:
            parts.append(f"restarts={self._restarts}/{self.max_restarts}")
        if self._last_exit_code is not None:
            parts.append(f"last_exit={self._last_exit_code}")
        return " | ".join(parts)


# ---------------------------------------------------------------------------
# HTTP Status API
# ---------------------------------------------------------------------------

class StatusAPI:
    """aiohttp 기반 경량 HTTP 상태 API."""

    def __init__(self, workers: list[Worker], worker_tasks: dict | None = None,
                 port: int = 8080):
        self._workers = workers
        self._worker_tasks: dict[str, asyncio.Task] = worker_tasks or {}
        self._port = port
        self._app = web.Application()
        self._app.router.add_get("/health", self._health)
        self._app.router.add_get("/status", self._status)
        self._app.router.add_post("/workers/{name}/stop", self._stop_worker)
        self._app.router.add_post("/workers/{name}/start", self._start_worker)
        self._app.router.add_post("/workers/{name}/restart", self._restart_worker)
        self._app.router.add_post("/workers/restart-all", self._restart_all)
        self._app.router.add_post("/workers/type/{stype}/stop", self._stop_type)
        self._app.router.add_post("/workers/type/{stype}/start", self._start_type)
        self._app.router.add_post("/workers/type/{stype}/restart", self._restart_type)
        self._app.router.add_post("/workers/pair/{pair}/stop", self._stop_pair)
        self._app.router.add_post("/workers/pair/{pair}/start", self._start_pair)
        self._app.router.add_post("/workers/pair/{pair}/restart", self._restart_pair)
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self._port)
        await site.start()
        log.info("Status API listening on port %d", self._port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            log.info("Status API stopped")

    async def _health(self, request: web.Request) -> web.Response:
        running = sum(1 for w in self._workers
                      if w.state == WorkerState.RUNNING)
        total = len(self._workers)
        healthy = running > 0
        body = {
            "healthy": healthy,
            "running": running,
            "total": total,
        }
        status_code = 200 if healthy else 503
        return web.json_response(body, status=status_code)

    async def _status(self, request: web.Request) -> web.Response:
        workers = []
        for w in self._workers:
            workers.append({
                "name": w.name,
                "type": w.stream_type,
                "state": w.state.value,
                "pid": w.pid,
                "uptime": round(w.uptime, 1),
                "restarts": w.restarts,
                "stats": w.stats,
            })
        return web.json_response({"workers": workers})

    async def _stop_and_cancel(self, worker: Worker) -> None:
        """워커를 중지하고 모니터링 태스크를 취소한다."""
        old_task = self._worker_tasks.get(worker.name)
        if old_task:
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        await worker.stop()
        worker._state = WorkerState.IDLE

    async def _sync_start_workers(self, workers: list[Worker]) -> list[str]:
        """페어별 동기 시작: SIGSTOP으로 준비 후 SIGCONT로 동시 시작."""
        pairs: dict[str, list[Worker]] = {}
        no_pair: list[Worker] = []
        for w in workers:
            if w.pair:
                pairs.setdefault(w.pair, []).append(w)
            else:
                no_pair.append(w)

        started: list[str] = []

        async def _start_pair_sync(pair_workers: list[Worker]) -> None:
            await asyncio.gather(*(w.prepare() for w in pair_workers))
            for w in pair_workers:
                w.release()
            for w in pair_workers:
                w._restarts = 0
                self._worker_tasks[w.name] = asyncio.create_task(
                    w.run(skip_first_start=True))
                started.append(w.name)

        await asyncio.gather(*(_start_pair_sync(pw) for pw in pairs.values()))

        for w in no_pair:
            w._restarts = 0
            w._state = WorkerState.IDLE
            self._worker_tasks[w.name] = asyncio.create_task(w.run())
            started.append(w.name)

        return started

    def _get_workers_by_pair(self, pair: str) -> list[Worker]:
        return [w for w in self._workers if w.pair == pair]

    async def _stop_worker(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        worker = next((w for w in self._workers if w.name == name), None)
        if not worker:
            return web.json_response({"error": f"Worker '{name}' not found"},
                                     status=404)
        if worker.state not in (WorkerState.RUNNING, WorkerState.BACKOFF):
            return web.json_response({"error": f"Worker '{name}' is not running",
                                      "state": worker.state.value}, status=400)
        log.info("[API] Stop requested for %s", name)
        old_task = self._worker_tasks.get(name)
        if old_task:
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        await worker.stop()
        worker._state = WorkerState.IDLE
        return web.json_response({"ok": True, "stopped": name})

    async def _start_worker(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        worker = next((w for w in self._workers if w.name == name), None)
        if not worker:
            return web.json_response({"error": f"Worker '{name}' not found"},
                                     status=404)
        if worker.state == WorkerState.RUNNING:
            return web.json_response({"error": f"Worker '{name}' is already running"},
                                     status=400)
        log.info("[API] Start requested for %s", name)
        worker._restarts = 0
        worker._state = WorkerState.IDLE
        self._worker_tasks[name] = asyncio.create_task(worker.run())
        return web.json_response({"ok": True, "started": name})

    async def _restart_worker(self, request: web.Request) -> web.Response:
        name = request.match_info["name"]
        worker = next((w for w in self._workers if w.name == name), None)
        if not worker:
            return web.json_response({"error": f"Worker '{name}' not found"},
                                     status=404)
        log.info("[API] Restart requested for %s", name)
        await worker.stop()
        # cancel old task and start new one
        old_task = self._worker_tasks.get(name)
        if old_task:
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        worker._restarts = 0
        worker._state = WorkerState.IDLE
        self._worker_tasks[name] = asyncio.create_task(worker.run())
        return web.json_response({"ok": True, "restarted": name})

    async def _restart_all(self, request: web.Request) -> web.Response:
        log.info("[API] Sync restart-all requested")
        await asyncio.gather(*(self._stop_and_cancel(w) for w in self._workers))
        started = await self._sync_start_workers(self._workers)
        return web.json_response({"ok": True, "restarted": started})

    def _get_workers_by_type(self, stype: str) -> list[Worker]:
        return [w for w in self._workers if w.stream_type == stype]

    async def _stop_type(self, request: web.Request) -> web.Response:
        stype = request.match_info["stype"]
        workers = self._get_workers_by_type(stype)
        if not workers:
            return web.json_response({"error": f"No workers with type '{stype}'"},
                                     status=404)
        to_stop = [w for w in workers
                    if w.state in (WorkerState.RUNNING, WorkerState.BACKOFF)]
        log.info("[API] Stop requested for type=%s (%d workers)", stype, len(to_stop))
        await asyncio.gather(*(self._stop_and_cancel(w) for w in to_stop))
        return web.json_response({"ok": True, "stopped": [w.name for w in to_stop]})

    async def _start_type(self, request: web.Request) -> web.Response:
        stype = request.match_info["stype"]
        workers = self._get_workers_by_type(stype)
        if not workers:
            return web.json_response({"error": f"No workers with type '{stype}'"},
                                     status=404)
        to_start = [w for w in workers if w.state != WorkerState.RUNNING]
        log.info("[API] Sync start requested for type=%s (%d workers)",
                 stype, len(to_start))
        started = await self._sync_start_workers(to_start)
        return web.json_response({"ok": True, "started": started})

    async def _restart_type(self, request: web.Request) -> web.Response:
        stype = request.match_info["stype"]
        workers = self._get_workers_by_type(stype)
        if not workers:
            return web.json_response({"error": f"No workers with type '{stype}'"},
                                     status=404)
        log.info("[API] Sync restart requested for type=%s (%d workers)",
                 stype, len(workers))
        await asyncio.gather(*(self._stop_and_cancel(w) for w in workers))
        started = await self._sync_start_workers(workers)
        return web.json_response({"ok": True, "restarted": started})

    async def _stop_pair(self, request: web.Request) -> web.Response:
        pair = request.match_info["pair"]
        workers = self._get_workers_by_pair(pair)
        if not workers:
            return web.json_response({"error": f"No workers with pair '{pair}'"},
                                     status=404)
        to_stop = [w for w in workers
                    if w.state in (WorkerState.RUNNING, WorkerState.BACKOFF)]
        log.info("[API] Stop requested for pair=%s (%d workers)", pair, len(to_stop))
        await asyncio.gather(*(self._stop_and_cancel(w) for w in to_stop))
        return web.json_response({"ok": True, "stopped": [w.name for w in to_stop]})

    async def _start_pair(self, request: web.Request) -> web.Response:
        pair = request.match_info["pair"]
        workers = self._get_workers_by_pair(pair)
        if not workers:
            return web.json_response({"error": f"No workers with pair '{pair}'"},
                                     status=404)
        to_start = [w for w in workers if w.state != WorkerState.RUNNING]
        if not to_start:
            return web.json_response(
                {"error": f"All workers in pair '{pair}' already running"},
                status=400)
        log.info("[API] Sync start requested for pair=%s (%d workers)",
                 pair, len(to_start))
        started = await self._sync_start_workers(to_start)
        return web.json_response({"ok": True, "started": started})

    async def _restart_pair(self, request: web.Request) -> web.Response:
        pair = request.match_info["pair"]
        workers = self._get_workers_by_pair(pair)
        if not workers:
            return web.json_response({"error": f"No workers with pair '{pair}'"},
                                     status=404)
        log.info("[API] Sync restart requested for pair=%s (%d workers)",
                 pair, len(workers))
        await asyncio.gather(*(self._stop_and_cancel(w) for w in workers))
        started = await self._sync_start_workers(workers)
        return web.json_response({"ok": True, "restarted": started})


# ---------------------------------------------------------------------------
# RelayManager
# ---------------------------------------------------------------------------

class RelayManager:
    """모든 Worker를 관리하고, 시그널 처리와 모니터 루프를 담당한다."""

    def __init__(self, input_file: str, loop: bool, max_restarts: int,
                 stream_types: dict, endpoints: list[dict],
                 api_port: int = 8080):
        self._workers: list[Worker] = []
        self._api_port = api_port
        for ep in endpoints:
            name = ep.get("name", f"{ep['host']}:{ep['port']}")
            st = stream_types[ep["type"]]
            cmd = self._build_cmd(input_file, loop, ep, st)
            self._workers.append(Worker(name=name, cmd=cmd, stream_type=ep["type"],
                                          pair=ep.get("pair", ""),
                                          max_restarts=max_restarts))
            log.info("[%s] type=%s pair=%s (%s)", name, ep["type"],
                     ep.get("pair", "-"), st["description"])

    @staticmethod
    def _build_cmd(input_file: str, loop: bool, ep: dict, st: dict) -> list[str]:
        cmd = ["ffmpeg", "-hide_banner", "-nostats", "-re"]
        if loop:
            cmd += ["-stream_loop", "-1"]
        cmd += ["-i", input_file]
        cmd += ["-progress", "pipe:1"]
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

        log.info("Starting %d workers (pair-sync) …", len(self._workers))

        worker_tasks: dict[str, asyncio.Task] = {}
        pairs: dict[str, list[Worker]] = {}
        no_pair: list[Worker] = []
        for w in self._workers:
            if w.pair:
                pairs.setdefault(w.pair, []).append(w)
            else:
                no_pair.append(w)

        async def _start_pair_sync(pair_workers: list[Worker]) -> None:
            await asyncio.gather(*(w.prepare() for w in pair_workers))
            for w in pair_workers:
                w.release()
            for w in pair_workers:
                worker_tasks[w.name] = asyncio.create_task(
                    w.run(skip_first_start=True))

        await asyncio.gather(*(_start_pair_sync(pw) for pw in pairs.values()))
        for w in no_pair:
            worker_tasks[w.name] = asyncio.create_task(w.run())

        log.info("All %d workers started (%d pairs)", len(self._workers), len(pairs))

        # Start HTTP Status API
        api = StatusAPI(self._workers, worker_tasks=worker_tasks, port=self._api_port)
        await api.start()

        status_task = asyncio.create_task(self._status_loop(stop_event))

        done, _ = await asyncio.wait(
            [asyncio.create_task(stop_event.wait()),
             asyncio.gather(*worker_tasks.values(), return_exceptions=True)],
            return_when=asyncio.FIRST_COMPLETED,
        )

        status_task.cancel()
        for t in worker_tasks.values():
            t.cancel()
        await asyncio.gather(*worker_tasks.values(), status_task, return_exceptions=True)

        log.info("Stopping remaining processes …")
        await asyncio.gather(*(w.stop() for w in self._workers))

        await api.stop()

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
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    config = load_config(str(config_path))

    # Setup logging (must come after config load)
    setup_logging(config)

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
    api_port = config.get("api_port", 8080)

    log.info("Input: %s | Loop: %s | Endpoints: %d | Max restarts: %d | API port: %d",
             input_file, loop, len(endpoints), max_restarts, api_port)

    manager = RelayManager(input_file, loop, max_restarts, stream_types, endpoints,
                           api_port=api_port)
    asyncio.run(manager.run())


if __name__ == "__main__":
    main()
