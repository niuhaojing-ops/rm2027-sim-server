"""Simulated RM2027 UDP :3334 HEVC Annex-B video streamer (directory playlist + synthetic fallback)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
import shutil
import socket
import struct
import subprocess
import threading
import time
from typing import Callable, Iterator

HEADER_SIZE = 8
PAYLOAD_MAX = 1392

RAW_HEVC_EXTS: frozenset[str] = frozenset({".hevc", ".h265", ".265"})
CONTAINER_VIDEO_EXTS: frozenset[str] = frozenset(
    {".mp4", ".mkv", ".mov", ".avi", ".flv", ".webm", ".ts", ".m4v", ".mpg", ".mpeg"}
)
SUPPORTED_VIDEO_EXTS: frozenset[str] = RAW_HEVC_EXTS | CONTAINER_VIDEO_EXTS

# Minimal valid Annex-B HEVC parameter sets + IDR slice header for zero-dependency synthetic fallback.
SYNTHETIC_VPS_SPS_PPS = bytes.fromhex(
    "0000000140010c01ffff016000000300b0000003000003005dac59"
    "00000001420101016000000300b0000003000003005da00280802d165959a4932bc05a20000003002000000303c1"
    "000000014401c172b46240"
)
SYNTHETIC_IDR = bytes.fromhex("000000012601af0884a17f")
SYNTHETIC_TRAIL = bytes.fromhex("000000010201d00884a1")


def detect_available_backends() -> list[str]:
    backends: list[str] = []
    if shutil.which("gst-launch-1.0") and shutil.which("gst-inspect-1.0"):
        try:
            chk = subprocess.run(
                ["gst-inspect-1.0", "x265enc"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=2,
                check=False,
            )
            if chk.returncode == 0:
                backends.append("gstreamer")
        except Exception:
            pass
    if shutil.which("ffmpeg"):
        try:
            chk = subprocess.run(
                ["ffmpeg", "-hide_banner", "-encoders"],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            if "libx265" in chk.stdout:
                backends.append("ffmpeg")
        except Exception:
            pass
    backends.append("file")
    backends.append("synthetic")
    return backends


def resolve_video_dir_path(video_dir: str) -> Path:
    raw = (video_dir or "videos").strip() or "videos"
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        cwd_candidate = (Path.cwd() / candidate).resolve()
        if cwd_candidate.exists():
            return cwd_candidate
        pkg_candidate = (Path(__file__).resolve().parents[2] / candidate).resolve()
        if pkg_candidate.exists():
            return pkg_candidate
        return cwd_candidate
    return candidate.resolve()


def scan_video_dir(video_dir: str) -> tuple[Path, bool, list[dict[str, object]]]:
    """Scan a directory for supported video files sorted by name."""
    resolved = resolve_video_dir_path(video_dir)
    if not resolved.exists() or not resolved.is_dir():
        return resolved, False, []

    files: list[dict[str, object]] = []
    try:
        entries = sorted(resolved.iterdir(), key=lambda p: p.name.lower())
    except OSError:
        return resolved, False, []

    for entry in entries:
        if entry.name.startswith("."):
            continue
        suffix = entry.suffix.lower()
        if suffix not in SUPPORTED_VIDEO_EXTS:
            continue
        try:
            if not entry.is_file():
                continue
            size = entry.stat().st_size
        except OSError:
            continue
        if size <= 0:
            continue
        files.append(
            {
                "name": entry.name,
                "path": str(entry.resolve()),
                "size_bytes": int(size),
                "ext": suffix.lstrip(".").upper(),
                "is_raw_hevc": suffix in RAW_HEVC_EXTS,
            }
        )
    return resolved, True, files


def packetize_hevc_frame(frame_number: int, frame_bytes: bytes) -> list[bytes]:
    """Split a complete HEVC Annex-B frame into RM2027 UDP packets (8-byte BE header + <=1392B)."""
    total = len(frame_bytes)
    if total == 0:
        return []
    frame_u16 = frame_number & 0xFFFF
    packets: list[bytes] = []
    frag = 0
    for offset in range(0, total, PAYLOAD_MAX):
        chunk = frame_bytes[offset : offset + PAYLOAD_MAX]
        header = struct.pack(">HHI", frame_u16, frag, total)
        packets.append(header + chunk)
        frag += 1
    return packets


def inspect_hevc_frame(frame_bytes: bytes) -> tuple[bool, bool]:
    """Return `(is_keyframe, has_param_sets)` for an Annex-B HEVC frame."""
    is_key = False
    has_params = False
    i = 0
    n = len(frame_bytes)
    while i + 3 < n:
        sc = 0
        if frame_bytes[i : i + 4] == b"\x00\x00\x00\x01":
            sc = 4
        elif frame_bytes[i : i + 3] == b"\x00\x00\x01":
            sc = 3
        if sc:
            if i + sc < n:
                nal_type = (frame_bytes[i + sc] >> 1) & 0x3F
                if 32 <= nal_type <= 34:
                    has_params = True
                elif 16 <= nal_type <= 21:
                    is_key = True
            i += sc
        else:
            i += 1
    return is_key, has_params


def iter_annexb_nal_units(read_fn: Callable[[int], bytes]) -> Iterator[bytes]:
    """Yield individual Annex-B NAL units (including start code) from a byte stream."""
    buf = bytearray()
    eof = False

    def find_start_code(data: bytearray, start: int) -> tuple[int, int] | None:
        i = start
        limit = len(data) - 3
        while i <= limit:
            if data[i] == 0 and data[i + 1] == 0:
                if i + 3 < len(data) and data[i + 2] == 0 and data[i + 3] == 1:
                    return i, 4
                if data[i + 2] == 1:
                    return i, 3
            i += 1
        return None

    while True:
        first = find_start_code(buf, 0)
        if first is None:
            if eof:
                break
            chunk = read_fn(65536)
            if not chunk:
                eof = True
            else:
                buf.extend(chunk)
            continue

        sc_pos, sc_len = first
        if sc_pos > 0:
            del buf[:sc_pos]

        while True:
            nxt = find_start_code(buf, sc_len)
            if nxt is not None:
                nal = bytes(buf[: nxt[0]])
                del buf[: nxt[0]]
                if len(nal) > sc_len:
                    yield nal
                break
            if eof:
                if len(buf) > sc_len:
                    yield bytes(buf)
                buf.clear()
                return
            chunk = read_fn(65536)
            if not chunk:
                eof = True
            else:
                buf.extend(chunk)


def iter_annexb_access_units(read_fn: Callable[[int], bytes]) -> Iterator[bytes]:
    """Assemble Annex-B HEVC NAL units into complete Access Units (frames)."""
    current = bytearray()
    has_vcl = False
    use_aud = False

    for nal in iter_annexb_nal_units(read_fn):
        sc_len = 4 if nal.startswith(b"\x00\x00\x00\x01") else 3
        if len(nal) <= sc_len + 1:
            continue
        nal_type = (nal[sc_len] >> 1) & 0x3F

        if nal_type == 35:  # AUD
            use_aud = True
            if current and has_vcl:
                yield bytes(current)
                current.clear()
                has_vcl = False
            current.extend(nal)
            continue

        is_vcl = 0 <= nal_type <= 31
        if not use_aud:
            if nal_type in (32, 33, 34) and has_vcl:
                yield bytes(current)
                current.clear()
                has_vcl = False
            elif is_vcl and has_vcl and len(nal) > sc_len + 2:
                first_slice_flag = (nal[sc_len + 2] & 0x80) != 0
                if first_slice_flag:
                    yield bytes(current)
                    current.clear()
                    has_vcl = False

        current.extend(nal)
        if is_vcl:
            has_vcl = True

    if current and has_vcl:
        yield bytes(current)


@dataclass
class VideoStreamerConfig:
    enabled: bool = True
    target_host: str = "127.0.0.1"
    target_port: int = 3334
    auto_mirror_clients: bool = True
    backend: str = "auto"  # auto | gstreamer | ffmpeg | file | synthetic
    width: int = 1280
    height: int = 720
    fps: int = 30
    bitrate_kbps: int = 2500
    pattern: str = "ball"
    video_dir: str = "videos"
    video_file: str = ""
    loop_mode: str = "all"  # all | single
    overlay_text: str = ""
    loss_percent: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "target_host": self.target_host,
            "target_port": self.target_port,
            "auto_mirror_clients": self.auto_mirror_clients,
            "backend": self.backend,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "bitrate_kbps": self.bitrate_kbps,
            "pattern": self.pattern,
            "video_dir": self.video_dir,
            "video_file": self.video_file,
            "loop_mode": self.loop_mode,
            "overlay_text": self.overlay_text,
            "loss_percent": self.loss_percent,
        }


class HevcVideoStreamer:
    """Background UDP :3334 HEVC directory streamer compatible with RM2027 video receivers."""

    def __init__(
        self,
        config: VideoStreamerConfig | None = None,
        *,
        is_cut_callback: Callable[[], tuple[bool, str]] | None = None,
        peer_ips_callback: Callable[[], set[str]] | None = None,
    ) -> None:
        self.config = config or VideoStreamerConfig()
        self.is_cut_callback = is_cut_callback
        self.peer_ips_callback = peer_ips_callback
        self.available_backends = detect_available_backends()

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._reload_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._proc: subprocess.Popen[bytes] | None = None
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rng = random.Random()

        self.active_backend = "idle"
        self.last_error = ""
        self.status_message = ""
        self.cut_active = False
        self.cut_reason = ""
        self.frame_number = 0
        self.frames_sent = 0
        self.keyframes_sent = 0
        self.packets_sent = 0
        self.packets_dropped_sim = 0
        self.bytes_sent = 0
        self.current_fps = 0.0
        self.current_bitrate_kbps = 0.0
        self._fps_win_start = time.monotonic()
        self._fps_win_frames = 0
        self._fps_win_bytes = 0

        self.current_file = ""
        self.current_file_path = ""
        self.playlist_index = 0
        self.playlist_count = 0
        self._available_files: list[dict[str, object]] = []
        self._video_dir_resolved = ""
        self._video_dir_exists = False
        self.rescan_files()

    def rescan_files(self) -> list[dict[str, object]]:
        with self._lock:
            video_dir = self.config.video_dir
        resolved, exists, files = scan_video_dir(video_dir)
        with self._lock:
            self._video_dir_resolved = str(resolved)
            self._video_dir_exists = exists
            self._available_files = files
            self.playlist_count = len(files)
            if files and self.playlist_index >= len(files):
                self.playlist_index = 0
        return list(files)

    def _step_playlist(self, delta: int) -> dict[str, object]:
        resolved, exists, files = scan_video_dir(self.config.video_dir)
        with self._lock:
            self._video_dir_resolved = str(resolved)
            self._video_dir_exists = exists
            self._available_files = files
            self.playlist_count = len(files)
            if files:
                cur_idx = self.playlist_index
                if self.current_file_path:
                    paths = [str(f["path"]) for f in files]
                    if self.current_file_path in paths:
                        cur_idx = paths.index(self.current_file_path)
                new_idx = (cur_idx + delta) % len(files)
                self.playlist_index = new_idx
                chosen = files[new_idx]
                self.current_file = str(chosen["name"])
                self.current_file_path = str(chosen["path"])
                if self.config.video_file:
                    if self.config.loop_mode == "single":
                        self.config.video_file = str(chosen["name"])
                    else:
                        self.config.video_file = ""
            self._reload_event.set()
        self._kill_proc()
        return self.to_dict()

    def next_file(self) -> dict[str, object]:
        return self._step_playlist(1)

    def prev_file(self) -> dict[str, object]:
        return self._step_playlist(-1)

    def resolve_backend(self, file_path: Path | None = None) -> str:
        req = self.config.backend.strip().lower()
        if req == "synthetic":
            return "synthetic"
        if file_path is not None and file_path.suffix.lower() in RAW_HEVC_EXTS:
            return "file"
        if req == "file":
            return "file"
        if req == "auto":
            if "gstreamer" in self.available_backends:
                return "gstreamer"
            if "ffmpeg" in self.available_backends:
                return "ffmpeg"
            return "unavailable"
        return req

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._reload_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="rm2027-hevc-sim", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._reload_event.set()
        self._kill_proc()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        try:
            self._sock.close()
        except Exception:
            pass

    def update_config(self, updates: dict[str, object]) -> dict[str, object]:
        action = str(updates.get("action", "")).strip().lower()
        if action == "next":
            return self.next_file()
        if action == "prev":
            return self.prev_file()
        if action == "rescan":
            prev_waiting = self.active_backend == "waiting_files"
            files = self.rescan_files()
            if prev_waiting and files:
                self._reload_event.set()
            return self.to_dict()

        need_reload = False
        with self._lock:
            if "enabled" in updates:
                self.config.enabled = bool(updates["enabled"])
                need_reload = True
            if "target_host" in updates and str(updates["target_host"]).strip():
                self.config.target_host = str(updates["target_host"]).strip()
            if "target_port" in updates:
                self.config.target_port = max(1, min(65535, int(updates["target_port"])))
            if "auto_mirror_clients" in updates:
                self.config.auto_mirror_clients = bool(updates["auto_mirror_clients"])
            if "backend" in updates and str(updates["backend"]).strip():
                self.config.backend = str(updates["backend"]).strip()
                need_reload = True
            if "width" in updates:
                self.config.width = max(320, min(1920, int(updates["width"])))
                need_reload = True
            if "height" in updates:
                self.config.height = max(240, min(1080, int(updates["height"])))
                need_reload = True
            if "fps" in updates:
                self.config.fps = max(1, min(60, int(updates["fps"])))
                need_reload = True
            if "bitrate_kbps" in updates:
                self.config.bitrate_kbps = max(200, min(20000, int(updates["bitrate_kbps"])))
                need_reload = True
            if "pattern" in updates and str(updates["pattern"]).strip():
                self.config.pattern = str(updates["pattern"]).strip()
                need_reload = True
            if "video_dir" in updates and str(updates["video_dir"]).strip():
                new_dir = str(updates["video_dir"]).strip()
                if new_dir != self.config.video_dir:
                    self.config.video_dir = new_dir
                    self.playlist_index = 0
                need_reload = True
            if "loop_mode" in updates and str(updates["loop_mode"]).strip().lower() in ("all", "single"):
                self.config.loop_mode = str(updates["loop_mode"]).strip().lower()
                if self.config.loop_mode == "all" and "video_file" not in updates and action != "play_file":
                    self.config.video_file = ""
            if "playlist_index" in updates:
                self.playlist_index = max(0, int(updates["playlist_index"]))
                need_reload = True
            if "video_file" in updates:
                vf = str(updates["video_file"]).strip()
                if action == "play_file" and self.config.loop_mode == "all":
                    _, _, files = scan_video_dir(self.config.video_dir)
                    names = [str(f["name"]) for f in files]
                    if vf in names:
                        self.playlist_index = names.index(vf)
                        self.config.video_file = ""
                    else:
                        self.config.video_file = vf
                else:
                    self.config.video_file = vf
                need_reload = True
            if "overlay_text" in updates:
                self.config.overlay_text = str(updates["overlay_text"]).strip()
                need_reload = True
            if "loss_percent" in updates:
                self.config.loss_percent = max(0.0, min(80.0, float(updates["loss_percent"])))

            if need_reload:
                self._reload_event.set()

        self.rescan_files()
        if need_reload:
            self._kill_proc()
        return self.to_dict()

    def _kill_proc(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1.0)
        except Exception:
            pass
        for pipe in (proc.stdout, proc.stderr):
            if pipe is not None:
                try:
                    pipe.close()
                except Exception:
                    pass

    def _build_gstreamer_cmd(self, cfg: VideoStreamerConfig, file_path: Path) -> list[str]:
        opt = "repeat-headers=1:annexb=1:aud=1:bframes=0"
        cmd = [
            "gst-launch-1.0",
            "-q",
            "uridecodebin",
            f"uri={file_path.resolve().as_uri()}",
            "!",
            "videoconvert",
            "!",
            "videoscale",
            "add-borders=true",
            "!",
            "videorate",
            "!",
            f"video/x-raw,format=I420,width={cfg.width},height={cfg.height},framerate={cfg.fps}/1",
        ]
        if cfg.overlay_text:
            cmd.extend(
                [
                    "!",
                    "textoverlay",
                    f"text={cfg.overlay_text}",
                    "valignment=top",
                    "halignment=right",
                ]
            )
        cmd.extend(
            [
                "!",
                "x265enc",
                "speed-preset=ultrafast",
                "tune=zerolatency",
                f"key-int-max={max(10, cfg.fps)}",
                f"bitrate={cfg.bitrate_kbps}",
                f"option-string={opt}",
                "!",
                "h265parse",
                "config-interval=1",
                "!",
                "video/x-h265,stream-format=byte-stream,alignment=au",
                "!",
                "fdsink",
                "fd=1",
                "sync=true",
            ]
        )
        return cmd

    def _build_ffmpeg_cmd(self, cfg: VideoStreamerConfig, file_path: Path) -> list[str]:
        vf = (
            f"scale={cfg.width}:{cfg.height}:force_original_aspect_ratio=decrease,"
            f"pad={cfg.width}:{cfg.height}:(ow-iw)/2:(oh-ih)/2,fps={cfg.fps}"
        )
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-re",
            "-i",
            str(file_path),
            "-an",
            "-vf",
            vf,
            "-c:v",
            "libx265",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-b:v",
            f"{cfg.bitrate_kbps}k",
            "-x265-params",
            f"repeat-headers=1:annexb=1:aud=1:bframes=0:keyint={max(10, cfg.fps)}",
            "-f",
            "hevc",
            "pipe:1",
        ]

    def _resolve_playlist(self, cfg: VideoStreamerConfig) -> tuple[Path, bool, list[dict[str, object]], list[Path]]:
        resolved, exists, files = scan_video_dir(cfg.video_dir)
        vf = cfg.video_file.strip()
        if vf:
            p = Path(vf).expanduser()
            if not p.is_absolute():
                in_dir = (resolved / p).resolve()
                p = in_dir if in_dir.is_file() else p.resolve()
            else:
                p = p.resolve()
            if p.is_file():
                return resolved, exists, files, [p]
            return resolved, exists, files, []
        return resolved, exists, files, [Path(str(f["path"])) for f in files]

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                self._reload_event.clear()
                cfg = VideoStreamerConfig(**vars(self.config))
                idx = self.playlist_index

            if not cfg.enabled:
                self.active_backend = "disabled"
                self.status_message = "图传推流已关闭"
                self.current_fps = 0.0
                self.current_bitrate_kbps = 0.0
                self._reload_event.wait(0.5)
                continue

            if cfg.backend.strip().lower() == "synthetic":
                self.active_backend = "synthetic"
                self.current_file = "(synthetic)"
                self.current_file_path = ""
                self.status_message = "合成 Annex-B 测试帧模式"
                try:
                    self._stream_synthetic(cfg)
                except Exception as exc:
                    self.last_error = str(exc)
                    self._reload_event.wait(0.5)
                continue

            resolved_dir, exists, files, playlist = self._resolve_playlist(cfg)
            with self._lock:
                self._video_dir_resolved = str(resolved_dir)
                self._video_dir_exists = exists
                self._available_files = files
                self.playlist_count = len(playlist)

            if not playlist:
                self.active_backend = "waiting_files"
                self.current_file = ""
                self.current_file_path = ""
                self.current_fps = 0.0
                self.current_bitrate_kbps = 0.0
                self.last_error = ""
                if cfg.video_file.strip():
                    self.status_message = f"指定的视频文件不存在: {cfg.video_file}"
                elif not exists:
                    self.status_message = f"视频目录不存在: {resolved_dir}"
                else:
                    self.status_message = f"目录中暂无视频文件，请将视频放入 {resolved_dir}"
                self._reload_event.wait(1.5)
                continue

            cur_idx = idx % len(playlist)
            target_path = playlist[cur_idx]
            backend = self.resolve_backend(target_path)
            self.active_backend = backend
            self.current_file = target_path.name
            self.current_file_path = str(target_path)
            self.status_message = f"正在从目录推流 ({cur_idx + 1}/{len(playlist)}): {target_path.name}"

            try:
                if backend in ("gstreamer", "ffmpeg"):
                    self._stream_subprocess(cfg, backend, target_path)
                elif backend == "file":
                    self._stream_hevc_file(cfg, target_path)
                else:
                    self.last_error = "未检测到 GStreamer(x265enc) 或 FFmpeg(libx265)，当前仅支持 .hevc/.h265 裸流文件"
                    self.status_message = self.last_error
                    self._reload_event.wait(2.0)
                    continue
            except Exception as exc:
                self.last_error = str(exc)
                self._kill_proc()
                self._reload_event.wait(0.8)
                continue

            if not self._stop_event.is_set() and not self._reload_event.is_set():
                with self._lock:
                    if self.config.loop_mode == "all" and len(playlist) > 1:
                        self.playlist_index = (cur_idx + 1) % len(playlist)

    def _stream_subprocess(self, cfg: VideoStreamerConfig, backend: str, file_path: Path) -> None:
        cmd = (
            self._build_gstreamer_cmd(cfg, file_path)
            if backend == "gstreamer"
            else self._build_ffmpeg_cmd(cfg, file_path)
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._proc = proc
        self.last_error = ""
        assert proc.stdout is not None
        stdout = proc.stdout

        def read_chunk(n: int) -> bytes:
            if self._stop_event.is_set() or self._reload_event.is_set():
                return b""
            return stdout.read(n)

        for frame_bytes in iter_annexb_access_units(read_chunk):
            if self._stop_event.is_set() or self._reload_event.is_set():
                break
            self._emit_frame(cfg, frame_bytes)

        rc = proc.poll()
        err_text = ""
        if proc.stderr is not None and rc not in (None, 0):
            try:
                err_text = proc.stderr.read().decode("utf-8", errors="replace").strip()
            except Exception:
                pass
        self._kill_proc()
        if err_text and not self._reload_event.is_set() and not self._stop_event.is_set():
            self.last_error = err_text[:240]

    def _stream_hevc_file(self, cfg: VideoStreamerConfig, file_path: Path) -> None:
        if not file_path.is_file():
            raise FileNotFoundError(f"视频文件不存在: {file_path}")
        frame_interval = 1.0 / max(1, cfg.fps)
        emitted = 0
        self.last_error = ""
        with open(file_path, "rb") as fh:
            for frame_bytes in iter_annexb_access_units(fh.read):
                if self._stop_event.is_set() or self._reload_event.is_set():
                    return
                t0 = time.monotonic()
                self._emit_frame(cfg, frame_bytes)
                emitted += 1
                dt = time.monotonic() - t0
                if dt < frame_interval:
                    self._reload_event.wait(frame_interval - dt)
        if emitted == 0 and not self._stop_event.is_set() and not self._reload_event.is_set():
            self.last_error = f"未能从 {file_path.name} 解析到有效的 HEVC Annex-B 帧"
            self._reload_event.wait(0.8)

    def _stream_synthetic(self, cfg: VideoStreamerConfig) -> None:
        frame_interval = 1.0 / max(1, cfg.fps)
        idx = 0
        self.last_error = ""
        while not self._stop_event.is_set() and not self._reload_event.is_set():
            t0 = time.monotonic()
            is_key = (idx % max(1, cfg.fps)) == 0
            if is_key:
                frame = SYNTHETIC_VPS_SPS_PPS + SYNTHETIC_IDR + bytes([idx & 0xFF] * 32)
            else:
                frame = SYNTHETIC_TRAIL + bytes([idx & 0xFF] * 16)
            self._emit_frame(cfg, frame)
            idx += 1
            dt = time.monotonic() - t0
            if dt < frame_interval:
                self._reload_event.wait(frame_interval - dt)

    def _emit_frame(self, cfg: VideoStreamerConfig, frame_bytes: bytes) -> None:
        if self.is_cut_callback is not None:
            cut, reason = self.is_cut_callback()
            self.cut_active = cut
            self.cut_reason = reason
            if cut:
                self.current_fps = 0.0
                self.current_bitrate_kbps = 0.0
                return
        else:
            self.cut_active = False
            self.cut_reason = ""

        self.frame_number = (self.frame_number + 1) & 0xFFFF
        is_key, _ = inspect_hevc_frame(frame_bytes)
        packets = packetize_hevc_frame(self.frame_number, frame_bytes)
        if not packets:
            return

        targets = {(cfg.target_host, cfg.target_port)}
        if cfg.auto_mirror_clients and self.peer_ips_callback is not None:
            for ip in self.peer_ips_callback():
                if ip and ip not in ("127.0.0.1", "::1"):
                    targets.add((ip, cfg.target_port))

        for pkt in packets:
            if cfg.loss_percent > 0.0 and self._rng.random() * 100.0 < cfg.loss_percent:
                self.packets_dropped_sim += 1
                continue
            for host, port in targets:
                try:
                    self._sock.sendto(pkt, (host, port))
                except OSError:
                    pass
            self.packets_sent += 1
            self.bytes_sent += len(pkt)

        self.frames_sent += 1
        if is_key:
            self.keyframes_sent += 1

        now = time.monotonic()
        self._fps_win_frames += 1
        self._fps_win_bytes += len(frame_bytes)
        elapsed = now - self._fps_win_start
        if elapsed >= 0.5:
            self.current_fps = round(self._fps_win_frames / elapsed, 1)
            self.current_bitrate_kbps = round((self._fps_win_bytes * 8.0 / 1000.0) / elapsed, 0)
            self._fps_win_start = now
            self._fps_win_frames = 0
            self._fps_win_bytes = 0

    def to_dict(self) -> dict[str, object]:
        with self._lock:
            vdir_resolved = self._video_dir_resolved
            vdir_exists = self._video_dir_exists
            files = list(self._available_files)
            p_idx = self.playlist_index
            p_cnt = self.playlist_count
            cfg_dict = self.config.to_dict()
        return {
            "config": cfg_dict,
            "available_backends": self.available_backends,
            "active_backend": self.active_backend,
            "video_dir_resolved": vdir_resolved,
            "video_dir_exists": vdir_exists,
            "available_files": files,
            "current_file": self.current_file,
            "current_file_path": self.current_file_path,
            "playlist_index": p_idx,
            "playlist_count": p_cnt,
            "status_message": self.status_message,
            "cut_active": self.cut_active,
            "cut_reason": self.cut_reason,
            "last_error": self.last_error,
            "frame_number": self.frame_number,
            "frames_sent": self.frames_sent,
            "keyframes_sent": self.keyframes_sent,
            "packets_sent": self.packets_sent,
            "packets_dropped_sim": self.packets_dropped_sim,
            "bytes_sent": self.bytes_sent,
            "current_fps": self.current_fps,
            "current_bitrate_kbps": self.current_bitrate_kbps,
        }
