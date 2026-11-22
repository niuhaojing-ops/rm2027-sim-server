"""CLI entrypoint for standalone RM2027 simulation server."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import signal
import sys
from typing import Any, Sequence

from . import __version__
from .diagnostics import build_full_diagnostics, format_diagnostics_report, run_mqtt_probe
from .match_engine import STAGE_NAMES, MatchSimulator
from .mqtt_broker import AuthConfig, MqttBrokerServer
from .video_streamer import HevcVideoStreamer, VideoStreamerConfig
from .web_ui import SimControlContext, SimWebServer


def parse_size(text: str) -> tuple[int, int]:
    parts = text.lower().split("x", 1)
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise argparse.ArgumentTypeError(f"分辨率格式应为 WIDTHxHEIGHT，例如 1280x720，收到: {text!r}")
    return int(parts[0]), int(parts[1])


def default_settings() -> dict[str, Any]:
    return {
        "host": "127.0.0.1",
        "port": 3333,
        "control_port": 18885,
        "auth": {
            "mode": "open",
            "users": {},
            "strict_client_id": False,
            "denied_robot_ids": [],
        },
        "match": {
            "mode": "auto",
            "random_events": "normal",
            "speed": 1.0,
            "stage": 0,
            "auto_loop": False,
            "respect_video_rules": True,
            "seed": None,
        },
        "video": {
            "enabled": True,
            "target_host": "127.0.0.1",
            "target_port": 3334,
            "auto_mirror_clients": True,
            "backend": "auto",
            "width": 1280,
            "height": 720,
            "fps": 30,
            "bitrate_kbps": 2500,
            "pattern": "ball",
            "video_dir": "videos",
            "video_file": "",
            "loop_mode": "all",
            "overlay_text": "",
            "loss_percent": 0.0,
        },
    }


def load_config_file(path_str: str) -> dict[str, Any]:
    path = Path(path_str).expanduser()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"配置文件顶层必须为 JSON 对象: {path}")
    return raw


def merge_settings(config_data: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    cfg = default_settings()

    if "host" in config_data:
        cfg["host"] = str(config_data["host"])
    if "port" in config_data:
        cfg["port"] = int(config_data["port"])
    if "control_port" in config_data:
        cfg["control_port"] = int(config_data["control_port"])

    if isinstance(config_data.get("auth"), dict):
        a = config_data["auth"]
        if isinstance(a.get("users"), dict):
            cfg["auth"]["users"] = {str(k): str(v) for k, v in a["users"].items()}
        if "mode" in a:
            cfg["auth"]["mode"] = str(a["mode"])
        elif cfg["auth"]["users"]:
            cfg["auth"]["mode"] = "required"
        if "strict_client_id" in a:
            cfg["auth"]["strict_client_id"] = bool(a["strict_client_id"])
        if isinstance(a.get("denied_robot_ids"), list):
            cfg["auth"]["denied_robot_ids"] = [int(x) for x in a["denied_robot_ids"]]

    if isinstance(config_data.get("match"), dict):
        m = config_data["match"]
        for key in ("mode", "random_events", "speed", "stage", "auto_loop", "respect_video_rules", "seed"):
            if key in m:
                cfg["match"][key] = m[key]

    if isinstance(config_data.get("video"), dict):
        v = config_data["video"]
        for key in (
            "enabled",
            "target_host",
            "target_port",
            "auto_mirror_clients",
            "backend",
            "width",
            "height",
            "fps",
            "bitrate_kbps",
            "pattern",
            "video_dir",
            "video_file",
            "loop_mode",
            "overlay_text",
            "loss_percent",
        ):
            if key in v:
                cfg["video"][key] = v[key]

    # Explicit CLI overrides (only override when user explicitly passed the flag)
    if args.host is not None:
        cfg["host"] = args.host
    if args.port is not None:
        cfg["port"] = args.port
    if args.control_port is not None:
        cfg["control_port"] = args.control_port

    cli_added_user = False
    if args.username is not None:
        cfg["auth"]["users"][args.username] = args.password or ""
        cli_added_user = True
    for item in args.user:
        if ":" not in item:
            raise ValueError(f"--user 参数格式应为 USER:PASS，收到: {item!r}")
        u, p = item.split(":", 1)
        cfg["auth"]["users"][u] = p
        cli_added_user = True
    if args.require_auth or cli_added_user:
        cfg["auth"]["mode"] = "required"
    if args.strict_client_id:
        cfg["auth"]["strict_client_id"] = True

    if args.mode is not None:
        cfg["match"]["mode"] = args.mode
    if args.random_events is not None:
        cfg["match"]["random_events"] = args.random_events
    if args.speed is not None:
        cfg["match"]["speed"] = args.speed
    if args.stage is not None:
        cfg["match"]["stage"] = args.stage
    if args.no_auto_loop:
        cfg["match"]["auto_loop"] = False
    if args.seed is not None:
        cfg["match"]["seed"] = args.seed

    if args.no_video:
        cfg["video"]["enabled"] = False
    if args.video_host is not None:
        cfg["video"]["target_host"] = args.video_host
    if args.video_port is not None:
        cfg["video"]["target_port"] = args.video_port
    if args.video_backend is not None:
        cfg["video"]["backend"] = args.video_backend
    if args.video_size is not None:
        cfg["video"]["width"], cfg["video"]["height"] = args.video_size
    if args.video_fps is not None:
        cfg["video"]["fps"] = args.video_fps
    if args.video_bitrate is not None:
        cfg["video"]["bitrate_kbps"] = args.video_bitrate
    if args.video_pattern is not None:
        cfg["video"]["pattern"] = args.video_pattern
    if args.video_dir is not None:
        cfg["video"]["video_dir"] = args.video_dir
    if args.video_file is not None:
        cfg["video"]["video_file"] = args.video_file
    if args.video_loop is not None:
        cfg["video"]["loop_mode"] = args.video_loop
    if args.video_loss is not None:
        cfg["video"]["loss_percent"] = args.video_loss

    return cfg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rm2027-sim",
        description="RM2027 模拟赛事服务器（MQTT 3.1.1 登录认证 + 自动/随机/手动赛程 + HEVC UDP:3334 图传 + 连接诊断）",
    )
    parser.add_argument("--version", action="store_true", help="显示版本号并退出")
    parser.add_argument("-c", "--config", metavar="FILE", help="加载 JSON 配置文件（命令行显式参数优先级更高）")
    parser.add_argument("--terminal-config", metavar="FILE", help="可选指定外部终端 terminal.json 以便诊断对比")

    parser.add_argument("--host", default=None, help="MQTT 与控制台监听地址 (默认 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="MQTT 监听端口 (默认 3333，与正式比赛端口一致)")
    parser.add_argument("--control-port", type=int, default=None, help="Web 控制台端口 (默认 18885，-1 为关闭)")

    # Auth options
    parser.add_argument("--username", help="MQTT 登录用户名（与 --password 配套使用）")
    parser.add_argument("--password", help="MQTT 登录密码（与 --username 配套使用）")
    parser.add_argument("--user", action="append", default=[], metavar="USER:PASS", help="追加 MQTT 账号（可多次指定）")
    parser.add_argument("--require-auth", action="store_true", help="强制要求用户名和密码登录")
    parser.add_argument("--strict-client-id", action="store_true", help="要求 Client ID 为合法 RM2027 机器人编号")

    # Match simulation options
    parser.add_argument("--mode", choices=["auto", "manual", "paused"], default=None, help="比赛推进模式")
    parser.add_argument(
        "--random-events",
        choices=["off", "low", "normal", "high"],
        default=None,
        help="规则自动随机事件触发频率",
    )
    parser.add_argument("--speed", type=float, default=None, help="时间推进倍速 (0.25~10.0)")
    parser.add_argument("--stage", type=int, default=None, choices=range(0, 6), help="初始阶段 0..5 (默认 0=未开始)")
    parser.add_argument("--no-auto-loop", action="store_true", help="比赛结束后不自动重新开启下一轮")
    parser.add_argument("--seed", type=int, default=None, help="随机种子")

    # Video options
    parser.add_argument("--no-video", action="store_true", help="禁用 UDP 图传推流")
    parser.add_argument("--video-host", default=None, help="图传 UDP 推流目标 IP (默认 127.0.0.1)")
    parser.add_argument("--video-port", type=int, default=None, help="图传 UDP 端口 (默认 3334)")
    parser.add_argument(
        "--video-backend",
        choices=["auto", "gstreamer", "ffmpeg", "file", "synthetic"],
        default=None,
        help="HEVC 编码/推流后端",
    )
    parser.add_argument("--video-size", type=parse_size, default=None, help="图传分辨率 (默认 1280x720)")
    parser.add_argument("--video-fps", type=int, default=None, help="图传帧率 (默认 30)")
    parser.add_argument("--video-bitrate", type=int, default=None, help="图传码率 kbps (默认 2500)")
    parser.add_argument("--video-pattern", default=None, help="synthetic 模式下的测试图案 (ball/smpte/snow)")
    parser.add_argument("--video-dir", default=None, help="图传视频目录 (默认 ./videos，自动扫描目录内视频文件循环推流)")
    parser.add_argument("--video-file", default=None, help="指定目录内某个视频文件名或绝对路径（留空则轮播整个目录）")
    parser.add_argument("--video-loop", choices=["all", "single"], default=None, help="目录轮播模式 (all=轮播全部视频, single=单文件循环)")
    parser.add_argument("--video-loss", type=float, default=None, help="模拟 UDP 分片丢包率 %%")

    # Diagnostic CLI tools
    parser.add_argument("--diagnose", action="store_true", help="运行本地连接与配置诊断并退出")
    parser.add_argument("--probe", metavar="HOST[:PORT]", help="向指定 MQTT 服务执行主动握手探测并退出")
    parser.add_argument("--probe-id", default="3", help="--probe 使用的 Client ID (默认 3)")
    return parser


def _build_auth(cfg: dict[str, Any]) -> AuthConfig:
    a = cfg["auth"]
    return AuthConfig(
        auth_mode="required" if a["mode"] == "required" else "open",
        users=dict(a["users"]),
        strict_client_id=bool(a["strict_client_id"]),
        denied_robot_ids=set(a["denied_robot_ids"]),
    )


def _build_video_cfg(cfg: dict[str, Any]) -> VideoStreamerConfig:
    v = cfg["video"]
    return VideoStreamerConfig(
        enabled=bool(v["enabled"]),
        target_host=str(v["target_host"]),
        target_port=int(v["target_port"]),
        auto_mirror_clients=bool(v["auto_mirror_clients"]),
        backend=str(v["backend"]),
        width=int(v["width"]),
        height=int(v["height"]),
        fps=int(v["fps"]),
        bitrate_kbps=int(v["bitrate_kbps"]),
        pattern=str(v["pattern"]),
        video_dir=str(v.get("video_dir", "videos")),
        video_file=str(v["video_file"]),
        loop_mode=str(v.get("loop_mode", "all")),
        overlay_text=str(v["overlay_text"]),
        loss_percent=float(v["loss_percent"]),
    )


async def run_server(cfg: dict[str, Any], terminal_config_path: str | None) -> None:
    auth = _build_auth(cfg)
    video_cfg = _build_video_cfg(cfg)
    m = cfg["match"]

    sim = MatchSimulator(
        mode=m["mode"],
        random_events=m["random_events"],
        speed=float(m["speed"]),
        start_stage=int(m["stage"]),
        auto_loop=bool(m["auto_loop"]),
        respect_video_rules=bool(m["respect_video_rules"]),
        seed=m["seed"],
    )

    async def on_subscribe(session, topics: list[str]) -> None:
        del topics
        await sim.push_full_snapshot_to_client(session)

    broker = MqttBrokerServer(
        host=cfg["host"],
        port=int(cfg["port"]),
        auth=auth,
        on_subscribe=on_subscribe,
        on_publish=sim.handle_client_command,
    )
    sim.attach_broker(broker)
    await broker.start()

    video = HevcVideoStreamer(
        video_cfg,
        is_cut_callback=sim.should_cut_video,
        peer_ips_callback=broker.active_peer_ips,
    )
    video.start()

    web: SimWebServer | None = None
    loop = asyncio.get_running_loop()
    control_port = int(cfg["control_port"])
    if control_port >= 0:
        ctx = SimControlContext(
            loop=loop,
            broker=broker,
            simulator=sim,
            video=video,
            listen_host=cfg["host"],
            control_port=control_port,
            terminal_config_path=terminal_config_path,
        )
        web = SimWebServer(cfg["host"], control_port, ctx)
        web.start()

    connect_ip = "127.0.0.1" if cfg["host"] == "0.0.0.0" else cfg["host"]
    auth_desc = f"required (users={list(auth.users.keys())})" if auth.auth_mode == "required" else "open (匿名可连)"
    print(f"RM2027 模拟赛事服务器已启动 (v{__version__}):", flush=True)
    print(f"  - MQTT Broker : {cfg['host']}:{broker.actual_port} (认证: {auth_desc}, strict_id={auth.strict_client_id})", flush=True)
    if web is not None:
        print(f"  - Web 控制台  : http://{connect_ip}:{web.actual_port}/", flush=True)
    if video_cfg.enabled:
        first_path = Path(str(video._available_files[0]["path"])) if video._available_files else None
        print(
            f"  - HEVC 图传流 : udp://{video_cfg.target_host}:{video_cfg.target_port} "
            f"({video_cfg.width}x{video_cfg.height}@{video_cfg.fps}fps, backend={video.resolve_backend(first_path)}, "
            f"dir={video._video_dir_resolved}, files={len(video._available_files)})",
            flush=True,
        )
    else:
        print("  - HEVC 图传流 : disabled (--no-video)", flush=True)
    if sim.stage == 0:
        print(
            f"  - 比赛模式    : stage=0 (未开始，请在控制台点击开始比赛), mode={sim.mode}, random={sim.random_events}, speed={sim.speed}x",
            flush=True,
        )
    else:
        print(
            f"  - 比赛模式    : stage={sim.stage} ({STAGE_NAMES.get(sim.stage, sim.stage)}), mode={sim.mode}, random={sim.random_events}, speed={sim.speed}x",
            flush=True,
        )
    print(f"  - 连接自检命令: rm2027-sim --probe {connect_ip}:{broker.actual_port}", flush=True)

    stop_event = asyncio.Event()

    def _on_sig() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_sig)
        except NotImplementedError:
            pass

    sim_task = asyncio.create_task(sim.run_loop())
    try:
        await stop_event.wait()
    finally:
        sim.stop_loop()
        sim_task.cancel()
        await asyncio.gather(sim_task, return_exceptions=True)
        if web is not None:
            web.stop()
        video.stop()
        await broker.stop()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print(f"rm2027-sim {__version__}")
        return 0
    if (args.username is None) != (args.password is None):
        parser.error("--username and --password must be supplied together")

    config_data = load_config_file(args.config) if args.config else {}
    cfg = merge_settings(config_data, args)

    if args.probe:
        if ":" in args.probe:
            h, p_str = args.probe.rsplit(":", 1)
            port = int(p_str)
        else:
            h, port = args.probe, int(cfg["port"])
        res = run_mqtt_probe(
            h,
            port,
            client_id=args.probe_id,
            username=args.username or "",
            password=args.password or "",
        )
        print(f"=== MQTT 主动探测目标: {res['target']} (ok={res['ok']}) ===")
        for step in res["steps"]:
            print(f"  [{step['status'].upper()}] {step['name']}: {step['detail']}")
            if step["hint"]:
                print(f"      -> 建议: {step['hint']}")
        return 0 if res["ok"] else 2

    if args.diagnose:
        report = build_full_diagnostics(
            listen_host=cfg["host"],
            mqtt_port=int(cfg["port"]),
            control_port=int(cfg["control_port"]),
            auth=_build_auth(cfg),
            video_cfg=_build_video_cfg(cfg),
            audit_records=[],
            active_sessions=[],
            terminal_config_path=args.terminal_config,
        )
        print(format_diagnostics_report(report))
        return 0

    try:
        asyncio.run(run_server(cfg, args.terminal_config))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
