"""Connection diagnostics, network/config audit, and active MQTT 3.1.1 probe."""

from __future__ import annotations

import json
from pathlib import Path
import re
import socket
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any

from .mqtt_broker import CONNACK_LABELS, AuthConfig, encode_remaining_length
from .video_streamer import VideoStreamerConfig


def _encode_mqtt_str(text: str) -> bytes:
    raw = text.encode("utf-8")
    return len(raw).to_bytes(2, "big") + raw


def build_mqtt_connect_packet(
    *,
    client_id: str,
    username: str = "",
    password: str = "",
    keep_alive: int = 15,
    clean_session: bool = True,
) -> bytes:
    flags = 0x02 if clean_session else 0x00
    payload = bytearray(_encode_mqtt_str(client_id))
    if username:
        flags |= 0x80
        payload += _encode_mqtt_str(username)
        if password != "":
            flags |= 0x40
            payload += _encode_mqtt_str(password)
    elif password:
        flags |= 0x40
        payload += _encode_mqtt_str(password)

    var_header = b"\x00\x04MQTT\x04" + bytes([flags]) + keep_alive.to_bytes(2, "big")
    body = var_header + bytes(payload)
    return b"\x10" + encode_remaining_length(len(body)) + body


def build_mqtt_subscribe_packet(packet_id: int, topic: str, qos: int = 1) -> bytes:
    body = packet_id.to_bytes(2, "big") + _encode_mqtt_str(topic) + bytes([qos & 0x03])
    return b"\x82" + encode_remaining_length(len(body)) + body


def _recv_exact_sync(sock: socket.socket, length: int) -> bytes:
    buf = bytearray()
    while len(buf) < length:
        chunk = sock.recv(length - len(buf))
        if not chunk:
            raise ConnectionError("peer closed connection")
        buf.extend(chunk)
    return bytes(buf)


def _recv_mqtt_packet_sync(sock: socket.socket) -> tuple[int, bytes]:
    first = _recv_exact_sync(sock, 1)[0]
    if first in (ord("H"), ord("G"), ord("P")) and first != 0x20:
        rest = sock.recv(128)
        banner = (bytes([first]) + rest).decode("ascii", errors="replace").splitlines()[0][:80]
        raise ValueError(f"对端返回了非 MQTT 响应: {banner!r}")
    mult = 1
    rem = 0
    for _ in range(4):
        digit = _recv_exact_sync(sock, 1)[0]
        rem += (digit & 0x7F) * mult
        if not (digit & 0x80):
            break
        mult *= 128
    payload = _recv_exact_sync(sock, rem) if rem else b""
    return first, payload


def run_mqtt_probe(
    host: str,
    port: int,
    *,
    client_id: str = "3",
    username: str = "",
    password: str = "",
    timeout: float = 2.5,
) -> dict[str, Any]:
    """Run an end-to-end TCP -> MQTT CONNECT/CONNACK -> SUBSCRIBE probe against a broker."""
    steps: list[dict[str, Any]] = []
    suggestions: list[str] = []
    target = f"{host}:{port}"

    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        resolved_ip = infos[0][4][0]
        steps.append(
            {
                "name": "地址解析",
                "status": "ok",
                "detail": f"{host} -> {resolved_ip}:{port}",
                "hint": "",
            }
        )
    except OSError as exc:
        steps.append(
            {
                "name": "地址解析",
                "status": "error",
                "detail": str(exc),
                "hint": "请检查主机名或 IP 地址拼写是否正确。",
            }
        )
        return {"ok": False, "target": target, "steps": steps, "suggestions": ["修正主机名/IP"]}

    t0 = time.monotonic()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((resolved_ip, port))
        rtt_ms = round((time.monotonic() - t0) * 1000.0, 2)
        steps.append(
            {
                "name": "TCP 连通性",
                "status": "ok",
                "detail": f"已建立 TCP 连接 ({rtt_ms} ms)",
                "hint": "",
            }
        )
    except ConnectionRefusedError:
        sock.close()
        hint = (
            f"目标 {resolved_ip}:{port} 拒绝连接：端口上没有服务在监听。"
            "请确认模拟服务器或官方选手端已启动，且地址和端口（模拟服默认 127.0.0.1:3333，正式比赛 192.168.12.1:3333）填写一致。"
        )
        steps.append(
            {
                "name": "TCP 连通性",
                "status": "error",
                "detail": "ConnectionRefusedError",
                "hint": hint,
            }
        )
        return {"ok": False, "target": target, "steps": steps, "suggestions": [hint]}
    except TimeoutError:
        sock.close()
        hint = (
            f"连接 {resolved_ip}:{port} 超时：检查网线、防火墙（ufw/iptables），"
            "若连接官方选手端 192.168.12.1，请确认本机网卡已设为 192.168.12.2/24。"
        )
        steps.append(
            {
                "name": "TCP 连通性",
                "status": "error",
                "detail": f"Timeout ({timeout}s)",
                "hint": hint,
            }
        )
        return {"ok": False, "target": target, "steps": steps, "suggestions": [hint]}
    except OSError as exc:
        sock.close()
        hint = f"网络不可达 ({exc})：请检查本机路由或目标 IP 网段。"
        steps.append(
            {
                "name": "TCP 连通性",
                "status": "error",
                "detail": str(exc),
                "hint": hint,
            }
        )
        return {"ok": False, "target": target, "steps": steps, "suggestions": [hint]}

    try:
        pkt = build_mqtt_connect_packet(
            client_id=client_id,
            username=username,
            password=password,
        )
        sock.sendall(pkt)
        fb, body = _recv_mqtt_packet_sync(sock)
        if (fb >> 4) != 2 or len(body) < 2:
            raise ValueError(f"期望 CONNACK，但收到报文类型 {fb >> 4}")
        code = body[1]
        label, hint = CONNACK_LABELS.get(code, (f"Code_{code}", "未知 CONNACK 错误码"))
        if code != 0:
            steps.append(
                {
                    "name": "MQTT 登录握手 (CONNECT/CONNACK)",
                    "status": "error",
                    "detail": f"被拒绝: 0x{code:02x} ({label})",
                    "hint": hint,
                }
            )
            sock.close()
            return {"ok": False, "target": target, "steps": steps, "suggestions": [hint]}
        steps.append(
            {
                "name": "MQTT 登录握手 (CONNECT/CONNACK)",
                "status": "ok",
                "detail": f"CONNACK Accepted (client_id={client_id})",
                "hint": "",
            }
        )

        sock.sendall(build_mqtt_subscribe_packet(1, "GameStatus", qos=1))
        got_suback = False
        got_publish = False
        deadline = time.monotonic() + min(timeout, 1.5)
        while time.monotonic() < deadline and not (got_suback and got_publish):
            sock.settimeout(max(0.2, deadline - time.monotonic()))
            try:
                pfb, _ = _recv_mqtt_packet_sync(sock)
            except TimeoutError:
                break
            ptype = pfb >> 4
            if ptype == 9:
                got_suback = True
            elif ptype == 3:
                got_publish = True
        steps.append(
            {
                "name": "主题订阅与下行流",
                "status": "ok" if got_suback else "warn",
                "detail": (
                    "已收到 SUBACK 与初始 GameStatus 下行包"
                    if got_suback and got_publish
                    else ("已收到 SUBACK，暂无即时推送包" if got_suback else "未收到 SUBACK")
                ),
                "hint": "" if got_suback else "服务端未响应 SUBSCRIBE，请确认其实现了完整 MQTT 3.1.1 协议。",
            }
        )
        try:
            sock.sendall(b"\xe0\x00")
        except Exception:
            pass
        sock.close()
        return {"ok": got_suback, "target": target, "steps": steps, "suggestions": suggestions}
    except Exception as exc:
        sock.close()
        hint = f"MQTT 握手异常: {exc}。常见原因：连到了非 MQTT 端口（例如把 Web 控制台 18885 当成了 MQTT 端口）。"
        steps.append(
            {
                "name": "MQTT 登录握手 (CONNECT/CONNACK)",
                "status": "error",
                "detail": str(exc),
                "hint": hint,
            }
        )
        return {"ok": False, "target": target, "steps": steps, "suggestions": [hint]}


def list_local_ipv4_addresses() -> list[dict[str, str]]:
    addrs: list[dict[str, str]] = []
    try:
        res = subprocess.run(
            ["ip", "-4", "-o", "addr", "show"],
            capture_output=True,
            text=True,
            timeout=1.5,
            check=False,
        )
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                m = re.search(r"^\d+:\s+(\S+)\s+inet\s+(\d+\.\d+\.\d+\.\d+)/(\d+)", line)
                if m:
                    addrs.append({"iface": m.group(1), "ip": m.group(2), "prefix": m.group(3)})
    except Exception:
        pass
    if not addrs:
        addrs.append({"iface": "lo", "ip": "127.0.0.1", "prefix": "8"})
    return addrs


def check_port_listeners(ports: tuple[int, ...] = (3333, 3334, 8090, 18091, 18884, 18885)) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    port_set = {str(p) for p in ports}
    try:
        res = subprocess.run(
            ["ss", "-tulnH"],
            capture_output=True,
            text=True,
            timeout=1.5,
            check=False,
        )
        if res.returncode == 0:
            for line in res.stdout.splitlines():
                parts = line.split()
                if len(parts) >= 5:
                    proto = parts[0]
                    local = parts[4]
                    port_str = local.rsplit(":", 1)[-1]
                    if port_str in port_set:
                        entries.append({"proto": proto, "state": parts[1], "local": local})
    except Exception:
        pass
    return entries


def find_terminal_config_path(explicit_path: str | None = None) -> Path | None:
    if explicit_path:
        p = Path(explicit_path).expanduser()
        return p if p.is_file() else None
    local_cfg = Path.cwd() / "config" / "terminal.json"
    return local_cfg if local_cfg.is_file() else None


def inspect_terminal_config(
    *,
    sim_listen_host: str,
    sim_mqtt_port: int,
    sim_auth: AuthConfig,
    sim_video_cfg: VideoStreamerConfig,
    terminal_config_path: str | None = None,
) -> dict[str, Any]:
    path = find_terminal_config_path(terminal_config_path)
    if path is None:
        return {"found": False, "path": terminal_config_path, "issues": [], "config": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"found": True, "path": str(path), "issues": [f"配置文件解析失败: {exc}"], "config": None}

    mqtt_cfg = data.get("mqtt", {})
    video_cfg = data.get("video", {})
    api_cfg = data.get("api", {})
    issues: list[str] = []

    cfg_host = str(mqtt_cfg.get("host", ""))
    cfg_port = int(mqtt_cfg.get("port", 0) or 0)
    expected_connect_host = "127.0.0.1" if sim_listen_host in ("0.0.0.0", "127.0.0.1", "localhost") else sim_listen_host
    if not mqtt_cfg.get("enabled", True):
        issues.append("客户端配置中 mqtt.enabled 为 false，启动时不会自动连接 MQTT。")
    if cfg_host == "192.168.12.1" and sim_listen_host in ("127.0.0.1", "localhost", "0.0.0.0"):
        if cfg_port != sim_mqtt_port:
            issues.append(
                f"客户端配置默认连接官方选手端 {cfg_host}:{cfg_port}，而模拟服务器监听在 {sim_listen_host}:{sim_mqtt_port}。"
                f"请在客户端改为连接 127.0.0.1:{sim_mqtt_port}（或将模拟服务器以 --host 192.168.12.1 --port {cfg_port} 启动）。"
            )
        else:
            issues.append(
                f"客户端配置目标主机为正式赛场地址 {cfg_host}:{cfg_port}，而当前模拟服务器运行在本机 {expected_connect_host}:{sim_mqtt_port}（主机地址不一致）。"
                f"离线联调请在客户端将主机改为 127.0.0.1（端口保持 {sim_mqtt_port}）。"
            )
    elif cfg_port != sim_mqtt_port:
        issues.append(
            f"客户端配置 mqtt.port={cfg_port} 与模拟服务器端口 {sim_mqtt_port} 不一致。"
        )

    if sim_auth.auth_mode == "required" and not mqtt_cfg.get("username"):
        issues.append("模拟服务器已开启账号密码登录校验，但客户端配置未填写 username/password。")

    if video_cfg.get("enabled", True):
        v_port = int(video_cfg.get("port", 3334) or 3334)
        if v_port != sim_video_cfg.target_port:
            issues.append(
                f"客户端配置 video.port={v_port} 与模拟服务器推流端口 {sim_video_cfg.target_port} 不一致。"
            )
    else:
        issues.append("客户端配置中 video.enabled 为 false，将不会监听 UDP 图传。")

    return {
        "found": True,
        "path": str(path),
        "config": {
            "robot_id": data.get("robot_id"),
            "mqtt_enabled": mqtt_cfg.get("enabled", True),
            "mqtt_host": cfg_host,
            "mqtt_port": cfg_port,
            "mqtt_has_username": bool(mqtt_cfg.get("username")),
            "api_port": api_cfg.get("port", 8090),
            "video_enabled": video_cfg.get("enabled", True),
            "video_port": video_cfg.get("port", 3334),
        },
        "issues": issues,
    }


def check_local_terminals(ports: tuple[int, ...] = (8090, 18091)) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for port in ports:
        url = f"http://127.0.0.1:{port}/api/v1/state"
        try:
            with urllib.request.urlopen(url, timeout=0.45) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                rt = data.get("runtime", {})
                vid = data.get("video", {})
                topics = rt.get("topics", {})
                active_topics = sum(1 for v in topics.values() if isinstance(v, dict) and v.get("count", 0) > 0)
                found.append(
                    {
                        "api_port": port,
                        "reachable": True,
                        "robot_id": rt.get("robot_id"),
                        "mqtt_enabled": rt.get("mqtt_enabled"),
                        "mqtt_connected": rt.get("mqtt_connected"),
                        "mqtt_connecting": rt.get("mqtt_connecting"),
                        "mqtt_host": rt.get("mqtt_host"),
                        "mqtt_port": rt.get("mqtt_port"),
                        "mqtt_last_error": rt.get("mqtt_last_error"),
                        "active_topics": active_topics,
                        "video_enabled": vid.get("enabled"),
                        "video_packets": vid.get("packets"),
                        "video_completed_frames": vid.get("completed_frames"),
                        "video_fps": vid.get("fps"),
                    }
                )
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            continue
    return found


def build_full_diagnostics(
    *,
    listen_host: str,
    mqtt_port: int,
    control_port: int,
    auth: AuthConfig,
    video_cfg: VideoStreamerConfig,
    audit_records: list[dict[str, Any]],
    active_sessions: list[dict[str, Any]],
    terminal_config_path: str | None = None,
    terminal_ports: tuple[int, ...] = (8090, 18091),
) -> dict[str, Any]:
    addrs = list_local_ipv4_addresses()
    listeners = check_port_listeners((3333, 3334, mqtt_port, control_port, *terminal_ports))
    term_cfg = inspect_terminal_config(
        sim_listen_host=listen_host,
        sim_mqtt_port=mqtt_port,
        sim_auth=auth,
        sim_video_cfg=video_cfg,
        terminal_config_path=terminal_config_path,
    )
    running_terminals = check_local_terminals(terminal_ports)

    connect_host = "127.0.0.1" if listen_host == "0.0.0.0" else listen_host
    issues: list[dict[str, str]] = []
    if listen_host in ("127.0.0.1", "localhost"):
        issues.append(
            {
                "level": "info",
                "title": "模拟服务器当前仅绑定回环地址 127.0.0.1",
                "detail": "本机联调推荐保持 127.0.0.1；若需要其他机器或跨网卡接入，请传 --host 0.0.0.0。",
            }
        )
    for msg in term_cfg.get("issues", []):
        issues.append({"level": "warn", "title": "客户端配置文件差异", "detail": msg})

    for term in running_terminals:
        if not term["mqtt_connected"]:
            err = term.get("mqtt_last_error") or "尚未发起连接或连接被拒绝"
            term_host = str(term.get("mqtt_host") or "")
            term_port = term.get("mqtt_port")
            target_desc = f"{term_host}:{term_port}"
            extra = ""
            if term_port != mqtt_port or (
                term_host
                and term_host != connect_host
                and not (listen_host == "0.0.0.0" and term_host == "127.0.0.1")
            ):
                extra = f"（该终端正在连 {target_desc}，而模拟服监听在 {connect_host}:{mqtt_port}）"
            issues.append(
                {
                    "level": "error",
                    "title": f"检测到本机终端 :{term['api_port']} 未连上 MQTT{extra}",
                    "detail": f"最近错误: {err}",
                }
            )
        elif term.get("video_enabled") and video_cfg.enabled and (term.get("video_packets") or 0) == 0:
            issues.append(
                {
                    "level": "warn",
                    "title": f"本机终端 :{term['api_port']} 未收到 UDP 图传包",
                    "detail": f"模拟服务器推向 {video_cfg.target_host}:{video_cfg.target_port}，请核对终端 video.port。",
                }
            )

    rejected_recent = [r for r in audit_records if r.get("status") not in ("accepted", "replaced")]
    for rec in rejected_recent[-3:]:
        issues.append(
            {
                "level": "error",
                "title": f"近期握手失败 ({rec.get('peer')})",
                "detail": f"{rec.get('reason')} → {rec.get('hint')}",
            }
        )

    auth_hint = ""
    if auth.auth_mode == "required" and auth.users:
        first_user = next(iter(auth.users.keys()))
        auth_hint = f" --username {first_user} --password <密码>"

    return {
        "simulator": {
            "listen_host": listen_host,
            "mqtt_port": mqtt_port,
            "control_port": control_port,
            "video_dir": video_cfg.video_dir,
            "video_target": f"{video_cfg.target_host}:{video_cfg.target_port}",
            "auth": auth.to_dict(),
            "active_clients": len(active_sessions),
        },
        "client_connect_info": {
            "mqtt_host": connect_host,
            "mqtt_port": mqtt_port,
            "video_udp_port": video_cfg.target_port,
            "auth_mode": auth.auth_mode,
            "example_client_ids": [1, 3, 4, 7, 101, 103, 104, 107],
        },
        "network_interfaces": addrs,
        "listening_sockets": listeners,
        "terminal_config": term_cfg,
        "running_terminals": running_terminals,
        "issues": issues,
        "recent_rejections": rejected_recent[-10:],
        "recommended_command": f"rm2027-sim --probe {connect_host}:{mqtt_port}{auth_hint}",
    }


def format_diagnostics_report(report: dict[str, Any]) -> str:
    sim = report["simulator"]
    cci = report["client_connect_info"]
    lines = [
        "================ RM2027 模拟服务器连接诊断报告 ================",
        f"预期 MQTT 监听: {sim['listen_host']}:{sim['mqtt_port']} | Web控制台: :{sim['control_port']} | 图传目标: {sim['video_target']}",
        f"认证模式: {sim['auth']['auth_mode']} | 严格RobotID: {sim['auth']['strict_client_id']}",
        f"客户端接入目标: MQTT={cci['mqtt_host']}:{cci['mqtt_port']} | UDP图传=:{cci['video_udp_port']} | 合法ClientID={cci['example_client_ids']}",
        "",
        "[1] 本机 IPv4 地址:",
    ]
    for item in report["network_interfaces"]:
        lines.append(f"  - {item['iface']}: {item['ip']}/{item['prefix']}")

    lines.append("\n[2] 相关端口监听状态 (ss -tuln):")
    if report["listening_sockets"]:
        for sock in report["listening_sockets"]:
            lines.append(f"  - {sock['proto']} {sock['state']} {sock['local']}")
    else:
        lines.append("  - (未检测到相关监听端口或无 ss 工具)")

    tc = report["terminal_config"]
    if tc["found"] and tc["config"]:
        c = tc["config"]
        lines.append("\n[3] 外部客户端配置检查 (--terminal-config):")
        lines.append(
            f"  - 文件: {tc['path']} | robot_id={c['robot_id']} mqtt={c['mqtt_host']}:{c['mqtt_port']} "
            f"(enabled={c['mqtt_enabled']}) video_port={c['video_port']}"
        )

    if report["running_terminals"]:
        lines.append("\n[4] 本机活跃自定义终端探测:")
        for t in report["running_terminals"]:
            lines.append(
                f"  - API :{t['api_port']} | mqtt_connected={t['mqtt_connected']} "
                f"target={t['mqtt_host']}:{t['mqtt_port']} err={t['mqtt_last_error']} "
                f"topics={t['active_topics']}/23 video_fps={t['video_fps']}"
            )

    lines.append("\n[5] 诊断结论与建议:")
    if report["issues"]:
        for issue in report["issues"]:
            lines.append(f"  - [{issue['level'].upper()}] {issue['title']}: {issue['detail']}")
    else:
        lines.append("  - 未发现配置冲突或异常。")

    lines.append(f"\n[主动握手探测命令]\n  {report['recommended_command']}")
    lines.append("==============================================================")
    return "\n".join(lines)
