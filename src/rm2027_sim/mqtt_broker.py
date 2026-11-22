"""Asyncio MQTT 3.1.1 broker with full login auth, multi-client isolation, and connection diagnostics."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import time
from typing import Awaitable, Callable, Literal

from . import INBOUND_TOPICS, VALID_ROBOT_IDS, team_for_robot_id

CONNACK_LABELS: dict[int, tuple[str, str]] = {
    0: ("Accepted", "登录成功"),
    1: (
        "UnacceptableProtocolVersion",
        "协议版本不兼容：客户端须使用 MQTT 3.1.1 (protocol_level=4)。",
    ),
    2: (
        "IdentifierRejected",
        "Client ID 被拒绝：请填写合法机器人编号（红方 1,3..9 或蓝方 101,103..109）。",
    ),
    3: (
        "ServerUnavailable",
        "服务端暂不可用（可能是故障注入开启或引擎正在维护）。",
    ),
    4: (
        "BadUserNameOrPassword",
        "用户名或密码错误：请核对模拟服务器认证账号与密码。",
    ),
    5: (
        "NotAuthorized",
        "客户端未授权：当前机器人 ID 或账号已被禁止接入。",
    ),
}


def encode_remaining_length(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


async def read_exact(reader: asyncio.StreamReader, length: int, timeout: float | None = None) -> bytes:
    coro = reader.readexactly(length)
    return await (asyncio.wait_for(coro, timeout=timeout) if timeout is not None else coro)


async def recv_mqtt_packet(
    reader: asyncio.StreamReader,
    timeout: float | None = None,
) -> tuple[int, bytes]:
    first_byte = (await read_exact(reader, 1, timeout=timeout))[0]
    multiplier = 1
    remaining = 0
    for _ in range(4):
        digit = (await read_exact(reader, 1, timeout=timeout))[0]
        remaining += (digit & 0x7F) * multiplier
        if not (digit & 0x80):
            break
        multiplier *= 128
    else:
        raise ValueError("malformed MQTT remaining length")
    if remaining > 16 * 1024 * 1024:
        raise ValueError(f"MQTT packet too large: {remaining}")
    payload = await read_exact(reader, remaining, timeout=timeout) if remaining else b""
    return first_byte, payload


def build_mqtt_packet(first_byte: int, payload: bytes) -> bytes:
    return bytes([first_byte]) + encode_remaining_length(len(payload)) + payload


def read_utf8_field(payload: bytes, index: int) -> tuple[str, int]:
    if index + 2 > len(payload):
        raise ValueError("truncated MQTT UTF-8 length")
    size = int.from_bytes(payload[index : index + 2], "big")
    index += 2
    if index + size > len(payload):
        raise ValueError("truncated MQTT UTF-8 field")
    return payload[index : index + size].decode("utf-8", errors="replace"), index + size


def read_bin_field(payload: bytes, index: int) -> tuple[bytes, int]:
    if index + 2 > len(payload):
        raise ValueError("truncated MQTT binary length")
    size = int.from_bytes(payload[index : index + 2], "big")
    index += 2
    if index + size > len(payload):
        raise ValueError("truncated MQTT binary field")
    return payload[index : index + size], index + size


def topic_matches(pattern: str, topic: str) -> bool:
    if pattern == "#" or pattern == topic:
        return True
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    for idx, p in enumerate(p_parts):
        if p == "#":
            return True
        if idx >= len(t_parts):
            return False
        if p != "+" and p != t_parts[idx]:
            return False
    return len(p_parts) == len(t_parts)


@dataclass
class ConnectPacket:
    protocol_name: str
    protocol_level: int
    flags: int
    keep_alive: int
    client_id: str
    username: str | None
    password: str | None
    clean_session: bool


def parse_connect_packet(payload: bytes) -> ConnectPacket:
    proto_name, idx = read_utf8_field(payload, 0)
    if idx + 4 > len(payload):
        raise ValueError("truncated CONNECT variable header")
    protocol_level = payload[idx]
    flags = payload[idx + 1]
    keep_alive = int.from_bytes(payload[idx + 2 : idx + 4], "big")
    idx += 4

    client_id, idx = read_utf8_field(payload, idx)
    if flags & 0x04:  # Will flag
        _, idx = read_bin_field(payload, idx)
        _, idx = read_bin_field(payload, idx)
    username: str | None = None
    password: str | None = None
    if flags & 0x80:
        username, idx = read_utf8_field(payload, idx)
    if flags & 0x40:
        raw_pw, idx = read_bin_field(payload, idx)
        password = raw_pw.decode("utf-8", errors="replace")
    return ConnectPacket(
        protocol_name=proto_name,
        protocol_level=protocol_level,
        flags=flags,
        keep_alive=keep_alive,
        client_id=client_id,
        username=username,
        password=password,
        clean_session=bool(flags & 0x02),
    )


@dataclass
class AuthConfig:
    auth_mode: Literal["open", "required"] = "open"
    users: dict[str, str] = field(default_factory=dict)
    strict_client_id: bool = False
    denied_robot_ids: set[int] = field(default_factory=set)
    fault_connack: int | None = None
    fault_once: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "auth_mode": self.auth_mode,
            "usernames": sorted(self.users.keys()),
            "has_credentials": bool(self.users),
            "strict_client_id": self.strict_client_id,
            "denied_robot_ids": sorted(self.denied_robot_ids),
            "fault_connack": self.fault_connack,
            "fault_once": self.fault_once,
        }


@dataclass
class ConnectionRecord:
    timestamp_ms: int
    peer: str
    client_id: str
    robot_id: int | None
    username: str
    protocol_level: int | None
    status: str
    connack_code: int | None
    reason: str
    hint: str

    def to_dict(self) -> dict[str, object]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "peer": self.peer,
            "client_id": self.client_id,
            "robot_id": self.robot_id,
            "username": self.username,
            "protocol_level": self.protocol_level,
            "status": self.status,
            "connack_code": self.connack_code,
            "reason": self.reason,
            "hint": self.hint,
        }


class ClientSession:
    def __init__(
        self,
        *,
        session_id: int,
        peer: str,
        client_id: str,
        robot_id: int,
        username: str | None,
        keep_alive: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.session_id = session_id
        self.peer = peer
        self.peer_ip = peer.rsplit(":", 1)[0]
        self.client_id = client_id
        self.robot_id = robot_id
        self.team = team_for_robot_id(robot_id)
        self.username = username
        self.keep_alive = keep_alive
        self.reader = reader
        self.writer = writer
        self.write_lock = asyncio.Lock()
        self.subscriptions: dict[str, int] = {}
        self.connected_at = time.time()
        self.last_rx_at = self.connected_at
        self.last_tx_at = self.connected_at
        self.tx_packets = 0
        self.tx_bytes = 0
        self.rx_packets = 0
        self.rx_bytes = 0
        self.packet_seq = 1
        self.closed = False
        self.close_reason: str | None = None
        self.pending_qos2: set[int] = set()

    def matches_topic(self, topic: str) -> bool:
        return any(topic_matches(pat, topic) for pat in self.subscriptions)

    def inbound_coverage(self) -> int:
        return sum(1 for t in INBOUND_TOPICS if self.matches_topic(t))

    async def send_packet(self, first_byte: int, payload: bytes) -> None:
        if self.closed:
            return
        raw = build_mqtt_packet(first_byte, payload)
        async with self.write_lock:
            if self.closed:
                return
            self.writer.write(raw)
            await self.writer.drain()
            self.last_tx_at = time.time()
            self.tx_packets += 1
            self.tx_bytes += len(raw)

    async def publish(self, topic: str, payload: bytes, qos: int = 1) -> None:
        if self.closed or not self.matches_topic(topic):
            return
        topic_bytes = topic.encode("utf-8")
        if qos == 0:
            body = len(topic_bytes).to_bytes(2, "big") + topic_bytes + payload
            await self.send_packet(0x30, body)
        else:
            pkt_id = self.packet_seq
            self.packet_seq = (self.packet_seq % 65000) + 1
            body = (
                len(topic_bytes).to_bytes(2, "big")
                + topic_bytes
                + pkt_id.to_bytes(2, "big")
                + payload
            )
            await self.send_packet(0x32, body)

    async def close(self, reason: str = "closed") -> None:
        if self.closed:
            return
        self.closed = True
        self.close_reason = reason
        try:
            self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=0.5)
        except Exception:
            pass

    def to_dict(self) -> dict[str, object]:
        now = time.time()
        return {
            "session_id": self.session_id,
            "peer": self.peer,
            "peer_ip": self.peer_ip,
            "client_id": self.client_id,
            "robot_id": self.robot_id,
            "team": self.team,
            "username": self.username or "",
            "keep_alive": self.keep_alive,
            "subscriptions": len(self.subscriptions),
            "inbound_coverage": self.inbound_coverage(),
            "inbound_total": len(INBOUND_TOPICS),
            "uptime_s": round(max(0.0, now - self.connected_at), 1),
            "last_rx_age_s": round(max(0.0, now - self.last_rx_at), 1),
            "tx_packets": self.tx_packets,
            "tx_bytes": self.tx_bytes,
            "rx_packets": self.rx_packets,
            "rx_bytes": self.rx_bytes,
        }


def parse_robot_id(client_id: str) -> tuple[int, bool]:
    """Extract numeric robot_id and whether it is an official RM2027 robot ID."""
    stripped = client_id.strip()
    if stripped.isdigit():
        rid = int(stripped)
        return rid, rid in VALID_ROBOT_IDS
    return 3, False


class MqttBrokerServer:
    """Concurrent asyncio MQTT 3.1.1 server for RM2027 client terminals."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3333,
        auth: AuthConfig | None = None,
        on_subscribe: Callable[[ClientSession, list[str]], Awaitable[None]] | None = None,
        on_publish: Callable[[ClientSession, str, bytes], Awaitable[None]] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.auth = auth or AuthConfig()
        self.on_subscribe = on_subscribe
        self.on_publish = on_publish
        self._server: asyncio.AbstractServer | None = None
        self._sessions_by_id: dict[int, ClientSession] = {}
        self._sessions_by_client_id: dict[str, ClientSession] = {}
        self._next_session_id = 1
        self.audit_log: deque[ConnectionRecord] = deque(maxlen=120)
        self.total_connections = 0
        self.total_rejected = 0
        self.total_rx_publishes = 0
        self.total_tx_publishes = 0

    @property
    def actual_port(self) -> int:
        if self._server and self._server.sockets:
            return int(self._server.sockets[0].getsockname()[1])
        return self.port

    def active_sessions(self) -> list[ClientSession]:
        return [s for s in self._sessions_by_id.values() if not s.closed]

    def active_peer_ips(self) -> set[str]:
        return {s.peer_ip for s in self.active_sessions() if s.peer_ip}

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_tcp, self.host, self.port)
        self.port = self.actual_port

    async def stop(self) -> None:
        for sess in list(self._sessions_by_id.values()):
            await sess.close("server shutdown")
        self._sessions_by_id.clear()
        self._sessions_by_client_id.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def kick_client(self, client_id: str) -> bool:
        sess = self._sessions_by_client_id.get(client_id)
        if sess is None:
            return False
        await sess.close("kicked by admin")
        self._record(
            peer=sess.peer,
            client_id=sess.client_id,
            robot_id=sess.robot_id,
            username=sess.username or "",
            protocol_level=4,
            status="kicked",
            connack_code=None,
            reason="管理员主动断开会话",
            hint="客户端将进入重连或等待重新登录。",
        )
        return True

    async def broadcast(self, topic: str, payload_builder: Callable[[ClientSession], bytes] | bytes, qos: int = 1) -> int:
        sessions = self.active_sessions()
        if not sessions:
            return 0
        sent = 0
        tasks: list[Awaitable[None]] = []
        for sess in sessions:
            if not sess.matches_topic(topic):
                continue
            payload = payload_builder(sess) if callable(payload_builder) else payload_builder
            sent += 1
            tasks.append(self._safe_publish(sess, topic, payload, qos))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            self.total_tx_publishes += sent
        return sent

    async def _safe_publish(self, sess: ClientSession, topic: str, payload: bytes, qos: int) -> None:
        try:
            await sess.publish(topic, payload, qos=qos)
        except Exception:
            await sess.close("write error")

    def _record(
        self,
        *,
        peer: str,
        client_id: str,
        robot_id: int | None,
        username: str,
        protocol_level: int | None,
        status: str,
        connack_code: int | None,
        reason: str,
        hint: str,
    ) -> None:
        self.audit_log.append(
            ConnectionRecord(
                timestamp_ms=int(time.time() * 1000),
                peer=peer,
                client_id=client_id,
                robot_id=robot_id,
                username=username,
                protocol_level=protocol_level,
                status=status,
                connack_code=connack_code,
                reason=reason,
                hint=hint,
            )
        )

    def _evaluate_connect(self, pkt: ConnectPacket) -> tuple[int, str, str]:
        if pkt.protocol_name != "MQTT" or pkt.protocol_level != 4:
            return (
                1,
                f"不支持的 MQTT 协议: {pkt.protocol_name!r} level={pkt.protocol_level}",
                "请将客户端协议设为 MQTT 3.1.1 (level=4)。",
            )

        if self.auth.fault_connack is not None:
            code = int(self.auth.fault_connack)
            if self.auth.fault_once:
                self.auth.fault_connack = None
            _, hint = CONNACK_LABELS.get(code, ("Error", "故障注入已启用"))
            return code, f"模拟服务器故障注入 CONNACK=0x{code:02x}", hint

        rid, valid_rid = parse_robot_id(pkt.client_id)
        if self.auth.strict_client_id and not valid_rid:
            return (
                2,
                f"Client ID '{pkt.client_id}' 不是合法 RM2027 机器人 ID",
                "请将 Client ID 设为红方 1,3..9 或蓝方 101,103..109。",
            )
        if valid_rid and rid in self.auth.denied_robot_ids:
            return (
                5,
                f"机器人 #{rid} 在拒绝接入黑名单中",
                "请在模拟服务器控制台「MQTT 认证」中移除禁用列表或更换 ID。",
            )

        if self.auth.auth_mode == "required":
            if not pkt.username or pkt.password is None:
                return (
                    4,
                    "服务端开启了账号密码校验，但客户端未携带用户名或密码",
                    "请在终端「连接引擎」弹窗填入正确的用户名与密码。",
                )
            expected_pw = self.auth.users.get(pkt.username)
            if expected_pw is None or pkt.password != expected_pw:
                return (
                    4,
                    f"用户名 '{pkt.username}' 不存在或密码不匹配",
                    "请核对账号密码（注意大小写和首尾空格）。",
                )

        return 0, "登录验证通过", "已允许接入。"

    async def _handle_tcp(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer_info = writer.get_extra_info("peername")
        peer = f"{peer_info[0]}:{peer_info[1]}" if isinstance(peer_info, tuple) else str(peer_info)
        self.total_connections += 1
        session: ClientSession | None = None

        try:
            first_chunk = await asyncio.wait_for(reader.readexactly(1), timeout=8.0)
        except asyncio.TimeoutError:
            self.total_rejected += 1
            self._record(
                peer=peer,
                client_id="",
                robot_id=None,
                username="",
                protocol_level=None,
                status="timeout",
                connack_code=None,
                reason="TCP 建连后 8 秒内未收到 MQTT CONNECT 报文",
                hint="客户端可能连接了错误的协议端口，或防火墙/代理拦截了首包。",
            )
            writer.close()
            return
        except Exception:
            writer.close()
            return

        # Detect accidental HTTP request sent to MQTT port
        first_byte = first_chunk[0]
        if first_byte in (ord("G"), ord("P"), ord("H"), ord("D"), ord("O")):
            rest = b""
            try:
                rest = await asyncio.wait_for(reader.read(64), timeout=0.2)
            except Exception:
                pass
            sample = (first_chunk + rest).decode("ascii", errors="replace").splitlines()[0][:60]
            if any(sample.startswith(m) for m in ("GET ", "POST ", "HEAD ", "PUT ", "DELETE ", "OPTIONS ")):
                self.total_rejected += 1
                self._record(
                    peer=peer,
                    client_id="",
                    robot_id=None,
                    username="",
                    protocol_level=None,
                    status="protocol_error",
                    connack_code=None,
                    reason=f"收到 HTTP 请求而非 MQTT 报文: {sample}",
                    hint="浏览器或 HTTP 客户端误连到了 MQTT 端口；Web 控制台请访问 --control-port (默认 18885)。",
                )
                try:
                    body = "RM2027 MQTT Broker: this is an MQTT 3.1.1 TCP port, not HTTP.\r\n".encode("utf-8")
                    writer.write(
                        b"HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n"
                        + body
                    )
                    await writer.drain()
                except Exception:
                    pass
                writer.close()
                return

        try:
            # Parse remaining length after the already-read first_byte
            multiplier = 1
            remaining = 0
            for _ in range(4):
                digit = (await asyncio.wait_for(reader.readexactly(1), timeout=5.0))[0]
                remaining += (digit & 0x7F) * multiplier
                if not (digit & 0x80):
                    break
                multiplier *= 128
            payload = (
                await asyncio.wait_for(reader.readexactly(remaining), timeout=5.0)
                if remaining
                else b""
            )
        except Exception as exc:
            self.total_rejected += 1
            self._record(
                peer=peer,
                client_id="",
                robot_id=None,
                username="",
                protocol_level=None,
                status="malformed",
                connack_code=None,
                reason=f"首包读取失败或格式非法: {exc}",
                hint="请确认对端是标准 MQTT 3.1.1 客户端而非普通 TCP 探针。",
            )
            writer.close()
            return

        if (first_byte >> 4) != 1:
            self.total_rejected += 1
            self._record(
                peer=peer,
                client_id="",
                robot_id=None,
                username="",
                protocol_level=None,
                status="rejected",
                connack_code=None,
                reason=f"首包类型不是 CONNECT (type={first_byte >> 4})",
                hint="MQTT 客户端连接后首个报文必须是 CONNECT。",
            )
            writer.close()
            return

        try:
            pkt = parse_connect_packet(payload)
        except Exception as exc:
            self.total_rejected += 1
            self._record(
                peer=peer,
                client_id="",
                robot_id=None,
                username="",
                protocol_level=None,
                status="malformed",
                connack_code=None,
                reason=f"CONNECT 报文解析失败: {exc}",
                hint="请检查客户端 MQTT 报文编码。",
            )
            writer.close()
            return

        rid, _ = parse_robot_id(pkt.client_id)
        code, reason, hint = self._evaluate_connect(pkt)
        if code != 0:
            self.total_rejected += 1
            self._record(
                peer=peer,
                client_id=pkt.client_id,
                robot_id=rid,
                username=pkt.username or "",
                protocol_level=pkt.protocol_level,
                status="rejected",
                connack_code=code,
                reason=reason,
                hint=hint,
            )
            try:
                writer.write(build_mqtt_packet(0x20, bytes([0, code])))
                await writer.drain()
            except Exception:
                pass
            writer.close()
            return

        # Kick previous session if same non-empty client_id reconnects
        if pkt.client_id and pkt.client_id in self._sessions_by_client_id:
            old = self._sessions_by_client_id[pkt.client_id]
            if not old.closed:
                await old.close("replaced by new session with same client_id")
                self._record(
                    peer=old.peer,
                    client_id=old.client_id,
                    robot_id=old.robot_id,
                    username=old.username or "",
                    protocol_level=4,
                    status="replaced",
                    connack_code=None,
                    reason=f"同 Client ID '{pkt.client_id}' 被新连接 {peer} 顶替下线",
                    hint="若终端频繁掉线重连，请检查是否同时运行了两个相同机器人 ID 的客户端。",
                )

        sid = self._next_session_id
        self._next_session_id += 1
        session = ClientSession(
            session_id=sid,
            peer=peer,
            client_id=pkt.client_id,
            robot_id=rid,
            username=pkt.username,
            keep_alive=pkt.keep_alive,
            reader=reader,
            writer=writer,
        )
        self._sessions_by_id[sid] = session
        if pkt.client_id:
            self._sessions_by_client_id[pkt.client_id] = session

        await session.send_packet(0x20, b"\x00\x00")
        self._record(
            peer=peer,
            client_id=pkt.client_id,
            robot_id=rid,
            username=pkt.username or "",
            protocol_level=pkt.protocol_level,
            status="accepted",
            connack_code=0,
            reason=f"登录成功 (#{rid} {session.team})",
            hint="连接正常，等待客户端订阅 23 个 RM2027 主题。",
        )

        read_timeout = max(pkt.keep_alive * 1.5, 15.0) if pkt.keep_alive > 0 else None
        try:
            while not session.closed:
                fb, body = await recv_mqtt_packet(reader, timeout=read_timeout)
                session.last_rx_at = time.time()
                session.rx_packets += 1
                session.rx_bytes += 2 + len(body)
                kind = fb >> 4
                if kind == 8:  # SUBSCRIBE
                    await self._handle_subscribe(session, body)
                elif kind == 10:  # UNSUBSCRIBE
                    await self._handle_unsubscribe(session, body)
                elif kind == 3:  # PUBLISH
                    await self._handle_publish(session, fb, body)
                elif kind == 4:  # PUBACK
                    pass
                elif kind == 6:  # PUBREL -> PUBCOMP
                    pkt_id = body[:2]
                    await session.send_packet(0x70, pkt_id)
                elif kind == 12:  # PINGREQ
                    await session.send_packet(0xD0, b"")
                elif kind == 14:  # DISCONNECT
                    session.close_reason = "client DISCONNECT"
                    break
        except asyncio.TimeoutError:
            self._record(
                peer=peer,
                client_id=pkt.client_id,
                robot_id=rid,
                username=pkt.username or "",
                protocol_level=pkt.protocol_level,
                status="keepalive_timeout",
                connack_code=None,
                reason=f"超过保活超时 ({read_timeout:.0f}s) 未收到 PINGREQ 或报文",
                hint="请检查网络连通性或终端心跳设置。",
            )
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as exc:
            self._record(
                peer=peer,
                client_id=pkt.client_id,
                robot_id=rid,
                username=pkt.username or "",
                protocol_level=pkt.protocol_level,
                status="session_error",
                connack_code=None,
                reason=f"会话异常结束: {exc}",
                hint="请检查客户端日志。",
            )
        finally:
            await session.close(session.close_reason or "disconnected")
            self._sessions_by_id.pop(sid, None)
            if pkt.client_id and self._sessions_by_client_id.get(pkt.client_id) is session:
                self._sessions_by_client_id.pop(pkt.client_id, None)

    async def _handle_subscribe(self, session: ClientSession, body: bytes) -> None:
        if len(body) < 2:
            return
        packet_id = body[:2]
        idx = 2
        new_topics: list[str] = []
        return_codes = bytearray()
        while idx < len(body):
            topic, idx = read_utf8_field(body, idx)
            if idx >= len(body):
                break
            req_qos = body[idx] & 0x03
            idx += 1
            granted = min(req_qos, 1)
            session.subscriptions[topic] = granted
            new_topics.append(topic)
            return_codes.append(granted)
        await session.send_packet(0x90, packet_id + bytes(return_codes))
        if self.on_subscribe is not None and new_topics:
            asyncio.create_task(self.on_subscribe(session, new_topics))

    async def _handle_unsubscribe(self, session: ClientSession, body: bytes) -> None:
        if len(body) < 2:
            return
        packet_id = body[:2]
        idx = 2
        while idx < len(body):
            topic, idx = read_utf8_field(body, idx)
            session.subscriptions.pop(topic, None)
        await session.send_packet(0xB0, packet_id)

    async def _handle_publish(self, session: ClientSession, first_byte: int, body: bytes) -> None:
        qos = (first_byte >> 1) & 0x03
        topic, idx = read_utf8_field(body, 0)
        pkt_id_bytes = b""
        if qos in (1, 2):
            if idx + 2 > len(body):
                return
            pkt_id_bytes = body[idx : idx + 2]
            idx += 2
        payload = body[idx:]
        if qos == 1:
            await session.send_packet(0x40, pkt_id_bytes)
        elif qos == 2:
            await session.send_packet(0x50, pkt_id_bytes)
        self.total_rx_publishes += 1
        if self.on_publish is not None:
            await self.on_publish(session, topic, payload)
