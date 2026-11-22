"""Unit and integration tests for standalone rm2027-sim-server."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from rm2027_sim import INBOUND_TOPICS, OUTBOUND_TOPICS, __version__
from rm2027_sim.cli import build_parser, load_config_file, main as cli_main, merge_settings
from rm2027_sim.diagnostics import (
    build_full_diagnostics,
    build_mqtt_connect_packet,
    build_mqtt_subscribe_packet,
    run_mqtt_probe,
)
from rm2027_sim.match_engine import MatchSimulator
from rm2027_sim.mqtt_broker import AuthConfig, MqttBrokerServer, encode_remaining_length
from rm2027_sim.proto import (
    bool_field,
    build_rmh1_hit_bytes,
    bytes_field,
    decode_assembly_command,
    decode_common_command,
    decode_custom_control,
    decode_dart_command,
    decode_fields,
    decode_keyboard_mouse_control,
    decode_map_click_cmd,
    decode_performance_selection_command,
    decode_rune_activate_command,
    decode_sentry_ctrl_command,
    encode_buff,
    encode_custom_byte_block,
    encode_dart_select_target_status_sync,
    encode_event,
    encode_game_status,
    encode_global_logistics_status,
    encode_global_special_mechanism,
    encode_global_unit_status,
    encode_map_click_info,
    encode_penalty_info,
    encode_radar_info_to_client,
    encode_robot_dynamic_status,
    encode_robot_injury_stat,
    encode_robot_module_status,
    encode_robot_path_plan_info,
    encode_robot_performance_selection_sync,
    encode_robot_position,
    encode_robot_respawn_status,
    encode_robot_static_status,
    encode_rune_status_sync,
    encode_sentry_ctrl_result,
    encode_sentry_status_sync,
    encode_tech_core_motion_state_sync,
    float_field,
    to_signed_i32,
    uint_field,
)
from rm2027_sim.video_streamer import (
    SYNTHETIC_IDR,
    SYNTHETIC_TRAIL,
    SYNTHETIC_VPS_SPS_PPS,
    HevcVideoStreamer,
    VideoStreamerConfig,
    detect_available_backends,
    inspect_hevc_frame,
    iter_annexb_access_units,
    packetize_hevc_frame,
)
from rm2027_sim.web_ui import SimControlContext, SimWebServer


def _mqtt_publish_pkt(topic: str, payload: bytes, pkt_id: int = 1) -> bytes:
    tb = topic.encode("utf-8")
    body = len(tb).to_bytes(2, "big") + tb + pkt_id.to_bytes(2, "big") + payload
    return b"\x32" + encode_remaining_length(len(body)) + body


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            raise ConnectionError("eof")
        buf.extend(c)
    return bytes(buf)


def _recv_pkt(sock: socket.socket) -> tuple[int, bytes]:
    fb = _recv_exact(sock, 1)[0]
    mult = 1
    rem = 0
    while True:
        d = _recv_exact(sock, 1)[0]
        rem += (d & 0x7F) * mult
        if not (d & 0x80):
            break
        mult *= 128
    return fb, (_recv_exact(sock, rem) if rem else b"")


def _parse_publish_payload(body: bytes) -> tuple[str, bytes]:
    tsize = int.from_bytes(body[:2], "big")
    topic = body[2 : 2 + tsize].decode("utf-8")
    idx = 2 + tsize + 2
    return topic, body[idx:]


class ProtobufTests(unittest.TestCase):
    def test_topic_counts(self) -> None:
        self.assertEqual(len(INBOUND_TOPICS), 23)
        self.assertEqual(len(OUTBOUND_TOPICS), 9)
        self.assertTrue(set(INBOUND_TOPICS).isdisjoint(OUTBOUND_TOPICS))

    def test_all_23_inbound_encoders_and_path_negatives(self) -> None:
        rmh1 = build_rmh1_hit_bytes(armor_id=2, reason=1, hit_sequence=7, x_mm=-45, y_mm=32, damage=20)
        self.assertTrue(rmh1.startswith(b"RMH1"))
        self.assertEqual(len(rmh1), 14)

        path = encode_robot_path_plan_info(
            intention=1,
            start_pos_x=138,
            start_pos_y=54,
            offset_x=[-15, 10],
            offset_y=[8, -12],
            sender_id=7,
        )
        f = decode_fields(path)
        self.assertEqual(f[1][0][1], 1)
        self.assertEqual(f[6][0][1], 7)
        packed_x = f[4][0][1]
        self.assertIsInstance(packed_x, bytes)
        self.assertGreater(len(packed_x), 2)

        samples = [
            encode_game_status(current_stage=4, stage_countdown_sec=300, stage_elapsed_sec=120),
            encode_global_unit_status(
                base_health=9000,
                base_status=1,
                base_shield=1,
                outpost_r_health=1000,
                outpost_r_status=1,
                enemy_base_health=8000,
                enemy_base_status=2,
                enemy_base_shield=1,
                outpost_b_health=700,
                outpost_b_status=4,
                robot_health=[400, 200, 200, 500, 350, 180, 190, 480],
                robot_bullets=[10, 150, 140, 300],
                total_damage_ally=1200,
                total_damage_enemy=900,
                total_cp_ally=60,
                total_cp_enemy=40,
            ),
            encode_global_logistics_status(
                remaining_economy=950, total_economy_obtained=1500, tech_level=2, encryption_level=1
            ),
            encode_global_special_mechanism([(4, 3), (2, 5)]),
            encode_event(9, "2,3"),
            encode_robot_injury_stat(total_damage=120, small_projectile_damage=100, killer_id=103),
            encode_robot_respawn_status(
                is_pending_respawn=True,
                total_respawn_progress=20,
                current_respawn_progress=10,
                can_free_respawn=False,
                gold_cost_for_respawn=160,
                can_pay_for_respawn=True,
            ),
            encode_robot_static_status(robot_id=3),
            encode_robot_dynamic_status(current_health=210, current_heat=42.5),
            encode_robot_module_status({"power_manager": 1, "video_transmission": 1}),
            encode_robot_position(x=10.5, y=7.2, z=0.1, yaw=90.0, robot_id=3),
            encode_buff(robot_id=3, buff_type=2, buff_level=-5, buff_max_time=30, buff_left_time=12),
            encode_penalty_info(penalty_type=1, penalty_effect_sec=5, total_penalty_num=1),
            path,
            encode_map_click_info(map_x=12.5, map_y=6.0),
            encode_radar_info_to_client([(1800, 720, 1)]),
            encode_custom_byte_block(rmh1),
            encode_tech_core_motion_state_sync(maximum_difficulty_level=3, basic_state=1),
            encode_robot_performance_selection_sync(shooter=1, chassis=2, sentry_control=1),
            encode_rune_status_sync(rune_status=3, activated_arms=8, average_rings=9.1),
            encode_sentry_status_sync(posture_id=1, is_weakened=False, is_powered=True),
            encode_dart_select_target_status_sync(target_id=3, open_state=2),
            encode_sentry_ctrl_result(command_id=10, result_code=0),
        ]
        self.assertEqual(len(samples), 23)
        for s in samples:
            self.assertGreater(len(s), 0)

    def test_all_9_outbound_decoders(self) -> None:
        km = decode_keyboard_mouse_control(uint_field(6, 0x15) + bool_field(4, True))
        self.assertEqual(km["keyboard_value"], 0x15)
        self.assertTrue(km["left_button_down"])

        cc = decode_custom_control(bytes_field(1, b"hello"))
        self.assertEqual(cc["data"], b"hello")

        mc = decode_map_click_cmd(
            uint_field(1, 2) + uint_field(3, 4) + uint_field(5, 67) + float_field(7, 14.5) + float_field(8, 6.2)
        )
        self.assertEqual(mc["ascii"], 67)
        self.assertAlmostEqual(float(mc["map_x"]), 14.5, places=3)

        ac = decode_assembly_command(uint_field(1, 1) + uint_field(2, 3))
        self.assertEqual((ac["operation"], ac["difficulty"]), (1, 3))

        pc = decode_performance_selection_command(uint_field(1, 2) + uint_field(2, 1) + uint_field(3, 1))
        self.assertEqual(pc["sentry_control"], 1)

        cm = decode_common_command(uint_field(1, 1) + uint_field(2, 250))
        self.assertEqual((cm["cmd_type"], cm["param"]), (1, 250))

        rc = decode_rune_activate_command(uint_field(1, 1))
        self.assertEqual(rc["activate"], 1)

        dc = decode_dart_command(uint_field(1, 5) + bool_field(2, True) + bool_field(3, True))
        self.assertEqual(dc["target_id"], 5)
        self.assertTrue(dc["launch_confirm"])

        sc = decode_sentry_ctrl_command(uint_field(1, 11))
        self.assertEqual(sc["command_id"], 11)
        self.assertEqual(to_signed_i32(0xFFFFFFFE), -2)


class BrokerMatchAndWebIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.sim = MatchSimulator(mode="auto", random_events="normal", speed=1.0, start_stage=4, seed=42)

        async def on_sub(sess, topics):
            del topics
            await self.sim.push_full_snapshot_to_client(sess)

        self.auth = AuthConfig(auth_mode="required", users={"rm2027": "secret123"}, strict_client_id=True)
        self.broker = MqttBrokerServer(
            host="127.0.0.1",
            port=0,
            auth=self.auth,
            on_subscribe=on_sub,
            on_publish=self.sim.handle_client_command,
        )
        self.sim.attach_broker(self.broker)

        self.video = HevcVideoStreamer(
            VideoStreamerConfig(enabled=False, backend="synthetic"),
            is_cut_callback=self.sim.should_cut_video,
        )

        async def boot() -> None:
            await self.broker.start()
            self._sim_task = asyncio.create_task(self.sim.run_loop())

        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._ready = threading.Event()
        self._thread.start()
        self._ready.wait(timeout=3.0)
        asyncio.run_coroutine_threadsafe(boot(), self.loop).result(timeout=3.0)

        ctx = SimControlContext(
            loop=self.loop,
            broker=self.broker,
            simulator=self.sim,
            video=self.video,
            listen_host="127.0.0.1",
            control_port=0,
        )
        self.web = SimWebServer("127.0.0.1", 0, ctx)
        self.web.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self.loop)
        self._ready.set()
        self.loop.run_forever()

    def tearDown(self) -> None:
        self.web.stop()
        self.video.stop()

        async def shutdown() -> None:
            self.sim.stop_loop()
            if hasattr(self, "_sim_task"):
                self._sim_task.cancel()
                await asyncio.gather(self._sim_task, return_exceptions=True)
            await self.broker.stop()

        asyncio.run_coroutine_threadsafe(shutdown(), self.loop).result(timeout=3.0)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=3.0)
        self.loop.close()

    def _http(self, path: str, body: dict | None = None) -> tuple[int, dict]:
        url = f"http://127.0.0.1:{self.web.actual_port}{path}"
        if body is None:
            req = urllib.request.Request(url, method="GET")
        else:
            req = urllib.request.Request(
                url,
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
        try:
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as err:
            return err.code, json.loads(err.read().decode("utf-8"))

    def test_mqtt_auth_failures_diagnostics_and_perspective_isolation(self) -> None:
        port = self.broker.actual_port

        res_bad = run_mqtt_probe("127.0.0.1", port, client_id="3", username="rm2027", password="wrong")
        self.assertFalse(res_bad["ok"])
        self.assertIn("0x04", res_bad["steps"][-1]["detail"])

        res_id = run_mqtt_probe("127.0.0.1", port, client_id="999", username="rm2027", password="secret123")
        self.assertFalse(res_id["ok"])
        self.assertIn("0x02", res_id["steps"][-1]["detail"])

        with socket.create_connection(("127.0.0.1", port), timeout=2.0) as sock_v:
            sock_v.sendall(b"\x10\x0d\x00\x04MQTT\x03\x02\x00\x0a\x00\x013")
            fb, body = _recv_pkt(sock_v)
            self.assertEqual(fb >> 4, 2)
            self.assertEqual(body[1], 1)

        with socket.create_connection(("127.0.0.1", port), timeout=2.0) as sock_h:
            sock_h.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            resp_banner = sock_h.recv(256)
            self.assertIn(b"HTTP/1.1 400", resp_banner)

        res_ok = run_mqtt_probe("127.0.0.1", port, client_id="3", username="rm2027", password="secret123")
        self.assertTrue(res_ok["ok"])

        _, auth_res = self._http("/api/auth", {"fault_connack": 5, "fault_once": True})
        self.assertEqual(auth_res["auth"]["fault_connack"], 5)
        res_fault = run_mqtt_probe("127.0.0.1", port, client_id="3", username="rm2027", password="secret123")
        self.assertFalse(res_fault["ok"])
        self.assertIn("0x05", res_fault["steps"][-1]["detail"])
        res_ok2 = run_mqtt_probe("127.0.0.1", port, client_id="3", username="rm2027", password="secret123")
        self.assertTrue(res_ok2["ok"])

        _, diag = self._http("/api/diagnostics")
        reasons = " ".join(r["reason"] for r in diag["recent_rejections"])
        self.assertIn("HTTP", reasons)
        self.assertIn("密码不匹配", reasons)

    def test_concurrent_red_blue_perspective_and_commands(self) -> None:
        port = self.broker.actual_port

        def connect_client(cid: str) -> socket.socket:
            s = socket.create_connection(("127.0.0.1", port), timeout=3.0)
            s.sendall(build_mqtt_connect_packet(client_id=cid, username="rm2027", password="secret123"))
            fb, body = _recv_pkt(s)
            self.assertEqual((fb >> 4, body), (2, b"\x00\x00"))
            for i, topic in enumerate(INBOUND_TOPICS, start=1):
                s.sendall(build_mqtt_subscribe_packet(i, topic, qos=1))
                _recv_pkt(s)
            return s

        red_sock = connect_client("3")
        blue_sock = connect_client("103")
        try:
            self._http("/api/trigger", {"action": "set_base_hp", "params": {"team": "red", "hp": 6200}})
            self._http("/api/trigger", {"action": "set_base_hp", "params": {"team": "blue", "hp": 8400}})

            def read_latest_global_unit(
                sock: socket.socket, exp_ally: int, exp_enemy: int
            ) -> dict[int, list[tuple[int, bytes | int]]]:
                sock.settimeout(2.0)
                latest = None
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    fb, body = _recv_pkt(sock)
                    if (fb >> 4) == 3:
                        topic, pl = _parse_publish_payload(body)
                        if topic == "GlobalUnitStatus":
                            latest = decode_fields(pl)
                            if latest[1][0][1] == exp_ally and latest[6][0][1] == exp_enemy:
                                return latest
                self.assertIsNotNone(latest)
                return latest

            red_units = read_latest_global_unit(red_sock, 6200, 8400)
            blue_units = read_latest_global_unit(blue_sock, 8400, 6200)
            self.assertEqual(red_units[1][0][1], 6200)
            self.assertEqual(red_units[6][0][1], 8400)
            self.assertEqual(blue_units[1][0][1], 8400)
            self.assertEqual(blue_units[6][0][1], 6200)

            before_ammo = self.sim.ensure_robot(3).remaining_ammo
            red_sock.sendall(_mqtt_publish_pkt("CommonCommand", uint_field(1, 1) + uint_field(2, 150), pkt_id=99))
            time.sleep(0.2)
            self.assertEqual(self.sim.ensure_robot(3).remaining_ammo, before_ammo + 150)

            red_sock.sendall(_mqtt_publish_pkt("SentryCtrlCommand", uint_field(1, 10), pkt_id=100))
            got_sentry_ack = False
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                fb, body = _recv_pkt(red_sock)
                if (fb >> 4) == 3:
                    topic, pl = _parse_publish_payload(body)
                    if topic == "SentryCtrlResult":
                        f = decode_fields(pl)
                        if f[1][0][1] == 10:
                            got_sentry_ack = True
                            break
            self.assertTrue(got_sentry_ack)
        finally:
            red_sock.close()
            blue_sock.close()

    def test_modes_random_events_and_manual_triggers(self) -> None:
        self._http("/api/mode", {"mode": "auto", "random_events": "high", "speed": 10.0})
        self._http("/api/trigger", {"action": "jump_time", "params": {"elapsed_s": 130}})
        asyncio.run_coroutine_threadsafe(self.sim.step(1.0), self.loop).result(timeout=3.0)
        _, state = self._http("/api/state")
        self.assertGreater(state["simulator"]["elapsed_s"], 130.0)
        self.assertGreater(len(state["simulator"]["events"]), 0)

        self._http("/api/mode", {"mode": "manual", "random_events": "off"})
        self._http("/api/trigger", {"action": "overheat", "params": {"robot_id": 3, "excess": 50}})
        _, state_oh = self._http("/api/state")
        self.assertTrue(state_oh["simulator"]["video_cut"])
        self.assertIn("超热", state_oh["simulator"]["video_cut_reason"])

        self._http("/api/trigger", {"action": "clear_compliance"})
        _, state_ok = self._http("/api/state")
        self.assertFalse(state_ok["simulator"]["video_cut"])

        _, probe_res = self._http(
            "/api/probe",
            {
                "host": "127.0.0.1",
                "port": self.broker.actual_port,
                "client_id": "3",
                "username": "rm2027",
                "password": "secret123",
            },
        )
        self.assertTrue(probe_res["ok"])

    def test_standby_default_start_reset_and_web_ui(self) -> None:
        standby_sim = MatchSimulator(seed=7)
        self.assertEqual(standby_sim.stage, 0)
        self.assertFalse(standby_sim.auto_loop)
        self.assertEqual(standby_sim.elapsed_s, 0.0)

        asyncio.run_coroutine_threadsafe(standby_sim.step(2.0), self.loop).result(timeout=3.0)
        self.assertEqual(standby_sim.stage, 0)
        self.assertEqual(standby_sim.elapsed_s, 0.0)

        res_start = asyncio.run_coroutine_threadsafe(
            standby_sim.trigger_manual("start_battle", {}), self.loop
        ).result(timeout=3.0)
        self.assertTrue(res_start["ok"])
        self.assertEqual(standby_sim.stage, 4)

        asyncio.run_coroutine_threadsafe(standby_sim.step(1.0), self.loop).result(timeout=3.0)
        self.assertGreater(standby_sim.elapsed_s, 0.0)

        res_reset = asyncio.run_coroutine_threadsafe(
            standby_sim.trigger_manual("reset", {}), self.loop
        ).result(timeout=3.0)
        self.assertTrue(res_reset["ok"])
        self.assertEqual(standby_sim.stage, 0)
        self.assertEqual(standby_sim.elapsed_s, 0.0)

        with urllib.request.urlopen(f"http://127.0.0.1:{self.web.actual_port}/", timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            html = resp.read().decode("utf-8")
            self.assertIn("RM2027 模拟赛事控制中心", html)

        _, state = self._http("/api/state")
        self.assertIn("video_dir", state["video"]["config"])
        self.assertIn("available_files", state["video"])

    def test_match_end_auto_pause_kills_assembly_and_low_yellow_card_rate(self) -> None:
        sim = MatchSimulator(mode="auto", random_events="normal", speed=1.0, start_stage=4, auto_loop=False, seed=19)
        kill_events = 0
        yellow_card_events = 0

        async def run_full_game() -> None:
            nonlocal kill_events, yellow_card_events
            for _ in range(425):
                if sim.stage != 4 or sim.mode == "paused":
                    break
                prev_len = len(sim.event_history)
                await sim.step(1.0)
                new_evs = list(sim.event_history)[prev_len:]
                for ev in new_evs:
                    if ev["event_id"] == 1:
                        kill_events += 1
                    elif ev["event_id"] == 2020:
                        yellow_card_events += 1

        asyncio.run_coroutine_threadsafe(run_full_game(), self.loop).result(timeout=5.0)
        # Match should have ended at 420s (or base destroyed) and auto-paused in settlement stage 5
        self.assertEqual(sim.stage, 5)
        self.assertEqual(sim.mode, "paused")
        self.assertTrue(sim.is_paused)
        self.assertGreaterEqual(sim.red_score + sim.blue_score + (1 if sim.game_result == 3 else 0), 1)
        # Both teams should have progressed tech core assembly to Lv3/Lv4
        self.assertGreaterEqual(sim.teams["red"].tech_level, 3)
        self.assertGreaterEqual(sim.teams["blue"].tech_level, 3)
        # Multiple robot kills should occur, while random yellow cards remain rare
        self.assertGreaterEqual(kill_events, 4)
        self.assertLessEqual(yellow_card_events, 2)


class VideoStreamerTests(unittest.TestCase):
    def test_packetize_and_annexb_au_splitting(self) -> None:
        payload = bytes(range(256)) * 12
        pkts = packetize_hevc_frame(42, payload[:3000])
        self.assertEqual(len(pkts), 3)
        reassembled = bytearray()
        for idx, pkt in enumerate(pkts):
            self.assertLessEqual(len(pkt), 1400)
            fno, frag, total = struct.unpack(">HHI", pkt[:8])
            self.assertEqual(fno, 42)
            self.assertEqual(frag, idx)
            self.assertEqual(total, 3000)
            reassembled.extend(pkt[8:])
        self.assertEqual(bytes(reassembled), payload[:3000])

        f1 = b"\x00\x00\x00\x01\x46\x01\x10" + b"\x00\x00\x00\x01\x40\x01\x0c\x01" + b"\x00\x00\x00\x01\x26\x01\xaf\x08"
        f2 = b"\x00\x00\x00\x01\x46\x01\x20" + b"\x00\x00\x00\x01\x02\x01\xd0\x08"
        stream = f1 + f2
        pos = 0

        def reader(n: int) -> bytes:
            nonlocal pos
            out = stream[pos : pos + n]
            pos += len(out)
            return out

        aus = list(iter_annexb_access_units(reader))
        self.assertEqual(len(aus), 2)
        self.assertEqual(inspect_hevc_frame(aus[0]), (True, True))
        self.assertEqual(inspect_hevc_frame(aus[1]), (False, False))

    def test_udp_streamer_and_compliance_cut(self) -> None:
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.bind(("127.0.0.1", 0))
        udp_port = udp_sock.getsockname()[1]
        udp_sock.settimeout(1.5)

        cut_flag = False
        streamer = HevcVideoStreamer(
            VideoStreamerConfig(
                enabled=True,
                target_host="127.0.0.1",
                target_port=udp_port,
                backend="synthetic",
                fps=20,
            ),
            is_cut_callback=lambda: (cut_flag, "超热" if cut_flag else ""),
        )
        streamer.start()
        try:
            pkt, _ = udp_sock.recvfrom(2048)
            fno, frag, total = struct.unpack(">HHI", pkt[:8])
            self.assertGreaterEqual(fno, 1)
            self.assertEqual(frag, 0)
            self.assertEqual(len(pkt) - 8, total)

            cut_flag = True
            time.sleep(0.15)
            while True:
                try:
                    udp_sock.recvfrom(2048)
                except (TimeoutError, OSError):
                    break
            sent_before = streamer.packets_sent
            time.sleep(0.15)
            self.assertEqual(streamer.packets_sent, sent_before)

            cut_flag = False
            pkt2, _ = udp_sock.recvfrom(2048)
            self.assertGreater(len(pkt2), 8)
        finally:
            streamer.stop()
            udp_sock.close()

    def test_directory_video_streamer_empty_hevc_and_next_cycle(self) -> None:
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.bind(("127.0.0.1", 0))
        udp_port = udp_sock.getsockname()[1]
        udp_sock.settimeout(2.5)

        with tempfile.TemporaryDirectory() as tmpdir:
            vdir = Path(tmpdir)
            streamer = HevcVideoStreamer(
                VideoStreamerConfig(
                    enabled=True,
                    target_host="127.0.0.1",
                    target_port=udp_port,
                    backend="auto",
                    video_dir=str(vdir),
                    fps=30,
                )
            )
            streamer.start()
            try:
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and streamer.active_backend != "waiting_files":
                    time.sleep(0.05)
                self.assertEqual(streamer.active_backend, "waiting_files")
                self.assertEqual(streamer.last_error, "")

                raw_frames = (SYNTHETIC_VPS_SPS_PPS + SYNTHETIC_IDR + b"\x11" * 24 + SYNTHETIC_TRAIL + b"\x22" * 16) * 15
                (vdir / "01.hevc").write_bytes(raw_frames)
                (vdir / "02.hevc").write_bytes(raw_frames)

                pkt, _ = udp_sock.recvfrom(2048)
                self.assertGreater(len(pkt), 8)
                self.assertEqual(streamer.current_file, "01.hevc")
                self.assertEqual(len(streamer.to_dict()["available_files"]), 2)

                res = streamer.update_config({"action": "next"})
                self.assertEqual(res["current_file"], "02.hevc")
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and streamer.frames_sent < 3:
                    time.sleep(0.05)
                pkt2, _ = udp_sock.recvfrom(2048)
                self.assertGreater(len(pkt2), 8)
                self.assertEqual(streamer.current_file, "02.hevc")
            finally:
                streamer.stop()
                udp_sock.close()

    def test_directory_video_streamer_real_mp4_gstreamer(self) -> None:
        if "gstreamer" not in detect_available_backends():
            self.skipTest("gstreamer x265enc not installed")

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.bind(("127.0.0.1", 0))
        udp_port = udp_sock.getsockname()[1]
        udp_sock.settimeout(4.0)

        with tempfile.TemporaryDirectory() as tmpdir:
            vdir = Path(tmpdir) / "中文 测试目录"
            vdir.mkdir()
            mp4 = vdir / "demo.mp4"
            subprocess.run(
                [
                    "gst-launch-1.0",
                    "-q",
                    "videotestsrc",
                    "num-buffers=30",
                    "!",
                    "video/x-raw,width=320,height=240,framerate=30/1",
                    "!",
                    "x265enc",
                    "speed-preset=ultrafast",
                    "tune=zerolatency",
                    "!",
                    "h265parse",
                    "!",
                    "mp4mux",
                    "!",
                    "filesink",
                    f"location={mp4}",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )

            streamer = HevcVideoStreamer(
                VideoStreamerConfig(
                    enabled=True,
                    target_host="127.0.0.1",
                    target_port=udp_port,
                    backend="auto",
                    video_dir=str(vdir),
                    width=640,
                    height=360,
                    fps=30,
                    bitrate_kbps=1200,
                )
            )
            streamer.start()
            try:
                pkt, _ = udp_sock.recvfrom(2048)
                fno, frag, total = struct.unpack(">HHI", pkt[:8])
                self.assertGreaterEqual(fno, 1)
                self.assertGreaterEqual(frag, 0)
                self.assertGreater(total, 0)
                self.assertEqual(streamer.active_backend, "gstreamer")
                self.assertEqual(streamer.current_file, "demo.mp4")
            finally:
                streamer.stop()
                udp_sock.close()


class StandalonePackagingAndConfigTests(unittest.TestCase):
    def test_config_file_loading_and_cli_override_priority(self) -> None:
        example_path = ROOT_DIR / "config.example.json"
        data = load_config_file(str(example_path))
        parser = build_parser()

        # Without explicit CLI flags -> keeps config.example.json values
        args_plain = parser.parse_args([])
        cfg_plain = merge_settings(data, args_plain)
        self.assertEqual(cfg_plain["host"], "127.0.0.1")
        self.assertEqual(cfg_plain["port"], 3333)
        self.assertEqual(cfg_plain["match"]["stage"], 0)
        self.assertEqual(cfg_plain["video"]["video_dir"], "videos")
        self.assertEqual(cfg_plain["video"]["fps"], 30)
        self.assertEqual(cfg_plain["auth"]["mode"], "open")

        # Explicit CLI flags override config.json values cleanly
        args_override = parser.parse_args(
            [
                "--host",
                "0.0.0.0",
                "--port",
                "18884",
                "--stage",
                "4",
                "--mode",
                "manual",
                "--video-fps",
                "60",
                "--require-auth",
                "--video-dir",
                "/tmp/custom_videos",
            ]
        )
        cfg_override = merge_settings(data, args_override)
        self.assertEqual(cfg_override["host"], "0.0.0.0")
        self.assertEqual(cfg_override["port"], 18884)
        self.assertEqual(cfg_override["match"]["stage"], 4)
        self.assertEqual(cfg_override["match"]["mode"], "manual")
        self.assertEqual(cfg_override["video"]["fps"], 60)
        self.assertEqual(cfg_override["video"]["video_dir"], "/tmp/custom_videos")
        self.assertEqual(cfg_override["auth"]["mode"], "required")
        # Non-overridden fields remain from config.example.json
        self.assertEqual(cfg_override["control_port"], 18885)

    def test_standalone_diagnostics_works_without_terminal_config(self) -> None:
        report = build_full_diagnostics(
            listen_host="127.0.0.1",
            mqtt_port=3333,
            control_port=18885,
            auth=AuthConfig(),
            video_cfg=VideoStreamerConfig(),
            audit_records=[],
            active_sessions=[],
            terminal_config_path=None,
        )
        self.assertFalse(report["terminal_config"]["found"])
        self.assertEqual(report["client_connect_info"]["mqtt_host"], "127.0.0.1")
        self.assertEqual(report["client_connect_info"]["mqtt_port"], 3333)
        self.assertEqual(report["simulator"]["video_dir"], "videos")
        self.assertTrue(report["recommended_command"].startswith("rm2027-sim --probe"))

    def test_cli_version_and_diagnose_and_run_py_launcher(self) -> None:
        self.assertEqual(cli_main(["--version"]), 0)
        self.assertEqual(cli_main(["--config", str(ROOT_DIR / "config.example.json"), "--diagnose"]), 0)

        res = subprocess.run(
            [sys.executable, str(ROOT_DIR / "run.py"), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
        self.assertIn(__version__, res.stdout)


if __name__ == "__main__":
    unittest.main()
