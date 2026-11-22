"""RM2027 full match simulator: auto progression, rule-based random events, and manual triggers."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
import math
import random
import time
from typing import Any, Literal

from . import INBOUND_TOPICS, robot_type_for_id, team_for_robot_id
from .mqtt_broker import ClientSession, MqttBrokerServer
from .proto import (
    MODULE_FIELD_ORDER,
    build_rmh1_hit_bytes,
    decode_assembly_command,
    decode_common_command,
    decode_custom_control,
    decode_dart_command,
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
)

MATCH_DURATION_S = 420.0
BASE_MAX_HP = 10_000
OUTPOST_MAX_HP = 1_000

STAGE_NAMES = {
    0: "未开始",
    1: "准备阶段",
    2: "15s 自检阶段",
    3: "5s 倒计时",
    4: "7 分钟比赛中",
    5: "比赛结算",
}

NATURAL_GOLD_MILESTONES: tuple[tuple[float, int], ...] = (
    (0.0, 800),
    (60.0, 100),
    (120.0, 100),
    (180.0, 100),
    (240.0, 100),
    (300.0, 100),
    (360.0, 300),
)


def max_unlocked_assembly_level(elapsed_s: float) -> int:
    if elapsed_s < 60.0:
        return 1
    if elapsed_s < 120.0:
        return 2
    if elapsed_s < 180.0:
        return 3
    return 4


def assembly_income_per_10s(level: int) -> int:
    incomes = {1: 40, 2: 20, 3: 20, 4: 40}
    return sum(incomes[lv] for lv in range(1, max(0, min(4, level)) + 1))


def remote_heal_cost(elapsed_s: float) -> int:
    e = max(0.0, min(420.0, elapsed_s))
    return 50 + math.ceil((e / 60.0) * 20.0)


def instant_respawn_cost(elapsed_s: float, level: int) -> int:
    e = max(0.0, min(420.0, elapsed_s))
    return math.ceil(e / 60.0) * 80 + max(1, level) * 20


def respawn_progress_required(elapsed_s: float, prior_instants: int, lam: float) -> int:
    e = max(0.0, min(420.0, elapsed_s))
    base = 10 + round(e / 10.0) + 20 * prior_instants
    return max(5, round(base / max(0.5, min(1.0, lam))))


def dart_blind_seconds(target_id: int, hit_count: int) -> int:
    if target_id in (4, 5):
        return 10
    if hit_count <= 1:
        return 10
    if hit_count == 2:
        return 5
    if hit_count == 3:
        return 3
    return 2


def default_modules() -> dict[str, int]:
    return {name: 1 for name in MODULE_FIELD_ORDER}


@dataclass
class RobotSimState:
    robot_id: int
    team: str
    role_type: int
    max_hp: int
    hp: int
    max_heat: int
    heat: float
    cooling_rate: float
    max_power: int
    chassis_power: float
    buffer_energy: float
    remaining_ammo: int
    level: int = 1
    experience: int = 0
    experience_for_upgrade: int = 400
    projectiles_fired: int = 0
    fire_rate: float = 24.0
    x: float = 5.0
    y: float = 7.5
    z: float = 0.0
    yaw: float = 0.0
    home_x: float = 5.0
    home_y: float = 7.5
    patrol_phase: float = 0.0
    online: bool = True
    alive: bool = True
    out_of_combat: bool = True
    out_of_combat_countdown: int = 0
    overheat_locked: bool = False
    overheat_permanent: bool = False
    penalty_blind_s: float = 0.0
    red_card: bool = False
    respawn_pending: bool = False
    respawn_total: int = 10
    respawn_progress: float = 0.0
    prior_instant_respawns: int = 0
    modules: dict[str, int] = field(default_factory=default_modules)
    injury: dict[str, int] = field(
        default_factory=lambda: {
            "total_damage": 0,
            "collision_damage": 0,
            "small_projectile_damage": 0,
            "large_projectile_damage": 0,
            "dart_splash_damage": 0,
            "module_offline_damage": 0,
            "offline_damage": 0,
            "penalty_damage": 0,
            "server_kill_damage": 0,
            "killer_id": 0,
        }
    )
    buffs: list[dict[str, float | int]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "robot_id": self.robot_id,
            "team": self.team,
            "hp": self.hp,
            "max_hp": self.max_hp,
            "heat": round(self.heat, 1),
            "max_heat": self.max_heat,
            "remaining_ammo": self.remaining_ammo,
            "level": self.level,
            "alive": self.alive,
            "online": self.online,
            "overheat_locked": self.overheat_locked,
            "penalty_blind_s": round(self.penalty_blind_s, 1),
            "red_card": self.red_card,
            "respawn_pending": self.respawn_pending,
            "x": round(self.x, 2),
            "y": round(self.y, 2),
            "yaw": round(self.yaw, 1),
        }


@dataclass
class TeamSimState:
    team: str
    base_hp: int = BASE_MAX_HP
    base_status: int = 0  # 0=invincible, 1=closed, 2=open
    outpost_hp: int = OUTPOST_MAX_HP
    outpost_stopped: bool = False
    control_score: float = 0.0
    total_damage: int = 0
    economy: int = 800
    total_economy: int = 800
    tech_level: int = 0
    encryption_level: int = 1
    max_base_advantage: float = 0.0
    max_control_advantage: float = 0.0
    launcher_unlocked: bool = False
    launcher_installed: bool = False
    shooter_perf: int = 1
    chassis_perf: int = 2
    sentry_control_mode: int = 0  # 0=auto, 1=semi
    sentry_posture_id: int = 3  # 1=attack, 2=defense, 3=move
    sentry_weakened: bool = False
    sentry_powered: bool = False
    sentry_enhanced_rem_s: float = 0.0
    sentry_last_switch_elapsed_s: float | None = None
    darts_remaining: int = 4
    dart_target_id: int = 1
    dart_gate_state: int = 0  # 0=closed, 1=opening, 2=open
    dart_gate_timer_s: float = 0.0
    dart_cooldown_s: float = 0.0
    dart_opportunities_used: int = 0
    dart_hits_dealt: int = 0
    dart_blind_remaining_s: float = 0.0
    rune_status: int = 1  # 1=idle, 2=activating, 3=active
    rune_arms: int = 0
    rune_avg_rings: float = 0.0
    rune_timer_s: float = 0.0
    tech_core_basic_state: int = 0  # 0=idle, 1=assembling, 2=done, 3=fail
    tech_core_target_level: int = 1
    tech_core_remain_all_s: float = 0.0
    tech_core_remain_step_s: float = 0.0
    tech_core_putin: int = 0
    tech_core_move: int = 0
    tech_core_rotate: int = 0

    def logistics_lambda(self) -> float:
        lost = BASE_MAX_HP - max(0, min(BASE_MAX_HP, self.base_hp))
        return max(0.5, min(1.0, 1.0 - lost / 20_000.0))


def _make_robot(robot_id: int) -> RobotSimState:
    team = team_for_robot_id(robot_id)
    suffix = robot_id % 100
    is_red = team == "red"
    if suffix == 1:
        max_hp, max_heat, cool, power, ammo = 400, 300, 100.0, 85, 20
        hx, hy = (4.8, 9.6) if is_red else (23.2, 5.4)
    elif suffix in (3, 4, 5):
        max_hp, max_heat, cool, power, ammo = 250, 240, 24.0, 85, 220
        if suffix == 3:
            hx, hy = (9.5, 7.6) if is_red else (18.5, 7.4)
        else:
            hx, hy = (7.4, 4.3) if is_red else (20.6, 10.7)
    elif suffix == 7:
        max_hp, max_heat, cool, power, ammo = 600, 400, 40.0, 100, 450
        hx, hy = (12.2, 5.8) if is_red else (15.8, 9.2)
    else:
        max_hp, max_heat, cool, power, ammo = 250, 240, 24.0, 85, 100
        hx, hy = (3.5, 7.5) if is_red else (24.5, 7.5)

    return RobotSimState(
        robot_id=robot_id,
        team=team,
        role_type=robot_type_for_id(robot_id),
        max_hp=max_hp,
        hp=max_hp,
        max_heat=max_heat,
        heat=0.0,
        cooling_rate=cool,
        max_power=power,
        chassis_power=38.0,
        buffer_energy=60.0,
        remaining_ammo=ammo,
        x=hx,
        y=hy,
        z=0.1 if suffix != 7 else 0.3,
        yaw=35.0 if is_red else 215.0,
        home_x=hx,
        home_y=hy,
        patrol_phase=(robot_id % 10) * 0.9,
    )


class MatchSimulator:
    """Authoritative RM2027 simulated match engine driving MQTT topics and video compliance."""

    def __init__(
        self,
        *,
        mode: Literal["auto", "manual", "paused"] = "auto",
        random_events: Literal["off", "low", "normal", "high"] = "normal",
        speed: float = 1.0,
        start_stage: int = 0,
        auto_loop: bool = False,
        respect_video_rules: bool = True,
        seed: int | None = None,
    ) -> None:
        self.mode: Literal["auto", "manual", "paused"] = mode
        self.random_events: Literal["off", "low", "normal", "high"] = random_events
        self.speed = max(0.25, min(10.0, float(speed)))
        self.auto_loop = auto_loop
        self.respect_video_rules = respect_video_rules
        self.rng = random.Random(seed)

        self.broker: MqttBrokerServer | None = None
        self.event_history: deque[dict[str, Any]] = deque(maxlen=80)
        self.command_history: deque[dict[str, Any]] = deque(maxlen=60)
        self.penalty_counter = 0
        self.armor_hit_seq = 0
        self._last_sim_tick = time.monotonic()
        self._accum_5hz = 0.0
        self._accum_1hz = 0.0
        self._accum_random = 0.0
        self._accum_combat = 0.0
        self._accum_assembly_econ = 0.0
        self._awarded_gold_milestones: set[int] = set()
        self._pending_snapshots: set[int] = set()
        self._running = False

        self.reset(stage=start_stage)

    def attach_broker(self, broker: MqttBrokerServer) -> None:
        self.broker = broker

    def reset(self, *, stage: int = 0, elapsed_s: float = 0.0) -> None:
        self.current_round = 1
        self.total_rounds = 3
        self.red_score = 0
        self.blue_score = 0
        self.stage = max(0, min(5, int(stage)))
        self.is_paused = self.mode == "paused"
        self.game_result = 0
        self.end_reason = 0

        if self.stage == 1:
            self.stage_countdown_s = 15.0
            self.elapsed_s = 0.0
        elif self.stage == 2:
            self.stage_countdown_s = 5.0
            self.elapsed_s = 0.0
        elif self.stage == 3:
            self.stage_countdown_s = 5.0
            self.elapsed_s = 0.0
        elif self.stage == 4:
            self.elapsed_s = max(0.0, min(MATCH_DURATION_S, float(elapsed_s)))
            self.stage_countdown_s = max(0.0, MATCH_DURATION_S - self.elapsed_s)
        elif self.stage == 5:
            self.stage_countdown_s = 15.0
            self.elapsed_s = MATCH_DURATION_S
        else:
            self.stage_countdown_s = MATCH_DURATION_S
            self.elapsed_s = 0.0

        self.teams: dict[str, TeamSimState] = {
            "red": TeamSimState("red"),
            "blue": TeamSimState("blue"),
        }
        # Field ownership: {"sentinel_red": (owner, level), "sentinel_blue": ..., "base_red": ..., "base_blue": ...}
        self.fields: dict[str, tuple[str, str]] = {
            "sentinel_red": ("red", "full"),
            "sentinel_blue": ("blue", "full"),
            "base_red": ("red", "full"),
            "base_blue": ("blue", "full"),
        }
        self.mechanisms: dict[int, float] = {}  # mechanism_id -> remaining/progress sec
        self.robots: dict[int, RobotSimState] = {
            rid: _make_robot(rid) for rid in (1, 3, 4, 7, 101, 103, 104, 107)
        }
        self.last_custom_block: bytes = build_rmh1_hit_bytes(
            armor_id=1, reason=0, hit_sequence=1, x_mm=-8, y_mm=10, damage=20
        )
        self.last_map_click: dict[str, Any] | None = None
        self.sentry_paths: dict[str, tuple[int, int, list[int], list[int], int]] = {
            "red": (1, 122, [12, 16, 14, -8], [8, 10, -6, -10], 7),
            "blue": (1, 158, [-14, -15, -10, 8], [-8, -10, 6, 12], 107),
        }

        self._awarded_gold_milestones = set()
        if self.stage >= 4:
            for idx, (t_mark, amt) in enumerate(NATURAL_GOLD_MILESTONES):
                if self.elapsed_s >= t_mark:
                    self._awarded_gold_milestones.add(idx)
                    if idx > 0:
                        self.teams["red"].economy += amt
                        self.teams["red"].total_economy += amt
                        self.teams["blue"].economy += amt
                        self.teams["blue"].total_economy += amt
        self._accum_assembly_econ = 0.0
        self._last_yellow_card_elapsed_s = -999.0
        self.event_history.clear()
        self._log_sim_event(2027, f"模拟赛局已就绪（阶段 {STAGE_NAMES.get(self.stage, self.stage)}）")

    def ensure_robot(self, robot_id: int) -> RobotSimState:
        if robot_id not in self.robots:
            self.robots[robot_id] = _make_robot(robot_id)
        return self.robots[robot_id]

    def _log_sim_event(self, event_id: int, detail: str) -> None:
        self.event_history.append(
            {
                "elapsed_s": round(self.elapsed_s, 1),
                "event_id": event_id,
                "detail": detail,
                "timestamp_ms": int(time.time() * 1000),
            }
        )

    def _log_command(self, client: ClientSession, topic: str, summary: str) -> None:
        self.command_history.append(
            {
                "elapsed_s": round(self.elapsed_s, 1),
                "client_id": client.client_id,
                "robot_id": client.robot_id,
                "topic": topic,
                "summary": summary,
                "timestamp_ms": int(time.time() * 1000),
            }
        )

    def should_cut_video(self) -> tuple[bool, str]:
        if not self.respect_video_rules:
            return False, ""
        # Check connected clients first; fall back to primary demo robot #3
        targets: list[RobotSimState] = []
        if self.broker is not None:
            for sess in self.broker.active_sessions():
                targets.append(self.ensure_robot(sess.robot_id))
        if not targets:
            targets.append(self.ensure_robot(3))
        for r in targets:
            if r.red_card:
                return True, f"红牌锁定 (#{r.robot_id})"
            if r.penalty_blind_s > 0.0:
                return True, f"黄牌遮挡 (#{r.robot_id}, {r.penalty_blind_s:.1f}s)"
            if r.overheat_locked:
                return True, f"枪口超热量断图传 (#{r.robot_id}, {r.heat:.0f}/{r.max_heat})"
            if r.modules.get("video_transmission", 1) != 1:
                return True, f"图传模块离线 (#{r.robot_id})"
        return False, ""

    # ------------------------------------------------------------------
    # Outbound payload builders per client perspective
    # ------------------------------------------------------------------

    def build_game_status_payload(self) -> bytes:
        if self.stage == 4:
            rem = max(0, int(round(MATCH_DURATION_S - self.elapsed_s)))
            el = int(round(self.elapsed_s))
        elif self.stage in (1, 2, 3, 5):
            rem = max(0, int(round(self.stage_countdown_s)))
            el = None
        else:
            rem = int(MATCH_DURATION_S)
            el = 0
        return encode_game_status(
            current_round=self.current_round,
            total_rounds=self.total_rounds,
            red_score=self.red_score,
            blue_score=self.blue_score,
            current_stage=self.stage,
            stage_countdown_sec=rem,
            stage_elapsed_sec=el,
            is_paused=self.mode == "paused",
            game_result=self.game_result,
            end_reason=self.end_reason,
        )

    @staticmethod
    def _relative_outpost_status(owner: str, level: str, viewer_team: str) -> int:
        if owner == viewer_team:
            return 1 if level == "full" else 2
        return 3 if level == "temporary" else 4

    @staticmethod
    def _relative_base_shield(owner: str, level: str, home_team: str) -> int:
        return 1 if owner == home_team and level == "full" else 2

    def build_global_unit_status_payload(self, viewer_team: str) -> bytes:
        ally_team = viewer_team
        enemy_team = "blue" if viewer_team == "red" else "red"
        ally = self.teams[ally_team]
        enemy = self.teams[enemy_team]
        ally_base = 0 if ally_team == "red" else 100
        enemy_base = 0 if enemy_team == "red" else 100
        ally_ids = [ally_base + s for s in (1, 3, 4, 7)]
        enemy_ids = [enemy_base + s for s in (1, 3, 4, 7)]
        robot_hp = [self.ensure_robot(rid).hp for rid in ally_ids + enemy_ids]
        robot_bullets = [self.ensure_robot(rid).remaining_ammo for rid in ally_ids]

        r_owner, r_lvl = self.fields["sentinel_red"]
        b_owner, b_lvl = self.fields["sentinel_blue"]
        ally_base_owner, ally_base_lvl = self.fields[f"base_{ally_team}"]
        enemy_base_owner, enemy_base_lvl = self.fields[f"base_{enemy_team}"]

        return encode_global_unit_status(
            base_health=ally.base_hp,
            base_status=ally.base_status,
            base_shield=self._relative_base_shield(ally_base_owner, ally_base_lvl, ally_team),
            outpost_r_health=self.teams["red"].outpost_hp,
            outpost_r_status=self._relative_outpost_status(r_owner, r_lvl, viewer_team),
            enemy_base_health=enemy.base_hp,
            enemy_base_status=enemy.base_status,
            enemy_base_shield=self._relative_base_shield(enemy_base_owner, enemy_base_lvl, enemy_team),
            outpost_b_health=self.teams["blue"].outpost_hp,
            outpost_b_status=self._relative_outpost_status(b_owner, b_lvl, viewer_team),
            robot_health=robot_hp,
            robot_bullets=robot_bullets,
            total_damage_ally=ally.total_damage,
            total_damage_enemy=enemy.total_damage,
            total_cp_ally=int(round(ally.control_score)),
            total_cp_enemy=int(round(enemy.control_score)),
        )

    def build_global_logistics_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        return encode_global_logistics_status(
            remaining_economy=t.economy,
            total_economy_obtained=t.total_economy,
            tech_level=t.tech_level,
            encryption_level=t.encryption_level,
        )

    def build_special_mechanism_payload(self) -> bytes:
        items = [(mid, max(0, int(round(sec)))) for mid, sec in sorted(self.mechanisms.items()) if sec > 0]
        return encode_global_special_mechanism(items)

    def build_robot_static_payload(self, robot_id: int) -> bytes:
        r = self.ensure_robot(robot_id)
        t = self.teams[r.team]
        return encode_robot_static_status(
            connection_state=1 if r.online else 0,
            field_state=1 if self.stage == 4 else 0,
            alive_state=1 if r.alive else 0,
            robot_id=r.robot_id,
            robot_type=r.role_type,
            performance_system_shooter=t.shooter_perf,
            performance_system_chassis=t.chassis_perf,
            level=r.level,
            max_health=r.max_hp,
            max_heat=r.max_heat,
            heat_cooldown_rate=r.cooling_rate,
            max_power=r.max_power,
            max_buffer_energy=60,
            max_chassis_energy=120,
        )

    def build_robot_dynamic_payload(self, robot_id: int) -> bytes:
        r = self.ensure_robot(robot_id)
        return encode_robot_dynamic_status(
            current_health=r.hp if r.alive else 0,
            current_heat=r.heat,
            last_projectile_fire_rate=r.fire_rate,
            current_buffer_energy=int(round(r.buffer_energy)),
            current_experience=r.experience,
            experience_for_upgrade=r.experience_for_upgrade,
            total_projectiles_fired=r.projectiles_fired,
            remaining_ammo=r.remaining_ammo,
            is_out_of_combat=r.out_of_combat,
            out_of_combat_countdown=r.out_of_combat_countdown,
            can_remote_heal=r.alive and r.out_of_combat and r.hp < r.max_hp,
            can_remote_ammo=r.alive,
        )

    def build_robot_modules_payload(self, robot_id: int) -> bytes:
        return encode_robot_module_status(self.ensure_robot(robot_id).modules)

    def build_robot_injury_payload(self, robot_id: int) -> bytes:
        inj = self.ensure_robot(robot_id).injury
        return encode_robot_injury_stat(**inj)

    def build_robot_respawn_payload(self, robot_id: int) -> bytes:
        r = self.ensure_robot(robot_id)
        t = self.teams[r.team]
        lam = t.logistics_lambda()
        total = respawn_progress_required(self.elapsed_s, r.prior_instant_respawns, lam)
        cur = min(total, int(round(r.respawn_progress))) if r.respawn_pending else 0
        cost = instant_respawn_cost(self.elapsed_s, r.level)
        return encode_robot_respawn_status(
            is_pending_respawn=r.respawn_pending,
            total_respawn_progress=total,
            current_respawn_progress=cur,
            can_free_respawn=r.respawn_pending and cur >= total,
            gold_cost_for_respawn=cost,
            can_pay_for_respawn=r.respawn_pending and t.economy >= cost,
        )

    def build_radar_payload(self, viewer_team: str) -> bytes:
        ally_base = 0 if viewer_team == "red" else 100
        enemy_base = 100 if viewer_team == "red" else 0
        order = [enemy_base + s for s in (1, 3, 4, 7)] + [ally_base + s for s in (1, 3, 4, 7)]
        entries: list[tuple[int, int, int]] = []
        for rid in order:
            r = self.ensure_robot(rid)
            if not r.alive or not r.online:
                entries.append((0, 0, 0))
            else:
                hl = 1 if (r.team != viewer_team and (rid % 100 in (1, 3))) else 0
                entries.append((max(0, int(round(r.x * 100))), max(0, int(round(r.y * 100))), hl))
        return encode_radar_info_to_client(entries)

    def build_sentry_path_payload(self, viewer_team: str) -> bytes:
        intention, sx, ox, oy, sid = self.sentry_paths[viewer_team]
        return encode_robot_path_plan_info(
            intention=intention,
            start_pos_x=sx,
            start_pos_y=54 if viewer_team == "red" else 92,
            offset_x=ox,
            offset_y=oy,
            sender_id=sid,
        )

    def build_tech_core_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        opp = self.teams["blue" if viewer_team == "red" else "red"]
        return encode_tech_core_motion_state_sync(
            maximum_difficulty_level=max_unlocked_assembly_level(self.elapsed_s),
            basic_state=t.tech_core_basic_state,
            putin_state=t.tech_core_putin,
            move_state=t.tech_core_move,
            rotate_state=t.tech_core_rotate,
            enemy_core_status=1 if opp.tech_core_basic_state == 1 else 0,
            remain_time_all=max(0, int(round(t.tech_core_remain_all_s))),
            remain_time_step=max(0, int(round(t.tech_core_remain_step_s))),
        )

    def build_perf_sync_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        return encode_robot_performance_selection_sync(
            shooter=t.shooter_perf,
            chassis=t.chassis_perf,
            sentry_control=t.sentry_control_mode,
        )

    def build_rune_sync_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        return encode_rune_status_sync(
            rune_status=t.rune_status,
            activated_arms=t.rune_arms,
            average_rings=t.rune_avg_rings,
        )

    def build_sentry_sync_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        return encode_sentry_status_sync(
            posture_id=t.sentry_posture_id,
            is_weakened=t.sentry_weakened,
            is_powered=t.sentry_powered,
        )

    def build_dart_sync_payload(self, viewer_team: str) -> bytes:
        t = self.teams[viewer_team]
        return encode_dart_select_target_status_sync(
            target_id=t.dart_target_id,
            open_state=t.dart_gate_state,
        )

    # ------------------------------------------------------------------
    # Initial snapshot & periodic broadcast
    # ------------------------------------------------------------------

    async def push_full_snapshot_to_client(self, session: ClientSession) -> None:
        sid = session.session_id
        if sid in self._pending_snapshots:
            return
        self._pending_snapshots.add(sid)
        try:
            await asyncio.sleep(0.06)
            if session.closed:
                return
            rid = session.robot_id
            team = session.team
            topics_and_payloads: list[tuple[str, bytes]] = [
                ("GameStatus", self.build_game_status_payload()),
                ("GlobalUnitStatus", self.build_global_unit_status_payload(team)),
                ("GlobalLogisticsStatus", self.build_global_logistics_payload(team)),
                ("GlobalSpecialMechanism", self.build_special_mechanism_payload()),
                ("RobotStaticStatus", self.build_robot_static_payload(rid)),
                ("RobotDynamicStatus", self.build_robot_dynamic_payload(rid)),
                ("RobotModuleStatus", self.build_robot_modules_payload(rid)),
                ("RobotInjuryStat", self.build_robot_injury_payload(rid)),
                ("RobotRespawnStatus", self.build_robot_respawn_payload(rid)),
                ("RadarInfoToClient", self.build_radar_payload(team)),
                ("RobotPathPlanInfo", self.build_sentry_path_payload(team)),
                ("TechCoreMotionStateSync", self.build_tech_core_payload(team)),
                ("RobotPerformanceSelectionSync", self.build_perf_sync_payload(team)),
                ("RuneStatusSync", self.build_rune_sync_payload(team)),
                ("SentryStatusSync", self.build_sentry_sync_payload(team)),
                ("DartSelectTargetStatusSync", self.build_dart_sync_payload(team)),
                ("CustomByteBlock", encode_custom_byte_block(self.last_custom_block)),
                (
                    "Event",
                    encode_event(2027, f"RM2027 模拟引擎已连接 · 模式 {self.mode} / 随机 {self.random_events}"),
                ),
            ]
            for buff in self.ensure_robot(rid).buffs:
                topics_and_payloads.append(
                    (
                        "Buff",
                        encode_buff(
                            robot_id=rid,
                            buff_type=int(buff["buff_type"]),
                            buff_level=int(buff["level"]),
                            buff_max_time=int(buff["max_time"]),
                            buff_left_time=max(1, int(round(float(buff["left_time"])))),
                        ),
                    )
                )
            for robot in self.robots.values():
                topics_and_payloads.append(
                    (
                        "RobotPosition",
                        encode_robot_position(
                            x=robot.x, y=robot.y, z=robot.z, yaw=robot.yaw, robot_id=robot.robot_id
                        ),
                    )
                )
            for topic, payload in topics_and_payloads:
                if session.closed:
                    break
                await session.publish(topic, payload, qos=1)
        finally:
            self._pending_snapshots.discard(sid)

    async def broadcast_1hz(self) -> None:
        if not self.broker or not self.broker.active_sessions():
            return
        await self.broker.broadcast("GameStatus", self.build_game_status_payload())
        await self.broker.broadcast(
            "GlobalUnitStatus", lambda s: self.build_global_unit_status_payload(s.team)
        )
        await self.broker.broadcast(
            "GlobalLogisticsStatus", lambda s: self.build_global_logistics_payload(s.team)
        )
        await self.broker.broadcast("GlobalSpecialMechanism", self.build_special_mechanism_payload())
        await self.broker.broadcast(
            "RobotStaticStatus", lambda s: self.build_robot_static_payload(s.robot_id)
        )
        await self.broker.broadcast(
            "RobotModuleStatus", lambda s: self.build_robot_modules_payload(s.robot_id)
        )
        await self.broker.broadcast(
            "RobotInjuryStat", lambda s: self.build_robot_injury_payload(s.robot_id)
        )
        await self.broker.broadcast(
            "RobotRespawnStatus", lambda s: self.build_robot_respawn_payload(s.robot_id)
        )
        await self.broker.broadcast(
            "RobotPathPlanInfo", lambda s: self.build_sentry_path_payload(s.team)
        )
        await self.broker.broadcast(
            "TechCoreMotionStateSync", lambda s: self.build_tech_core_payload(s.team)
        )
        await self.broker.broadcast(
            "RobotPerformanceSelectionSync", lambda s: self.build_perf_sync_payload(s.team)
        )
        await self.broker.broadcast(
            "RuneStatusSync", lambda s: self.build_rune_sync_payload(s.team)
        )
        await self.broker.broadcast(
            "SentryStatusSync", lambda s: self.build_sentry_sync_payload(s.team)
        )
        await self.broker.broadcast(
            "DartSelectTargetStatusSync", lambda s: self.build_dart_sync_payload(s.team)
        )
        for sess in self.broker.active_sessions():
            for buff in self.ensure_robot(sess.robot_id).buffs:
                await sess.publish(
                    "Buff",
                    encode_buff(
                        robot_id=sess.robot_id,
                        buff_type=int(buff["buff_type"]),
                        buff_level=int(buff["level"]),
                        buff_max_time=int(buff["max_time"]),
                        buff_left_time=max(0, int(round(float(buff["left_time"])))),
                    ),
                )

    async def broadcast_5hz(self) -> None:
        if not self.broker or not self.broker.active_sessions():
            return
        await self.broker.broadcast(
            "RobotDynamicStatus", lambda s: self.build_robot_dynamic_payload(s.robot_id)
        )
        await self.broker.broadcast("RadarInfoToClient", lambda s: self.build_radar_payload(s.team))
        for robot in self.robots.values():
            await self.broker.broadcast(
                "RobotPosition",
                encode_robot_position(
                    x=robot.x, y=robot.y, z=robot.z, yaw=robot.yaw, robot_id=robot.robot_id
                ),
            )

    # ------------------------------------------------------------------
    # Simulation Loop & Rule Advancement
    # ------------------------------------------------------------------

    async def run_loop(self) -> None:
        self._running = True
        self._last_sim_tick = time.monotonic()
        try:
            while self._running:
                await asyncio.sleep(0.1)
                now = time.monotonic()
                wall_dt = min(0.5, max(0.01, now - self._last_sim_tick))
                self._last_sim_tick = now
                await self.step(wall_dt)
        except asyncio.CancelledError:
            pass

    def stop_loop(self) -> None:
        self._running = False

    async def step(self, wall_dt: float) -> None:
        if self.mode != "paused":
            sim_dt = wall_dt * self.speed
            await self._advance_state(sim_dt)

        self._accum_5hz += wall_dt
        self._accum_1hz += wall_dt
        if self._accum_5hz >= 0.2:
            self._accum_5hz = 0.0
            await self.broadcast_5hz()
        if self._accum_1hz >= 1.0:
            self._accum_1hz = 0.0
            await self.broadcast_1hz()

    async def _advance_state(self, dt: float) -> None:
        # Stage progression when mode == "auto"
        if self.mode == "auto":
            if self.stage == 1:
                self.stage_countdown_s -= dt
                if self.stage_countdown_s <= 0.0:
                    self.stage = 2
                    self.stage_countdown_s = 5.0
                    await self._emit_event(2001, "进入 15s 自检阶段")
            elif self.stage == 2:
                self.stage_countdown_s -= dt
                if self.stage_countdown_s <= 0.0:
                    self.stage = 3
                    self.stage_countdown_s = 5.0
                    await self._emit_event(2002, "进入 5s 倒计时")
            elif self.stage == 3:
                self.stage_countdown_s -= dt
                if self.stage_countdown_s <= 0.0:
                    self.stage = 4
                    self.elapsed_s = 0.0
                    self.stage_countdown_s = MATCH_DURATION_S
                    self._awarded_gold_milestones = {0}
                    await self._emit_event(2003, "比赛正式开始！")
            elif self.stage == 5:
                self.stage_countdown_s -= dt
                if self.stage_countdown_s <= 0.0:
                    if self.auto_loop:
                        prev_round = self.current_round
                        prev_r, prev_b = self.red_score, self.blue_score
                        self.reset(stage=1)
                        self.current_round = min(self.total_rounds, prev_round + 1)
                        self.red_score, self.blue_score = prev_r, prev_b
                    else:
                        self.stage_countdown_s = 0.0
                        self.mode = "paused"
                        self.is_paused = True
                    await self.broadcast_1hz()
                    return

        # Cooldowns & robot cooling/penalties always tick unless paused
        for r in self.robots.values():
            if r.penalty_blind_s > 0.0:
                r.penalty_blind_s = max(0.0, r.penalty_blind_s - dt)
            if r.heat > 0.0:
                r.heat = max(0.0, r.heat - r.cooling_rate * dt)
                if r.heat <= 0.0 and r.overheat_locked and not r.overheat_permanent:
                    r.overheat_locked = False
            if r.out_of_combat_countdown > 0:
                r.out_of_combat_countdown = max(0, int(math.ceil(r.out_of_combat_countdown - dt)))
                if r.out_of_combat_countdown == 0:
                    r.out_of_combat = True
            for buff in list(r.buffs):
                buff["left_time"] = max(0.0, float(buff["left_time"]) - dt)
                if float(buff["left_time"]) <= 0.0:
                    r.buffs.remove(buff)
            if r.respawn_pending:
                r.respawn_progress += dt
                lam = self.teams[r.team].logistics_lambda()
                req = respawn_progress_required(self.elapsed_s, r.prior_instant_respawns, lam)
                r.respawn_total = req
                if r.respawn_progress >= req and self.mode == "auto":
                    r.respawn_pending = False
                    r.respawn_progress = 0.0
                    r.alive = True
                    r.hp = r.max_hp

        for team_name, t in self.teams.items():
            if t.dart_blind_remaining_s > 0.0:
                t.dart_blind_remaining_s = max(0.0, t.dart_blind_remaining_s - dt)
            if t.dart_gate_state in (1, 2):
                t.dart_gate_timer_s -= dt
                if t.dart_gate_state == 1 and t.dart_gate_timer_s <= 30.0:
                    t.dart_gate_state = 2
                elif t.dart_gate_timer_s <= 0.0:
                    t.dart_gate_state = 0
                    t.dart_cooldown_s = 15.0
            elif t.dart_cooldown_s > 0.0:
                t.dart_cooldown_s = max(0.0, t.dart_cooldown_s - dt)

            if t.sentry_powered:
                t.sentry_enhanced_rem_s -= dt
                if t.sentry_enhanced_rem_s <= 0.0:
                    t.sentry_powered = False
                    t.sentry_enhanced_rem_s = 0.0

            if t.rune_status == 2:
                t.rune_timer_s -= dt
                if t.rune_timer_s <= 0.0:
                    t.rune_status = 3
                    t.rune_arms = self.rng.randint(6, 10)
                    t.rune_avg_rings = round(self.rng.uniform(7.5, 9.7), 1)
                    is_big = self.elapsed_s >= 180.0
                    await self._emit_event(4, "2" if is_big else "1")
                    if is_big:
                        await self._emit_event(3, f"{t.rune_arms},{t.rune_avg_rings:.1f}")

            if t.tech_core_basic_state == 1:
                t.tech_core_remain_all_s = max(0.0, t.tech_core_remain_all_s - dt)
                t.tech_core_remain_step_s = max(0.0, t.tech_core_remain_step_s - dt)
                if t.tech_core_remain_step_s <= 0.0:
                    if t.tech_core_putin == 0:
                        t.tech_core_putin = 1
                        t.tech_core_remain_step_s = 4.0
                    elif t.tech_core_move == 0:
                        t.tech_core_move = 1
                        t.tech_core_remain_step_s = 4.0
                    elif t.tech_core_rotate == 0:
                        t.tech_core_rotate = 1
                        t.tech_core_remain_step_s = 2.0
                    else:
                        await self._complete_tech_core(team_name, t.tech_core_target_level)

        if self.stage != 4:
            return

        # Battle stage active progression
        self.elapsed_s = min(MATCH_DURATION_S, self.elapsed_s + dt)
        self.stage_countdown_s = max(0.0, MATCH_DURATION_S - self.elapsed_s)

        # Control scores (+1/s all sentinels, +3/s all bases)
        for team_name in ("red", "blue"):
            rate = 0.0
            if self.fields["sentinel_red"][0] == team_name and self.fields["sentinel_blue"][0] == team_name:
                rate += 1.0
            if self.fields["base_red"][0] == team_name and self.fields["base_blue"][0] == team_name:
                rate += 3.0
            self.teams[team_name].control_score += rate * dt

        # Track max advantages & time unlock 42mm
        red_t, blue_t = self.teams["red"], self.teams["blue"]
        red_t.max_base_advantage = max(red_t.max_base_advantage, float(blue_t.base_hp - red_t.base_hp))
        blue_t.max_base_advantage = max(blue_t.max_base_advantage, float(red_t.base_hp - blue_t.base_hp))
        red_t.max_control_advantage = max(red_t.max_control_advantage, blue_t.control_score - red_t.control_score)
        blue_t.max_control_advantage = max(blue_t.max_control_advantage, red_t.control_score - blue_t.control_score)

        for team_name, t in self.teams.items():
            if not t.launcher_unlocked:
                thresh = max(0.0, min(300.0, 300.0 - t.max_base_advantage / 50.0 - 2.0 * t.max_control_advantage / 3.0))
                if self.elapsed_s >= thresh:
                    t.launcher_unlocked = True
                    t.launcher_installed = True
                    if team_name == "red":
                        self.ensure_robot(1).modules["modular_42mm"] = 1
                    else:
                        self.ensure_robot(101).modules["modular_42mm"] = 1
                    await self._emit_event(16, f"{team_name}")

        # Natural gold schedule
        for idx, (t_mark, amt) in enumerate(NATURAL_GOLD_MILESTONES):
            if idx not in self._awarded_gold_milestones and self.elapsed_s >= t_mark:
                self._awarded_gold_milestones.add(idx)
                red_t.economy += amt
                red_t.total_economy += amt
                blue_t.economy += amt
                blue_t.total_economy += amt
                await self._emit_event(2010, f"自然金币 +{amt}")

        # Assembly 10s income
        self._accum_assembly_econ += dt
        if self._accum_assembly_econ >= 10.0:
            self._accum_assembly_econ -= 10.0
            for t in self.teams.values():
                inc = assembly_income_per_10s(t.tech_level)
                if inc > 0:
                    t.economy += inc
                    t.total_economy += inc

        # Mechanism timers (capture progress bars)
        for mid in list(self.mechanisms):
            self.mechanisms[mid] -= dt
            if self.mechanisms[mid] <= 0.0:
                del self.mechanisms[mid]

        # Robot motion + skirmish simulation in auto mode
        if self.mode == "auto":
            await self._tick_auto_movement_and_skirmish(dt)
            if self.random_events != "off":
                await self._maybe_trigger_random_event(dt)

        # Win/end condition check
        if red_t.base_hp == 0 or blue_t.base_hp == 0 or self.elapsed_s >= MATCH_DURATION_S:
            self._finalize_match()
            if self.auto_loop:
                await self._emit_event(2099, "比赛结束，进入结算阶段（15s 后自动开启下一局）")
            else:
                await self._emit_event(2099, "本局比赛结束，已进入结算并自动暂停")
            await self.broadcast_5hz()
            await self.broadcast_1hz()

    def _finalize_match(self) -> None:
        self.stage = 5
        self.stage_countdown_s = 15.0 if self.auto_loop else 0.0
        if not self.auto_loop:
            self.mode = "paused"
            self.is_paused = True
        r, b = self.teams["red"], self.teams["blue"]
        if r.base_hp != b.base_hp and (r.base_hp == 0 or b.base_hp == 0):
            winner = "red" if r.base_hp > b.base_hp else "blue"
        elif round(r.control_score) != round(b.control_score):
            winner = "red" if r.control_score > b.control_score else "blue"
        elif r.total_damage != b.total_damage:
            winner = "red" if r.total_damage > b.total_damage else "blue"
        else:
            winner = "draw"
        if winner == "red":
            self.red_score += 1
            self.game_result = 1
        elif winner == "blue":
            self.blue_score += 1
            self.game_result = 2
        else:
            self.game_result = 3

    async def _tick_auto_movement_and_skirmish(self, dt: float) -> None:
        for r in self.robots.values():
            if not r.alive or not r.online:
                continue
            r.patrol_phase += dt * (0.45 if r.robot_id % 100 != 1 else 0.25)
            dx = 2.6 * math.sin(r.patrol_phase)
            dy = 1.6 * math.cos(r.patrol_phase * 0.8)
            r.x = max(1.2, min(26.8, r.home_x + dx))
            r.y = max(1.2, min(13.8, r.home_y + dy))
            r.yaw = (math.degrees(math.atan2(dy, dx)) + 360.0) % 360.0
            r.chassis_power = max(18.0, min(float(r.max_power), 42.0 + 18.0 * abs(math.sin(r.patrol_phase))))

        # Proactive tech-core assembly initiation when new difficulty level unlocks
        unlocked_lv = max_unlocked_assembly_level(self.elapsed_s)
        for team_name, t in self.teams.items():
            if t.tech_level < unlocked_lv and t.tech_core_basic_state != 1:
                unlock_at = (t.tech_level) * 60.0
                if self.elapsed_s >= unlock_at + (12.0 if team_name == "red" else 18.0):
                    t.tech_core_basic_state = 1
                    t.tech_core_target_level = t.tech_level + 1
                    t.tech_core_putin = 0
                    t.tech_core_move = 0
                    t.tech_core_rotate = 0
                    t.tech_core_remain_all_s = 6.0
                    t.tech_core_remain_step_s = 1.8

        self._accum_combat += dt
        if self._accum_combat >= 1.0:
            self._accum_combat = 0.0
            # Periodic combat skirmish between alive units (can reduce HP to 0 and trigger kills)
            if self.rng.random() < 0.80:
                shooter_team = self.rng.choice(("red", "blue"))
                s_base = 0 if shooter_team == "red" else 100
                v_base = 100 if shooter_team == "red" else 0
                alive_shooters = [
                    self.ensure_robot(s_base + s)
                    for s in (1, 3, 4, 7)
                    if self.ensure_robot(s_base + s).alive and self.ensure_robot(s_base + s).remaining_ammo > 2
                ]
                alive_victims = [
                    self.ensure_robot(v_base + s)
                    for s in (1, 3, 4, 7)
                    if self.ensure_robot(v_base + s).alive
                ]
                if alive_shooters and alive_victims:
                    shooter = self.rng.choice(alive_shooters)
                    victim = self.rng.choice(alive_victims)
                    is_heavy = (shooter.robot_id % 100) == 1
                    shots = 1 if is_heavy else self.rng.randint(3, 7)
                    shooter.remaining_ammo = max(0, shooter.remaining_ammo - shots)
                    shooter.projectiles_fired += shots
                    shooter.heat = min(float(shooter.max_heat) * 0.9, shooter.heat + shots * (25.0 if is_heavy else 7.0))
                    shooter.experience += 25
                    if shooter.experience >= shooter.experience_for_upgrade and shooter.level < 10:
                        shooter.level += 1
                        shooter.experience -= shooter.experience_for_upgrade
                        shooter.experience_for_upgrade += 120
                    dmg = self.rng.randint(75, 115) if is_heavy else shots * 14
                    victim.hp = max(0, victim.hp - dmg)
                    victim.out_of_combat = False
                    victim.out_of_combat_countdown = 6
                    victim.injury["total_damage"] += dmg
                    if is_heavy:
                        victim.injury["large_projectile_damage"] += dmg
                    else:
                        victim.injury["small_projectile_damage"] += dmg
                    victim.injury["killer_id"] = shooter.robot_id
                    self.teams[shooter_team].total_damage += dmg
                    if victim.hp <= 0:
                        await self.trigger_manual(
                            "kill_robot",
                            {"victim_id": victim.robot_id, "killer_id": shooter.robot_id},
                        )

    async def _maybe_trigger_random_event(self, dt: float) -> None:
        intervals = {"low": 12.0, "normal": 6.5, "high": 3.2}
        period = intervals.get(self.random_events, 6.5)
        self._accum_random += dt
        if self._accum_random < period:
            return
        self._accum_random = 0.0

        choices: list[str] = [
            "armor_hit",
            "control_contest",
            "sentry_posture",
            "grant_buff",
            "outpost_damage",
        ]
        unlocked_lv = max_unlocked_assembly_level(self.elapsed_s)
        if any(t.tech_level < unlocked_lv for t in self.teams.values()):
            choices.extend(["tech_core_auto", "tech_core_auto", "tech_core_auto"])
        if self.elapsed_s >= 15.0:
            choices.extend(["kill_skirmish", "kill_skirmish"])
        if self.elapsed_s >= 30.0:
            choices.extend(["dart_gate", "dart_hit"])
        if self.elapsed_s >= 120.0:
            choices.append("rune_auto")
        if self.random_events in ("normal", "high") and self.elapsed_s >= 45.0:
            choices.extend(["module_glitch", "overheat_brief"])
            if (self.elapsed_s - self._last_yellow_card_elapsed_s) >= 90.0:
                choices.append("yellow_card_brief")

        action = self.rng.choice(choices)
        team = self.rng.choice(("red", "blue"))
        if action == "armor_hit":
            await self.trigger_manual("armor_hit", {"team": team, "count": self.rng.randint(1, 3)})
        elif action == "control_contest":
            home = self.rng.choice(("red", "blue"))
            await self.trigger_manual("flip_control", {"kind": "sentinel", "home": home, "owner": team})
        elif action == "sentry_posture":
            pid = self.rng.choice((1, 2, 3))
            self.teams[team].sentry_posture_id = pid
            await self._emit_event(2012, f"{team} 哨兵切换姿态 #{pid}")
        elif action == "grant_buff":
            rid = (0 if team == "red" else 100) + self.rng.choice((1, 3, 4))
            await self.trigger_manual("grant_buff", {"robot_id": rid, "buff_type": self.rng.choice((1, 2, 3)), "duration": 20})
        elif action == "outpost_damage":
            await self.trigger_manual("damage_outpost", {"team": team, "damage": 150})
        elif action == "dart_gate":
            await self.trigger_manual("dart_gate_open", {"firing_team": team})
        elif action == "dart_hit":
            await self.trigger_manual("dart_hit", {"firing_team": team, "target_id": self.rng.randint(1, 3)})
        elif action == "tech_core_auto":
            eligible_teams = [
                t_name for t_name, t_obj in self.teams.items() if t_obj.tech_level < unlocked_lv
            ]
            target_team = self.rng.choice(eligible_teams) if eligible_teams else team
            next_lv = min(unlocked_lv, self.teams[target_team].tech_level + 1)
            if next_lv > self.teams[target_team].tech_level:
                await self.trigger_manual("tech_core_step", {"team": target_team, "level": next_lv})
        elif action == "kill_skirmish":
            v_base = 100 if team == "red" else 0
            k_base = 0 if team == "red" else 100
            alive_victims = [v_base + s for s in (1, 3, 4, 7) if self.ensure_robot(v_base + s).alive]
            alive_killers = [k_base + s for s in (1, 3, 4, 7) if self.ensure_robot(k_base + s).alive]
            victim_id = self.rng.choice(alive_victims) if alive_victims else (v_base + self.rng.choice((3, 4)))
            killer_id = self.rng.choice(alive_killers) if alive_killers else (k_base + self.rng.choice((1, 3)))
            await self.trigger_manual("kill_robot", {"victim_id": victim_id, "killer_id": killer_id})
        elif action == "rune_auto":
            await self.trigger_manual("rune_activate", {"team": team})
        elif action == "module_glitch":
            rid = (0 if team == "red" else 100) + 3
            await self.trigger_manual("module_fault", {"robot_id": rid, "module": "capacitor", "state": 0})
        elif action == "yellow_card_brief":
            prob = 0.15 if self.random_events == "high" else 0.08
            if self.rng.random() < prob:
                rid = self._primary_connected_robot_id()
                if self.ensure_robot(rid).penalty_blind_s <= 0.0:
                    self._last_yellow_card_elapsed_s = self.elapsed_s
                    await self.trigger_manual("yellow_card", {"robot_id": rid, "seconds": 3})
        elif action == "overheat_brief":
            prob = 0.16 if self.random_events == "high" else 0.08
            if self.rng.random() < prob:
                rid = self._primary_connected_robot_id()
                await self.trigger_manual("overheat", {"robot_id": rid, "excess": 25})

    def _primary_connected_robot_id(self) -> int:
        if self.broker and self.broker.active_sessions():
            return self.broker.active_sessions()[0].robot_id
        return 3

    async def _emit_event(self, event_id: int, param: str = "", team: str | None = None) -> None:
        self._log_sim_event(event_id, param or f"Event {event_id}")
        if not self.broker:
            return
        payload = encode_event(event_id, param)
        if team is None:
            await self.broker.broadcast("Event", payload)
        else:
            for sess in self.broker.active_sessions():
                if sess.team == team:
                    await sess.publish("Event", payload)

    async def _complete_tech_core(self, team_name: str, level: int) -> None:
        t = self.teams[team_name]
        t.tech_level = max(t.tech_level, min(4, level))
        t.tech_core_basic_state = 2
        t.tech_core_remain_all_s = 0.0
        t.tech_core_remain_step_s = 0.0
        heavy_id = 1 if team_name == "red" else 101
        heavy = self.ensure_robot(heavy_id)
        if t.tech_level == 1:
            heavy.level = max(heavy.level, 5)
            heavy.max_hp = max(heavy.max_hp, 450)
            heavy.hp = min(heavy.max_hp, heavy.hp + 50)
        elif t.tech_level == 2:
            heavy.level = max(heavy.level, 7)
            heavy.max_hp = max(heavy.max_hp, 500)
            heavy.hp = min(heavy.max_hp, heavy.hp + 50)
        elif t.tech_level >= 3:
            heavy.level = 10
            heavy.max_hp = 600
            heavy.hp = min(600, heavy.hp + 100)
            heavy.modules["modular_42mm"] = 1
            t.launcher_unlocked = True
            t.launcher_installed = True
            await self._emit_event(16, team_name)
        await self._emit_event(15, "0", team=team_name)
        await self.broadcast_1hz()

    # ------------------------------------------------------------------
    # Manual Triggers & Client Upbound Commands
    # ------------------------------------------------------------------

    async def trigger_manual(self, action: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        p = params or {}
        action = action.strip()

        if action == "start_full_match":
            self.mode = "auto"
            self.reset(stage=1)
            await self.broadcast_1hz()
            return {"ok": True, "message": "已从准备阶段开启完整赛程"}

        if action == "start_battle":
            if self.mode == "paused":
                self.mode = "auto"
            self.reset(stage=4, elapsed_s=float(p.get("elapsed_s", 0.0)))
            await self.broadcast_1hz()
            return {"ok": True, "message": "已直接进入 7 分钟比赛阶段"}

        if action == "reset":
            self.reset(stage=int(p.get("stage", 0)), elapsed_s=float(p.get("elapsed_s", 0.0)))
            await self.broadcast_1hz()
            return {"ok": True, "message": "已重置模拟赛局"}

        if action == "set_stage":
            st = max(0, min(5, int(p.get("stage", 4))))
            self.stage = st
            if st == 1:
                self.stage_countdown_s = float(p.get("countdown_s", 15))
            elif st == 2:
                self.stage_countdown_s = float(p.get("countdown_s", 5))
            elif st == 3:
                self.stage_countdown_s = float(p.get("countdown_s", 5))
            elif st == 4:
                self.elapsed_s = max(0.0, min(MATCH_DURATION_S, float(p.get("elapsed_s", self.elapsed_s))))
                self.stage_countdown_s = MATCH_DURATION_S - self.elapsed_s
            elif st == 5:
                self.stage_countdown_s = float(p.get("countdown_s", 15))
            await self.broadcast_1hz()
            return {"ok": True, "message": f"阶段已切换至 {STAGE_NAMES.get(st, st)}"}

        if action == "jump_time":
            target_elapsed = max(0.0, min(MATCH_DURATION_S, float(p.get("elapsed_s", 150.0))))
            self.stage = 4
            self.elapsed_s = target_elapsed
            self.stage_countdown_s = MATCH_DURATION_S - target_elapsed
            for idx, (t_mark, amt) in enumerate(NATURAL_GOLD_MILESTONES):
                if idx not in self._awarded_gold_milestones and self.elapsed_s >= t_mark:
                    self._awarded_gold_milestones.add(idx)
                    self.teams["red"].economy += amt
                    self.teams["red"].total_economy += amt
                    self.teams["blue"].economy += amt
                    self.teams["blue"].total_economy += amt
            await self.broadcast_1hz()
            return {"ok": True, "message": f"比赛时间已跳转至 {int(self.elapsed_s)}s"}

        if action == "dart_gate_open":
            firing_team = str(p.get("firing_team") or ("blue" if team_for_robot_id(self._primary_connected_robot_id()) == "red" else "red"))
            target_team = "blue" if firing_team == "red" else "red"
            t = self.teams[firing_team]
            t.dart_gate_state = 2
            t.dart_gate_timer_s = 30.0
            await self._emit_event(10, "", team=target_team)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{firing_team} 飞镖闸门已开启（已通知 {target_team} 37s 预警）"}

        if action == "dart_hit":
            firing_team = str(p.get("firing_team") or ("blue" if team_for_robot_id(self._primary_connected_robot_id()) == "red" else "red"))
            target_team = "blue" if firing_team == "red" else "red"
            target_id = max(1, min(5, int(p.get("target_id", 3))))
            ft = self.teams[firing_team]
            tt = self.teams[target_team]
            ft.darts_remaining = max(0, ft.darts_remaining - 1)
            ft.dart_hits_dealt += 1
            if target_id == 1:
                tt.outpost_hp = max(0, tt.outpost_hp - 1000)
                if tt.outpost_hp == 0:
                    await self._emit_event(2, "11" if target_team == "red" else "111")
            else:
                dmg = {2: 200, 3: 300, 4: 2000, 5: 5000}.get(target_id, 300)
                tt.base_hp = max(0, tt.base_hp - dmg)
                if tt.base_hp < BASE_MAX_HP:
                    tt.base_status = 2
            blind_s = dart_blind_seconds(target_id, ft.dart_hits_dealt)
            tt.dart_blind_remaining_s += blind_s
            side = 1 if firing_team == "red" else 2
            await self._emit_event(9, f"{side},{target_id}")
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{firing_team} 飞镖命中目标 #{target_id}（{target_team} 致盲 +{blind_s}s）"}

        if action in ("yellow_card", "second_yellow_card", "red_card"):
            rid = int(p.get("robot_id") or self._primary_connected_robot_id())
            r = self.ensure_robot(rid)
            self.penalty_counter += 1
            if action == "red_card":
                ptype, sec = 3, 0
                r.red_card = True
            elif action == "second_yellow_card":
                ptype, sec = 2, int(p.get("seconds", 10))
                r.penalty_blind_s += sec
            else:
                ptype, sec = 1, int(p.get("seconds", 5))
                r.penalty_blind_s += sec
            if self.broker:
                payload = encode_penalty_info(
                    penalty_type=ptype, penalty_effect_sec=sec, total_penalty_num=self.penalty_counter
                )
                for sess in self.broker.active_sessions():
                    if sess.robot_id == rid or (p.get("broadcast") and sess.team == r.team):
                        await sess.publish("PenaltyInfo", payload)
            await self._emit_event(2020, f"#{rid} 判罚 type={ptype} ({sec}s)")
            return {"ok": True, "message": f"已对 #{rid} 下发判罚 type={ptype}"}

        if action == "overheat":
            rid = int(p.get("robot_id") or self._primary_connected_robot_id())
            r = self.ensure_robot(rid)
            excess = float(p.get("excess", 40.0))
            r.heat = float(r.max_heat) + excess
            r.overheat_locked = True
            await self.broadcast_5hz()
            await self._emit_event(2021, f"#{rid} 枪口超热量 ({r.heat:.0f}/{r.max_heat})，图传断开")
            return {"ok": True, "message": f"#{rid} 已进入超热量断图传状态"}

        if action == "clear_compliance":
            for r in self.robots.values():
                r.red_card = False
                r.penalty_blind_s = 0.0
                r.overheat_locked = False
                r.overheat_permanent = False
                r.heat = 0.0
                r.modules["video_transmission"] = 1
            for t in self.teams.values():
                t.dart_blind_remaining_s = 0.0
            await self.broadcast_5hz()
            await self.broadcast_1hz()
            await self._emit_event(2022, "已解除全部合规遮挡与断流状态")
            return {"ok": True, "message": "已清除所有合规锁定/致盲/超热状态"}

        if action == "armor_hit":
            team = str(p.get("team") or team_for_robot_id(self._primary_connected_robot_id()))
            count = max(1, min(20, int(p.get("count", 1))))
            for _ in range(count):
                self.armor_hit_seq += 1
                x_mm = int(p.get("x_mm", self.rng.randint(-42, 42)))
                y_mm = int(p.get("y_mm", self.rng.randint(-28, 28)))
                dmg = int(p.get("damage", 20))
                armor_id = int(p.get("armor_id", (self.armor_hit_seq % 4) + 1))
                raw = build_rmh1_hit_bytes(
                    armor_id=armor_id,
                    reason=0,
                    hit_sequence=self.armor_hit_seq,
                    x_mm=x_mm,
                    y_mm=y_mm,
                    damage=dmg,
                )
                self.last_custom_block = raw
                if self.broker:
                    payload = encode_custom_byte_block(raw)
                    for sess in self.broker.active_sessions():
                        if sess.team == team:
                            await sess.publish("CustomByteBlock", payload)
            return {"ok": True, "message": f"已向 {team} 注入 {count} 发 RMH1 命中数据"}

        if action == "flip_control":
            kind = str(p.get("kind", "sentinel"))
            home = str(p.get("home", "blue"))
            key = f"{kind}_{home}"
            if key not in self.fields:
                return {"ok": False, "error": f"未知阵地 {key}"}
            cur_owner, cur_lvl = self.fields[key]
            owner = str(p.get("owner") or ("blue" if cur_owner == "red" else "red"))
            level = "full" if owner == home else "temporary"
            self.fields[key] = (owner, level)
            if kind == "sentinel":
                eid = 5 if home == "red" else 6
                code = {("red", "temporary"): "1", ("red", "full"): "2", ("blue", "temporary"): "3", ("blue", "full"): "4"}[(owner, level)]
                await self._emit_event(eid, code)
            else:
                eid = 7 if home == "red" else 8
                code = "1" if owner == home else "2"
                await self._emit_event(eid, code)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{key} 已变更为 {owner} ({level})"}

        if action == "mechanism_progress":
            mid = max(1, min(6, int(p.get("mechanism_id", 4))))
            sec = max(1.0, min(15.0, float(p.get("seconds", 5.0))))
            self.mechanisms[mid] = sec
            await self.broadcast_1hz()
            return {"ok": True, "message": f"机制 #{mid} 激活进度 {sec}s"}

        if action == "damage_outpost":
            team = str(p.get("team", "blue"))
            dmg = int(p.get("damage", 250))
            t = self.teams[team]
            t.outpost_hp = max(0, t.outpost_hp - dmg)
            if t.outpost_hp == 0:
                t.base_status = 1
                await self._emit_event(2, "11" if team == "red" else "111")
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} 前哨站血量 -> {t.outpost_hp}"}

        if action == "outpost_stop":
            team = str(p.get("team", "blue"))
            self.teams[team].outpost_stopped = True
            opp = "blue" if team == "red" else "red"
            await self._emit_event(12, "", team=opp)
            return {"ok": True, "message": f"已触发 {team} 前哨站停转"}

        if action == "set_base_hp":
            team = str(p.get("team", "red"))
            hp = max(0, min(BASE_MAX_HP, int(p.get("hp", 5000))))
            t = self.teams[team]
            t.base_hp = hp
            if hp < BASE_MAX_HP and t.base_status == 0:
                t.base_status = 1
            await self._emit_event(11, "", team=team)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} 基地血量 -> {hp} (λ={t.logistics_lambda():.3f})"}

        if action == "base_armor_open":
            team = str(p.get("team", "blue"))
            self.teams[team].base_status = 2
            opp = "blue" if team == "red" else "red"
            await self._emit_event(13, "", team=opp)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} 基地护甲已展开"}

        if action == "kill_robot":
            victim_id = int(p.get("victim_id", 103))
            killer_id = int(p.get("killer_id", 3))
            v = self.ensure_robot(victim_id)
            v.hp = 0
            v.alive = False
            v.respawn_pending = True
            v.respawn_progress = 0.0
            v.respawn_total = respawn_progress_required(
                self.elapsed_s, v.prior_instant_respawns, self.teams[v.team].logistics_lambda()
            )
            v.injury["killer_id"] = killer_id
            await self._emit_event(1, f"{victim_id},{killer_id}")
            if victim_id % 100 == 1:
                await self._emit_event(14, f"{victim_id}", team=v.team)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"#{victim_id} 被 #{killer_id} 击杀，进入复活读条"}

        if action == "respawn_finish":
            rid = int(p.get("robot_id") or self._primary_connected_robot_id())
            r = self.ensure_robot(rid)
            r.respawn_pending = False
            r.respawn_progress = 0.0
            r.alive = True
            r.hp = r.max_hp
            await self.broadcast_1hz()
            return {"ok": True, "message": f"#{rid} 已复活满血"}

        if action == "tech_core_step":
            team = str(p.get("team") or team_for_robot_id(self._primary_connected_robot_id()))
            level = int(p.get("level", min(4, self.teams[team].tech_level + 1)))
            await self._complete_tech_core(team, level)
            return {"ok": True, "message": f"{team} 重装科技核心已装配至 Lv{level}"}

        if action == "tech_core_fail":
            team = str(p.get("team") or team_for_robot_id(self._primary_connected_robot_id()))
            reason_code = str(p.get("reason_code", "2"))
            t = self.teams[team]
            t.tech_core_basic_state = 3
            await self._emit_event(15, reason_code, team=team)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} 科技核心装配失败事件 ({reason_code})"}

        if action == "install_42mm":
            team = str(p.get("team") or team_for_robot_id(self._primary_connected_robot_id()))
            t = self.teams[team]
            t.launcher_unlocked = True
            t.launcher_installed = True
            rid = 1 if team == "red" else 101
            self.ensure_robot(rid).modules["modular_42mm"] = 1
            await self._emit_event(16, team)
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} 重装 42mm 发射机构已安装"}

        if action == "rune_activate":
            team = str(p.get("team") or team_for_robot_id(self._primary_connected_robot_id()))
            t = self.teams[team]
            t.rune_status = 3
            t.rune_arms = int(p.get("arms", 8))
            t.rune_avg_rings = float(p.get("avg_rings", 9.1))
            is_big = self.elapsed_s >= 180.0
            await self._emit_event(4, "2" if is_big else "1")
            if is_big:
                await self._emit_event(3, f"{t.rune_arms},{t.rune_avg_rings:.1f}")
            await self.broadcast_1hz()
            return {"ok": True, "message": f"{team} {'大' if is_big else '小'}能量机关已激活"}

        if action == "module_fault":
            rid = int(p.get("robot_id") or self._primary_connected_robot_id())
            mod = str(p.get("module", "video_transmission"))
            st = int(p.get("state", 0))
            r = self.ensure_robot(rid)
            if mod in r.modules:
                r.modules[mod] = st
                await self.broadcast_1hz()
                return {"ok": True, "message": f"#{rid} 模块 {mod} -> {st}"}
            return {"ok": False, "error": f"未知模块 {mod}"}

        if action == "grant_buff":
            rid = int(p.get("robot_id") or self._primary_connected_robot_id())
            btype = int(p.get("buff_type", 2))
            lvl = int(p.get("level", 25))
            dur = int(p.get("duration", 30))
            r = self.ensure_robot(rid)
            r.buffs = [b for b in r.buffs if b["buff_type"] != btype]
            r.buffs.append({"buff_type": btype, "level": lvl, "max_time": dur, "left_time": float(dur)})
            await self.broadcast_1hz()
            return {"ok": True, "message": f"已给予 #{rid} Buff #{btype} ({dur}s)"}

        if action == "custom_block":
            text = str(p.get("text", "SIM-CUSTOM-0311"))
            raw = text.encode("utf-8")
            self.last_custom_block = raw
            if self.broker:
                await self.broker.broadcast("CustomByteBlock", encode_custom_byte_block(raw))
            return {"ok": True, "message": f"已推送 CustomByteBlock ({len(raw)}B)"}

        if action == "map_click":
            x = float(p.get("map_x", 14.0))
            y = float(p.get("map_y", 7.5))
            mtype = int(p.get("marker_type", 1))
            if self.broker:
                await self.broker.broadcast(
                    "MapClickInfo",
                    encode_map_click_info(is_send_all=2, mode=mtype, marker_type=1, map_x=x, map_y=y),
                )
            return {"ok": True, "message": f"已推送地图标点 ({x:.1f}, {y:.1f})"}

        if action == "raw_publish":
            topic = str(p.get("topic", "")).strip()
            hex_str = str(p.get("payload_hex", "")).strip().replace(" ", "")
            if topic not in INBOUND_TOPICS:
                return {"ok": False, "error": f"不支持的下行主题: {topic}"}
            raw = bytes.fromhex(hex_str)
            if self.broker:
                cnt = await self.broker.broadcast(topic, raw)
                return {"ok": True, "message": f"已向 {cnt} 个客户端推送 {topic} ({len(raw)}B)"}
            return {"ok": True, "message": "无连接客户端"}

        return {"ok": False, "error": f"未知触发动作: {action}"}

    async def handle_client_command(self, session: ClientSession, topic: str, payload: bytes) -> None:
        team = session.team
        t = self.teams[team]
        rid = session.robot_id
        r = self.ensure_robot(rid)

        if topic == "KeyboardMouseControl":
            cmd = decode_keyboard_mouse_control(payload)
            self._log_command(session, topic, f"key=0x{int(cmd['keyboard_value']):04x}")
            if cmd["left_button_down"] and self.stage == 4 and r.alive:
                r.heat = min(float(r.max_heat) + 30.0, r.heat + 6.0)
                if r.heat > r.max_heat:
                    r.overheat_locked = True
            return

        if topic == "CustomControl":
            cmd = decode_custom_control(payload)
            data = cmd["data"]
            self._log_command(session, topic, f"0x0311 ({len(data)}B)")
            self.last_custom_block = data
            await session.publish("CustomByteBlock", encode_custom_byte_block(data))
            return

        if topic == "MapClickCmd":
            cmd = decode_map_click_cmd(payload)
            self._log_command(
                session,
                topic,
                f"({cmd['map_x']:.1f}, {cmd['map_y']:.1f}) type={cmd['mode']}",
            )
            info_payload = encode_map_click_info(
                is_send_all=int(cmd["is_send_all"]),
                robot_id=bytes(cmd["robot_id"]),
                mode=int(cmd["mode"]),
                enemy_id=int(cmd["enemy_id"]),
                ascii_code=int(cmd["ascii"]),
                marker_type=int(cmd["type"]),
                map_x=float(cmd["map_x"]),
                map_y=float(cmd["map_y"]),
            )
            if self.broker:
                for peer_sess in self.broker.active_sessions():
                    if peer_sess.team == team:
                        await peer_sess.publish("MapClickInfo", info_payload)
            return

        if topic == "SentryCtrlCommand":
            cmd = decode_sentry_ctrl_command(payload)
            cid = cmd["command_id"]
            self._log_command(session, topic, f"cmd_id={cid}")
            result_code = 0
            cost = 0 if t.sentry_control_mode == 1 else 50

            if cid in (7, 8, 9, 10, 11, 12):
                if t.sentry_last_switch_elapsed_s is not None and (self.elapsed_s - t.sentry_last_switch_elapsed_s) < 5.0:
                    result_code = 1  # cooldown
                else:
                    t.sentry_posture_id = ((cid - 7) % 3) + 1
                    t.sentry_powered = cid >= 10
                    t.sentry_enhanced_rem_s = 15.0 if t.sentry_powered else 0.0
                    t.sentry_last_switch_elapsed_s = self.elapsed_s
            elif cid in (1, 3, 4, 5, 6):
                if t.economy < cost:
                    result_code = 2  # insufficient economy
                else:
                    t.economy -= cost
                    sentry = self.ensure_robot(7 if team == "red" else 107)
                    if cid in (1, 3):
                        sentry.remaining_ammo += 100
                    elif cid == 4:
                        sentry.hp = min(sentry.max_hp, sentry.hp + 180)
                    elif cid in (5, 6):
                        sentry.alive = True
                        sentry.respawn_pending = False
                        sentry.hp = sentry.max_hp
            else:
                result_code = 3

            await session.publish("SentryCtrlResult", encode_sentry_ctrl_result(command_id=cid, result_code=result_code))
            if self.broker:
                for peer_sess in self.broker.active_sessions():
                    if peer_sess.team == team:
                        await peer_sess.publish("SentryStatusSync", self.build_sentry_sync_payload(team))
                        await peer_sess.publish("GlobalLogisticsStatus", self.build_global_logistics_payload(team))
            return

        if topic == "DartCommand":
            cmd = decode_dart_command(payload)
            target_id = max(1, min(5, int(cmd["target_id"])))
            t.dart_target_id = target_id
            self._log_command(
                session,
                topic,
                f"target={target_id} open={cmd['open']} fire={cmd['launch_confirm']}",
            )
            if cmd["launch_confirm"] and t.dart_gate_state == 2 and t.darts_remaining > 0:
                await self.trigger_manual("dart_hit", {"firing_team": team, "target_id": target_id})
            elif cmd["open"] and t.dart_gate_state == 0:
                await self.trigger_manual("dart_gate_open", {"firing_team": team})
            elif not cmd["open"] and t.dart_gate_state != 0:
                t.dart_gate_state = 0
                t.dart_cooldown_s = 15.0
            await session.publish("DartSelectTargetStatusSync", self.build_dart_sync_payload(team))
            return

        if topic == "AssemblyCommand":
            cmd = decode_assembly_command(payload)
            op, diff = cmd["operation"], max(1, min(4, cmd["difficulty"]))
            self._log_command(session, topic, f"op={op} diff={diff}")
            if op == 0:
                if diff <= max_unlocked_assembly_level(self.elapsed_s):
                    t.tech_core_basic_state = 1
                    t.tech_core_target_level = diff
                    t.tech_core_putin = 0
                    t.tech_core_move = 0
                    t.tech_core_rotate = 0
                    t.tech_core_remain_all_s = 14.0
                    t.tech_core_remain_step_s = 4.0
            elif op == 1:
                if diff <= max_unlocked_assembly_level(self.elapsed_s):
                    await self._complete_tech_core(team, diff)
            else:
                t.tech_core_basic_state = 0
                t.tech_core_remain_all_s = 0.0
                t.tech_core_remain_step_s = 0.0
                await self._emit_event(15, "6", team=team)
            await session.publish("TechCoreMotionStateSync", self.build_tech_core_payload(team))
            return

        if topic == "RobotPerformanceSelectionCommand":
            cmd = decode_performance_selection_command(payload)
            t.shooter_perf = cmd["shooter"]
            t.chassis_perf = cmd["chassis"]
            t.sentry_control_mode = cmd["sentry_control"]
            self._log_command(session, topic, f"s={t.shooter_perf} c={t.chassis_perf} sen={t.sentry_control_mode}")
            if self.broker:
                for peer_sess in self.broker.active_sessions():
                    if peer_sess.team == team:
                        await peer_sess.publish("RobotPerformanceSelectionSync", self.build_perf_sync_payload(team))
            return

        if topic == "RuneActivateCommand":
            cmd = decode_rune_activate_command(payload)
            self._log_command(session, topic, f"activate={cmd['activate']}")
            if cmd["activate"] == 1:
                t.rune_status = 2
                t.rune_timer_s = 2.0
                await session.publish("RuneStatusSync", self.build_rune_sync_payload(team))
            return

        if topic == "CommonCommand":
            cmd = decode_common_command(payload)
            ctype, param = cmd["cmd_type"], cmd["param"]
            self._log_command(session, topic, f"cmd_type={ctype} param={param}")
            if ctype == 1:  # 17mm ammo exchange
                cost = param
                if t.economy >= cost and param > 0:
                    t.economy -= cost
                    r.remaining_ammo += param
            elif ctype == 2:  # 42mm ammo exchange
                cost = param * 10
                if t.economy >= cost and param > 0:
                    t.economy -= cost
                    r.remaining_ammo += param
            elif ctype == 3:  # confirm respawn
                if r.respawn_pending:
                    r.respawn_pending = False
                    r.respawn_progress = 0.0
                    r.alive = True
                    r.hp = r.max_hp
            elif ctype == 4:  # instant gold respawn
                cost = instant_respawn_cost(self.elapsed_s, r.level)
                if r.respawn_pending and t.economy >= cost:
                    t.economy -= cost
                    r.prior_instant_respawns += 1
                    r.respawn_pending = False
                    r.respawn_progress = 0.0
                    r.alive = True
                    r.hp = r.max_hp
            elif ctype == 5:  # remote ammo
                if t.economy >= 150:
                    t.economy -= 150
                    r.remaining_ammo += 10 if (rid % 100 == 1) else 100
            elif ctype == 6:  # remote heal
                cost = remote_heal_cost(self.elapsed_s)
                if t.economy >= cost:
                    t.economy -= cost
                    heal = round(r.max_hp * 0.6 * t.logistics_lambda())
                    r.hp = min(r.max_hp, r.hp + heal)

            await session.publish("RobotDynamicStatus", self.build_robot_dynamic_payload(rid))
            await session.publish("RobotRespawnStatus", self.build_robot_respawn_payload(rid))
            await session.publish("GlobalLogisticsStatus", self.build_global_logistics_payload(team))
            await session.publish("GlobalUnitStatus", self.build_global_unit_status_payload(team))

    def to_dict(self) -> dict[str, Any]:
        cut, cut_reason = self.should_cut_video()
        return {
            "mode": self.mode,
            "random_events": self.random_events,
            "speed": self.speed,
            "auto_loop": self.auto_loop,
            "respect_video_rules": self.respect_video_rules,
            "video_cut": cut,
            "video_cut_reason": cut_reason,
            "stage": self.stage,
            "stage_name": STAGE_NAMES.get(self.stage, str(self.stage)),
            "stage_countdown_s": round(self.stage_countdown_s, 1),
            "elapsed_s": round(self.elapsed_s, 1),
            "remaining_s": round(max(0.0, MATCH_DURATION_S - self.elapsed_s), 1),
            "round": f"{self.current_round}/{self.total_rounds}",
            "score": {"red": self.red_score, "blue": self.blue_score},
            "teams": {
                k: {
                    "base_hp": t.base_hp,
                    "base_status": t.base_status,
                    "outpost_hp": t.outpost_hp,
                    "control_score": round(t.control_score, 1),
                    "total_damage": t.total_damage,
                    "economy": t.economy,
                    "total_economy": t.total_economy,
                    "tech_level": t.tech_level,
                    "lambda": round(t.logistics_lambda(), 3),
                    "launcher_unlocked": t.launcher_unlocked,
                    "darts_remaining": t.darts_remaining,
                    "dart_gate_state": t.dart_gate_state,
                    "dart_blind_remaining_s": round(t.dart_blind_remaining_s, 1),
                    "sentry_posture_id": t.sentry_posture_id,
                    "sentry_control_mode": t.sentry_control_mode,
                    "rune_status": t.rune_status,
                }
                for k, t in self.teams.items()
            },
            "fields": {k: {"owner": v[0], "level": v[1]} for k, v in self.fields.items()},
            "mechanisms": {str(k): round(v, 1) for k, v in self.mechanisms.items()},
            "robots": {str(rid): r.to_dict() for rid, r in sorted(self.robots.items())},
            "events": list(self.event_history)[-20:],
            "commands": list(self.command_history)[-20:],
        }
