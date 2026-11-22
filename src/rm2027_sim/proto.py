"""Pure-Python Protobuf v3 encoder/decoder matching RM2027 V1.0.0 (`robomaster.proto`)."""

from __future__ import annotations

import struct
from typing import Iterable, Mapping


def encode_varint(value: int) -> bytes:
    """Encode an unsigned or signed 64-bit integer as Protobuf base-128 varint."""
    if value < 0:
        value &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        digit = value & 0x7F
        value >>= 7
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def decode_varint(buf: bytes, idx: int) -> tuple[int, int]:
    val = 0
    shift = 0
    while idx < len(buf):
        b = buf[idx]
        idx += 1
        val |= (b & 0x7F) << shift
        if not (b & 0x80):
            return val, idx
        shift += 7
    raise ValueError("truncated varint")


def to_signed_i32(value: int) -> int:
    value &= 0xFFFFFFFF
    if value & 0x80000000:
        return value - 0x100000000
    return value


def uint_field(number: int, value: int) -> bytes:
    return encode_varint(number << 3) + encode_varint(max(0, int(value)))


def int32_field(number: int, value: int) -> bytes:
    return encode_varint(number << 3) + encode_varint(int(value))


def bool_field(number: int, value: bool) -> bytes:
    return encode_varint(number << 3) + (b"\x01" if value else b"\x00")


def float_field(number: int, value: float) -> bytes:
    return encode_varint((number << 3) | 5) + struct.pack("<f", float(value))


def bytes_field(number: int, value: bytes) -> bytes:
    return encode_varint((number << 3) | 2) + encode_varint(len(value)) + value


def string_field(number: int, value: str) -> bytes:
    return bytes_field(number, value.encode("utf-8"))


def packed_int32_field(number: int, values: Iterable[int]) -> bytes:
    packed = b"".join(encode_varint(int(v)) for v in values)
    return bytes_field(number, packed) if packed else b""


def decode_fields(buf: bytes) -> dict[int, list[tuple[int, bytes | int]]]:
    """Decode protobuf wire fields into `{field_no: [(wire_type, value), ...]}`."""
    result: dict[int, list[tuple[int, bytes | int]]] = {}
    idx = 0
    n = len(buf)
    while idx < n:
        tag, idx = decode_varint(buf, idx)
        field_no = tag >> 3
        wire = tag & 0x07
        if wire == 0:
            val, idx = decode_varint(buf, idx)
            result.setdefault(field_no, []).append((wire, val))
        elif wire == 1:
            if idx + 8 > n:
                break
            result.setdefault(field_no, []).append((wire, buf[idx : idx + 8]))
            idx += 8
        elif wire == 2:
            ln, idx = decode_varint(buf, idx)
            if idx + ln > n:
                break
            result.setdefault(field_no, []).append((wire, buf[idx : idx + ln]))
            idx += ln
        elif wire == 5:
            if idx + 4 > n:
                break
            result.setdefault(field_no, []).append((wire, buf[idx : idx + 4]))
            idx += 4
        else:
            break
    return result


def _first_uint(fields: dict[int, list[tuple[int, bytes | int]]], number: int, default: int = 0) -> int:
    for wire, val in fields.get(number, ()):
        if wire == 0 and isinstance(val, int):
            return val
    return default


def _first_int32(fields: dict[int, list[tuple[int, bytes | int]]], number: int, default: int = 0) -> int:
    for wire, val in fields.get(number, ()):
        if wire == 0 and isinstance(val, int):
            return to_signed_i32(val)
    return default


def _first_bool(fields: dict[int, list[tuple[int, bytes | int]]], number: int, default: bool = False) -> bool:
    return bool(_first_uint(fields, number, 1 if default else 0))


def _first_float(fields: dict[int, list[tuple[int, bytes | int]]], number: int, default: float = 0.0) -> float:
    for wire, val in fields.get(number, ()):
        if wire == 5 and isinstance(val, bytes) and len(val) == 4:
            return struct.unpack("<f", val)[0]
    return default


def _first_bytes(fields: dict[int, list[tuple[int, bytes | int]]], number: int, default: bytes = b"") -> bytes:
    for wire, val in fields.get(number, ()):
        if wire == 2 and isinstance(val, bytes):
            return val
    return default


# ============================================================================
# 23 Inbound Protobuf Encoders (Server -> Client)
# ============================================================================


def encode_game_status(
    *,
    current_round: int = 1,
    total_rounds: int = 3,
    red_score: int = 0,
    blue_score: int = 0,
    current_stage: int = 4,
    stage_countdown_sec: int | None = None,
    stage_elapsed_sec: int | None = None,
    is_paused: bool = False,
    game_result: int = 0,
    end_reason: int = 0,
) -> bytes:
    out = bytearray()
    out += uint_field(1, current_round)
    out += uint_field(2, total_rounds)
    out += uint_field(3, red_score)
    out += uint_field(4, blue_score)
    out += uint_field(5, current_stage)
    if stage_countdown_sec is not None:
        out += int32_field(6, stage_countdown_sec)
    if stage_elapsed_sec is not None:
        out += int32_field(7, stage_elapsed_sec)
    out += bool_field(8, is_paused)
    out += uint_field(9, game_result)
    out += uint_field(10, end_reason)
    return bytes(out)


def encode_global_unit_status(
    *,
    base_health: int,
    base_status: int,
    base_shield: int,
    outpost_r_health: int,
    outpost_r_status: int,
    enemy_base_health: int,
    enemy_base_status: int,
    enemy_base_shield: int,
    outpost_b_health: int,
    outpost_b_status: int,
    robot_health: Iterable[int],
    robot_bullets: Iterable[int],
    total_damage_ally: int,
    total_damage_enemy: int,
    total_cp_ally: int,
    total_cp_enemy: int,
) -> bytes:
    out = bytearray()
    out += uint_field(1, base_health)
    out += uint_field(2, base_status)
    out += uint_field(3, base_shield)
    out += uint_field(4, outpost_r_health)
    out += uint_field(5, outpost_r_status)
    out += uint_field(6, enemy_base_health)
    out += uint_field(7, enemy_base_status)
    out += uint_field(8, enemy_base_shield)
    out += uint_field(9, outpost_b_health)
    out += uint_field(10, outpost_b_status)
    for hp in robot_health:
        out += uint_field(11, hp)
    for bullets in robot_bullets:
        out += int32_field(12, bullets)
    out += uint_field(13, total_damage_ally)
    out += uint_field(14, total_damage_enemy)
    out += uint_field(15, total_cp_ally)
    out += uint_field(16, total_cp_enemy)
    return bytes(out)


def encode_global_logistics_status(
    *,
    remaining_economy: int,
    total_economy_obtained: int,
    tech_level: int,
    encryption_level: int,
) -> bytes:
    return (
        uint_field(1, remaining_economy)
        + uint_field(2, total_economy_obtained)
        + uint_field(3, tech_level)
        + uint_field(4, encryption_level)
    )


def encode_global_special_mechanism(items: Iterable[tuple[int, int]]) -> bytes:
    out = bytearray()
    pairs = list(items)
    for mech_id, _ in pairs:
        out += uint_field(1, mech_id)
    for _, time_sec in pairs:
        out += int32_field(2, time_sec)
    return bytes(out)


def encode_event(event_id: int, param: str = "") -> bytes:
    return int32_field(1, event_id) + string_field(2, param)


def encode_robot_injury_stat(
    *,
    total_damage: int = 0,
    collision_damage: int = 0,
    small_projectile_damage: int = 0,
    large_projectile_damage: int = 0,
    dart_splash_damage: int = 0,
    module_offline_damage: int = 0,
    offline_damage: int = 0,
    penalty_damage: int = 0,
    server_kill_damage: int = 0,
    killer_id: int = 0,
) -> bytes:
    return (
        uint_field(1, total_damage)
        + uint_field(2, collision_damage)
        + uint_field(3, small_projectile_damage)
        + uint_field(4, large_projectile_damage)
        + uint_field(5, dart_splash_damage)
        + uint_field(6, module_offline_damage)
        + uint_field(7, offline_damage)
        + uint_field(8, penalty_damage)
        + uint_field(9, server_kill_damage)
        + uint_field(10, killer_id)
    )


def encode_robot_respawn_status(
    *,
    is_pending_respawn: bool,
    total_respawn_progress: int,
    current_respawn_progress: int,
    can_free_respawn: bool,
    gold_cost_for_respawn: int,
    can_pay_for_respawn: bool,
) -> bytes:
    return (
        bool_field(1, is_pending_respawn)
        + uint_field(2, total_respawn_progress)
        + uint_field(3, current_respawn_progress)
        + bool_field(4, can_free_respawn)
        + uint_field(5, gold_cost_for_respawn)
        + bool_field(6, can_pay_for_respawn)
    )


def encode_robot_static_status(
    *,
    connection_state: int = 1,
    field_state: int = 1,
    alive_state: int = 1,
    robot_id: int = 3,
    robot_type: int = 3,
    performance_system_shooter: int = 1,
    performance_system_chassis: int = 2,
    level: int = 1,
    max_health: int = 250,
    max_heat: int = 240,
    heat_cooldown_rate: float = 24.0,
    max_power: int = 85,
    max_buffer_energy: int = 60,
    max_chassis_energy: int = 120,
) -> bytes:
    return (
        uint_field(1, connection_state)
        + uint_field(2, field_state)
        + uint_field(3, alive_state)
        + uint_field(4, robot_id)
        + uint_field(5, robot_type)
        + uint_field(6, performance_system_shooter)
        + uint_field(7, performance_system_chassis)
        + uint_field(8, level)
        + uint_field(9, max_health)
        + uint_field(10, max_heat)
        + float_field(11, heat_cooldown_rate)
        + uint_field(12, max_power)
        + uint_field(13, max_buffer_energy)
        + uint_field(14, max_chassis_energy)
    )


def encode_robot_dynamic_status(
    *,
    current_health: int = 250,
    current_heat: float = 0.0,
    last_projectile_fire_rate: float = 24.0,
    current_buffer_energy: int = 60,
    current_experience: int = 0,
    experience_for_upgrade: int = 400,
    total_projectiles_fired: int = 0,
    remaining_ammo: int = 200,
    is_out_of_combat: bool = True,
    out_of_combat_countdown: int = 0,
    can_remote_heal: bool = True,
    can_remote_ammo: bool = True,
) -> bytes:
    return (
        uint_field(1, current_health)
        + float_field(2, current_heat)
        + float_field(3, last_projectile_fire_rate)
        + uint_field(4, 0)
        + uint_field(5, current_buffer_energy)
        + uint_field(6, current_experience)
        + uint_field(7, experience_for_upgrade)
        + uint_field(8, total_projectiles_fired)
        + uint_field(9, remaining_ammo)
        + bool_field(10, is_out_of_combat)
        + uint_field(11, out_of_combat_countdown)
        + bool_field(12, can_remote_heal)
        + bool_field(13, can_remote_ammo)
    )


MODULE_FIELD_ORDER: tuple[str, ...] = (
    "power_manager",
    "rfid",
    "light_strip",
    "small_shooter",
    "big_shooter",
    "uwb",
    "armor",
    "video_transmission",
    "capacitor",
    "main_controller",
    "modular_42mm",
)


def encode_robot_module_status(modules: Mapping[str, int]) -> bytes:
    out = bytearray()
    for idx, name in enumerate(MODULE_FIELD_ORDER, start=1):
        out += uint_field(idx, modules.get(name, 1))
    return bytes(out)


def encode_robot_position(
    *,
    x: float,
    y: float,
    z: float,
    yaw: float,
    robot_id: int,
) -> bytes:
    return (
        float_field(1, x)
        + float_field(2, y)
        + float_field(3, z)
        + float_field(4, yaw)
        + uint_field(5, robot_id)
    )


def encode_buff(
    *,
    robot_id: int,
    buff_type: int,
    buff_level: int,
    buff_max_time: int,
    buff_left_time: int,
) -> bytes:
    return (
        uint_field(1, robot_id)
        + uint_field(2, buff_type)
        + int32_field(3, buff_level)
        + uint_field(4, buff_max_time)
        + uint_field(5, buff_left_time)
    )


def encode_penalty_info(
    *,
    penalty_type: int,
    penalty_effect_sec: int,
    total_penalty_num: int,
) -> bytes:
    return (
        uint_field(1, penalty_type)
        + uint_field(2, penalty_effect_sec)
        + uint_field(3, total_penalty_num)
    )


def encode_robot_path_plan_info(
    *,
    intention: int,
    start_pos_x: int,
    start_pos_y: int,
    offset_x: Iterable[int],
    offset_y: Iterable[int],
    sender_id: int,
) -> bytes:
    return (
        uint_field(1, intention)
        + uint_field(2, start_pos_x)
        + uint_field(3, start_pos_y)
        + packed_int32_field(4, offset_x)
        + packed_int32_field(5, offset_y)
        + uint_field(6, sender_id)
    )


def encode_map_click_info(
    *,
    is_send_all: int = 1,
    robot_id: bytes = b"\x00" * 7,
    mode: int = 1,
    enemy_id: int = 0,
    ascii_code: int = 0,
    marker_type: int = 1,
    map_x: float = 0.0,
    map_y: float = 0.0,
) -> bytes:
    return (
        uint_field(1, is_send_all)
        + bytes_field(2, robot_id)
        + uint_field(3, mode)
        + uint_field(4, enemy_id)
        + uint_field(5, ascii_code)
        + uint_field(6, marker_type)
        + float_field(7, map_x)
        + float_field(8, map_y)
    )


def encode_radar_info_to_client(entries: Iterable[tuple[int, int, int]]) -> bytes:
    out = bytearray()
    for x_cm, y_cm, highlight in entries:
        single = uint_field(1, x_cm) + uint_field(2, y_cm) + uint_field(3, highlight)
        out += bytes_field(1, single)
    return bytes(out)


def build_rmh1_hit_bytes(
    *,
    armor_id: int = 1,
    reason: int = 0,
    hit_sequence: int = 1,
    x_mm: int = 0,
    y_mm: int = 0,
    damage: int = 20,
) -> bytes:
    return b"RMH1" + struct.pack(
        "<BBHhhH",
        armor_id & 0x0F,
        reason & 0x0F,
        hit_sequence & 0xFFFF,
        max(-32768, min(32767, int(x_mm))),
        max(-32768, min(32767, int(y_mm))),
        max(0, min(65535, int(damage))),
    )


def encode_custom_byte_block(data: bytes) -> bytes:
    return bytes_field(1, data)


def encode_tech_core_motion_state_sync(
    *,
    maximum_difficulty_level: int = 1,
    basic_state: int = 0,
    putin_state: int = 0,
    move_state: int = 0,
    rotate_state: int = 0,
    enemy_core_status: int = 0,
    remain_time_all: int = 0,
    remain_time_step: int = 0,
) -> bytes:
    return (
        uint_field(1, maximum_difficulty_level)
        + uint_field(2, basic_state)
        + uint_field(3, putin_state)
        + uint_field(4, move_state)
        + uint_field(5, rotate_state)
        + uint_field(6, enemy_core_status)
        + uint_field(7, remain_time_all)
        + uint_field(8, remain_time_step)
    )


def encode_robot_performance_selection_sync(
    *,
    shooter: int = 1,
    chassis: int = 2,
    sentry_control: int = 0,
) -> bytes:
    return uint_field(1, shooter) + uint_field(2, chassis) + uint_field(3, sentry_control)


def encode_rune_status_sync(
    *,
    rune_status: int = 1,
    activated_arms: int = 0,
    average_rings: float = 0.0,
) -> bytes:
    return uint_field(1, rune_status) + uint_field(2, activated_arms) + float_field(3, average_rings)


def encode_sentry_status_sync(
    *,
    posture_id: int = 3,
    is_weakened: bool = False,
    is_powered: bool = False,
) -> bytes:
    return uint_field(1, posture_id) + bool_field(2, is_weakened) + bool_field(3, is_powered)


def encode_dart_select_target_status_sync(*, target_id: int = 1, open_state: int = 0) -> bytes:
    return uint_field(1, target_id) + uint_field(2, open_state)


def encode_sentry_ctrl_result(*, command_id: int, result_code: int = 0) -> bytes:
    return uint_field(1, command_id) + uint_field(2, result_code)


# ============================================================================
# 9 Outbound Protobuf Decoders (Client -> Server)
# ============================================================================


def decode_keyboard_mouse_control(payload: bytes) -> dict[str, int | bool]:
    fields = decode_fields(payload)
    return {
        "mouse_x": _first_int32(fields, 1),
        "mouse_y": _first_int32(fields, 2),
        "mouse_z": _first_int32(fields, 3),
        "left_button_down": _first_bool(fields, 4),
        "right_button_down": _first_bool(fields, 5),
        "keyboard_value": _first_uint(fields, 6),
        "mid_button_down": _first_bool(fields, 7),
    }


def decode_custom_control(payload: bytes) -> dict[str, bytes]:
    fields = decode_fields(payload)
    return {"data": _first_bytes(fields, 1)}


def decode_map_click_cmd(payload: bytes) -> dict[str, object]:
    fields = decode_fields(payload)
    return {
        "is_send_all": _first_uint(fields, 1, 0),
        "robot_id": _first_bytes(fields, 2, b"\x00" * 7),
        "mode": _first_uint(fields, 3, 1),
        "enemy_id": _first_uint(fields, 4, 0),
        "ascii": _first_uint(fields, 5, 0),
        "type": _first_uint(fields, 6, 1),
        "map_x": _first_float(fields, 7, 0.0),
        "map_y": _first_float(fields, 8, 0.0),
    }


def decode_assembly_command(payload: bytes) -> dict[str, int]:
    fields = decode_fields(payload)
    return {
        "operation": _first_uint(fields, 1, 0),
        "difficulty": _first_uint(fields, 2, 1),
    }


def decode_performance_selection_command(payload: bytes) -> dict[str, int]:
    fields = decode_fields(payload)
    return {
        "shooter": _first_uint(fields, 1, 1),
        "chassis": _first_uint(fields, 2, 1),
        "sentry_control": _first_uint(fields, 3, 0),
    }


def decode_common_command(payload: bytes) -> dict[str, int]:
    fields = decode_fields(payload)
    return {
        "cmd_type": _first_uint(fields, 1, 0),
        "param": _first_uint(fields, 2, 0),
    }


def decode_rune_activate_command(payload: bytes) -> dict[str, int]:
    fields = decode_fields(payload)
    return {"activate": _first_uint(fields, 1, 0)}


def decode_dart_command(payload: bytes) -> dict[str, int | bool]:
    fields = decode_fields(payload)
    return {
        "target_id": _first_uint(fields, 1, 1),
        "open": _first_bool(fields, 2, False),
        "launch_confirm": _first_bool(fields, 3, False),
    }


def decode_sentry_ctrl_command(payload: bytes) -> dict[str, int]:
    fields = decode_fields(payload)
    return {"command_id": _first_uint(fields, 1, 0)}
