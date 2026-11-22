"""RM2027 simulation server package (MQTT 3.1.1 broker, match engine, HEVC UDP stream, diagnostics & web UI)."""

from __future__ import annotations

__version__ = "2027.1.0"

INBOUND_TOPICS: tuple[str, ...] = (
    "GameStatus",
    "GlobalUnitStatus",
    "GlobalLogisticsStatus",
    "GlobalSpecialMechanism",
    "Event",
    "RobotInjuryStat",
    "RobotRespawnStatus",
    "RobotStaticStatus",
    "RobotDynamicStatus",
    "RobotModuleStatus",
    "RobotPosition",
    "Buff",
    "PenaltyInfo",
    "RobotPathPlanInfo",
    "MapClickInfo",
    "RadarInfoToClient",
    "CustomByteBlock",
    "TechCoreMotionStateSync",
    "RobotPerformanceSelectionSync",
    "RuneStatusSync",
    "SentryStatusSync",
    "DartSelectTargetStatusSync",
    "SentryCtrlResult",
)

OUTBOUND_TOPICS: tuple[str, ...] = (
    "KeyboardMouseControl",
    "CustomControl",
    "MapClickCmd",
    "AssemblyCommand",
    "RobotPerformanceSelectionCommand",
    "CommonCommand",
    "RuneActivateCommand",
    "DartCommand",
    "SentryCtrlCommand",
)

VALID_ROBOT_IDS: frozenset[int] = frozenset(
    {1, 3, 4, 5, 6, 7, 8, 9, 101, 103, 104, 105, 106, 107, 108, 109}
)


def team_for_robot_id(robot_id: int) -> str:
    return "blue" if robot_id >= 100 else "red"


def robot_type_for_id(robot_id: int) -> int:
    suffix = robot_id % 100
    if suffix in (1, 2):
        return 1  # Hero / Heavy
    if suffix in (3, 4, 5):
        return 3  # Infantry
    if suffix == 6:
        return 4  # Aerial
    if suffix == 7:
        return 5  # Sentry
    if suffix == 8:
        return 6  # Dart
    if suffix == 9:
        return 7  # Radar
    return 3
