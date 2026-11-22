# REST API 与 RM2027 通信协议说明 (`API_AND_PROTOCOL.md`)

---

## 一、Web 控制台 REST API

默认绑定 `http://127.0.0.1:18885`（可通过 `--control-port` 修改）。所有响应均为 `application/json`。

### 1. 查询接口

| 方法与路径 | 说明 |
| :--- | :--- |
| `GET /` | 内置中文 Web 管理控制台 |
| `GET /api/state` | 获取完整快照：`mqtt` 统计、`auth` 策略、`clients` 在线客户端、`audit` 握手审计、`simulator` 比赛状态、`video` 推流状态 |
| `GET /api/diagnostics` | 执行完整环境检查，返回网卡 IP、端口监听、客户端配置比对、活跃终端检测、问题列表与推荐探测命令 |

### 2. 模式与倍速控制 (`POST /api/mode`)

可传任意部分字段进行热更新：

```json
{
  "mode": "auto",
  "random_events": "normal",
  "speed": 2.0,
  "auto_loop": true,
  "respect_video_rules": true
}
```

- `mode`: `"auto"`（常规自动比赛）| `"manual"`（纯手动控制）| `"paused"`（暂停）
- `random_events`: `"off"` | `"low"` | `"normal"` | `"high"`
- `speed`: `0.25` ~ `10.0`
- `respect_video_rules`: 设为 `true` 时，触发超热/黄牌/红牌将自动暂停发送 UDP HEVC 图传流。

### 3. 手动事件与状态触发 (`POST /api/trigger`)

请求体格式：`{"action": "<动作名>", "params": { ... }}`

| `action` | `params` 示例 | 效果与触发协议 |
| :--- | :--- | :--- |
| `start_full_match` | `{}` | 切换到 `auto` 并从阶段 1（准备阶段）开始完整流程 |
| `start_battle` | `{"elapsed_s": 0}` | 直接进入阶段 4（7 分钟比赛） |
| `set_stage` | `{"stage": 3, "countdown_s": 5}` | 跳转至阶段 `0..5` |
| `jump_time` | `{"elapsed_s": 180}` | 跳转比赛已进行秒数（自动补发对应自然金币） |
| `reset` | `{"stage": 0, "elapsed_s": 0}` | 重置比赛状态（默认回到阶段 0 待机） |
| `dart_gate_open` | `{"firing_team": "blue"}` | 敌方飞镖开闸，向对方发送 `Event(10)`（37s 预警） |
| `dart_hit` | `{"firing_team": "blue", "target_id": 3}` | 飞镖命中，扣减前哨/基地血量并广播 `Event(9, "side,target")` 触发致盲 |
| `yellow_card` | `{"robot_id": 3, "seconds": 5}` | 下发 `PenaltyInfo(type=1)` 黄牌遮挡 |
| `second_yellow_card` | `{"robot_id": 3, "seconds": 10}` | 下发 `PenaltyInfo(type=2)` 二次黄牌遮挡 |
| `red_card` | `{"robot_id": 3}` | 下发 `PenaltyInfo(type=3)` 红牌永久锁定 |
| `overheat` | `{"robot_id": 3, "excess": 45}` | 设置热量超过上限并置超热锁，若 `respect_video_rules=true` 则断图传 |
| `clear_compliance` | `{}` | 清除超热/黄牌/红牌/致盲与图传模块故障并恢复图传 |
| `armor_hit` | `{"team": "red", "count": 10, "damage": 20}` | 注入 1~20 发 `RMH1` 格式 `CustomByteBlock` 装甲命中 |
| `flip_control` | `{"kind": "sentinel", "home": "blue"}` | 翻转控制区归属并触发 `Event(5..8)` |
| `mechanism_progress` | `{"mechanism_id": 4, "seconds": 5}` | 注入 `GlobalSpecialMechanism` 进度条 |
| `damage_outpost` | `{"team": "blue", "damage": 250}` | 扣减前哨血量，归零时触发 `Event(2)` |
| `outpost_stop` | `{"team": "blue"}` | 触发对方前哨停转 `Event(12)` |
| `set_base_hp` | `{"team": "red", "hp": 5000}` | 调整基地血量并触发 `Event(11)`（同步改变后勤系数 `λ`） |
| `base_armor_open` | `{"team": "blue"}` | 展开基地护甲并发送 `Event(13)` |
| `kill_robot` | `{"victim_id": 103, "killer_id": 3}` | 击杀指定单位进入复活读条并发送 `Event(1)` |
| `respawn_finish` | `{"robot_id": 3}` | 立即完成复活并恢复满血 |
| `tech_core_step` | `{"team": "red", "level": 3}` | 完成重装科技核心升级，3 级起解锁 42mm 并触发 `Event(15)`/`Event(16)` |
| `tech_core_fail` | `{"team": "red", "reason_code": "2"}` | 触发装配失败 `Event(15)` |
| `install_42mm` | `{"team": "red"}` | 安装模块化 42mm 发射机构并触发 `Event(16)` |
| `rune_activate` | `{"team": "red", "arms": 8, "avg_rings": 9.1}` | 激活大小能量机关并触发 `Event(4)`/`Event(3)` |
| `module_fault` | `{"robot_id": 3, "module": "video_transmission", "state": 0}` | 设定 11 个模块之一的状态（`0` 离线 / `1` 在线 / `2` 违规） |
| `grant_buff` | `{"robot_id": 3, "buff_type": 2, "level": 25, "duration": 30}` | 授予机器人 Buff |
| `map_click` | `{"map_x": 14.0, "map_y": 7.5, "marker_type": 1}` | 广播地图标点 `MapClickInfo` |
| `custom_block` | `{"text": "TEST-0311"}` | 推送自定义 `CustomByteBlock` |
| `raw_publish` | `{"topic": "Event", "payload_hex": "080a1200"}` | 向所有订阅客户端广播任意原始 Protobuf 字节流 |

### 4. MQTT 登录认证与客户端管理 (`POST /api/auth`)

```json
{
  "auth_mode": "required",
  "username": "rm2027",
  "password": "secret",
  "strict_client_id": true,
  "fault_connack": 4,
  "fault_once": true,
  "kick_client_id": "3"
}
```

### 5. HEVC 模拟图传控制 (`POST /api/video`)

```json
{
  "enabled": true,
  "target_host": "127.0.0.1",
  "target_port": 3334,
  "auto_mirror_clients": true,
  "backend": "auto",
  "width": 1280,
  "height": 720,
  "fps": 30,
  "bitrate_kbps": 2500,
  "video_dir": "videos",
  "video_file": "",
  "loop_mode": "all",
  "overlay_text": "",
  "loss_percent": 0.0
}
```

也支持目录播放列表控制快捷指令：
- `{"action": "rescan"}`：立即重新扫描 `video_dir` 目录
- `{"action": "next"}` / `{"action": "prev"}`：切换到上/下一个视频文件
- `{"action": "play_file", "video_file": "match_01.mp4"}`：立即播放指定视频

### 6. 主动握手探测 (`POST /api/probe`)

```json
{
  "host": "127.0.0.1",
  "port": 3333,
  "client_id": "3",
  "username": "",
  "password": ""
}
```

---

## 二、RM2027 V1.0.0 MQTT 主题清单

### 下行主题（服务端 → 客户端，共 23 个）
`GameStatus`、`GlobalUnitStatus`、`GlobalLogisticsStatus`、`GlobalSpecialMechanism`、`Event`、`RobotInjuryStat`、`RobotRespawnStatus`、`RobotStaticStatus`、`RobotDynamicStatus`、`RobotModuleStatus`、`RobotPosition`、`Buff`、`PenaltyInfo`、`RobotPathPlanInfo`、`MapClickInfo`、`RadarInfoToClient`、`CustomByteBlock`、`TechCoreMotionStateSync`、`RobotPerformanceSelectionSync`、`RuneStatusSync`、`SentryStatusSync`、`DartSelectTargetStatusSync`、`SentryCtrlResult`。

### 上行主题（客户端 → 服务端，共 9 个）
`KeyboardMouseControl`、`CustomControl`、`MapClickCmd`、`AssemblyCommand`、`RobotPerformanceSelectionCommand`、`CommonCommand`、`RuneActivateCommand`、`DartCommand`、`SentryCtrlCommand`。

---

## 三、UDP `:3334` 图传包与 `RMH1` 格式

### 1. HEVC UDP 分片包结构
- 每包最大 `1400` 字节 = `8` 字节大端头 + 最大 `1392` 字节 Annex-B H.265 负载：
  - `bytes[0..2]`：`frame_number`（`uint16` 大端递增帧号）
  - `bytes[2..4]`：`fragment_index`（`uint16` 大端，当前帧分片序号，从 `0` 开始）
  - `bytes[4..8]`：`total_size`（`uint32` 大端，当前完整帧总字节数）

### 2. 自适应装甲 `CustomByteBlock` (`RMH1`) 结构
共 14 字节小端结构：
- `bytes[0..4]`：固定魔数 `b"RMH1"`
- `bytes[4]`：`armor_id`（低 4 位）
- `bytes[5]`：`reason`（低 4 位）
- `bytes[6..8]`：`hit_sequence`（`uint16` LE）
- `bytes[8..10]`：`x_mm`（`int16` LE）
- `bytes[10..12]`：`y_mm`（`int16` LE）
- `bytes[12..14]`：`damage`（`uint16` LE）
