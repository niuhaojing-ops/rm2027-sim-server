# RM2027 模拟赛事服务器 (`rm2027-sim-server`)

面向 **RoboMaster 2027 高校系列赛通信协议 V1.0.0（2026-10-29）** 与比赛规则构建的**独立、零第三方 Python 依赖**模拟赛事服务器。集成了 **MQTT 3.1.1 并发认证 Broker（默认 `127.0.0.1:3333`，与正式赛场端口一致）**、**待机控场与双模式赛程引擎（自动推进 / 规则随机 / 手动触发）**、**基于本地视频目录的 UDP `:3334` HEVC 实时图传流**、**半透明磨砂玻璃 Web 控制中心** 以及 **连接排错与主动探针工具**。

> 可直接脱离官方选手端独立运行，适用于自定义客户端联调、操作手赛前训练、规则合规遮挡测试与自动化回归。

---

## 核心特性

- **与正式比赛一致的 MQTT 接入端口 + 完整登录控制**
  - 默认监听 **`127.0.0.1:3333`**（与正式比赛选手端端口 `3333` 一致；需要跨机联调时可传 `--host 0.0.0.0`）。
  - 原生 `asyncio` 并发服务器，支持 `CONNECT` / `CONNACK` / `PUBLISH`（QoS 0/1/2）/ `SUBSCRIBE`（含 `+` / `#` 通配符）/ `UNSUBSCRIBE` / `PINGREQ` / `DISCONNECT`。
  - 支持 **开放匿名模式**、**单账号/多账号密码鉴权**（`--username`/`--password` 或 `--user USER:PASS`）、**严格 Robot ID 校验**（红方 `1, 3..9`、蓝方 `101, 103..109`）、黑名单拒绝以及实时 **`CONNACK` 故障注入**（`0x01`~`0x05`）。
  - 同 `Client ID` 重连自动顶替旧会话；按红蓝双方客户端视角自动转换 `GlobalUnitStatus`、`RadarInfoToClient`、`Event` 等 23 个下行主题与 9 个上行指令。
- **待机启动 & 双模式模拟赛程（自动 + 规则随机 + 手动触发）**
  - **默认待机启动（`--stage 0`，未开始）**：服务启动后先停留在待机未开始状态（时间冻结、机器人满血就绪），打开或不打开比赛均可先接收目录视频图传流；随时可在 Web 控制中心点击 **「开始完整赛程」** 或 **「直接开始 7 分钟」** 开赛。
  - **常规自动比赛（`--mode auto`）**：支持从 `Preparation(1) -> SelfCheck(2) -> Countdown(3) -> Battle(4, 420s) -> Settlement(5)` 走完完整赛程，按 2027 规则推进自然金币（`800 / +100×5 / +300`）、四区占领控制分（`+1/s`、`+3/s`）、基地后勤系数 `λ`、重装科技核心 1~4 级与 42mm 时间解锁、大小能量机关（`3:00` 切换）、8 车巡逻与交火，并真实处理客户端上行的全部 **9 个控制主题**。
  - **规则随机事件（`--random-events off|low|normal|high`）**：在比赛合法时间窗口内自动随机触发飞镖开闸/命中致盲、控制区争夺与 `GlobalSpecialMechanism` 进度条、击杀复活、能量机关激活、`RMH1` 自适应装甲命中、黄牌/超热断图传、模块掉线等事件。
  - **手动精确干预（`--mode manual` 或随时调用 Web / API）**：随时调节 `0.5×~10×` 倍速、跳转比赛阶段或秒数、手动触发飞镖致盲/黄牌/二次黄牌/红牌/超热/RMH1 命中/模块故障/基地掉血等任意场景。
- **基于目录的 HEVC 图传推流（`UDP :3334`，启动即推流）**
  - **直接从指定目录读取视频文件**（默认 `./videos`，支持 `--video-dir` 修改或 Web 控制台切换目录），放入 `.mp4`、`.mkv`、`.mov`、`.avi`、`.ts`、`.webm`、`.flv`、`.m4v` 或 `.hevc`/`.h265` 裸流文件后 **1.5 秒内自动识别并循环轮播**，无需等待开始比赛。
  - 严格按照官方协议封装 8 字节大端头（`frame_number:u16 + fragment_index:u16 + total_size:u32`）+ 最大 `1392B` 分片。
  - 编码优先级：`gst-launch-1.0`（`uridecodebin` + `x265enc`） → `ffmpeg`（`libx265`） → `.hevc`/`.h265` 裸流直读；另可选 `--video-backend synthetic` 发送合成 Annex-B 测试帧。
  - 支持目录轮播 / 单文件循环、上一/下一视频、Web 控制台点选指定文件、UDP 丢包率模拟，以及超热/黄牌/红牌规则联动自动断流。
- **内置连接排查与诊断体系**
  - 自动审计每次 TCP/MQTT 握手（精准捕获端口未监听、HTTP 误连 MQTT 端口、协议版本不兼容、Client ID 拒、账密错误、保活超时、同 ID 顶号等并给出中文修复建议）。
  - 提供 `--diagnose` 本机环境/端口/配置排查、CLI `--probe HOST:PORT` 主动握手探针，以及 Web 控制中心 **「连接诊断」** 专页。

---

## 架构与默认端口

```
+------------------------------------------------------------------------+
|                      rm2027-sim-server (单进程引擎)                      |
|                                                                        |
|  [MatchSimulator]  <-- 9 上行指令 / 23 下行 Protobuf -->  [MQTT] 127.0.0.1:3333 (TCP)
|         |                                                              |
|         +--> 规则断流联动 (超热 / 黄牌 / 红牌)                             |
|         v                                                              |
|  [HevcVideoStreamer] <--- 读取 ./videos 目录 ---> UDP 127.0.0.1:3334   |
|                                                                        |
|  [SimWebServer] http://127.0.0.1:18885/ (磨砂玻璃 Web 控制中心 + API)    |
+------------------------------------------------------------------------+
```

| 服务 | 协议 / 默认地址 | 用途 |
| :--- | :--- | :--- |
| MQTT 赛事引擎 | `TCP 127.0.0.1:3333`（与正式赛场端口一致） | 收发 23 个下行 + 9 个上行 Protobuf 主题、登录认证 |
| Web 控制中心与 API | `HTTP http://127.0.0.1:18885/`（`--control-port`） | 开赛控场、目录播放列表管理、登录策略、连接诊断 |
| HEVC 图传推流 | `UDP -> 127.0.0.1:3334`（默认读取 `./videos` 目录） | 推送标准 Annex-B H.265 分片流（启动即推，不依赖开赛） |

---

## 5 分钟快速开始

### 环境要求
- **Python >= 3.10**（零第三方 Python 包依赖，仅需标准库）
- **图传目录视频转码推荐**：`gstreamer1.0-tools` + `x265` 插件（`gst-launch-1.0`）或 `ffmpeg`（`libx265`）；未安装时目录仍支持直接读取 `.hevc`/`.h265` 裸流文件（或用 `--video-backend synthetic` 调试）。

### 方式 A：源码免安装直接运行

```bash
cd rm2027-sim-server

# 1. 将待推流的视频文件（.mp4 / .mkv / .mov / .hevc 等）放入 videos/ 目录
cp /path/to/your_match.mp4 ./videos/

# 2. 启动模拟服务器（默认 127.0.0.1:3333，待机未开赛，自动循环推送 videos/ 内视频）
python3 run.py

# 3. 如需强制账号密码登录验证：
python3 run.py --username rm2027 --password secret

# 4. 如需指定其他视频目录或启动即开赛：
python3 run.py --video-dir /path/to/videos --stage 4
```

### 方式 B：通过 `pip` 安装 `rm2027-sim` 命令

```bash
cd rm2027-sim-server
python3 -m venv .venv
source .venv/bin/activate
pip install .

# 安装后即可直接执行：
rm2027-sim --video-dir ./videos --username rm2027 --password secret
```

启动后：
- 若已在 `./videos/` 中放入视频，服务端**无需开始比赛即可直接向 `127.0.0.1:3334` 推送 HEVC 图传流**。
- 打开浏览器访问 Web 控制中心：`http://127.0.0.1:18885/`，点击 **「开始完整赛程」** 或 **「直接开始 7 分钟」** 即可开始比赛进程。
- 让你的自定义终端连接 MQTT `127.0.0.1:3333`（`Client ID` 设为机器人编号，如 `3`、`1`、`7`、`103`），并监听本地 `UDP :3334` 接收图传流。

---

## 核心功能使用指南

### 1. 比赛模式控制（待机 / 自动 / 规则随机 / 手动）

| 场景 | 启动参数 | 说明 |
| :--- | :--- | :--- |
| 默认待机启动（推荐） | `python3 run.py` | 停留在 `stage=0 (未开始)`，图传目录正常工作，随时在控制台点击开赛 |
| 启动即从准备阶段走完完整流程 | `--stage 1 --mode auto` | 依次经历 准备 → 自检 → 5s 倒计时 → 7 分钟比赛 → 结算（可通过 `--speed 5` 加速） |
| 启动即进入 7 分钟比赛中 | `--stage 4 --random-events normal` | 立即推进 7 分钟赛程并在合法时间窗口触发各类规则随机事件 |
| 纯手动控场（无随机干扰） | `--stage 4 --mode manual --random-events off` | 时钟与状态仅按基础规则/手动指令变化，适合精确单点测试 |

在运行期间，也可直接访问 Web 控制中心 **「赛程总览」**，或通过 API 触发开赛/规则事件：

```bash
# 直接开启 7 分钟模拟比赛
curl -s -X POST http://127.0.0.1:18885/api/trigger \
  -H 'Content-Type: application/json' \
  -d '{"action":"start_battle"}'

# 手动触发 5 秒黄牌遮挡（对红 3）
curl -s -X POST http://127.0.0.1:18885/api/trigger \
  -H 'Content-Type: application/json' \
  -d '{"action":"yellow_card","params":{"robot_id":3,"seconds":5}}'
```

### 2. 图传媒体目录推流（`./videos`）

- 将 `.mp4`、`.mkv`、`.mov`、`.avi`、`.ts`、`.hevc` 等文件放入 `./videos/`（或 `--video-dir /your/folder`），服务端无需重启，约 **1.5 秒内自动识别** 并按文件名顺序循环推流到 `UDP :3334`。
- 如果目录暂时为空，状态栏会显示 `waiting_files` 并持续监听目录，放入视频后自动开播。
- 在 Web 控制中心 **「图传媒体库」** 页签中，你可以随时：
  - 修改扫描目录路径或点击 **「刷新列表」**；
  - 切换 **「目录轮播」 / 「单文件循环」**、切歌（上一视频 / 下一视频）或点击列表任意视频立即切换；
  - 调整目标 IP/端口、分辨率、帧率、码率、丢包率模拟，或用 API 热切换目录：
    ```bash
    curl -s -X POST http://127.0.0.1:18885/api/video \
      -H 'Content-Type: application/json' \
      -d '{"video_dir":"/path/to/videos","loop_mode":"all"}'
    ```

### 3. MQTT 登录认证与故障模拟

- **开放模式（默认）**：不传用户名密码，任意合法或测试 `Client ID` 均可登录 `127.0.0.1:3333`。
- **单账号强制认证**：`python3 run.py --username rm2027 --password secret`
- **多账号 + 严格机器人 ID 校验**：
  ```bash
  python3 run.py --user red:redpass --user blue:bluepass --strict-client-id
  ```
- **动态修改策略与故障注入**：在 Web 控制中心 **「登录与客户端」** 页签可随时切换认证策略、踢出已连客户端，或注入一次性的 `CONNACK 0x01~0x05` 拒绝码用于验证客户端报错提示。

### 4. 一键排查“为什么连不上模拟服务器”

```bash
# 检查本机网卡 IP、相关端口监听状态、客户端配置差异与本地活跃终端状态
python3 run.py --diagnose

# 如果需要对照特定客户端配置文件排查：
python3 run.py --diagnose --terminal-config /path/to/terminal.json

# 从客户端侧主动向服务器执行 TCP -> MQTT CONNECT/CONNACK -> SUBSCRIBE 握手探测：
python3 run.py --probe 127.0.0.1:3333 --probe-id 3 --username rm2027 --password secret
```

也可以在 Web 控制中心 **「连接诊断」** 页签查看 **近期握手失败审计日志**（例如是否有人误将浏览器打开了 `3333` 导致 HTTP 误连、密码错误、地址写成 `192.168.12.1` 等）。

---

## 常见部署方式速览

| 部署方式 | 适用场景 | 命令 / 文件 |
| :--- | :--- | :--- |
| 源码免安装 | 本地开发调试、快速启停 | `python3 run.py` 或 `./scripts/start.sh` |
| Python 虚拟环境安装 | 长期固定机器使用 `rm2027-sim` 全局命令 | `pip install .` |
| Linux `systemd` 服务 | 工控机/服务器开机自启后台守护 | [deploy/rm2027-sim.service](deploy/rm2027-sim.service) |
| Docker / Compose | 隔离环境一键启动（自动挂载 `./videos`） | `docker compose up --build`（见 [Dockerfile](Dockerfile)） |

详细部署步骤（含跨机局域网、`192.168.12.1:3333` 官方网段仿真、防火墙放行）请查阅 **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)**。

---

## 常用 CLI 参数一览

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `-c, --config FILE` | 无 | 加载 JSON 配置文件（见 [config.example.json](config.example.json)） |
| `--host HOST` | `127.0.0.1` | MQTT 与 Web 控制台监听地址（跨机联调请传 `0.0.0.0`） |
| `--port PORT` | `3333` | MQTT Broker 监听端口（与正式比赛端口 `3333` 一致） |
| `--control-port PORT` | `18885` | Web 控制台 HTTP 端口（`-1` 关闭） |
| `--username` / `--password` | 无 | MQTT 认证用户名与密码（需成对提供） |
| `--user USER:PASS` | 无 | 追加账号（可重复多次） |
| `--require-auth` | `false` | 强制开启 MQTT 账号密码校验 |
| `--strict-client-id` | `false` | 仅允许合法 RM2027 机器人编号作为 `Client ID` |
| `--mode` | `auto` | `auto`（自动）/ `manual`（手动）/ `paused`（暂停） |
| `--random-events` | `normal` | `off` / `low` / `normal` / `high` |
| `--speed` | `1.0` | 时间倍速（`0.25 ~ 10.0`） |
| `--stage` | `0` | 初始阶段 `0..5`（`0=未开始, 1=准备, 2=自检, 3=倒计时, 4=比赛中, 5=结算`） |
| `--no-video` | `false` | 关闭 UDP 图传推流 |
| `--video-dir DIR` | `videos` | 图传视频目录（自动扫描并循环推流目录内所有支持视频） |
| `--video-file FILE` | `""` | 指定某个视频文件名或绝对路径（留空则轮播整个目录） |
| `--video-loop MODE` | `all` | `all`（目录轮播） / `single`（单文件循环） |
| `--video-host` / `--video-port` | `127.0.0.1` / `3334` | UDP 图传推流目标地址与端口 |
| `--video-backend` | `auto` | `auto` / `gstreamer` / `ffmpeg` / `file` / `synthetic` |
| `--video-size` / `--video-fps` | `1280x720` / `30` | 图传分辨率与帧率 |
| `--video-loss` | `0.0` | 模拟 UDP 分片丢包百分比 |
| `--diagnose` | `false` | 执行本地连接排查诊断并退出 |
| `--probe HOST[:PORT]` | 无 | 对指定 MQTT 服务执行主动登录握手探测并退出 |
| `--terminal-config FILE` | 无 | 诊断时额外对照指定 `terminal.json` 配置 |
| `--version` | — | 打印版本号并退出 |

---

## 文档导航

- **[部署指南 (`docs/DEPLOYMENT.md`)](docs/DEPLOYMENT.md)**：免安装、`pip` 安装、`config.json`、`systemd` 守护、Docker Compose、官方 `192.168.12.1:3333` 仿真及防火墙配置。
- **[接口与协议手册 (`docs/API_AND_PROTOCOL.md`)](docs/API_AND_PROTOCOL.md)**：Web REST API 全量说明、手动触发 `action` 参数表、23 下行 + 9 上行 Protobuf 主题、`RMH1` 与 UDP `:3334` 封包规范。
- **[连接排查手册 (`docs/TROUBLESHOOTING.md`)](docs/TROUBLESHOOTING.md)**：10 类“连不上模拟服务器 / 图传无画面 / 认证被拒”故障的诊断定位方法。

---

## 目录结构

```text
rm2027-sim-server/
├── README.md                  # 项目开源总览
├── LICENSE                    # MIT 开源协议
├── pyproject.toml             # Python 包构建配置（PEP 621，零外部依赖）
├── run.py                     # 免安装一键启动入口
├── config.example.json        # 完整配置示例
├── videos/                    # 图传媒体视频目录（放入 .mp4/.mkv/.hevc 即自动推流）
├── Dockerfile / docker-compose.yml
├── deploy/rm2027-sim.service  # systemd 后台服务模板
├── scripts/
│   ├── start.sh               # 一键启动脚本
│   └── diagnose.sh            # 一键排查脚本
├── src/rm2027_sim/            # 核心源码（MQTT Broker / Match Engine / Proto / Video / Diagnostics / Web UI）
├── tests/test_sim.py          # 单元与集成测试套件
└── docs/                      # 部署、API 与故障排查专题文档
```

## 运行测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 开源协议

本项目基于 **[MIT License](LICENSE)** 开源发布。欢迎提交 Issue 与 Pull Request。
