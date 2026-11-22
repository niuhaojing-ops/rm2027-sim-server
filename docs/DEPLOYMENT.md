# 部署指南 (`DEPLOYMENT.md`)

本文档详细介绍 **RM2027 模拟赛事服务器 (`rm2027-sim-server`)** 在不同环境下的部署方式，包括单机免安装、虚拟环境安装、视频目录准备、配置文件持久化、`systemd` 后台服务、Docker 容器部署、跨机局域网联调以及官方 `192.168.12.1:3333` 赛场网段仿真。

---

## 1. 环境准备

### 1.1 基础要求
- **操作系统**：Linux（Ubuntu 22.04/24.04/26.04、Debian 12+、Arch）或 macOS（除 `ip`/`ss` 系统级诊断外核心能力通用）。
- **Python**：`Python >= 3.10`（无任何第三方 `pip` 依赖）。

### 1.2 视频编码器与媒体文件（用于目录视频 HEVC 图传）
模拟服务器默认从 `./videos` 目录读取视频文件并实时推流到 `UDP :3334`：
- 若目录中放入 `.mp4`、`.mkv`、`.mov`、`.avi`、`.ts` 等常见封装格式视频，建议安装 **GStreamer + x265** 或 **FFmpeg (`libx265`)**：
  - **Ubuntu / Debian**:
    ```bash
    sudo apt-get update
    sudo apt-get install -y gstreamer1.0-tools gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly
    # 验证 x265enc 是否可用：
    gst-inspect-1.0 x265enc
    ```
  - **或安装 FFmpeg (`libx265`) 作为替代**:
    ```bash
    sudo apt-get install -y ffmpeg
    ffmpeg -encoders | grep libx265
    ```
  - **Arch Linux**:
    ```bash
    sudo pacman -S gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly x265
    ```
- 若机器未安装外部编码器，目录中也可直接放入 `.hevc` / `.h265` Annex-B 裸流文件直读推流（或在联调无视频源时传 `--video-backend synthetic`）。

---

## 2. 部署方式

### 方式一：源码免安装部署（推荐开发机使用）

```bash
git clone <your-repo-url> rm2027-sim-server
cd rm2027-sim-server

# 放入测试视频到 videos/ 目录（可选，支持启动后热放入，1.5s 内自动开播）
cp /path/to/demo.mp4 ./videos/

# 直接运行（默认 MQTT 127.0.0.1:3333、Web 控制台 :18885、UDP 图传 :3334）
python3 run.py

# 或使用封装脚本启动并开启账号密码认证
./scripts/start.sh --username rm2027 --password secret
```

### 方式二：Python 虚拟环境安装 `rm2027-sim` 命令

```bash
cd rm2027-sim-server
python3 -m venv .venv
source .venv/bin/activate
pip install .

# 验证安装
rm2027-sim --version
rm2027-sim --diagnose
```

> 若在离线/内网环境中且虚拟环境不含 `setuptools`，可先创建继承系统包的虚拟环境 `python3 -m venv --system-site-packages .venv`，再执行 `pip install --no-build-isolation .`。

### 方式三：配置文件部署（`config.json`）

复制示例配置并按需编辑：

```bash
cp config.example.json config.json
```

示例关键字段说明（[config.example.json](../config.example.json)）：
- `host` / `port` / `control_port`：默认 `127.0.0.1` / `3333` / `18885`（设为 `-1` 可禁用 Web 控制台）。
- `auth.mode`：`open`（允许匿名）或 `required`（强制账号密码）。
- `auth.users`：账号密码字典，如 `{"red_team": "pass1", "blue_team": "pass2"}`。
- `auth.strict_client_id`：是否限制只允许 RM2027 官方机器人编号接入。
- `match.mode` / `match.random_events` / `match.speed` / `match.stage`：默认 `stage=0`（待机未开始），如需启动即开赛可设为 `4`。
- `video.video_dir` / `video.loop_mode` / `video.video_file`：配置待轮播的视频目录（默认 `videos`）、轮播模式（`all` 轮播目录全部视频 / `single` 单文件循环）以及目标 IP/端口/分辨率/码率。

启动时指定配置文件（**命令行显式参数优先级高于配置文件**）：

```bash
# 完全按 config.json 启动
python3 run.py --config config.json

# 复用 config.json，但临时允许跨机访问并启动即进赛程
python3 run.py --config config.json --host 0.0.0.0 --stage 1 --speed 2.0
```

### 方式四：Linux `systemd` 长期后台守护部署

适合部署在战队实验室工控机或内网测试服务器上开机自启：

```bash
# 1. 复制工程到 /opt/rm2027-sim-server
sudo mkdir -p /opt/rm2027-sim-server
sudo cp -r . /opt/rm2027-sim-server/
sudo cp /opt/rm2027-sim-server/config.example.json /opt/rm2027-sim-server/config.json

# 2. 按需把视频放入 /opt/rm2027-sim-server/videos/ 并安装服务文件
sudo cp /opt/rm2027-sim-server/deploy/rm2027-sim.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rm2027-sim.service

# 3. 查看运行状态与日志
sudo systemctl status rm2027-sim.service
sudo journalctl -u rm2027-sim.service -f
```

### 方式五：Docker / Docker Compose 部署

1. **Docker Compose（默认映射 `3333` / `18885` 并只读挂载 `./videos`）**：
   ```bash
   docker compose up --build -d
   docker compose logs -f
   ```
   > **提示**：如果需要模拟服务器自动探测宿主机网卡、或跨机直推 UDP `:3334` 图传，建议取消 `docker-compose.yml` 中的 `network_mode: host` 注释并删除 `ports` 映射。

2. **纯 `docker run`（推荐 Linux 下使用 `--network host`）**：
   ```bash
   docker build -t rm2027-sim-server:latest .
   docker run -d --name rm2027-sim --network host \
     -v "$(pwd)/videos:/app/videos:ro" \
     rm2027-sim-server:latest \
     --host 0.0.0.0 --port 3333 --video-dir /app/videos --username rm2027 --password secret
   ```

---

## 3. 网络、跨机联调与赛场网段仿真

### 3.1 跨机局域网联调
假设：
- 模拟服务器运行在 **机器 A（`192.168.1.50`）**
- 自定义终端运行在 **机器 B（`192.168.1.88`）**

1. 机器 A 启动模拟服务器（注意跨机访问需显式加 `--host 0.0.0.0`）：
   ```bash
   python3 run.py --host 0.0.0.0 --port 3333 --video-host 192.168.1.88 --video-port 3334
   ```
   > 默认开启 `auto_mirror_clients=true`，当机器 B 连上 MQTT 后，模拟服务器会自动向机器 B 的来源 IP 推送 `UDP :3334` 图传流。
2. 机器 A 放行防火墙端口：
   ```bash
   # UFW (Ubuntu)
   sudo ufw allow 3333/tcp
   sudo ufw allow 18885/tcp

   # 或 firewalld (RHEL/CentOS/Fedora)
   sudo firewall-cmd --add-port=3333/tcp --add-port=18885/tcp --permanent
   sudo firewall-cmd --reload
   ```
3. 在机器 B 上执行主动探测验证链路：
   ```bash
   python3 run.py --probe 192.168.1.50:3333
   ```

### 3.2 仿真官方选手端网段 (`192.168.12.1:3333`)

模拟服务器默认已使用 `3333` 端口；若还希望在不修改任何客户端默认主机 IP（默认连 `192.168.12.1:3333`）的情况下测试，可在 Linux 测试机上为回环接口添加赛场虚拟地址：

```bash
# 添加官方选手端 IP（仅当前开机会话有效，重启自动恢复）
sudo ip addr add 192.168.12.1/24 dev lo

# 以官方地址 192.168.12.1:3333 启动模拟服务器
python3 run.py --host 192.168.12.1 --port 3333 --video-host 127.0.0.1 --video-port 3334
```

如使用两台主机网线直连，则将服务端网卡配置为 `192.168.12.1/24`，客户端网卡配置为 `192.168.12.2/24`，再以 `--host 192.168.12.1 --port 3333 --video-host 192.168.12.2` 启动即可。
