# P2P 通信库

基于 **QUIC + KCP + Cloudflare TURN** 的点对点通信工具,支持 NAT 穿透、低延迟传输,并内置游戏隧道功能(可用于 Minecraft 等游戏联机)。

## 特性

- **多协议传输**:QUIC(基于 aioquic)、KCP(纯 Python 实现)、AUTO(优先 KCP)
- **NAT 穿透**:完整 ICE 流程(Host → SRFLX → Relay),Cloudflare TURN 作为中继兜底
- **WebRTC DataChannel**:基于 aiortc,支持 TURN 中继,跨网络可靠连通
- **WebSocket 信令**:内置信令服务器与客户端,支持房间管理、自动重连(指数退避)
- **游戏隧道**:通过 P2P 连接转发 TCP 流量,实现 Minecraft 等游戏联机
- **纯 Python 实现**:KCP 协议无需 C 扩展,跨平台运行

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                        P2P 节点 (P2PNode)                    │
│                                                             │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────────┐  │
│  │ SignalingCli │←→ │  IceManager  │←→ │ KCP/QUIC 传输层 │  │
│  │  (WebSocket) │   │ (aiortc DC)  │   │                │  │
│  └──────────────┘   └──────┬───────┘   └────────────────┘  │
│                            │                               │
│                            ▼                               │
│                   Cloudflare TURN                          │
│                 (turn.cloudflare.com:3478)                 │
└─────────────────────────────────────────────────────────────┘
```

**数据流向(游戏隧道)**:

```
MC客户端 → localhost:25565 (CLIENT本地TCP) → P2P DataChannel → HOST → localhost:25565 (MC服务端)
```

## 环境要求

- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) 包管理器(推荐)

## 安装

```bash
# 克隆项目后,使用 uv 安装依赖
uv sync
```

依赖包:

| 依赖 | 用途 |
|------|------|
| `aioquic` | QUIC 协议实现 |
| `aiortc` | WebRTC / ICE / DataChannel |
| `websockets` | 信令服务器 WebSocket |
| `cryptography` | DTLS 加密 |
| `python-socketio` | 信令备选 |
| `pydantic` | 数据校验 |
| `loguru` | 日志 |

## 快速开始

提供两个 CLI 命令:`p2p`(节点)和 `p2p-signaling`(信令服务器)。

### 1. 启动信令服务器

```bash
uv run p2p-signaling --port 8765
```

### 2. P2P 通信测试

两个终端分别运行:

```bash
# 终端 A (发起方)
uv run p2p --mode initiator --room test123 --transport auto

# 终端 B (响应方)
uv run p2p --mode responder --room test123 --transport auto
```

### 3. Minecraft 联机

详见下方 [游戏联机教程](#游戏联机教程)。

## 游戏联机教程

以 **Minecraft Java Edition** 为例,通过 P2P 隧道转发 TCP 25565 端口流量。

### 角色说明

| 角色 | CLI 参数 | 含义 | 本地行为 |
|------|----------|------|----------|
| **HOST** | `--role host` | 运行 MC 服务端的人(RESPONDER) | 接收 P2P 数据,转发到本地 MC 服务端 |
| **CLIENT** | `--role client` | 运行 MC 客户端的人(INITIATOR) | 本地起 TCP 监听,MC 客户端连这里 |

### 操作步骤

**前提**:HOST 玩家先启动 Minecraft 服务端(默认监听 `127.0.0.1:25565`)。

**终端 1 — 信令服务器**(可在任意机器运行,或部署到公网):

```bash
uv run p2p-signaling --port 8765
```

**终端 2 — HOST 端**(运行 MC 服务端的人):

```bash
uv run p2p --mode game --game mc-java --role host \
  --room mc-room-001 \
  --signaling ws://<信令服务器IP>:8765
```

**终端 3 — CLIENT 端**(运行 MC 客户端的人):

```bash
uv run p2p --mode game --game mc-java --role client \
  --room mc-room-001 \
  --signaling ws://<信令服务器IP>:8765
```

**最后**:打开 Minecraft Java 版 → 多人游戏 → 直接连接 → 输入 `127.0.0.1` → 进入服务器。

### 端口冲突处理

如果 CLIENT 端本机 25565 被占用(例如本机也跑了 MC 服务端),改用其他端口:

```bash
uv run p2p --mode game --game mc-java --role client \
  --room mc-room-001 --local-port 35565
```

然后 MC 客户端连接 `127.0.0.1:35565`。

如果 HOST 端 MC 服务端端口非默认(在 `server.properties` 中改过),用 `--remote-port` 指定:

```bash
uv run p2p --mode game --game mc-java --role host \
  --room mc-room-001 --remote-port 25566
```

### 支持的游戏预设

| 预设 | 游戏 | 协议 | 默认端口 |
|------|------|------|----------|
| `mc-java` | Minecraft Java Edition | TCP | 25565 |
| `mc-bedrock` | Minecraft Bedrock Edition | UDP | 19132 |
| `terraria` | 泰拉瑞亚 | TCP | 7777 |
| `dont-starve` | 饥荒联机版 | UDP | 10999 |
| `custom` | 自定义 | TCP | 25565 |

> **注意**:`mc-bedrock` 和 `dont-starve` 为 UDP 协议,当前 `GameTunnel` 仅实现了 TCP 转发,UDP 游戏需补充 UDP 转发逻辑后才能使用。

## CLI 参数

### `p2p` 命令

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--mode` | 运行模式:`initiator` / `responder` / `benchmark` / `game` | `initiator` |
| `--transport` | 传输协议:`quic` / `kcp` / `auto` | `auto` |
| `--signaling` | 信令服务器地址 | `ws://localhost:8765` |
| `--room` | 房间 ID | `default-room` |
| `--role` | 角色:`initiator` / `responder`(通信)或 `host` / `client`(游戏) | - |
| `--game` | 游戏预设(仅 game 模式) | `mc-java` |
| `--local-port` | 本地监听端口(仅 client 端) | 游戏默认端口 |
| `--remote-port` | 远端转发端口(仅 host 端) | 游戏默认端口 |
| `--log-level` | 日志级别:`DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |

### `p2p-signaling` 命令

```bash
uv run p2p-signaling --port 8765 [--host 0.0.0.0]
```

## 编程式使用

除 CLI 外,也可作为 Python 库使用:

```python
import asyncio
from p2p import P2PConfig, P2PNode, TransportProtocol, ConnectionRole, IceConfig, MessageType

async def main():
    config = P2PConfig(
        transport=TransportProtocol.AUTO,
        role=ConnectionRole.INITIATOR,
        ice=IceConfig.with_cloudflare_turn(),
    )
    config.signaling.server_url = "ws://localhost:8765"

    node = P2PNode(
        config=config,
        on_message=lambda msg: print(f"收到: {msg.payload}"),
        on_peer_connected=lambda peer: print(f"已连接: {peer.peer_id}"),
    )

    await node.initialize()
    await node.connect_to_signaling()
    await node.join_room("my-room", ConnectionRole.INITIATOR)

    # 等待对端加入后发送消息
    # await node.send_to_peer(peer_id, MessageType.DATA_JSON, {"hello": "world"})

asyncio.run(main())
```

游戏隧道编程式使用:

```python
import asyncio
from p2p import P2PConfig, TransportProtocol, ConnectionRole, IceConfig
from p2p.game_tunnel import GameTunnel, GAME_PRESETS

async def main():
    tunnel_config = GAME_PRESETS["mc-java"]
    p2p_config = P2PConfig(
        transport=TransportProtocol.AUTO,
        role=ConnectionRole.INITIATOR,  # client 端
        ice=IceConfig.with_cloudflare_turn(),
    )
    p2p_config.signaling.server_url = "ws://localhost:8765"

    tunnel = GameTunnel(p2p_config, tunnel_config, ConnectionRole.INITIATOR)
    await tunnel.start("ws://localhost:8765", "mc-room-001")

    while True:
        await asyncio.sleep(1)

asyncio.run(main())
```

## 项目结构

```
p2p/
├── src/p2p/
│   ├── __init__.py          # 库导出
│   ├── config.py            # 配置类(P2PConfig / IceConfig / KcpConfig ...)
│   ├── types.py             # 数据类型(Message / PeerInfo / ConnectionState ...)
│   ├── kcp.py               # KCP 协议纯 Python 实现
│   ├── kcp_transport.py     # KCP 异步传输层封装
│   ├── quic_transport.py    # QUIC 传输层封装(基于 aioquic)
│   ├── ice_manager.py       # ICE / STUN / TURN 管理(基于 aiortc)
│   ├── signaling_server.py  # WebSocket 信令服务器
│   ├── signaling_client.py  # 信令客户端(自动重连)
│   ├── node.py              # P2P 节点(整合所有模块)
│   ├── game_tunnel.py       # 游戏隧道(TCP 流量转发)
│   └── main.py              # CLI 入口
├── pyproject.toml
└── README.md
```

## 模块说明

### config.py

定义所有配置类。核心 `P2PConfig` 包含传输、ICE、信令等子配置。`IceConfig.with_cloudflare_turn()` 返回内置 Cloudflare TURN 服务器配置。

### kcp.py

纯 Python 实现的 KCP 协议,使用 fast 模式:`nodelay=1, interval=10ms, resend=2, nc=1`(无拥塞控制,低延迟)。

### ice_manager.py

基于 aiortc 管理 ICE 协商、DataChannel 收发。使用 Cloudflare TURN 服务器(`turn.cloudflare.com:3478`)作为中继兜底。ICE 候选收集顺序:Host(内网)→ SRFLX(STUN 反射)→ Relay(TURN 中继)。

### signaling_client.py / signaling_server.py

WebSocket 信令,用于交换 SDP Offer/Answer 和 ICE 候选。客户端支持自动重连(指数退避),异步回调通过 `asyncio.create_task()` 调度。

### node.py

`P2PNode` 整合 ICE、QUIC/KCP 传输、信令,提供统一的消息收发接口。初始化流程:`initialize()` → `connect_to_signaling()` → `join_room()` → `connect_to_peer()`。

### game_tunnel.py

`GameTunnel` 在 P2P DataChannel 上转发 TCP 流量。CLIENT 端在本地起 TCP 监听,HOST 端连接本地 MC 服务端,双向转发数据。内置 `_pending_data` 缓冲机制,解决隧道建立前数据丢失问题。

## 配置说明

### 传输协议

- `auto`(默认):优先使用 KCP,低延迟适合游戏
- `quic`:基于 aioquic,可靠有序
- `kcp`:纯 Python 实现,fast 模式低延迟

### Cloudflare TURN

默认使用 Cloudflare 免费 TURN 服务,无需账号:

```python
IceConfig.with_cloudflare_turn()
```

包含 STUN 和 TURN 服务器(UDP + TCP),支持严格 NAT 环境穿透。

### KCP 参数

默认 fast 模式,适合实时游戏:

| 参数 | 值 | 说明 |
|------|----|------|
| `nodelay` | `True` | 启用快速模式 |
| `interval` | `10` ms | 轮询间隔 |
| `resend` | `2` | 快速重传阈值 |
| `nc` | `True` | 关闭拥塞控制 |
| `sndwnd` / `rcvwnd` | `1024` | 发送/接收窗口 |
| `mtu` | `1400` | 最大传输单元 |

## 开发

```bash
# 安装依赖(含开发工具)
uv sync

# 运行语法检查
uv run python -c "import p2p"

# 启动信令服务器
uv run p2p-signaling --port 8765

# 测试 P2P 连接
uv run p2p --mode initiator --room test
uv run p2p --mode responder --room test
```

## 许可

MIT
