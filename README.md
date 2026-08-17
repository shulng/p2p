# P2P 通信库

基于 **KCP + Cloudflare TURN** 的点对点通信工具，支持 NAT 穿透、低延迟传输，并内置游戏隧道功能（可用于 Minecraft 等游戏联机）。

## 特性

- **统一传输通道管理**：数据优先走 KCP（纯 Python 实现，fast 模式低延迟），KCP 直连不可用时自动降级到 SCTP (DataChannel)；控制信令统一走 SCTP
- **NAT 穿透**：完整 ICE 流程（Host → SRFLX → Relay），Cloudflare TURN 作为中继兜底
- **WebRTC DataChannel**：基于 aiortc，支持 TURN 中继，跨网络可靠连通
- **WebSocket 信令**：内置信令服务器与客户端，支持房间管理、自动重连（指数退避）
- **游戏隧道**：通过 P2P 连接转发 TCP/UDP 流量，实现 Minecraft 等游戏联机
- **纯 Python 实现**：KCP 协议无需 C 扩展，跨平台运行

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                        P2P 节点 (P2PNode)                    │
│                                                             │
│  ┌──────────────┐   ┌──────────────┐   ┌────────────────┐  │
│  │ SignalingCli │←→ │  IceManager  │←→ │ 传输层          │  │
│  │  (WebSocket) │   │ (aiortc DC)  │   │ (KCP + SCTP)   │  │
│  └──────────────┘   └──────┬───────┘   └────────────────┘  │
│                            │                               │
│                            ▼                               │
│                   Cloudflare TURN                          │
│                 (turn.cloudflare.com:3478)                 │
└─────────────────────────────────────────────────────────────┘
```

**通道策略**：

| 通道 | 用途 | 承载协议 |
|------|------|----------|
| `control` | 隧道控制消息（open/close） | SCTP (DataChannel) |
| `data` | 业务数据（隧道 TCP/UDP 数据） | KCP 直连优先，降级 SCTP |

> 说明：`chat` / `bench` 命令通过 `send_text` / `send_bytes`（走 SCTP/DataChannel）发送；`server` / `client` 隧道的数据消息通过 `send_to_peer_channel(channel=CHANNEL_DATA)` 走 **KCP 直连优先、SCTP 降级**。

**数据流向(游戏隧道)**:

```
MC客户端 → localhost:25565 (CLIENT本地TCP) → P2P KCP → HOST → localhost:25565 (MC服务端)
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
| `aiortc` | WebRTC / ICE / DataChannel |
| `websockets` | 信令服务器 WebSocket |
| `cryptography` | DTLS 加密 |
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
# 终端 A (发起方, 主动发测试消息)
uv run p2p chat test123 --as a

# 终端 B (响应方, 回显消息)
uv run p2p chat test123 --as b
```

### 3. 通用隧道(TCP/UDP 转发)

详见下方 [通用隧道教程](#通用隧道教程)。

## 通用隧道教程

通过 P2P 隧道转发任意 **TCP / UDP** 流量,可用于游戏联机(Minecraft、泰拉瑞亚、饥荒等)、服务代理、SSH 转发等场景。不绑定任何游戏预设,完全由参数指定协议与端口。

### 工作原理

```
┌─────────────────┐                              ┌─────────────────┐
│  CLIENT 端       │                              │  HOST 端         │
│                 │                              │                 │
│  用户客户端      │                              │  目标服务        │
│     ↓           │                              │     ↑           │
│  127.0.0.1:PORT │  ←─ P2P KCP ──→             │  127.0.0.1:PORT │
│  (本地监听)      │     (KCP+TURN)              │  (转发到服务)    │
└─────────────────┘                              └─────────────────┘
```

### 角色说明

| 角色 | CLI 子命令 | 含义 | 本地行为 |
|------|------------|------|----------|
| **SERVER** | `p2p server` | 运行目标服务端的人(RESPONDER) | 接收 P2P 数据,转发到本地目标服务 |
| **CLIENT** | `p2p client` | 运行用户客户端的人(INITIATOR) | 本地起监听,用户客户端连这里 |

### 操作步骤

**终端 1 — 信令服务器**(可在任意机器运行,或部署到公网):

```bash
uv run p2p-signaling --port 8765
```

**终端 2 — SERVER 端**(运行目标服务的人):

```bash
uv run p2p server my-room --tcp 25565 \
  -s ws://<信令服务器IP>:8765
```

**终端 3 — CLIENT 端**(运行用户客户端的人):

```bash
uv run p2p client my-room --tcp 25565 \
  -s ws://<信令服务器IP>:8765
```

**最后**:用户客户端连接 `127.0.0.1:25565` 即可(等于直连 SERVER 的目标服务)。

### 协议自动推断

不用再手动指定 `--protocol`,由 `--tcp` / `--udp` 是否给出自动推断:

| 给出的参数 | 推断协议 | 用途 |
|------------|----------|------|
| `--tcp PORT` | `tcp` | TCP 流量转发(Minecraft Java 25565、泰拉瑞亚 7777、SSH 22) |
| `--udp PORT` | `udp` | UDP 数据包转发(Minecraft 基岩版 19132、饥荒联机版 10999) |
| `--tcp PORT --udp PORT2` | `both` | 同时转发 TCP + UDP,端口可不同 |

### 常见场景示例

**Minecraft Java 版(TCP 25565)**:

```bash
# SERVER (运行 MC 服务端)
uv run p2p server mc-room --tcp 25565

# CLIENT (运行 MC 客户端)
uv run p2p client mc-room --tcp 25565
# MC 客户端连接 127.0.0.1:25565
```

**Minecraft 基岩版(UDP 19132)**:

```bash
# SERVER
uv run p2p server mc-bedrock --udp 19132

# CLIENT
uv run p2p client mc-bedrock --udp 19132
# MC 基岩版客户端连接 127.0.0.1:19132
```

**同时转发 TCP + UDP(端口不同)**:

```bash
# SERVER: TCP 25565 + UDP 19132
uv run p2p server both-room --tcp 25565 --udp 19132

# CLIENT: TCP 25565 + UDP 19132
uv run p2p client both-room --tcp 25565 --udp 19132
```

### 端口冲突处理

如果 CLIENT 端本机端口被占用,改用其他端口:

```bash
uv run p2p client mc-room --tcp 35565
# 客户端连接 127.0.0.1:35565
```

如果 SERVER 端目标服务端口非默认,直接在 `--tcp` / `--udp` 指定:

```bash
uv run p2p server mc-room --tcp 25566
```

## CLI 参数

### `p2p` 命令(子命令式)

```
p2p [--log-level LEVEL] <cmd> [options]
```

| 子命令 | 用途 | 必需参数 |
|--------|------|----------|
| `chat <room> --as a\|b` | P2P 通信测试(A 发消息,B 回) | `--as` |
| `bench <room> --as a\|b` | 性能测试(A 发大数据,B 收) | `--as` |
| `server <room> --tcp\|--udp PORT` | SERVER 端隧道(转发 P2P→本地服务) | `--tcp` 或 `--udp` 至少一个 |
| `client <room> --tcp\|--udp PORT` | CLIENT 端隧道(本地监听→P2P) | `--tcp` 或 `--udp` 至少一个 |

**通用选项**(所有子命令):

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `-s, --signaling URL` | 信令服务器地址 | `ws://localhost:8765` |
| `--log-level LEVEL` | 日志级别:`DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |

**`chat` / `bench` 专属:**

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--as a\|b` | a=initiator(sender), b=responder(receiver) | 必填 |
| `--duration SECONDS`(仅 bench) | A 端发送时长 | `30` |

**`server` / `client` 专属:**

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `--tcp PORT` | TCP 端口(SERVER=转发目标,CLIENT=本地监听) | - |
| `--udp PORT` | UDP 端口(同上) | - |
| `--name NAME` | 日志显示名 | `SERVER` / `CLIENT` |

> 协议由 `--tcp` / `--udp` 是否给出自动推断:只给 `--tcp` → `tcp`,只给 `--udp` → `udp`,两者都给 → `both`。

### `p2p-signaling` 命令

```bash
uv run p2p-signaling --port 8765 [--host 0.0.0.0]
```

## 编程式使用

除 CLI 外,也可作为 Python 库使用:

```python
import asyncio
from p2p import P2PConfig, P2PNode, ConnectionRole, IceConfig, MessageType

async def main():
    config = P2PConfig(
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

通用隧道编程式使用:

```python
import asyncio
from p2p import P2PConfig, ConnectionRole, GameTunnel, IceConfig
from p2p.tunnel.game_tunnel import TunnelConfig

async def main():
    # 通用配置：协议、端口、目标都由参数指定
    tunnel_config = TunnelConfig(
        protocol="both",           # tcp / udp / both
        local_listen_port=25565,   # CLIENT 端本地 TCP 监听
        remote_forward_port=25565, # HOST 端转发目标 TCP 端口
        local_listen_port_udp=19132,
        remote_forward_port_udp=19132,
        name="mc-tunnel",
    )
    p2p_config = P2PConfig(
        role=ConnectionRole.INITIATOR,  # client 端
        ice=IceConfig.with_cloudflare_turn(),
    )
    p2p_config.signaling.server_url = "ws://localhost:8765"

    tunnel = GameTunnel(p2p_config, tunnel_config, ConnectionRole.INITIATOR)
    await tunnel.start("ws://localhost:8765", "my-room")

    while True:
        await asyncio.sleep(1)

asyncio.run(main())
```

## 项目结构

```
p2p/
├── src/p2p/
│   ├── __init__.py          # 库导出(公共 API)
│   ├── config.py            # 配置类(P2PConfig / IceConfig / KcpConfig ...)
│   ├── types.py             # 数据类型(Message / PeerInfo / ConnectionState ...)
│   ├── _compat.py           # websockets 版本兼容层
│   ├── _utils.py            # 公共工具函数(cancel_task 等)
│   ├── node.py              # P2P 节点(整合各层,门面)
│   ├── main.py              # CLI 入口
│   ├── transport/           # 传输层
│   │   ├── kcp_core.py      # KCP 协议纯 Python 实现
│   │   ├── kcp.py           # KCP 异步传输层封装
│   │   └── hybrid.py        # 传输管理器(数据走 KCP,控制走 SCTP)
│   ├── signaling/           # 信令层
│   │   ├── client.py        # 信令客户端(自动重连)
│   │   └── server.py        # WebSocket 信令服务器
│   ├── ice/                 # NAT 穿透层
│   │   └── ice_manager.py   # ICE / STUN / TURN 管理(基于 aiortc)
│   └── tunnel/              # 应用层
│       └── game_tunnel.py   # 通用隧道(TCP/UDP 流量转发)
├── pyproject.toml
└── README.md
```

## 模块说明

### transport/kcp_core.py（原 kcp.py）

纯 Python 实现的 KCP 协议,使用 fast 模式:`nodelay=1, interval=10ms, resend=2, nc=1`(无拥塞控制,低延迟)。支持分片、可靠有序传输、丢包重传、拥塞窗口与窗口探测。

### transport/kcp.py（原 kcp_transport.py）

KCP 的异步 UDP 传输层封装：绑定端口、收发循环、KCP update 循环、连接握手。

### transport/hybrid.py（原 hybrid_transport.py）

传输管理器，协调 SCTP (DataChannel) 与 KCP：
- **control** 通道走 SCTP，始终可用（含 TURN 中继场景）
- **data** 通道优先 KCP 直连，失败自动降级 SCTP
- RESPONDER 绑定 KCP 端口并通过 SCTP 交换地址，INITIATOR 据此发起 KCP 直连

### ice/ice_manager.py

基于 aiortc 管理 ICE 协商、DataChannel 收发。使用 Cloudflare TURN 服务器(`turn.cloudflare.com:3478`)作为中继兜底。ICE 候选收集顺序:Host(内网)→ SRFLX(STUN 反射)→ Relay(TURN 中继)。

### signaling/client.py / signaling/server.py

WebSocket 信令,用于交换 SDP Offer/Answer 和 ICE 候选。客户端支持自动重连(指数退避),异步回调通过 `asyncio.create_task()` 调度。

### node.py

`P2PNode` 整合 ICE、KCP 传输、信令,提供统一的消息收发接口。支持多 Peer 并发连接(每 Peer 独立 IceManager)，并按 Peer 维护消息有序队列(保证同一对端消息按到达顺序处理,不因并发回调乱序)。

初始化流程:`initialize()` → `connect_to_signaling()` → `join_room()` → `connect_to_peer()`。

> 注意：`send_to_peer` / `send_text` / `send_bytes` 走 SCTP(DataChannel)；`send_to_peer_channel(channel=CHANNEL_DATA)` 走 KCP 优先、SCTP 降级。

### tunnel/game_tunnel.py

`GameTunnel` 在 P2P 连接上转发 **TCP / UDP** 流量。CLIENT 端在本地起监听(TCP 用 `asyncio.start_server`,UDP 用 `create_datagram_endpoint`),HOST 端连接本地目标服务,双向转发数据。

- TCP:每条连接分配 `conn_id`,通过 `tunnel.tcp.open/data/close` 消息转发,内置 `_tcp_pending` 缓冲解决隧道建立前数据丢失
- UDP:按客户端源地址分配 `conn_id`,HOST 端为每个会话创建独立 UDP relay,空闲 60 秒自动清理
- `both` 模式:同时启动 TCP 和 UDP 转发,端口可独立配置(`local_listen_port_udp` / `remote_forward_port_udp`)
- 通道选择:控制消息(open/close)走 `control`(SCTP),数据消息走 `data`(KCP 优先)

## 配置说明

### 传输协议

数据通过 `transport.hybrid.KCPDataTransport` 管理：
- **control** 信令走 SCTP (DataChannel)
- **data** 数据优先走 **KCP**(纯 Python 实现,fast 模式低延迟,适合实时游戏),KCP 直连不可用(如严格 NAT 下 KCP 无法直连)时自动降级到 SCTP

> 简单文本/二进制消息(`chat`/`bench` 及 `send_text`/`send_bytes`/`send_to_peer`)直接走 DataChannel(SCTP)；隧道数据(`GameTunnel`)走 KCP 优先。

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
uv run p2p chat test --as a
uv run p2p chat test --as b
```

## 许可

MIT
