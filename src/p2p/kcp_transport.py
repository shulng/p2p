"""KCP 异步传输层"""
from __future__ import annotations

import asyncio
import socket
import struct
import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Tuple, Any
from loguru import logger

from .config import KcpConfig
from .types import ConnectionState, TransportStats, generate_peer_id
from .kcp import KCP


class KCPTransport:
    """基于 KCP 的可靠 UDP 传输层"""

    def __init__(
        self,
        config: KcpConfig,
        conv_id: Optional[int] = None,
        on_data_received: Optional[Callable[[bytes], None]] = None,
        on_connection_state: Optional[Callable[[ConnectionState], None]] = None,
    ):
        self.config = config
        self.conv_id: int = conv_id if conv_id is not None else int(time.time() * 1000) & 0x7fffffff
        self.on_data_received = on_data_received
        self.on_connection_state = on_connection_state
        
        # 传输状态
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.stats: TransportStats = TransportStats()
        
        # UDP 套接字
        self._sock: Optional[socket.socket] = None
        self._local_addr: Optional[Tuple[str, int]] = None
        self._remote_addr: Optional[Tuple[str, int]] = None
        
        # KCP 实例
        self._kcp: Optional[KCP] = None
        
        # 异步任务
        self._recv_task: Optional[asyncio.Task] = None
        self._update_task: Optional[asyncio.Task] = None
        self._running: bool = False
        
        # 发送队列
        self._send_queue: asyncio.Queue = asyncio.Queue()
        
        # 回调
        self._connected_event: asyncio.Event = asyncio.Event()
        self._peer_id: str = generate_peer_id()

    @property
    def peer_id(self) -> str:
        return self._peer_id

    @property
    def local_address(self) -> Optional[Tuple[str, int]]:
        return self._local_addr

    @property
    def remote_address(self) -> Optional[Tuple[str, int]]:
        return self._remote_addr

    def _set_state(self, state: ConnectionState) -> None:
        """设置连接状态并通知回调"""
        if self.state != state:
            old_state = self.state
            self.state = state
            logger.info(f"[KCP] State changed: {old_state} -> {state}")
            if self.on_connection_state:
                self.on_connection_state(state)

    def _kcp_output(self, data: bytes, kcp: KCP, user: Any) -> int:
        """KCP 输出回调 - 将 KCP 段通过 UDP 发送"""
        try:
            if self._sock and self._remote_addr:
                self._sock.sendto(data, self._remote_addr)
                self.stats.bytes_sent += len(data)
                self.stats.packets_sent += 1
                return len(data)
        except Exception as e:
            logger.error(f"[KCP] Send error: {e}")
        return -1

    def _init_kcp(self) -> None:
        """初始化 KCP 实例"""
        self._kcp = KCP(self.conv_id, self._kcp_output)
        self._kcp.nodelay_config(
            nodelay=1 if self.config.nodelay else 0,
            interval=self.config.interval,
            resend=self.config.resend,
            nc=1 if self.config.nc else 0,
        )
        self._kcp.wndsize(self.config.sndwnd, self.config.rcvwnd)
        self._kcp.setmtu(self.config.mtu)

    async def bind(self, host: str = "", port: int = 0) -> Tuple[str, int]:
        """
        绑定本地 UDP 端口
        :return: (host, port)
        """
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)

        if port == 0:
            # 让系统自动分配可用端口，避免冲突
            self._sock.bind((host, 0))
        else:
            self._sock.bind((host, port))

        self._local_addr = self._sock.getsockname()
        logger.info(f"[KCP] Bound to {self._local_addr}")
        return self._local_addr

    def set_remote(self, address: Tuple[str, int]) -> None:
        """
        设置远端地址
        """
        self._remote_addr = address
        logger.info(f"[KCP] Remote address set to {address}")

    async def connect(self, remote_addr: Tuple[str, int]) -> bool:
        """
        连接到远端
        """
        try:
            self._set_state(ConnectionState.CONNECTING)
            
            if not self._local_addr:
                await self.bind(self.config.host, self.config.port)
            
            self.set_remote(remote_addr)
            self._init_kcp()
            
            self._running = True
            self._recv_task = asyncio.create_task(self._recv_loop())
            self._update_task = asyncio.create_task(self._update_loop())
            
            # 发送握手包
            await self._send_handshake()
            
            # 等待连接确认
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=10.0)
                self._set_state(ConnectionState.CONNECTED)
                logger.info(f"[KCP] Connected to {remote_addr}")
                return True
            except asyncio.TimeoutError:
                logger.warning(f"[KCP] Connection timeout to {remote_addr}")
                self._set_state(ConnectionState.FAILED)
                return False
                
        except Exception as e:
            logger.error(f"[KCP] Connect error: {e}")
            self._set_state(ConnectionState.FAILED)
            return False

    async def _send_handshake(self) -> None:
        """发送 KCP 握手包"""
        # 发送一个特殊的握手段
        if self._kcp:
            handshake_data = b"KCP_HANDSHAKE"
            self._kcp.send(handshake_data)
            self._kcp.flush()

    async def accept_connection(self, remote_addr: Optional[Tuple[str, int]] = None) -> bool:
        """
        作为服务端接受连接
        """
        try:
            self._set_state(ConnectionState.CONNECTING)
            
            if not self._local_addr:
                await self.bind(self.config.host, self.config.port)
            
            if remote_addr:
                self.set_remote(remote_addr)
            
            self._init_kcp()
            
            self._running = True
            self._recv_task = asyncio.create_task(self._recv_loop())
            self._update_task = asyncio.create_task(self._update_loop())
            
            # 等待握手数据
            try:
                await asyncio.wait_for(self._connected_event.wait(), timeout=15.0)
                self._set_state(ConnectionState.CONNECTED)
                logger.info(f"[KCP] Connection accepted from {self._remote_addr}")
                return True
            except asyncio.TimeoutError:
                logger.warning("[KCP] Accept connection timeout")
                self._set_state(ConnectionState.FAILED)
                return False
                
        except Exception as e:
            logger.error(f"[KCP] Accept error: {e}")
            self._set_state(ConnectionState.FAILED)
            return False

    async def _recv_loop(self) -> None:
        """接收循环"""
        loop = asyncio.get_running_loop()
        logger.info("[KCP] Recv loop started")
        
        try:
            while self._running:
                try:
                    # 异步接收数据
                    data, addr = await loop.sock_recvfrom(self._sock, 65535)
                    
                    if data and len(data) >= 4:
                        # 如果没有设置远端地址，从第一个包获取
                        if not self._remote_addr:
                            self.set_remote(addr)
                            self._connected_event.set()
                        
                        self.stats.bytes_received += len(data)
                        self.stats.packets_received += 1
                        
                        # 输入到 KCP
                        if self._kcp:
                            self._kcp.input(data)
                            
                            # 尝试从 KCP 接收数据
                            while True:
                                recv_data = self._kcp.recv(65535)
                                if not recv_data:
                                    break
                                
                                # 处理握手
                                if recv_data == b"KCP_HANDSHAKE":
                                    logger.debug("[KCP] Received handshake")
                                    if not self._connected_event.is_set():
                                        self._connected_event.set()
                                    # 回应握手确认
                                    self._kcp.send(b"KCP_ACK")
                                    self._kcp.flush()
                                elif recv_data == b"KCP_ACK":
                                    logger.debug("[KCP] Received handshake ACK")
                                    if not self._connected_event.is_set():
                                        self._connected_event.set()
                                else:
                                    if self.on_data_received:
                                        self.on_data_received(recv_data)
                                    
                except BlockingIOError:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"[KCP] Recv loop error: {e}")
                    await asyncio.sleep(0.01)
        finally:
            logger.info("[KCP] Recv loop stopped")

    async def _update_loop(self) -> None:
        """KCP 更新循环 - 需要定期调用 KCP.update()"""
        logger.info("[KCP] Update loop started")
        try:
            while self._running:
                if self._kcp:
                    # 发送队列中的数据
                    while not self._send_queue.empty():
                        try:
                            data = self._send_queue.get_nowait()
                            self._kcp.send(data)
                        except asyncio.QueueEmpty:
                            break
                    
                    # 更新 KCP
                    next_update = self._kcp.update()
                    wait_time = min(next_update / 1000.0, 0.05)
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[KCP] Update loop error: {e}")
        finally:
            logger.info("[KCP] Update loop stopped")

    async def send(self, data: bytes) -> bool:
        """
        发送数据
        """
        if self.state != ConnectionState.CONNECTED:
            logger.warning(f"[KCP] Cannot send, state: {self.state}")
            return False
        
        try:
            await self._send_queue.put(data)
            return True
        except Exception as e:
            logger.error(f"[KCP] Send queue error: {e}")
            return False

    def send_sync(self, data: bytes) -> bool:
        """同步发送（立即放入队列，不等待）"""
        if self.state != ConnectionState.CONNECTED:
            return False
        try:
            self._send_queue.put_nowait(data)
            return True
        except Exception:
            return False

    async def close(self) -> None:
        """关闭传输"""
        logger.info("[KCP] Closing transport")
        self._running = False
        self._set_state(ConnectionState.CLOSED)
        
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        
        self._kcp = None
        logger.info("[KCP] Transport closed")

    def get_stats(self) -> TransportStats:
        """获取传输统计"""
        from datetime import datetime
        self.stats.last_updated = datetime.now()
        return self.stats
