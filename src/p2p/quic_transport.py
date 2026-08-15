"""QUIC 传输层封装 (基于 aioquic)"""
from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Callable, Optional, Tuple, Dict, Any
from loguru import logger

try:
    from aioquic.asyncio import QuicConnectionProtocol, connect, serve
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.events import (
        QuicEvent,
        StreamDataReceived,
        ConnectionTerminated,
        HandshakeCompleted,
        ConnectionIdIssued,
        ConnectionIdRetired,
        DatagramFrameReceived,
    )
    from aioquic.quic.logger import QuicLogger
    AIOQUIC_AVAILABLE = True
except ImportError:
    AIOQUIC_AVAILABLE = False
    logger.warning("aioquic not installed, QUIC transport will not work")

from .config import QuicConfig
from .types import ConnectionState, TransportStats, generate_peer_id


class QUICTransport:
    """基于 QUIC 的可靠传输层"""

    def __init__(
        self,
        config: QuicConfig,
        on_data_received: Optional[Callable[[bytes], None]] = None,
        on_connection_state: Optional[Callable[[ConnectionState], None]] = None,
    ):
        if not AIOQUIC_AVAILABLE:
            raise RuntimeError("aioquic is not installed. Please install it: pip install aioquic")
        
        self.config = config
        self.on_data_received = on_data_received
        self.on_connection_state = on_connection_state
        
        # 状态
        self.state: ConnectionState = ConnectionState.DISCONNECTED
        self.stats: TransportStats = TransportStats()
        
        # QUIC 配置
        self._quic_config: Optional[QuicConfiguration] = None
        
        # 连接对象
        self._protocol: Optional[QuicConnectionProtocol] = None
        self._server = None
        
        # 流
        self._stream_id: Optional[int] = None
        self._stream_queue: asyncio.Queue[bytes] = asyncio.Queue()
        
        # 事件
        self._connected_event: asyncio.Event = asyncio.Event()
        self._handshake_done: asyncio.Event = asyncio.Event()
        
        # 本地地址
        self._local_addr: Optional[Tuple[str, int]] = None
        self._remote_addr: Optional[Tuple[str, int]] = None
        
        self._peer_id: str = generate_peer_id()
        self._transport_lock: asyncio.Lock = asyncio.Lock()

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
        if self.state != state:
            old_state = self.state
            self.state = state
            logger.info(f"[QUIC] State changed: {old_state} -> {state}")
            if self.on_connection_state:
                self.on_connection_state(state)

    def _create_quic_config(self, is_server: bool = False) -> QuicConfiguration:
        """创建 QUIC 配置"""
        config = QuicConfiguration(
            is_client=not is_server,
            alpn_protocols=["p2p-quic-v1"],
            max_data=self.config.max_data,
            max_stream_data=self.config.max_stream_data,
            max_streams_bidirectional=self.config.max_streams_bidi,
            max_streams_unidirectional=self.config.max_streams_uni,
            idle_timeout=self.config.idle_timeout,
        )
        
        # 开发环境使用自签名证书
        if is_server:
            # 生成临时自签名证书
            try:
                from cryptography import x509
                from cryptography.x509.oid import NameOID
                from cryptography.hazmat.primitives import hashes, serialization
                from cryptography.hazmat.primitives.asymmetric import rsa
                import datetime
                
                key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
                subject = issuer = x509.Name([
                    x509.NameAttribute(NameOID.COMMON_NAME, "p2p-local"),
                ])
                cert = (
                    x509.CertificateBuilder()
                    .subject_name(subject)
                    .issuer_name(issuer)
                    .public_key(key.public_key())
                    .serial_number(x509.random_serial_number())
                    .not_valid_before(datetime.datetime.utcnow())
                    .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
                    .sign(key, hashes.SHA256())
                )
                
                # 保存到临时变量
                cert_pem = cert.public_bytes(serialization.Encoding.PEM)
                key_pem = key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                
                import tempfile
                import os
                
                cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
                key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
                cert_file.write(cert_pem)
                key_file.write(key_pem)
                cert_file.close()
                key_file.close()
                
                config.load_cert_chain(cert_file.name, key_file.name)
                
                # 清理临时文件
                os.unlink(cert_file.name)
                os.unlink(key_file.name)
                
            except Exception as e:
                logger.warning(f"[QUIC] Failed to generate self-signed cert: {e}")
        else:
            # 客户端跳过证书验证（用于自签名证书的 P2P 场景）
            config.verify_mode = ssl.CERT_NONE
        
        return config

    def _event_handler(self, event: QuicEvent) -> None:
        """处理 QUIC 事件"""
        from datetime import datetime
        
        if isinstance(event, HandshakeCompleted):
            logger.info("[QUIC] Handshake completed")
            self._handshake_done.set()
            self._connected_event.set()
            self._set_state(ConnectionState.CONNECTED)
            
        elif isinstance(event, ConnectionTerminated):
            logger.info(f"[QUIC] Connection terminated: {event.reason_phrase}")
            self._set_state(ConnectionState.CLOSED)
            
        elif isinstance(event, StreamDataReceived):
            self.stats.bytes_received += len(event.data)
            self.stats.packets_received += 1
            
            if self.on_data_received:
                self.on_data_received(event.data)
            
            # 发送确认
            if event.end_stream:
                pass
            
        elif isinstance(event, DatagramFrameReceived):
            self.stats.bytes_received += len(event.data)
            self.stats.packets_received += 1
            
            if self.on_data_received:
                self.on_data_received(event.data)
                
        elif isinstance(event, ConnectionIdIssued):
            logger.debug(f"[QUIC] Connection ID issued: {event.connection_id.hex()}")
            
        elif isinstance(event, ConnectionIdRetired):
            logger.debug(f"[QUIC] Connection ID retired: {event.connection_id.hex()}")
        
        self.stats.last_updated = datetime.now()

    async def serve(self, host: str = "0.0.0.0", port: int = 0) -> Tuple[str, int]:
        """
        作为服务端启动 QUIC 监听
        :return: (host, port)
        """
        try:
            self._set_state(ConnectionState.CONNECTING)
            self._quic_config = self._create_quic_config(is_server=True)
            
            # 创建自定义协议工厂
            def create_protocol() -> QuicConnectionProtocol:
                protocol = QuicConnectionProtocol(
                    quic_configuration=self._quic_config,
                    create_protocol=None,
                )
                # 注册事件处理器
                original_event = protocol._quic
                
                # 包装事件回调
                def wrap_event_handler(ev: QuicEvent):
                    self._event_handler(ev)
                
                protocol.quic_event_received = wrap_event_handler
                self._protocol = protocol
                return protocol
            
            # 启动服务
            self._server = await serve(
                host,
                port,
                configuration=self._quic_config,
                create_protocol=create_protocol,
            )
            
            # 获取实际监听地址
            actual_port = port
            for sock in getattr(self._server, 'sockets', []):
                addr = sock.getsockname()
                if len(addr) >= 2:
                    actual_port = addr[1]
                    break
            
            self._local_addr = (host if host != "0.0.0.0" else "127.0.0.1", actual_port)
            logger.info(f"[QUIC] Server listening on {self._local_addr}")
            
            return self._local_addr
            
        except Exception as e:
            logger.error(f"[QUIC] Serve error: {e}")
            self._set_state(ConnectionState.FAILED)
            raise

    async def connect(self, host: str, port: int) -> bool:
        """
        作为客户端连接到 QUIC 服务端
        """
        try:
            self._set_state(ConnectionState.CONNECTING)
            self._quic_config = self._create_quic_config(is_server=False)
            self._remote_addr = (host, port)
            
            # 创建协议
            def create_protocol() -> QuicConnectionProtocol:
                protocol = QuicConnectionProtocol(
                    quic_configuration=self._quic_config,
                    create_protocol=None,
                )
                protocol.quic_event_received = self._event_handler
                self._protocol = protocol
                return protocol
            
            # 连接
            _, protocol = await connect(
                host,
                port,
                configuration=self._quic_config,
                create_protocol=create_protocol,
                local_port=0,
            )
            
            self._protocol = protocol
            self._local_addr = ("0.0.0.0", 0)
            
            # 等待握手完成
            try:
                await asyncio.wait_for(self._handshake_done.wait(), timeout=self.config.handshake_timeout)
                self._set_state(ConnectionState.CONNECTED)
                logger.info(f"[QUIC] Connected to {host}:{port}")
                return True
            except asyncio.TimeoutError:
                logger.warning(f"[QUIC] Handshake timeout to {host}:{port}")
                self._set_state(ConnectionState.FAILED)
                return False
                
        except Exception as e:
            logger.error(f"[QUIC] Connect error: {e}")
            self._set_state(ConnectionState.FAILED)
            return False

    async def send(self, data: bytes, use_stream: bool = True) -> bool:
        """
        发送数据
        :param data: 要发送的数据
        :param use_stream: True=使用流(可靠), False=使用数据报(不可靠但更快)
        """
        if self.state != ConnectionState.CONNECTED:
            logger.warning(f"[QUIC] Cannot send, state: {self.state}")
            return False
        
        if not self._protocol:
            return False
        
        try:
            async with self._transport_lock:
                if use_stream:
                    # 可靠方式：使用流
                    if self._stream_id is None:
                        self._stream_id = self._protocol._quic.get_next_available_stream_id()
                    
                    self._protocol._quic.send_stream_data(
                        stream_id=self._stream_id,
                        data=data,
                        end_stream=False,
                    )
                    self._protocol.transmit()
                else:
                    # 快速方式：使用数据报
                    self._protocol._quic.send_datagram_frame(data)
                    self._protocol.transmit()
                
                self.stats.bytes_sent += len(data)
                self.stats.packets_sent += 1
                return True
                
        except Exception as e:
            logger.error(f"[QUIC] Send error: {e}")
            return False

    async def close(self) -> None:
        """关闭 QUIC 传输"""
        logger.info("[QUIC] Closing transport")
        self._set_state(ConnectionState.CLOSED)
        
        if self._protocol:
            try:
                self._protocol.close()
            except Exception:
                pass
            self._protocol = None
        
        if self._server:
            try:
                self._server.close()
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        
        logger.info("[QUIC] Transport closed")

    def get_stats(self) -> TransportStats:
        """获取传输统计"""
        from datetime import datetime
        self.stats.last_updated = datetime.now()
        return self.stats
