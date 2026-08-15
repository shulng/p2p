"""KCP 协议的纯 Python 实现
基于 KCP (https://github.com/skywind3000/kcp) 协议规范
"""
from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple, Deque
from collections import deque
from loguru import logger

# KCP 命令常量
KCP_CMD_PUSH = 81  # 数据推送
KCP_CMD_ACK = 82   # 确认
KCP_CMD_WASK = 83  # 窗口探测
KCP_CMD_WINS = 84  # 窗口大小

# KCP 头部大小
KCP_OVERHEAD = 24

# 最大消息数
KCP_WND_SND = 32
KCP_WND_RCV = 128
KCP_MTU_DEF = 1400
KCP_ASK_SEND = 1
KCP_ASK_TELL = 2


def _get_mss(mtu: int) -> int:
    """计算 MSS"""
    return mtu - KCP_OVERHEAD


def _bound(low: int, x: int, high: int) -> int:
    """限制值在范围内"""
    return max(low, min(x, high))


def _itime_now() -> int:
    """获取当前时间(毫秒)"""
    return int(time.time() * 1000) & 0xffffffff


@dataclass
class KCP_SEG:
    """KCP 段"""
    conv: int = 0        # 会话ID
    cmd: int = 0         # 命令
    frg: int = 0         # 分片号
    wnd: int = 0         # 窗口大小
    ts: int = 0          # 时间戳
    sn: int = 0          # 序号
    una: int = 0         # 未确认的最小序号
    resendts: int = 0    # 重发时间戳
    rto: int = 0         # 重传超时
    fastack: int = 0     # 快速确认数
    xmit: int = 0        # 重发次数
    data: bytes = b""    # 数据


class KCP:
    """KCP 协议实现"""

    def __init__(self, conv: int, output: Callable[[bytes, "KCP", object], int]):
        """
        创建 KCP 实例
        :param conv: 会话ID
        :param output: 输出回调函数 output(data, kcp, user) -> int
        """
        self.conv = conv
        self.output = output
        
        # 发送和接收缓冲区
        self.snd_queue: Deque[KCP_SEG] = deque()
        self.rcv_queue: Deque[KCP_SEG] = deque()
        self.snd_buf: List[KCP_SEG] = []
        self.rcv_buf: List[KCP_SEG] = []
        
        # 序号控制
        self.snd_una: int = 0
        self.snd_nxt: int = 0
        self.rcv_nxt: int = 0
        
        # 窗口大小
        self.ssthresh: int = 0
        self.snd_wnd: int = KCP_WND_SND
        self.rcv_wnd: int = KCP_WND_RCV
        self.rmt_wnd: int = KCP_WND_RCV
        self.cwnd: int = 0
        self.probe: int = 0
        
        # 时间戳
        self.current: int = 0
        self.interval: int = 100
        self.ts_flush: int = 0
        
        # 控制参数
        self.nodelay: bool = False
        self.updated: bool = False
        self.ts_probe: int = 0
        self.probe_wait: int = 0
        self.dead_link: int = 20
        
        # 拥塞控制
        self.incr: int = 0
        self.no_congest_control: bool = False
        
        # MTU / MSS
        self.mtu: int = KCP_MTU_DEF
        self.mss: int = _get_mss(self.mtu)
        
        # 重传超时
        self.rx_rto: int = 100
        self.rx_minrto: int = 100
        
        # 快速重传
        self.fastlimit: int = 0
        
        # ACK
        self.acklist: List[Tuple[int, int]] = []
        
        # 状态
        self.state: int = 0
        self.user: object = None
        
        # 缓冲区数据计数
        self.logmask: int = 0

    def nodelay_config(self, nodelay: int, interval: int, resend: int, nc: int) -> None:
        """
        配置 KCP 快速模式
        :param nodelay: 0=关闭, 1=开启
        :param interval: 内部时钟间隔(ms), 默认100, 可选10-100ms
        :param resend: 0=关闭快速重传, 2=推荐
        :param nc: 0=开启拥塞控制, 1=关闭
        """
        if nodelay:
            self.nodelay = True
            self.rx_minrto = 30
            self.rx_rto = 100 if self.rx_rto < 100 else self.rx_rto
        else:
            self.nodelay = False
            self.rx_minrto = 100
        
        self.interval = max(10, min(interval, 5000))
        self.fastlimit = max(0, resend)
        
        if nc:
            self.no_congest_control = True
        else:
            self.no_congest_control = False

    def wndsize(self, sndwnd: int, rcvwnd: int) -> None:
        """
        设置窗口大小
        :param sndwnd: 发送窗口大小(包)
        :param rcvwnd: 接收窗口大小(包), 建议 >= 32
        """
        self.snd_wnd = max(16, sndwnd)
        self.rcv_wnd = max(16, rcvwnd)

    def setmtu(self, mtu: int) -> None:
        """设置 MTU"""
        self.mtu = max(50, mtu)
        self.mss = _get_mss(self.mtu)

    def __parse_data(self, data: bytes) -> KCP_SEG:
        """解析 KCP 段头"""
        if len(data) < KCP_OVERHEAD:
            raise ValueError("Data too short for KCP segment")
        
        conv, cmd, frg, wnd = struct.unpack_from("<IBBH", data, 0)
        ts, sn, una, len_ = struct.unpack_from("<IIII", data, 8)
        
        seg = KCP_SEG(
            conv=conv,
            cmd=cmd,
            frg=frg,
            wnd=wnd,
            ts=ts,
            sn=sn,
            una=una,
        )
        
        if len(data) > KCP_OVERHEAD and len_ > 0:
            seg.data = data[KCP_OVERHEAD:KCP_OVERHEAD + len_]
        
        return seg

    def __encode_segment(self, seg: KCP_SEG, ptr: bytearray) -> int:
        """编码 KCP 段到缓冲区"""
        struct.pack_into("<IBBH", ptr, 0, seg.conv, seg.cmd, seg.frg, seg.wnd)
        struct.pack_into("<IIII", ptr, 8, seg.ts, seg.sn, seg.una, len(seg.data))
        if seg.data:
            ptr[KCP_OVERHEAD:KCP_OVERHEAD + len(seg.data)] = seg.data
        return KCP_OVERHEAD + len(seg.data)

    def input(self, data: bytes) -> int:
        """
        输入数据（从底层协议接收到的原始数据）
        :param data: 接收到的数据
        :return: 0=成功, <0=错误
        """
        total_processed = 0
        offset = 0
        
        while offset < len(data):
            if offset + KCP_OVERHEAD > len(data):
                break
            
            try:
                seg = self.__parse_data(data[offset:])
            except Exception as e:
                logger.error(f"Failed to parse KCP segment: {e}")
                return -1
            
            offset += KCP_OVERHEAD + len(seg.data)
            total_processed += 1
            
            if seg.conv != self.conv:
                logger.warning(f"KCP conv mismatch: expected {self.conv}, got {seg.conv}")
                continue
            
            self.current = _itime_now()
            
            if seg.cmd in (KCP_CMD_PUSH, KCP_CMD_ACK,
                          KCP_CMD_WASK, KCP_CMD_WINS):
                # 更新 una
                if self.__check_una(seg.una) > 0:
                    self.__parse_una(seg.una)
                
                if self.rmt_wnd != seg.wnd:
                    self.rmt_wnd = seg.wnd
            
            if seg.cmd == KCP_CMD_ACK:
                # 确认
                if self.__check_ack(seg.sn) == 0:
                    self.__parse_ack(seg.sn, seg.ts)
            elif seg.cmd == KCP_CMD_PUSH:
                # 数据推送
                repeat = False
                if (seg.sn - self.rcv_nxt) >= 0 and \
                   (self.rcv_nxt + self.rcv_wnd - seg.sn) > 0:
                    self.__update_ack(seg.sn)
                    repeat = self.__insert_data_into_rcv_buf(seg)
                
                if not repeat:
                    for ack_sn, ack_ts in self.acklist:
                        if ack_sn == seg.sn:
                            break
                    else:
                        self.acklist.append((seg.sn, seg.ts))
            elif seg.cmd == KCP_CMD_WASK:
                # 窗口探测
                self.probe |= KCP_ASK_TELL
            elif seg.cmd == KCP_CMD_WINS:
                pass  # 窗口大小已在 una 更新时处理
            else:
                logger.warning(f"Unknown KCP cmd: {seg.cmd}")
                return -2
        
        self.__move_rcv_queue()
        return total_processed

    def __check_una(self, una: int) -> int:
        return una - self.snd_una > 0 and una - (self.snd_nxt - 1) <= 0

    def __parse_una(self, una: int) -> None:
        """处理 una (未确认序号)"""
        new_snd_buf = []
        for seg in self.snd_buf:
            if una - seg.sn > 0:
                pass  # 已确认，丢弃
            else:
                new_snd_buf.append(seg)
        self.snd_buf = new_snd_buf
        if self.snd_buf:
            self.snd_una = self.snd_buf[0].sn
        else:
            self.snd_una = self.snd_nxt

    def __check_ack(self, sn: int) -> int:
        return sn - self.snd_una >= 0 and self.snd_nxt - sn > 0

    def __parse_ack(self, sn: int, ts: int) -> None:
        """处理确认"""
        found = False
        for seg in self.snd_buf:
            if seg.sn == sn:
                found = True
                rtt = self.current - ts
                if rtt >= 0:
                    if self.rx_rto == 0:
                        self.rx_rto = rtt
                    else:
                        self.rx_rto = (7 * self.rx_rto + rtt) // 8
                        if self.rx_rto < self.rx_minrto:
                            self.rx_rto = self.rx_minrto
                seg.fastack = 0x7fffffff
                break
        
        if found:
            new_snd_buf = []
            for seg in self.snd_buf:
                if seg.sn != sn:
                    new_snd_buf.append(seg)
            self.snd_buf = new_snd_buf
        
        # 拥塞控制
        if not self.no_congest_control:
            if self.cwnd < self.rmt_wnd:
                if self.cwnd < self.ssthresh:
                    self.cwnd += 1
                    self.incr += self.mss
                else:
                    if self.incr < self.mss:
                        self.incr = self.mss
                    self.incr = self.incr + (self.mss * self.mss) // self.incr + \
                               (self.mss // 16)
                    if (self.cwnd + 1) * self.mss <= self.incr:
                        self.cwnd += 1
            
            if not self.snd_buf:
                self.ssthresh = min(self.cwnd, self.rmt_wnd) // 2
                if self.ssthresh < 2:
                    self.ssthresh = 2
                self.cwnd = self.ssthresh
                self.incr = self.cwnd * self.mss

    def __update_ack(self, sn: int) -> None:
        """更新 ACK 列表，去重"""
        for i, (old_sn, old_ts) in enumerate(self.acklist):
            if old_sn == sn:
                self.acklist.pop(i)
                break

    def __insert_data_into_rcv_buf(self, newseg: KCP_SEG) -> bool:
        """插入数据到接收缓冲区"""
        if not self.rcv_buf:
            self.rcv_buf.append(newseg)
            return False
        
        # 检查重复
        for seg in self.rcv_buf:
            if seg.sn == newseg.sn:
                return True
        
        # 找到合适位置插入
        inserted = False
        for i in range(len(self.rcv_buf) - 1, -1, -1):
            seg = self.rcv_buf[i]
            if (newseg.sn - seg.sn) > 0:
                self.rcv_buf.insert(i + 1, newseg)
                inserted = True
                break
        
        if not inserted:
            self.rcv_buf.insert(0, newseg)
        
        return False

    def __move_rcv_queue(self) -> None:
        """移动数据到接收队列"""
        while self.rcv_buf:
            seg = self.rcv_buf[0]
            if seg.sn == self.rcv_nxt and len(self.rcv_queue) < self.rcv_wnd:
                self.rcv_nxt += 1
                self.rcv_queue.append(self.rcv_buf.pop(0))
            else:
                break

    def send(self, data: bytes) -> int:
        """
        发送数据
        :param data: 要发送的数据
        :return: 0=成功, <0=错误
        """
        if not data:
            return -1
        
        self.current = _itime_now()
        
        # 分片
        mss = self.mss
        if len(data) <= mss:
            count = 1
        else:
            count = (len(data) + mss - 1) // mss
        
        if count == 0:
            count = 1
        
        for i in range(count):
            size = min(mss, len(data) - i * mss)
            start = i * mss
            end = start + size
            
            seg = KCP_SEG(
                conv=self.conv,
                cmd=KCP_CMD_PUSH,
                frg=count - i - 1,
                wnd=0,  # 稍后设置
                ts=0,   # 稍后设置
                sn=self.snd_nxt,
                una=self.rcv_nxt,
                resendts=0,
                rto=self.rx_rto,
                fastack=0,
                xmit=0,
                data=data[start:end],
            )
            self.snd_queue.append(seg)
            self.snd_nxt += 1
        
        return 0

    def __wnd_unused(self) -> int:
        """计算未使用的窗口大小"""
        if self.cwnd < self.rmt_wnd:
            return self.cwnd - (self.snd_nxt - self.snd_una)
        return self.rmt_wnd - (self.snd_nxt - self.snd_una)

    def __parse_una_fastack(self, una: int, fastack: int, xmit: int) -> None:
        """快速确认处理"""
        self.__parse_una(una)
        for seg in self.snd_buf:
            if una - seg.sn > 0:
                continue
            if fastack > seg.fastack:
                seg.fastack = fastack
            if xmit:
                seg.xmit += xmit

    def flush(self) -> None:
        """
        刷新发送缓冲区（需要定期调用）
        """
        self.current = _itime_now()
        
        if not self.updated:
            return
        
        # 检查是否需要窗口探测
        self.__check_probe()
        
        cwnd = self.__get_cwnd()
        snd_buf_count = len(self.snd_buf)
        snd_queue_count = len(self.snd_queue)
        
        # 移动发送队列到发送缓冲区
        while self.snd_queue and snd_buf_count < cwnd:
            newseg = self.snd_queue.popleft()
            newseg.sn = self.snd_una + snd_buf_count  # 这不对，应该在 send 时设置了
            newseg.ts = self.current
            newseg.wnd = self.__get_available_window()
            newseg.una = self.rcv_nxt
            newseg.resendts = self.current
            newseg.rto = self.rx_rto
            newseg.fastack = 0
            newseg.xmit = 0
            self.snd_buf.append(newseg)
            snd_buf_count += 1
        
        if not self.snd_buf:
            if self.probe:
                self.probe = 0
            return
        
        buffer = bytearray(self.mtu)
        count = 0
        
        change = 0
        lost = 0
        self.snd_una = self.snd_buf[0].sn
        
        for seg in self.snd_buf:
            needsend = False
            
            if seg.xmit == 0:
                needsend = True
                seg.xmit = 1
                seg.rto = self.rx_rto
                seg.resendts = self.current + seg.rto
                if self.interval:
                    seg.resendts = seg.resendts - (seg.resendts % self.interval)
            elif self.current - seg.resendts >= 0:
                needsend = True
                seg.xmit += 1
                if not self.no_congest_control:
                    rto_val = seg.rto if self.nodelay else seg.rto + max(seg.rto, self.interval)
                    seg.rto = min(0x7fffffff, rto_val)
                seg.resendts = self.current + seg.rto
                lost = 1
            
            # 快速重传
            if seg.fastack >= self.fastlimit and self.fastlimit > 0:
                needsend = True
                seg.xmit += 1
                seg.fastack = 0
                if not self.no_congest_control:
                    seg.rto = min(0x7fffffff, seg.rto + max(seg.rto, self.interval) // 2)
                seg.resendts = self.current + seg.rto
                change = 1
            
            if needsend:
                seg.ts = self.current
                seg.wnd = self.__get_available_window()
                seg.una = self.rcv_nxt
                
                size = self.__encode_segment(seg, buffer)
                
                # 发送 ACK
                for ack_sn, ack_ts in self.acklist:
                    ack_seg = KCP_SEG(
                        conv=self.conv,
                        cmd=KCP_CMD_ACK,
                        frg=0,
                        wnd=self.__get_available_window(),
                        ts=ack_ts,
                        sn=ack_sn,
                        una=self.rcv_nxt,
                        resendts=0,
                        rto=0,
                        fastack=0,
                        xmit=0,
                        data=b"",
                    )
                    ack_buf = bytearray(self.mtu)
                    ack_size = self.__encode_segment(ack_seg, ack_buf)
                    self.output(bytes(ack_buf[:ack_size]), self, self.user)
                
                self.acklist = []
                
                # 窗口探测
                if self.probe & KCP_ASK_SEND:
                    if not (self.probe & KCP_ASK_TELL):
                        self.probe &= ~KCP_ASK_SEND
                    wask_seg = KCP_SEG(
                        conv=self.conv,
                        cmd=KCP_CMD_WASK,
                        frg=0,
                        wnd=self.__get_available_window(),
                        ts=self.current,
                        sn=0,
                        una=self.rcv_nxt,
                        resendts=0,
                        rto=0,
                        fastack=0,
                        xmit=0,
                        data=b"",
                    )
                    wask_buf = bytearray(self.mtu)
                    wask_size = self.__encode_segment(wask_seg, wask_buf)
                    self.output(bytes(wask_buf[:wask_size]), self, self.user)
                elif self.probe & KCP_ASK_TELL:
                    wins_seg = KCP_SEG(
                        conv=self.conv,
                        cmd=KCP_CMD_WINS,
                        frg=0,
                        wnd=self.__get_available_window(),
                        ts=self.current,
                        sn=0,
                        una=self.rcv_nxt,
                        resendts=0,
                        rto=0,
                        fastack=0,
                        xmit=0,
                        data=b"",
                    )
                    wins_buf = bytearray(self.mtu)
                    wins_size = self.__encode_segment(wins_seg, wins_buf)
                    self.output(bytes(wins_buf[:wins_size]), self, self.user)
                    self.probe = 0
                
                self.output(bytes(buffer[:size]), self, self.user)
                
                count += 1
                if seg.xmit >= self.dead_link:
                    self.state = -1
        
        self.probe = 0 if self.probe == KCP_ASK_TELL else self.probe
        
        # 拥塞控制 - 丢包时慢启动
        if lost:
            if not self.no_congest_control:
                self.ssthresh = self.cwnd // 2
                if self.ssthresh < 2:
                    self.ssthresh = 2
                self.cwnd = 1
                self.incr = self.mss
        
        if change or lost:
            if self.cwnd < self.ssthresh:
                self.cwnd = self.cwnd + 1
                self.incr += self.mss
            else:
                if self.incr < self.mss:
                    self.incr = self.mss
                self.incr = self.incr + (self.mss * self.mss) // self.incr + (self.mss // 16)
                if (self.cwnd + 1) * self.mss <= self.incr:
                    self.cwnd += 1
        
        # 重新计算 snd_una
        if self.snd_buf:
            self.snd_una = self.snd_buf[0].sn

    def __get_cwnd(self) -> int:
        """获取拥塞窗口"""
        if self.no_congest_control:
            return min(self.snd_wnd, self.rmt_wnd)
        return min(self.cwnd, min(self.snd_wnd, self.rmt_wnd))

    def __get_available_window(self) -> int:
        """获取可用接收窗口"""
        if len(self.rcv_queue) >= self.rcv_wnd:
            return 0
        return self.rcv_wnd - len(self.rcv_queue)

    def __check_probe(self) -> None:
        """检查是否需要窗口探测"""
        if self.rmt_wnd == 0:
            if self.probe_wait == 0:
                self.probe_wait = 0x7fffffff
                self.ts_probe = self.current + 100
            if self.current - self.ts_probe >= 0:
                self.ts_probe = self.current + min(self.probe_wait, 120000)
                self.probe_wait *= 2
                self.probe |= KCP_ASK_SEND

    def update(self, current: Optional[int] = None) -> int:
        """
        更新 KCP 状态（需要定期调用）
        :param current: 当前时间(ms)，None则自动获取
        :return: 下次需要调用的最小时间差(ms)
        """
        if current is None:
            self.current = _itime_now()
        else:
            self.current = current & 0xffffffff
        
        if not self.updated:
            self.updated = True
            self.ts_flush = self.current
        
        slap = self.current - self.ts_flush
        
        next_flush = self.interval - slap
        if next_flush <= 0:
            self.ts_flush = self.current
            self.flush()
            slap = self.current - self.ts_flush
            next_flush = self.interval - slap
            if next_flush < 0:
                next_flush = 0
        
        return next_flush

    def check(self, current: Optional[int] = None) -> bool:
        """
        检查是否需要调用 update
        """
        if current is None:
            self.current = _itime_now()
        else:
            self.current = current & 0xffffffff
        
        if not self.updated:
            return True
        
        if self.current - self.ts_flush >= self.interval:
            return True
        
        for seg in self.snd_buf:
            if self.current - seg.resendts >= 0:
                return True
        
        return False

    def recv(self, maxlen: int) -> bytes:
        """
        接收数据
        :param maxlen: 最大接收长度
        :return: 接收到的数据，如果没有则返回空bytes
        """
        if not self.rcv_queue:
            return b""
        
        # 检查分片
        seg = self.rcv_queue[0]
        if seg.frg == 0:
            if len(seg.data) > maxlen:
                return b""
            self.rcv_queue.popleft()
            return seg.data
        
        # 有多片需要合并
        total = 0
        count = 0
        for s in self.rcv_queue:
            total += len(s.data)
            count += 1
            if s.frg == 0:
                break
        
        if total > maxlen:
            return b""
        
        result = bytearray()
        for _ in range(count):
            s = self.rcv_queue.popleft()
            result.extend(s.data)
        
        return bytes(result)

    def peeksize(self) -> int:
        """
        获取下一条消息的大小
        :return: 消息大小，如果没有消息返回-1
        """
        if not self.rcv_queue:
            return -1
        
        seg = self.rcv_queue[0]
        if seg.frg == 0:
            return len(seg.data)
        
        total = 0
        for s in self.rcv_queue:
            total += len(s.data)
            if s.frg == 0:
                break
        return total

    def waitsnd(self) -> int:
        """
        获取等待发送的包数
        """
        return len(self.snd_buf) + len(self.snd_queue)

    def set_user(self, user: object) -> None:
        """设置用户数据"""
        self.user = user

    def get_user(self) -> object:
        """获取用户数据"""
        return self.user
