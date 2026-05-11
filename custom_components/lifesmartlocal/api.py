"""LifeSmart API implementation."""
import asyncio
import socket
import json
import time
import hashlib
import struct
import logging
from collections.abc import Callable
from typing import Any, Dict, Optional, Tuple
from .const import API_PORT, REMARK, CMD_NOTIFY, CMD_REPORT, CMD_SET

_LOGGER = logging.getLogger(__name__)

class LifeSmartAPI:
    def __init__(self, host: str, model: str, token: str, timeout: int = 5, local_port: int = 12346):
        self.host = host
        self.model = model
        self.token = token
        self.sequence = 1
        self.timeout = timeout
        self.local_port = local_port
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional["_LifeSmartDatagramProtocol"] = None
        self._pending: Dict[int, asyncio.Future] = {}
        self._report_listeners: set[Callable[[Dict[str, Any]], None]] = set()
        self._state_listeners: Dict[Tuple[str, str], set[Callable[[Any], None]]] = {}
        ###
        self.gateway_available = True
        self._gw_listeners: set[Callable[[bool], None]] = set()
        ###

    def _create_signature(self, obj: str, args: Dict[str, Any], ts: int) -> str:
        sortable_items = []
        for k, v in args.items():
            # 終極規範對齊：API 嚴格規定 Array(list) 與 Object(dict) 絕對不能參與簽名！
            if isinstance(v, (list, tuple, dict)):
                continue
            if isinstance(v, bool):
                v = str(v).lower()
            sortable_items.append((k, v))
            
        sorted_args = sorted(sortable_items)
        args_string = ",".join(f"{k}:{v}" for k, v in sorted_args)
        
        if args_string:
            base_string = f"obj:{obj},{args_string},ts:{ts},model:{self.model},token:{self.token}"
        else:
            base_string = f"obj:{obj},ts:{ts},model:{self.model},token:{self.token}"
            
        return hashlib.md5(base_string.encode()).hexdigest()

    def _create_message(self, obj: str, args: Dict[str, Any], pkg_type: int, msg_id: int) -> bytes:
        ts = int(time.time())
        sign = self._create_signature(obj, args, ts)

        body = {
            "sys": {
                "ver": 1,
                "sign": sign,
                "model": self.model,
                "ts": ts
            },
            "id": msg_id,
            "obj": obj,
            "args": args
        }

        body_json = json.dumps(body).encode('utf-8')
        header = struct.pack('>2sHHI', 
                           REMARK.encode(),
                           0,
                           pkg_type,
                           len(body_json))

        return header + body_json

    async def async_start(self) -> None:
        if self._transport is not None:
            return
        
        import socket
        import asyncio
        loop = asyncio.get_running_loop()
        protocol = _LifeSmartDatagramProtocol(self)
        
        # 手動建立 Socket 並放大緩衝區 (防大型封包截斷)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 262144)
        
        # 智慧 Port 綁定演算法：
        # 優先嘗試 LifeSmart 官方預設的 12348 (相容性最高)，若被佔用則 fallback 到 0 (讓 OS 動態分配)
        try:
            sock.bind(("0.0.0.0", 12348))
        except OSError:
            sock.bind(("0.0.0.0", 0))

        transport, _ = await loop.create_datagram_endpoint(
            lambda: protocol,
            sock=sock,
        )
        self._transport = transport
        self._protocol = protocol
        
        # 動態取得最終成功綁定的 Port，並用於後續向網關註冊
        sockname = transport.get_extra_info("sockname")
        if isinstance(sockname, tuple) and len(sockname) >= 2:
            self.local_port = int(sockname[1])
    
    
    async def async_stop(self) -> None:
        if self._transport is None:
            return
        transport = self._transport
        self._transport = None
        self._protocol = None
        try:
            transport.close()
        finally:
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.cancel()
            self._pending.clear()
            
            # 終極記憶體防護：徹底清除所有註冊的監聽器，避免 Reload 造成的記憶體洩漏
            if hasattr(self, '_report_listeners'):
                self._report_listeners.clear()
            if hasattr(self, '_state_listeners'):
                self._state_listeners.clear()
            if hasattr(self, '_gw_listeners'):
                self._gw_listeners.clear()


    def register_report_listener(self, listener: Callable[[Dict[str, Any]], None]) -> Callable[[], None]:
        self._report_listeners.add(listener)
        def _unsub() -> None:
            self._report_listeners.discard(listener)
        return _unsub

    def register_state_listener(self, me: str, idx: str, listener: Callable[[Any], None]) -> Callable[[], None]:
        key = (me, idx)
        listeners = self._state_listeners.setdefault(key, set())
        listeners.add(listener)
        def _unsub() -> None:
            bucket = self._state_listeners.get(key)
            if not bucket:
                return
            bucket.discard(listener)
            if not bucket:
                self._state_listeners.pop(key, None)
        return _unsub

    def register_gw_listener(self, listener: Callable[[bool], None]) -> Callable[[], None]:
        """註冊網關存活狀態監聽器"""
        self._gw_listeners.add(listener)
        def _unsub() -> None:
            self._gw_listeners.discard(listener)
        return _unsub

    def set_gateway_status(self, status: bool) -> None:
        """更新網關狀態並廣播給所有實體"""
        if self.gateway_available != status:
            self.gateway_available = status
            _LOGGER.debug("Gateway availability changed to: %s", status)
            for listener in tuple(self._gw_listeners):
                try:
                    listener(status)
                except Exception as err:
                    _LOGGER.debug("Gateway listener error: %s", err)    
    
    async def send_command(self, obj: str, args: Dict[str, Any], pkg_type: int, timeout: Optional[float] = None) -> Dict[str, Any]:
        await self.async_start()
        if self._transport is None:
            raise RuntimeError("UDP transport not started")

        msg_id = self.sequence
        self.sequence += 1
        message = self._create_message(obj, args, pkg_type, msg_id)

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending[msg_id] = fut
        try:
            self._transport.sendto(message, (self.host, API_PORT))
            response = await asyncio.wait_for(fut, timeout=timeout or self.timeout)
            return response
        finally:
            self._pending.pop(msg_id, None)
    

    async def send_command_oneshot(self, obj: str, args: Dict[str, Any], pkg_type: int, timeout: Optional[float] = None) -> Dict[str, Any]:
        msg_id = self.sequence
        self.sequence += 1
        message = self._create_message(obj, args, pkg_type, msg_id)
        to = timeout or self.timeout

        def _send_recv() -> Dict[str, Any]:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.settimeout(to)
                sock.sendto(message, (self.host, API_PORT))
                data, _ = sock.recvfrom(65535)
                if len(data) < 10:
                    raise ValueError("Short UDP reply")
                _, _, _, pkg_size = struct.unpack(">2sHHI", data[:10])
                body = data[10:10 + pkg_size]
                return json.loads(body.decode("utf-8"))

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _send_recv)

    async def discover_devices(self):
        args = {"me": "2d02"}
        return await self.send_command("eps", args, 1)

    async def get_remote_list(self) -> Dict[str, Any]:
        """Retrieve IR remote list for devices."""
        args = {
            "cmd": "getlist"
        }
        response = await self.send_command_oneshot("spotremote", args, 3)
        
        if response and response.get("code") == 0 and "msg" in response:
            remote_list = response["msg"]
            all_keys = []
            
            for remote in remote_list:
                if "id" in remote:
                    keys = await self.get_remote_keys(remote["id"])
                    if keys and keys.get("code") == 0 and "msg" in keys:
                        all_keys.append({"remote": remote, "keys": keys["msg"]})
            return all_keys
        return response

    async def configure_event_service(self, host: str, port: int) -> Dict[str, Any]:
        args = {"cfg": "notify", "host": host, "port": port}
        return await self.send_command("config", args, CMD_SET)

    async def get_remote_keys(self, remote_id: str) -> Dict[str, Any]:
        """Retrieve IR remote keys for a specific device."""
        args = {
            "id": remote_id,
            "cmd": "getkeys"
        }
        return await self.send_command_oneshot("spotremote", args, 3)

    async def send_remote_key(self, remote_id: str, key: str) -> Dict[str, Any]:
        """Send IR remote key command."""
        args = {
            "id": remote_id,
            "cmd": "sendkey",
            "key": key
        }
        return await self.send_command_oneshot("spotremote", args, 3)

    def _handle_datagram(self, data: bytes) -> None:
        if len(data) < 10:
            return
        try:
            remark, _, pkg_type, pkg_size = struct.unpack(">2sHHI", data[:10])
            if remark.decode(errors="ignore") != REMARK:
                return
            body = data[10:10 + pkg_size]
            #message = json.loads(body.decode("utf-8"))
            # 修正盲區三：過濾結尾可能的 \x00 空字元與換行符，徹底消滅 JSONDecodeError
            clean_body = body.decode("utf-8", errors="ignore").strip('\x00\r\n\t ')
            message = json.loads(clean_body)
            # 開啟 X 光機：攔截並印出抵達的封包 (如果覺得太吵，未來可以改為 .debug)
            #logging.getLogger(__name__).warning("【UDP 攔截】收到推播: %s", message)
        except Exception as err:
            _LOGGER.debug("Failed to parse UDP datagram: %s", err)
            return

        msg_id = message.get("id")
        if isinstance(msg_id, int):
            fut = self._pending.get(msg_id)
            if fut is not None and not fut.done():
                fut.set_result(message)
                return

        if pkg_type in (CMD_REPORT, CMD_NOTIFY):
            for listener in tuple(self._report_listeners):
                try:
                    listener(message)
                except Exception as err:
                    _LOGGER.debug("Report listener failed: %s", err)

        for me, idx, val in _extract_state_changes(message):
            bucket = self._state_listeners.get((me, idx))
            if not bucket:
                continue
            for listener in tuple(bucket):
                try:
                    listener(val)
                except Exception as err:
                    _LOGGER.debug("State listener failed: %s", err)

def _extract_state_changes(message: Dict[str, Any]) -> list[tuple[str, str, Any]]:
    out: list[tuple[str, str, Any]] = []

    # 修復 1：優先攔截 Root 最外層的 chg 推播 (實體按鍵與 Apple Home 觸發)
    chg_list = message.get("chg")
    
    # 如果 Root 沒有，再去找 msg 裡面有沒有 chg (防禦舊版韌體)
    if not isinstance(chg_list, list):
        msg = message.get("msg")
        if isinstance(msg, dict):
            chg_list = msg.get("chg")

    # 處理所有的 chg 推播
    if isinstance(chg_list, list):
        for change in chg_list:
            if not isinstance(change, dict):
                continue
            c_me = change.get("me")
            if not isinstance(c_me, str):
                continue

            # 攔截斷線狀態
            if "stat" in change:
                out.append((c_me, "stat", change["stat"]))

            # 遍歷所有的通道 (例如 L1, L2, P1 等)
            for k, v in change.items():
                if k in ("me", "agt", "agtid", "devtype", "fulltype", "stat", "id"):
                    continue
                if isinstance(v, dict):
                    # 雙標修復：同時認得 v 與 val
                    val = v.get("v") if "v" in v else v.get("val")
                    if val is not None:
                        out.append((c_me, str(k), val))
                elif isinstance(v, (int, float, str)):
                    out.append((c_me, str(k), v))

    # 修復 2：處理 msg 內部的單一狀態 (通常是 HA 主動 GET 查詢的回傳)
    msg = message.get("msg")
    if isinstance(msg, dict):
        if "stat" in msg:
            out.append((msg.get("me", ""), "stat", msg["stat"]))

        me = msg.get("me")
        idx = msg.get("idx")
        if isinstance(me, str) and isinstance(idx, str):
            data = msg.get("data")
            if isinstance(data, dict) and isinstance(data.get(idx), dict):
                idx_data = data[idx]
                val = idx_data.get("v") if "v" in idx_data else idx_data.get("val")
                if val is not None:
                    out.append((me, idx, val))
            else:
                val = msg.get("v") if "v" in msg else msg.get("val")
                if val is not None:
                    out.append((me, idx, val))

    return out
    
class _LifeSmartDatagramProtocol(asyncio.DatagramProtocol):
    def __init__(self, api: LifeSmartAPI) -> None:
        self._api = api

    def datagram_received(self, data: bytes, addr) -> None:
        self._api._handle_datagram(data)
