"""小黑盒 (HeyBoxChat) 平台客户端。

包含两部分：
1. WebSocket 客户端：连接黑盒语音服务端，接收事件，心跳保活，断线自动重连。
2. HTTP 客户端：发送频道/私聊消息、上传媒体文件。

接口参考：
- 黑盒语音开发者文档: https://s.apifox.cn/43256fe4-9a8c-4f22-949a-74a3f8b431f5/llms.txt
- 官方 Demo: https://github.com/QingFengOpen/HeychatDemo
"""

import asyncio
import base64
import json
import random
import ssl
import time
import urllib.parse
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from astrbot import logger

# 黑盒语音服务端地址与通用参数
HEYCHAT_WSS_URL = "wss://chat.xiaoheihe.cn/chatroom/ws/connect"
HEYCHAT_HTTP_HOST = "https://chat.xiaoheihe.cn"
HEYCHAT_COMMON_PARAMS = (
    "chat_os_type=bot&client_type=heybox_chat&chat_version=999.0.0&chat_version=1.24.5"
)

# 心跳周期（秒），官方推荐 30s
PING_INTERVAL = 30
# 断线重连退避
RECONNECT_BASE_DELAY = 5
RECONNECT_MAX_DELAY = 60
# 消息去重缓存上限
MAX_MSG_ID_CACHE = 2048


class HeychatMessageType:
    """频道消息 msg_type 取值。"""

    TEXT = 1
    IMAGE = 3
    MARKDOWN = 4
    AT_MARKDOWN = 10
    CARD = 20


class HeychatClient:
    """小黑盒 WebSocket + HTTP 客户端。"""

    def __init__(
        self,
        token: str,
        on_message: Callable[[dict], Awaitable[None]],
    ) -> None:
        self.token = token
        self.on_message = on_message

        # 机器人自身 ID 与名称（name 暂未使用）
        self.bot_id: str = ""
        self.bot_name: str = ""

        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._http: aiohttp.ClientSession | None = None
        self._running = False
        self._ack_counter = int(time.time() * 1000) % (10**12)
        self._processed_msg_ids: deque[str] = deque(maxlen=MAX_MSG_ID_CACHE)

        # 机器人自身 ID：优先从 token 解码（token 为 base64，第一段为 bot_id），
        # 连接后若收到 notify 事件也会更新。
        self.bot_id = self._parse_bot_id_from_token(token)
        if self.bot_id:
            logger.info(f"[heychat] 从 token 解析出机器人 ID: {self.bot_id}")

    @staticmethod
    def _parse_bot_id_from_token(token: str) -> str:
        """从 token 解码机器人 ID。

        token 格式: base64("bot_id;...")，第一段为机器人 ID。
        """
        try:
            decoded = base64.b64decode(token).decode("utf-8", errors="ignore")
            return decoded.split(";", 1)[0].strip()
        except Exception:
            return ""

    # ---------------------------------------------------------------
    # WebSocket 连接管理
    # ---------------------------------------------------------------
    def _wss_url(self) -> str:
        token = urllib.parse.quote(self.token, safe="")
        return f"{HEYCHAT_WSS_URL}?{HEYCHAT_COMMON_PARAMS}&token={token}"

    def _next_ack_id(self) -> str:
        """生成全局自增的 heychat_ack_id，防止消息重复。"""
        self._ack_counter += 1
        return str(self._ack_counter)

    def is_duplicate(self, msg_id: str) -> bool:
        """检查消息 ID 是否已处理过（WebSocket 断线重连可能导致重复推送）。"""
        if msg_id in self._processed_msg_ids:
            return True
        self._processed_msg_ids.append(msg_id)
        return False

    async def run(self) -> None:
        """主运行循环：连接、监听、异常时指数退避重连。"""
        self._running = True
        delay = RECONNECT_BASE_DELAY
        while self._running:
            try:
                await self._connect_once()
                # 连接断开（正常或异常）后进入重连
                if self._running:
                    logger.warning("[heychat] WebSocket 连接已断开，准备重连...")
                delay = RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"[heychat] WebSocket 连接异常: {e}")
            if not self._running:
                break
            await asyncio.sleep(delay + random.uniform(0, 1))
            delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def _connect_once(self) -> None:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        logger.info(f"[heychat] 正在连接黑盒语音 WebSocket...")
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(
                self._wss_url(),
                ssl=ssl_ctx,
                headers=headers,
                heartbeat=None,
            ) as ws:
                self._ws = ws
                logger.info("[heychat] 黑盒语音 WebSocket 已连接")
                # 心跳任务
                ping_task = asyncio.create_task(self._heartbeat_loop())
                try:
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_raw_message(raw)
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass
                self._ws = None

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            try:
                if self._ws and not self._ws.closed:
                    await self._ws.send_str("PING")
            except Exception as e:
                logger.error(f"[heychat] 心跳发送失败: {e}")

    async def _handle_raw_message(self, raw: str) -> None:
        text = raw if isinstance(raw, str) else raw.data
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="replace")
        # 心跳响应 PONG，忽略
        if text.startswith("PONG"):
            return
        try:
            event = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            logger.warning(f"[heychat] 无法解析的服务端消息: {text[:200]}")
            return
        await self._dispatch_event(event)

    async def _dispatch_event(self, event: dict) -> None:
        event_type = str(event.get("type", ""))
        data = event.get("data", {}) or {}

        # notify 事件（event: 80）为心跳/状态通知，不包含消息内容，
        # 但其 userid 即为机器人自身 ID，用于 self_id。
        if isinstance(data, dict) and (
            data.get("event") == "80" or event_type.lower() == "notify"
        ):
            if data.get("userid") and not self.bot_id:
                self.bot_id = str(data["userid"])
                logger.info(f"[heychat] 已获知机器人自身 ID: {self.bot_id}")
            return

        # PUSH 包装结构，递归处理内部事件
        if event_type.upper() == "PUSH":
            if isinstance(data, dict):
                await self._dispatch_event(data)
            return

        if not isinstance(data, dict):
            return
        logger.info(
            f"[heychat] 收到 WebSocket 事件: type={event_type}, "
            f"data={json.dumps(data, ensure_ascii=False)[:1000]}"
        )
        await self.on_message(event)

    async def close(self) -> None:
        """关闭 WebSocket 与 HTTP 会话。"""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._http:
            try:
                await self._http.close()
            except Exception:
                pass
            self._http = None

    # ---------------------------------------------------------------
    # HTTP 接口
    # ---------------------------------------------------------------
    async def _http_session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession()
        return self._http

    async def _post_json(self, path: str, payload: dict) -> dict:
        """发送 JSON 请求，返回解析后的响应 dict。"""
        url = f"{HEYCHAT_HTTP_HOST}{path}?{HEYCHAT_COMMON_PARAMS}"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "token": self.token,
        }
        session = await self._http_session()
        async with session.post(url, headers=headers, json=payload) as resp:
            body = await resp.json(content_type=None)
        if body.get("status") != "ok":
            raise RuntimeError(
                f"[heychat] 接口 {path} 调用失败: {body.get('status')} {body.get('msg')}"
            )
        return body

    async def send_channel_message(
        self,
        room_id: str,
        channel_id: str,
        msg: str,
        msg_type: int = HeychatMessageType.MARKDOWN,
        reply_id: str = "",
        img: str = "",
        addition: str = "{}",
        at_user_id: str = "",
        at_role_id: str = "",
        mention_channel_id: str = "",
    ) -> None:
        """发送频道消息。

        官方接口：POST /chatroom/v2/channel_msg/send
        msg_type: 1=文本 3=图片 4=markdown 10=支持@的markdown
        """
        payload = {
            "msg": msg,
            "msg_type": msg_type,
            "heychat_ack_id": self._next_ack_id(),
            "reply_id": reply_id,
            "room_id": room_id,
            "addition": addition,
            "at_user_id": at_user_id,
            "at_role_id": at_role_id,
            "mention_channel_id": mention_channel_id,
            "channel_id": channel_id,
            "channel_type": 1,
        }
        if img:
            payload["img"] = img
        await self._post_json("/chatroom/v2/channel_msg/send", payload)

    async def send_private_message(
        self,
        to_user_id: str,
        msg: str,
        msg_type: int = HeychatMessageType.MARKDOWN,
        img: str = "",
    ) -> None:
        """给用户发送私聊消息。

        官方接口：POST /chatroom/v2/direct_msg/send
        私聊仅支持 markdown(msg_type=4) 和图片(msg_type=3)。
        """
        try:
            to_uid = int(to_user_id)
        except (ValueError, TypeError) as e:
            raise RuntimeError(
                f"[heychat] 无效的私聊用户 ID: {to_user_id!r}"
            ) from e
        payload = {
            "msg": msg,
            "msg_type": msg_type,
            "heychat_ack_id": self._next_ack_id(),
            "addition": "{}",
            "to_user_id": to_uid,
        }
        if img:
            payload["img"] = img
        await self._post_json("/chatroom/v2/direct_msg/send", payload)

    async def upload_media(self, file_path: str) -> str:
        """上传媒体文件，返回 CDN 地址。

        官方接口：POST /upload
        """
        url = (
            f"{HEYCHAT_HTTP_HOST}/upload?"
            f"client_type=heybox_chat&x_client_type=web&os_type=web&x_os_type=bot"
            f"&x_app=heybox_chat&chat_os_type=bot&chat_version=1.30.0"
        )
        headers = {"token": self.token}
        session = await self._http_session()
        with open(file_path, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("file", f, filename=file_path.split("/")[-1].split("\\")[-1])
            async with session.post(url, headers=headers, data=form) as resp:
                body = await resp.json(content_type=None)
        if body.get("status") != "ok":
            raise RuntimeError(
                f"[heychat] 上传媒体失败: {body.get('status')} {body.get('msg')}"
            )
        return body["result"]["url"]
