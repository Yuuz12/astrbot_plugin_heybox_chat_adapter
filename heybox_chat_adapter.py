"""小黑盒 (HeyBoxChat) 平台适配器。

负责：
1. 启动小黑盒 WebSocket 客户端，接收事件。
2. 将小黑盒事件转换为 AstrBotMessage 并提交给 AstrBot 事件总线。
3. 提供 send_by_session 支持按会话直接发送消息。

参考：
- AstrBot 平台适配器文档: https://docs.astrbot.app/dev/plugin-platform-adapter.html
- 黑盒语音开发者文档: https://s.apifox.cn/43256fe4-9a8c-4f22-949a-74a3f8b431f5/llms.txt
"""

import asyncio
import json
import re
import time

from astrbot import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, AtAll, Image, Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.platform.astr_message_event import MessageSesion

from .heybox_chat_client import HeychatClient
from .heybox_chat_event import HeyboxChatEvent


@register_platform_adapter(
    "heychat",
    "小黑盒 (HeyBoxChat) 语音平台适配器",
    default_config_tmpl={
        "token": "",
    },
    adapter_display_name="小黑盒 (HeyBoxChat)",
)
class HeyboxChatAdapter(Platform):
    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings
        self.token = str(self.config.get("token", "")).strip()
        if not self.token:
            logger.error("[heychat] 未配置小黑盒机器人 token，适配器将无法连接服务端")
        self.client = HeychatClient(self.token, self._on_received)
        self.running = False
        # type 5002 艾特通知信号：msg_id -> {time, channel_id, user_id}
        # 用于关联随后的 type 5 消息事件，标记其艾特了机器人
        self._mention_signals: dict[str, dict] = {}

    def meta(self) -> PlatformMetadata:
        return PlatformMetadata(
            "heychat",
            "小黑盒 (HeyBoxChat) 语音平台适配器",
            id=self.config.get("id", "heychat"),
        )

    # ---------------------------------------------------------------
    # 生命周期
    # ---------------------------------------------------------------
    async def run(self) -> None:
        self.running = True
        if not self.token:
            self.record_error("未配置小黑盒机器人 token")
            logger.error("[heychat] 适配器因缺少 token 未启动")
            return
        logger.info("[heychat] 启动小黑盒适配器")
        try:
            await self.client.run()
        except asyncio.CancelledError:
            logger.info("[heychat] 适配器被取消")
        except Exception as e:
            logger.error(f"[heychat] 适配器运行异常: {e}")
        finally:
            self.running = False

    async def terminate(self) -> None:
        await self.client.close()

    async def send_by_session(
        self, session: MessageSesion, message_chain: MessageChain
    ) -> None:
        inner = AstrBotMessage()
        inner.type = session.message_type
        inner.session_id = session.session_id
        inner.message_str = message_chain.get_plain_text()
        event = self.create_event(inner)
        await event.send(message_chain)

    # ---------------------------------------------------------------
    # 事件接收与转换
    # ---------------------------------------------------------------
    async def _on_received(self, event: dict) -> None:
        event_type = str(event.get("type", ""))
        data = event.get("data", {})
        if not isinstance(data, dict):
            return
        msg_id = str(data.get("msg_id") or data.get("im_seq") or "")

        # type 5002：艾特通知事件。
        # 小黑盒在有人 @机器人 时推送该通知（不含消息正文），随后通常会推送
        # 带正文的 type 5 消息事件。此处记录信号用于关联；若短时间内未收到
        # 对应正文，则作为"空艾特"提交，触发 AstrBot 的空艾特等待逻辑。
        if event_type == "5002":
            logger.info(
                f"[heychat] 收到艾特通知 type=5002: "
                f"{json.dumps(data, ensure_ascii=False)}"
            )
            self._record_mention_signal(data, msg_id)
            return

        # 消息事件判定：type 5/1/50，或任何包含消息内容且带消息 ID 的事件
        if event_type not in ("5", "1", "50"):
            if not (msg_id and (data.get("msg") or data.get("command_info"))):
                logger.info(f"[heychat] 忽略类型 {event_type} 的事件")
                return
        logger.info(
            f"[heychat] 收到消息事件 type={event_type}: "
            f"{json.dumps(event, ensure_ascii=False)}"
        )
        if not msg_id:
            logger.debug("[heychat] 事件缺少 msg_id，忽略")
            return
        if self.client.is_duplicate(msg_id):
            logger.debug(f"[heychat] 重复消息已忽略: {msg_id}")
            return
        try:
            abm = await self.convert_message(event)
            await self.handle_msg(abm)
        except Exception as e:
            logger.error(f"[heychat] 消息转换/处理异常: {e}")

    async def convert_message(self, event: dict) -> AstrBotMessage:
        event_type = str(event.get("type", ""))
        data = event.get("data", {}) or {}

        abm = AstrBotMessage()
        abm.raw_message = event
        abm.self_id = self.client.bot_id or ""

        # 不同类型事件的字段来源不同：
        # type 50（命令）使用嵌套的 room_base_info/channel_base_info/sender_info；
        # type 5/1（普通消息）字段多为扁平结构（channel_id、avatar 等）。
        room_info = data.get("room_base_info") or data.get("room_info") or {}
        channel_info = data.get("channel_base_info") or data.get("channel_info") or {}
        sender_info = (
            data.get("sender_info")
            or data.get("user_base_info")
            or data.get("user_info")
            or {}
        )
        room_id = str(room_info.get("room_id") or data.get("room_id") or "")
        channel_id = str(
            channel_info.get("channel_id") or data.get("channel_id") or ""
        )
        user_id = str(sender_info.get("user_id") or data.get("user_id") or "")
        nickname = (
            sender_info.get("nickname")
            or sender_info.get("name")
            or data.get("nickname")
            or "未知用户"
        )
        msg_id = str(data.get("msg_id") or data.get("im_seq") or "")

        # 群聊/私聊判断：小黑盒中私聊的 room_id 与 channel_id 相同；
        # 若 room_id 缺失但有 channel_id，按群聊（频道消息）处理，避免误判私聊
        if room_id and channel_id:
            is_group = room_id != channel_id
        else:
            is_group = bool(channel_id)

        if is_group:
            abm.type = MessageType.GROUP_MESSAGE
            abm.group_id = channel_id
            # 群聊会话 ID 使用 "room_id:channel_id"，以便发送时还原
            abm.session_id = f"{room_id}:{channel_id}"
        else:
            abm.type = MessageType.FRIEND_MESSAGE
            abm.group_id = ""
            abm.session_id = user_id or room_id or channel_id

        abm.sender = MessageMember(user_id=user_id, nickname=nickname)
        abm.message_id = msg_id

        send_time = data.get("send_time") or event.get("timestamp")
        if isinstance(send_time, (int, float)) and send_time > 0:
            abm.timestamp = int(send_time / 1000)

        if event_type == "50" or data.get("command_info"):
            self._convert_command_data(data, abm)
        else:
            self._convert_message_data(data, abm)

        # 若 type 5002 艾特信号匹配此消息，补上 At(self_id) 以触发唤醒。
        # 这样无论用户用 @{id:xxx} 还是 @昵称 艾特机器人，都能被识别。
        if self.client.bot_id and self._consume_mention_signal(
            msg_id, channel_id, user_id
        ):
            logger.info(
                f"[heychat] 消息 {msg_id} 匹配艾特信号，标记为艾特机器人"
            )
            has_at_self = any(
                isinstance(c, At) and str(c.qq) == str(self.client.bot_id)
                for c in abm.message
            )
            if not has_at_self:
                abm.message.insert(0, At(qq=self.client.bot_id, name=""))

        return abm

    def _convert_command_data(self, data: dict, abm: AstrBotMessage) -> None:
        """转换 Bot 命令事件 (type 50)。

        小黑盒命令通过 `/命令名 参数...` 触发，WebSocket 推送的是命令结构。
        这里将其还原为 AstrBot 可识别的命令文本。

        注意：当用户仅是 @机器人 而未真正触发命令时，小黑盒也会推送 type 50，
        但 command_info 为空（name/options 为空），此时回退为普通消息解析。
        """
        cmd = data.get("command_info") or {}
        name = str(cmd.get("name") or "")
        options = cmd.get("options") or []
        if not name and not options:
            self._convert_message_data(data, abm)
            return
        parts = [name]
        for opt in options:
            value = opt.get("value")
            if value is not None:
                parts.append(str(value))
        text = " ".join(parts).strip()
        abm.message_str = text
        abm.message = [Plain(text=text)]

    def _convert_message_data(self, data: dict, abm: AstrBotMessage) -> None:
        """转换普通频道消息事件 (type 5/1)。"""
        raw_msg = str(data.get("msg") or "")
        components, message_str = self._parse_rich_message(raw_msg)

        # 纯图片消息（msg 中未内嵌图片时）：img 字段或 addition.img_files_info
        if not any(isinstance(c, Image) for c in components):
            img_url = str(data.get("img") or "")
            if not img_url:
                try:
                    addition = json.loads(str(data.get("addition") or "{}"))
                except (json.JSONDecodeError, TypeError):
                    addition = {}
                img_files = addition.get("img_files_info") or []
                if img_files and isinstance(img_files[0], dict):
                    img_url = str(img_files[0].get("url") or "")
            if img_url:
                components.append(Image(file=img_url, url=img_url))

        if not components:
            components.append(Plain(text=message_str))

        abm.message = components
        abm.message_str = message_str

    @staticmethod
    def _parse_rich_message(msg: str) -> tuple[list, str]:
        """解析小黑盒消息中的 @ 语法与 markdown 图片为 AstrBot 组件。

        支持的语法：
        - @{id:用户ID} / @{id:all} → At / AtAll
        - ![](https://...) → Image
        """
        if not msg:
            return [], ""
        pattern = re.compile(
            r"@\{id:(?:(\d+)|(all))\}|!\[[^\]]*\]\((https?://[^\s)]+)\)"
        )
        components: list = []
        text_buf: list[str] = []
        cursor = 0
        for m in pattern.finditer(msg):
            if m.start() > cursor:
                text_buf.append(msg[cursor : m.start()])
            if text_buf:
                joined = "".join(text_buf)
                if joined.strip():
                    components.append(Plain(text=joined))
                text_buf = []
            if m.group(1):
                components.append(At(qq=m.group(1), name=""))
            elif m.group(2):
                components.append(AtAll())
            elif m.group(3):
                components.append(Image(file=m.group(3), url=m.group(3)))
            cursor = m.end()
        if cursor < len(msg):
            text_buf.append(msg[cursor:])
        if text_buf:
            joined = "".join(text_buf)
            if joined.strip():
                components.append(Plain(text=joined))
        # 纯文本：移除 @ 与图片标记，供 LLM 使用
        message_str = re.sub(
            r"@\{id:(?:[^}]*)\}|!\[[^\]]*\]\([^)]*\)", "", msg
        ).strip()
        return components, message_str

    # ---------------------------------------------------------------
    # 事件提交
    # ---------------------------------------------------------------
    def create_event(self, message: AstrBotMessage) -> HeyboxChatEvent:
        session_id = message.session_id
        if message.type == MessageType.GROUP_MESSAGE:
            # 群聊 session_id 形如 "room_id:channel_id"
            if ":" in session_id:
                room_id, channel_id = session_id.split(":", 1)
            else:
                room_id = channel_id = message.group_id or session_id
        else:
            room_id = ""
            channel_id = ""
        return HeyboxChatEvent(
            message_str=message.message_str,
            message_obj=message,
            platform_meta=self.meta(),
            session_id=session_id,
            client=self.client,
            room_id=room_id,
            channel_id=channel_id,
            message_type=message.type,
            reply_with_mention=self.settings.get("reply_with_mention", False),
            reply_with_quote=self.settings.get("reply_with_quote", False),
        )

    # ---------------------------------------------------------------
    # type 5002 艾特通知处理
    # ---------------------------------------------------------------
    def _record_mention_signal(self, data: dict, msg_id: str) -> None:
        """记录 type 5002 艾特通知信号，用于关联后续消息事件。"""
        if not msg_id:
            return
        now = time.time()
        # 清理 30 秒前的残留信号，防止字典无限增长
        stale = [
            mid
            for mid, info in self._mention_signals.items()
            if now - info.get("time", 0) > 30
        ]
        for mid in stale:
            del self._mention_signals[mid]
        self._mention_signals[msg_id] = {
            "time": now,
            "channel_id": str(data.get("channel_id") or ""),
            "user_id": str(data.get("user_id") or ""),
        }
        asyncio.create_task(self._mention_fallback(msg_id, data))

    async def _mention_fallback(self, msg_id: str, notify_data: dict) -> None:
        """艾特通知兜底：3 秒内未收到对应正文消息则提交空艾特。

        纯空艾特（只 @机器人 没说话）时不会有 type 5 事件，此时构造一条
        仅含 At(self_id) 的消息，触发 AstrBot 的 empty_mention_waiting 逻辑。
        """
        await asyncio.sleep(3)
        if msg_id not in self._mention_signals:
            return  # 已被 type 5 消息消费
        del self._mention_signals[msg_id]
        logger.info(f"[heychat] 艾特通知 {msg_id} 未匹配到正文，提交空艾特消息")
        try:
            abm = self._build_mention_only_message(notify_data, msg_id)
            await self.handle_msg(abm)
        except Exception as e:
            logger.error(f"[heychat] 空艾特消息提交异常: {e}")

    def _consume_mention_signal(
        self, msg_id: str, channel_id: str, user_id: str
    ) -> bool:
        """检查并消费艾特信号。

        先按 msg_id 精确匹配；若不匹配（type 5002 与 type 5 的消息 ID 字段不同），
        再按 channel_id + user_id + 5 秒时间窗口模糊匹配。
        """
        now = time.time()
        if msg_id and msg_id in self._mention_signals:
            del self._mention_signals[msg_id]
            return True
        for mid, info in list(self._mention_signals.items()):
            if (
                info.get("channel_id") == channel_id
                and info.get("user_id") == user_id
                and now - info.get("time", 0) < 5
            ):
                del self._mention_signals[mid]
                return True
        return False

    def _build_mention_only_message(
        self, notify_data: dict, msg_id: str
    ) -> AstrBotMessage:
        """根据 type 5002 艾特通知构造空艾特消息（仅含 At(self_id)）。"""
        abm = AstrBotMessage()
        abm.raw_message = {"type": "5002", "data": notify_data}
        abm.self_id = self.client.bot_id or ""

        room_id = str(notify_data.get("room_id") or "")
        channel_id = str(notify_data.get("channel_id") or "")
        user_id = str(notify_data.get("user_id") or "")
        nickname = notify_data.get("nickname") or "未知用户"

        is_group = (bool(room_id) and bool(channel_id) and room_id != channel_id) or (
            not room_id and bool(channel_id)
        )

        if is_group:
            abm.type = MessageType.GROUP_MESSAGE
            abm.group_id = channel_id
            abm.session_id = f"{room_id}:{channel_id}"
        else:
            abm.type = MessageType.FRIEND_MESSAGE
            abm.group_id = ""
            abm.session_id = user_id or room_id or channel_id

        abm.sender = MessageMember(user_id=user_id, nickname=nickname)
        abm.message_id = msg_id

        send_time = notify_data.get("send_time")
        if isinstance(send_time, (int, float)) and send_time > 0:
            abm.timestamp = int(send_time / 1000)

        abm.message = [At(qq=self.client.bot_id or "", name="")]
        abm.message_str = ""
        return abm

    async def handle_msg(self, message: AstrBotMessage) -> None:
        self.commit_event(self.create_event(message))
