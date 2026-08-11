"""小黑盒 (HeyBoxChat) 平台消息事件。

继承 AstrMessageEvent，实现 send() 将 AstrBot 消息链转换为小黑盒平台消息。
"""

import asyncio
import re
from collections.abc import AsyncGenerator

from astrbot import logger
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, AtAll, Image, Plain, Record, Reply, Video
from astrbot.api.platform import AstrBotMessage, PlatformMetadata
from astrbot.core.platform import MessageType

from .heybox_chat_client import HeychatClient, HeychatMessageType


class HeyboxChatEvent(AstrMessageEvent):
    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        client: HeychatClient,
        room_id: str,
        channel_id: str,
        message_type: MessageType,
        reply_with_mention: bool = False,
        reply_with_quote: bool = False,
    ):
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.client = client
        self.room_id = room_id
        self.channel_id = channel_id
        self.astrbot_message_type = message_type
        self.reply_with_mention = reply_with_mention
        self.reply_with_quote = reply_with_quote

    # ---------------------------------------------------------------
    # 消息链解析
    # ---------------------------------------------------------------
    def _parse_chain(self, message: MessageChain) -> list[tuple]:
        """将消息链解析为有序段列表。

        段类型：
        - ("text", text)
        - ("at", user_id)
        - ("at_all",)
        - ("image", file)
        - ("reply", message_id)
        - ("unsupported", 组件类型)  # 记录/视频等暂不支持的组件
        """
        segments: list[tuple] = []
        for item in message.chain:
            if isinstance(item, Plain):
                if item.text:
                    segments.append(("text", item.text))
            elif isinstance(item, At):
                segments.append(("at", str(item.qq)))
            elif isinstance(item, AtAll):
                segments.append(("at_all",))
            elif isinstance(item, Image):
                segments.append(("image", item))
            elif isinstance(item, Reply):
                if item.id:
                    segments.append(("reply", item.id))
            elif isinstance(item, (Record, Video)):
                logger.warning(
                    f"[heychat] 小黑盒暂不支持发送 {item.__class__.__name__} 组件，已忽略。"
                )
            else:
                logger.debug(
                    f"[heychat] 忽略消息组件: {item.__class__.__name__}"
                )
        return segments

    # ---------------------------------------------------------------
    # 发送逻辑
    # ---------------------------------------------------------------
    async def send(self, message: MessageChain) -> None:
        try:
            if self.astrbot_message_type == MessageType.FRIEND_MESSAGE:
                await self._send_private(message)
            else:
                await self._send_channel(message)
        except Exception as e:
            logger.error(f"[heychat] 发送消息失败: {e}")
        await super().send(message)

    async def send_streaming(
        self,
        generator: AsyncGenerator[MessageChain, None],
        use_fallback: bool = False,
    ) -> None:
        """发送流式消息。

        小黑盒不支持平台级流式展示（打字机效果），采用与 aiocqhttp 相同的降级策略：
        - use_fallback=False：累积全部内容后一次性发送（对应"关闭流式回复"）
        - use_fallback=True：按标点符号分段发送（对应"实时分段"）

        重要：流式输出时 respond stage 走 STREAMING_RESULT 分支，调用 send_streaming
        后直接 return，不会处理 result.chain 中的 At/Reply 组件。因此这里需要手动
        从 result 中提取这些组件，添加到发送的消息链中，以支持 reply_with_mention /
        reply_with_quote 配置在流式模式下也生效。
        """
        # 从 result 中提取 At/Reply 组件
        # 注意：流式输出时 result_decorate stage 会跳过 At/Reply 插入逻辑
        # （STREAMING_RESULT 时直接 return），因此需要手动构造。
        header_comps: list = []
        has_at = False
        has_reply = False
        result = self.get_result()
        if result and result.chain:
            for comp in result.chain:
                if isinstance(comp, (At, AtAll)):
                    has_at = True
                    header_comps.append(comp)
                elif isinstance(comp, Reply):
                    has_reply = True
                    header_comps.append(comp)
        # 流式输出时手动构造 At/Reply（result_decorate 在 STREAMING_RESULT 时跳过插入）
        if (
            not has_at
            and self.reply_with_mention
            and self.astrbot_message_type != MessageType.FRIEND_MESSAGE
        ):
            header_comps.append(
                At(qq=self.get_sender_id(), name=self.get_sender_name())
            )
        if not has_reply and self.reply_with_quote:
            header_comps.append(Reply(id=self.message_obj.message_id))
        logger.info(
            f"[heychat] send_streaming 被调用, use_fallback={use_fallback}, "
            f"header_comps={[type(c).__name__ for c in header_comps]}"
        )

        if not use_fallback:
            buffer: MessageChain | None = None
            chunk_count = 0
            async for chain in generator:
                chunk_count += 1
                if not buffer:
                    buffer = chain
                else:
                    buffer.chain.extend(chain.chain)
            logger.info(f"[heychat] 流式接收完成, chunks={chunk_count}")
            if not buffer:
                logger.info("[heychat] buffer 为空，走基类 send_streaming")
                return await super().send_streaming(generator, use_fallback)
            # 在消息链前面添加 At/Reply 组件
            if header_comps:
                buffer.chain = header_comps + buffer.chain
            buffer.squash_plain()
            await self.send(buffer)
            return await super().send_streaming(generator, use_fallback)

        # fallback：按标点分段发送
        text_buffer = ""
        pattern = re.compile(r"[^。？！~…]+[。？！~…]+")
        header_sent = False
        async for chain in generator:
            if isinstance(chain, MessageChain):
                for comp in chain.chain:
                    if isinstance(comp, Plain):
                        text_buffer += comp.text
                        if any(p in text_buffer for p in "。？！~…"):
                            # 第一段发送时带上 At/Reply 组件
                            if not header_sent and header_comps:
                                await self.send(
                                    MessageChain(
                                        chain=header_comps + [Plain(text_buffer)]
                                    )
                                )
                                header_sent = True
                                text_buffer = ""
                            else:
                                text_buffer = await self.process_buffer(
                                    text_buffer, pattern
                                )
                    else:
                        await self.send(MessageChain(chain=[comp]))
                        await asyncio.sleep(1.5)  # 限速
        text_buffer = text_buffer.strip()
        if text_buffer:
            if not header_sent and header_comps:
                await self.send(
                    MessageChain(chain=header_comps + [Plain(text_buffer)])
                )
                header_sent = True
            else:
                await self.send(MessageChain([Plain(text_buffer)]))
        elif not header_sent and header_comps:
            await self.send(MessageChain(chain=header_comps))
        return await super().send_streaming(generator, use_fallback)

    async def _send_channel(self, message: MessageChain) -> None:
        segments = self._parse_chain(message)
        logger.info(
            f"[heychat] 消息链段: {[(s[0], s[1] if len(s) > 1 else '') for s in segments]}"
        )
        text_buf: list[str] = []
        has_at = False
        at_user_id = ""
        reply_id = ""

        async def flush_text() -> None:
            nonlocal text_buf, has_at, at_user_id, reply_id
            if not text_buf:
                return
            text = "".join(text_buf)
            msg_type = (
                HeychatMessageType.AT_MARKDOWN if has_at else HeychatMessageType.MARKDOWN
            )
            logger.info(
                f"[heychat] 频道发送: text={text[:80]!r}, msg_type={msg_type}, "
                f"reply_id={reply_id!r}, at_user_id={at_user_id!r}"
            )
            await self.client.send_channel_message(
                self.room_id,
                self.channel_id,
                text,
                msg_type,
                reply_id=reply_id,
                at_user_id=at_user_id,
            )
            text_buf = []
            has_at = False
            at_user_id = ""
            reply_id = ""

        for seg in segments:
            kind = seg[0]
            if kind == "text":
                text_buf.append(seg[1])
            elif kind == "at":
                text_buf.append(f"@{{id:{seg[1]}}}")
                has_at = True
                if not at_user_id:
                    at_user_id = seg[1]
            elif kind == "at_all":
                text_buf.append("@{id:all}")
                has_at = True
            elif kind == "image":
                await flush_text()
                await self._send_image_channel(seg[1], reply_id)
                reply_id = ""
            elif kind == "reply":
                reply_id = seg[1]
        await flush_text()

    async def _send_private(self, message: MessageChain) -> None:
        """私聊消息：仅支持 markdown(msg_type=4) 和图片(msg_type=3)。"""
        segments = self._parse_chain(message)
        to_user_id = self.session_id
        text_buf: list[str] = []
        reply_id = ""

        for seg in segments:
            kind = seg[0]
            if kind in ("text", "at", "at_all"):
                if kind == "text":
                    text_buf.append(seg[1])
                elif kind == "at":
                    text_buf.append(f"@{{id:{seg[1]}}}")
                else:
                    text_buf.append("@{id:all}")
            elif kind == "image":
                if text_buf:
                    await self.client.send_private_message(
                        to_user_id, "".join(text_buf), HeychatMessageType.MARKDOWN
                    )
                    text_buf = []
                await self._send_image_private(seg[1])
            elif kind == "reply":
                # 私聊接口无 reply_id 参数，忽略
                logger.debug("[heychat] 私聊消息不支持引用回复，已忽略 reply 组件")
        if text_buf:
            await self.client.send_private_message(
                to_user_id, "".join(text_buf), HeychatMessageType.MARKDOWN
            )

    async def _send_image_channel(self, image: Image, reply_id: str) -> None:
        url = await self.client.upload_media(await image.convert_to_file_path())
        await self.client.send_channel_message(
            self.room_id,
            self.channel_id,
            "",
            HeychatMessageType.IMAGE,
            reply_id=reply_id,
            img=url,
            addition='{"img_files_info":[]}',
        )

    async def _send_image_private(self, image: Image) -> None:
        url = await self.client.upload_media(await image.convert_to_file_path())
        await self.client.send_private_message(
            self.session_id, "", HeychatMessageType.IMAGE, img=url
        )
