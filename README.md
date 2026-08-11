# AstrBot 小黑盒 (HeyBoxChat) 平台适配器插件

将 AstrBot 接入[黑盒语音（小黑盒）](https://chat.xiaoheihe.cn/)机器人平台，实现消息收发。

## 功能

- 接收小黑盒频道消息（文本 / 图片 / @）与 Bot 命令
- 自动识别私聊（私聊场景下 room_id 与 channel_id 相同）
- 发送文本（Markdown）、图片、@ 用户 / @全体、引用回复
- WebSocket 心跳保活（30s PING）、断线指数退避自动重连、消息去重

## 安装

1. 将本插件目录（`astrbot_plugin_heybox_chat_adapter`）复制到 AstrBot 的 `data/plugins/` 目录下。
2. 重启 AstrBot。
3. 打开 AstrBot WebUI → 配置 → 消息平台适配器 → 添加「小黑盒 (HeyBoxChat)」：
   - `token`：小黑盒机器人开发平台（https://bot.xiaoheihe.cn ）中机器人的 Token。
   - `enable`：勾选启用。
4. 保存并重启，等待日志出现 `[heychat] 黑盒语音 WebSocket 已连接`。

> 机器人 ID 会从 Token 中自动解析（Token 为 base64，第一段即机器人 ID），无需手动填写。

## 前置要求

- 已通过小黑盒[开发者认证](https://open.xiaoheihe.cn/zh_cn/chat_robot/home)并创建机器人（教程见黑盒官方帮助文档）。
- 已将机器人通过邀请链接添加到目标房间，并授予「查看频道」「发送消息」等权限。

## 支持的消息类型

| 方向 | 类型 | 说明 |
| --- | --- | --- |
| 接收 | 普通频道消息 | WebSocket 事件 `type: 5/1` |
| 接收 | Bot 命令 | WebSocket 事件 `type: 50`，命令参数会被还原为 `/命令 参数...` 供 AstrBot 命令系统处理 |
| 接收 | 私聊消息 | 识别为好友消息，会话按用户 ID 独立 |
| 发送 | 文本 | `msg_type=4`（Markdown） |
| 发送 | 图片 | 自动上传媒体后以 `msg_type=3` 发送 |
| 发送 | @ / @全体 | 渲染为 `@{id:xxx}` / `@{id:all}`（`msg_type=10`） |
| 发送 | 引用回复 | 填充 `reply_id` |

暂不支持：语音（Record）、视频（Video）、卡片消息（Json）发送。

## 注意事项

- **私聊限制**：小黑盒私聊仅支持向同一房间内的用户发送消息；对方未回复时最多发送 3 条；私聊频率限制为每分钟 9 条。
- **速率限制**：小黑盒接口默认限速 300 次/分钟。
- **命令机制**：小黑盒的 Bot 命令需要在机器人开发平台配置（名称、参数），用户通过 `/` 呼出，WebSocket 会推送 `type: 50` 事件。
- **@ 机器人唤醒**：小黑盒在有人 @机器人 时会推送 `type: 5002` 艾特通知事件，适配器据此标记后续消息艾特了机器人——无论用户使用 `@{id:<机器人ID>}` 还是 `@昵称` 格式，都能触发 AstrBot 的唤醒逻辑。纯空艾特（只 @机器人 不说话）会触发 AstrBot 的空艾特等待。
- **流式输出**：小黑盒不支持平台级打字机效果，适配器实现了 `send_streaming` 降级——开启 `streaming_response` 后，会按 AstrBot 的 `unsupported_streaming_strategy` 配置自动降级：`关闭流式回复` 时累积全文一次性发送，`实时分段` 时按标点分段发送。无需手动关闭流式输出。

## 开发参考

- AstrBot 平台适配器文档：https://docs.astrbot.app/dev/plugin-platform-adapter.html
- 黑盒语音开发者文档：https://s.apifox.cn/43256fe4-9a8c-4f22-949a-74a3f8b431f5/llms.txt
- 黑盒官方 Demo：https://github.com/QingFengOpen/HeychatDemo

## 依赖

复用 AstrBot 自带依赖（`aiohttp`），无需额外安装第三方库。
