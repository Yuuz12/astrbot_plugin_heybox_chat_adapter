from astrbot.api.star import Context, Star


class HeyboxChatPlugin(Star):
    """小黑盒平台适配器插件入口。

    该插件本身不提供任何指令，只负责注册小黑盒平台适配器。
    适配器通过装饰器自动注册，这里仅需要导入模块触发注册。
    """

    def __init__(self, context: Context):
        super().__init__(context)
        from .heybox_chat_adapter import HeyboxChatAdapter  # noqa: F401

        # 注入默认配置引用，使适配器能读取 wake_prefix 等顶层配置项。
        # Platform 适配器初始化时只拿到 platform 配置项和 platform_settings，
        # 访问不到 wake_prefix（顶层配置）。type 50 命令还原时需要加回
        # 唤醒词前缀，必须从配置动态读取而非硬编码。
        HeyboxChatAdapter.config_ref = context._config
