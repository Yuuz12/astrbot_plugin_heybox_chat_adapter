from astrbot.api.star import Context, Star


class HeyboxChatPlugin(Star):
    """小黑盒平台适配器插件入口。

    该插件本身不提供任何指令，只负责注册小黑盒平台适配器。
    适配器通过装饰器自动注册，这里仅需要导入模块触发注册。
    """

    def __init__(self, context: Context):
        super().__init__(context)
        from .heybox_chat_adapter import HeyboxChatAdapter  # noqa: F401
