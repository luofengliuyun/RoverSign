from gsuid_core.bot import Bot
from gsuid_core.logger import logger
from gsuid_core.models import Event
from gsuid_core.sv import SV, get_plugin_available_prefix

from ..utils.database.models import WavesBind
from .set_config import set_config_func

sv_rover_config = SV("RoverSign配置")


PREFIX = get_plugin_available_prefix("RoverSign")


@sv_rover_config.on_prefix(("开启", "关闭"))
async def open_switch_func(bot: Bot, ev: Event):
    if ev.text != "自动签到":
        return

    at_sender = True if ev.group_id else False
    uid = await WavesBind.get_uid_by_game(ev.user_id, ev.bot_id)
    if uid is None:
        msg = f"您还未绑定鸣潮特征码, 请使用【{PREFIX}绑定uid】完成绑定！"
        return await bot.send(
            (" " if at_sender else "") + msg,
            at_sender,
        )

    from ..utils.rover_api import rover_api

    token = await rover_api.get_self_waves_ck(uid, ev.user_id, ev.bot_id)
    if not token:
        from ..utils.errors import WAVES_CODE_101_MSG

        msg = f"当前特征码：{uid}\n{WAVES_CODE_101_MSG.rstrip(chr(10))}"
        return await bot.send((" " if at_sender else "") + msg, at_sender)

    logger.info(f"[{ev.user_id}]尝试[{ev.command[0:2]}]了[{ev.text}]功能")

    im = await set_config_func(ev, uid)
    im = im.rstrip("\n") if isinstance(im, str) else im
    await bot.send((" " if at_sender and isinstance(im, str) else "") + im if isinstance(im, str) else im, at_sender)
