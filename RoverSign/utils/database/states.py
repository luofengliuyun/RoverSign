from typing import Iterable, Optional

from .models import RoverSign


class SignStatus(int):
    GAME_SIGN = 1  # 游戏签到
    BBS_SIGN = 1  # 社区签到
    BBS_DETAIL = 3  # 社区浏览
    BBS_LIKE = 5  # 社区点赞
    BBS_SHARE = 1  # 社区分享

    @classmethod
    def game_sign_complete(cls, rover_sign: RoverSign):
        return cls.GAME_SIGN == rover_sign.game_sign

    @classmethod
    def bbs_sign_complete(
        cls, rover_sign: RoverSign, tasks: Optional[Iterable[str]] = None
    ):
        task_set = set(tasks) if tasks is not None else {
            "bbs_sign",
            "bbs_detail",
            "bbs_like",
            "bbs_share",
        }

        if "bbs_sign" in task_set and cls.BBS_SIGN != rover_sign.bbs_sign:
            return False
        if "bbs_detail" in task_set and cls.BBS_DETAIL != rover_sign.bbs_detail:
            return False
        if "bbs_like" in task_set and cls.BBS_LIKE != rover_sign.bbs_like:
            return False
        if "bbs_share" in task_set and cls.BBS_SHARE != rover_sign.bbs_share:
            return False

        return True
