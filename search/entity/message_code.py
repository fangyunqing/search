# @Time    : 23/02/20 13:04
# @Author  : fyq
# @File    : message_code.py
# @Software: PyCharm

__author__ = 'fyq'

import enum


class MessageCode(enum.Enum):
    SUCCESS = 200, "成功"
    NOT_FOUND = 404, "未找到"
    NOT_READY = 201, "文件加载中"
    NOT_PROGRESS = 405, "未查询到进度"
    ERROR = 400, "错误"

    def __init__(self, code, desc):
        self._code = code
        self._desc = desc

    @property
    def code(self):
        return self._code

    @property
    def desc(self):
        return self._desc
