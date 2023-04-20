# @Time    : 2023/04/20 9:17
# @Author  : fyq
# @File    : request.py
# @Software: PyCharm

__author__ = 'fyq'

from dataclasses import dataclass

from search.entity.common_result import BaseDataClass


@dataclass
class BeforeRequest(BaseDataClass):

    # 请求方式
    method: str = ""

    # 请求路径
    path: str = ""

    # 请求地址
    remote_addr: str = ""

    # args
    args: str = ""

    # form
    form: str = ""

    # data
    data: str = ""


@dataclass
class AfterRequest(BaseDataClass):

    # 响应路径
    path: str = ""

    # json
    json: str = ""

    # 响应长度
    content_length: int = 0

    # data
    data: str = ""