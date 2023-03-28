# @Time    : 23/03/08 10:17
# @Author  : fyq
# @File    : page.py
# @Software: PyCharm

__author__ = 'fyq'

from dataclasses import dataclass

from search.entity.common_result import BaseDataClass


@dataclass
class Page(BaseDataClass):
    # 总数
    total: str = 0
    # 页大小
    size: str = 0
    # 页数
    pages: str = 0
