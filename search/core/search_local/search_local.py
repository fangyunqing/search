# @Time    : 2023/03/12 20:04
# @Author  : fyq
# @File    : search_local.py
# @Software: PyCharm

__author__ = 'fyq'

from contextlib import contextmanager
from threading import local
from typing import Any


class SearchLocal:

    def __init__(self):
        self._local = local()

    @contextmanager
    def __call__(self, key: str, value: Any):
        setattr(self._local, key, value)
        try:
            yield
        finally:
            delattr(self._local, key)

    def get_value(self, key: str):
        return getattr(self._local, key, None)


search_local = SearchLocal()

