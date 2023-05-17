# @Time    : 2023/04/25 13:29
# @Author  : fyq
# @File    : redis.py
# @Software: PyCharm

__author__ = 'fyq'

import time
from contextlib import contextmanager
from typing import List, Optional, Dict

import redis
import simplejson
from munch import Munch

from search import constant
from search.extend import redis_pool


@contextmanager
def redis_lock(key: str, ex: int = None, retry: int = None, retry_time: int = None, forever: bool = False) -> bool:
    r = redis.Redis(connection_pool=redis_pool)
    if retry is None or retry < 0:
        retry = 0
    if retry_time is None or retry_time < 100:
        retry_time = 50
    while True:
        res: bool = r.set(name=key,
                          value=1,
                          nx=True,
                          ex=ex)
        if res:
            break
        else:
            if forever:
                time.sleep(retry_time / 1000)
            else:
                retry -= 1
                if retry < 0:
                    break
    if res:
        try:
            yield res
        finally:
            r.delete(key)
    else:
        yield res


def redis_search_config(*args) -> Optional[Munch]:
    r = redis.Redis(connection_pool=redis_pool)
    value: bytes = r.get(constant.RedisKey.SEARCH_CONFIG)
    if value:
        datas: List[Dict] = simplejson.loads(value.decode())
        if len(args) == 0:
            return Munch({data["name"]: data["value"]
                          for data in datas})
        else:
            return Munch({data["name"]: data["value"]
                          for data in datas
                          if data["name"] in args})
