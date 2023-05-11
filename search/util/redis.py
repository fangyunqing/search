# @Time    : 2023/04/25 13:29
# @Author  : fyq
# @File    : redis.py
# @Software: PyCharm

__author__ = 'fyq'

import time
from contextlib import contextmanager
import redis

from search.extend import redis_pool


@contextmanager
def redis_lock(key: str, ex: int = None, retry: int = None, retry_time: int = None, forever: bool = False) -> bool:
    r = redis.Redis(connection_pool=redis_pool)
    if retry is None or retry < 0:
        retry = 0
    if retry_time is None or retry_time < 100:
        retry_time = 100
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
